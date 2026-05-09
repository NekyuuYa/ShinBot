"""LiteLLM-backed provider/model/route runtime."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from shinbot.persistence import AIInteractionRecord, DatabaseManager, ModelExecutionRecord

from . import litellm_adapter
from .extraction import (
    extract_embedding,
    extract_estimated_cost,
    extract_image_urls,
    extract_injected_context,
    extract_rerank_results,
    extract_speech_bytes,
    extract_text,
    extract_think_text,
    extract_tool_calls_list,
    extract_transcription_text,
    extract_usage,
    maybe_get,
    response_to_dict,
    utc_now,
)
from .persistence import persist_ai_interaction, persist_model_execution
from .planning import (
    build_litellm_kwargs,
    resolve_runtime_targets,
    sanitize_litellm_kwargs,
)
from .types import (
    EmbedResult,
    GenerateResult,
    ImageResult,
    ModelCallError,
    ModelRuntimeCall,
    ModelRuntimeObserver,
    RerankResult,
    SpeechResult,
    TranscriptionResult,
    VideoResult,
)

logger = logging.getLogger(__name__)

_ResultT = TypeVar("_ResultT")


@dataclass(slots=True)
class _RuntimeExecution:
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str
    response: Any
    response_payload: dict[str, Any]
    usage: dict[str, Any]
    return_payload: dict[str, Any]
    started_timestamp: float
    latency_ms: float


class ModelRuntime:
    """Unified runtime for route-based LiteLLM calls."""

    def __init__(self, database: DatabaseManager | None) -> None:
        self._database = database
        self._random = random.Random()
        self._observers: list[ModelRuntimeObserver] = []

    def register_observer(self, observer: ModelRuntimeObserver) -> None:
        if observer not in self._observers:
            self._observers.append(observer)

    def unregister_observer(self, observer: ModelRuntimeObserver) -> None:
        self._observers = [item for item in self._observers if item is not observer]

    async def generate(self, call: ModelRuntimeCall) -> GenerateResult:
        return await self._execute_with_retry(
            call,
            operation="generate",
            mode="completion",
            invoke=litellm_adapter.completion,
            failure_message="Model call failed",
            build_result=self._build_generate_result,
            record_usage=True,
            record_cache_metrics=True,
            record_first_token=True,
            record_prompt_snapshot=True,
            include_response_model_metadata=True,
            include_usage_raw_metadata=True,
            build_response_return=self._build_generate_return_payload,
            after_success=self._persist_generate_ai_interaction,
            notify_response=True,
        )

    async def embed(self, call: ModelRuntimeCall) -> EmbedResult:
        return await self._execute_with_retry(
            call,
            operation="embed",
            mode="embedding",
            invoke=litellm_adapter.embedding,
            failure_message="Embedding call failed",
            build_result=self._build_embed_result,
            record_usage=True,
            record_cache_metrics=True,
            include_usage_raw_metadata=True,
        )

    async def rerank(self, call: ModelRuntimeCall) -> RerankResult:
        return await self._execute_with_retry(
            call,
            operation="rerank",
            mode="rerank",
            invoke=litellm_adapter.rerank,
            failure_message="Rerank call failed",
            build_result=self._build_rerank_result,
            record_usage=True,
        )

    async def speak(self, call: ModelRuntimeCall) -> SpeechResult:
        """Text-to-speech via litellm.speech."""
        return await self._execute_with_retry(
            call,
            operation="speak",
            mode="speech",
            invoke=litellm_adapter.speech,
            failure_message="Speech call failed",
            build_result=self._build_speech_result,
            record_estimated_cost=False,
        )

    async def transcribe(self, call: ModelRuntimeCall) -> TranscriptionResult:
        """Speech-to-text via litellm.transcription."""
        return await self._execute_with_retry(
            call,
            operation="transcribe",
            mode="transcription",
            invoke=litellm_adapter.transcription,
            failure_message="Transcription call failed",
            build_result=self._build_transcription_result,
            record_usage=True,
        )

    async def generate_image(self, call: ModelRuntimeCall) -> ImageResult:
        """Image generation via litellm.image_generation."""
        return await self._execute_with_retry(
            call,
            operation="generate_image",
            mode="image",
            invoke=litellm_adapter.image_generation,
            failure_message="Image generation call failed",
            build_result=self._build_image_result,
        )

    async def generate_video(self, call: ModelRuntimeCall) -> VideoResult:
        """Video generation via litellm.video_generation."""
        return await self._execute_with_retry(
            call,
            operation="generate_video",
            mode="video",
            invoke=litellm_adapter.video_generation,
            failure_message="Video generation call failed",
            build_result=self._build_video_result,
        )

    async def _execute_with_retry(
        self,
        call: ModelRuntimeCall,
        *,
        operation: str,
        mode: str,
        invoke: Callable[..., Any],
        failure_message: str,
        build_result: Callable[[_RuntimeExecution], _ResultT],
        record_usage: bool = False,
        record_cache_metrics: bool = False,
        record_first_token: bool = False,
        record_estimated_cost: bool = True,
        record_prompt_snapshot: bool = False,
        include_response_model_metadata: bool = False,
        include_usage_raw_metadata: bool = False,
        build_response_return: Callable[[_RuntimeExecution], dict[str, Any]] | None = None,
        after_success: Callable[[_RuntimeExecution, ModelRuntimeCall], None] | None = None,
        notify_response: bool = False,
    ) -> _ResultT:
        if not call.route_id and not call.model_id:
            raise ValueError(f"{operation}() requires route_id or model_id")

        attempts = resolve_runtime_targets(
            database=self._database,
            call=call,
            picker=self._random,
        )
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = utc_now()
            started_at = started.isoformat()
            route_id = call.route_id or attempt["model"]["id"]
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode=mode,
            )
            await self._notify_observers(
                self._build_request_observer_payload(
                    mode=mode,
                    execution_id=execution_id,
                    call=call,
                    attempt=attempt,
                    kwargs=kwargs,
                )
            )
            try:
                response = await asyncio.to_thread(invoke, **kwargs)
                finished = utc_now()
                finished_at = finished.isoformat()
                latency_ms = (finished - started).total_seconds() * 1000
                response_payload = response_to_dict(response)
                usage = extract_usage(response)
                execution = _RuntimeExecution(
                    execution_id=execution_id,
                    route_id=route_id,
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    response=response,
                    response_payload=response_payload,
                    usage=usage,
                    return_payload={},
                    started_timestamp=started.timestamp(),
                    latency_ms=latency_ms,
                )
                if build_response_return is not None:
                    execution.return_payload = build_response_return(execution)

                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=route_id,
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    first_token_at=finished_at if record_first_token else None,
                    finished_at=finished_at,
                    latency_ms=latency_ms,
                    time_to_first_token_ms=latency_ms if record_first_token else None,
                    input_tokens=usage["input_tokens"] if record_usage else 0,
                    output_tokens=usage["output_tokens"] if record_usage else 0,
                    cache_hit=usage["cache_read_tokens"] > 0 if record_cache_metrics else False,
                    cache_read_tokens=usage["cache_read_tokens"] if record_cache_metrics else 0,
                    cache_write_tokens=usage["cache_write_tokens"] if record_cache_metrics else 0,
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    prompt_snapshot_id=call.prompt_snapshot_id if record_prompt_snapshot else "",
                    metadata=self._build_execution_metadata(
                        call=call,
                        attempt=attempt,
                        response=response,
                        response_payload=response_payload,
                        include_response_model=include_response_model_metadata,
                        include_usage_raw=include_usage_raw_metadata,
                    ),
                    estimated_cost=(
                        extract_estimated_cost(response) if record_estimated_cost else None
                    ),
                    currency="USD" if record_estimated_cost else "",
                )
                persist_model_execution(self._database, record)

                result = build_result(execution)
                if after_success is not None:
                    after_success(execution, call)
                if notify_response:
                    await self._notify_observers(
                        self._build_response_observer_payload(
                            mode=mode,
                            execution=execution,
                            call=call,
                            attempt=attempt,
                            status="success",
                        )
                    )
                return result
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                latency_ms = (finished - started).total_seconds() * 1000
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=route_id,
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=latency_ms,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    prompt_snapshot_id=call.prompt_snapshot_id if record_prompt_snapshot else "",
                    metadata=self._build_execution_metadata(call=call, attempt=attempt),
                )
                persist_model_execution(self._database, record)
                if notify_response:
                    await self._notify_observers(
                        self._build_error_observer_payload(
                            mode=mode,
                            execution_id=execution_id,
                            call=call,
                            attempt=attempt,
                            latency_ms=latency_ms,
                            exc=exc,
                        )
                    )
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else failure_message)

    def _build_request_observer_payload(
        self,
        *,
        mode: str,
        execution_id: str,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        payload = self._base_observer_payload(
            event="model_runtime.request",
            mode=mode,
            execution_id=execution_id,
            call=call,
            attempt=attempt,
        )
        if mode == "completion":
            payload.update(
                {
                    "messages": list(call.messages),
                    "tools": list(call.tools),
                    "response_format": call.response_format,
                }
            )
        else:
            payload["input_data"] = call.input_data
        payload.update(
            {
                "params": dict(call.params),
                "metadata": dict(call.metadata),
                "kwargs": sanitize_litellm_kwargs(kwargs),
                "prompt_snapshot_id": call.prompt_snapshot_id,
            }
        )
        return payload

    def _build_response_observer_payload(
        self,
        *,
        mode: str,
        execution: _RuntimeExecution,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
        status: str,
    ) -> dict[str, Any]:
        payload = self._base_observer_payload(
            event="model_runtime.response",
            mode=mode,
            execution_id=execution.execution_id,
            call=call,
            attempt=attempt,
        )
        payload.update(
            {
                "status": status,
                "latency_ms": execution.latency_ms,
                "usage": execution.usage,
                "cache_hit": execution.usage["cache_read_tokens"] > 0,
                "cache_read_tokens": execution.usage["cache_read_tokens"],
                "cache_write_tokens": execution.usage["cache_write_tokens"],
                "return": execution.return_payload,
                "response": execution.response_payload,
                "metadata": dict(call.metadata),
                "prompt_snapshot_id": call.prompt_snapshot_id,
            }
        )
        return payload

    def _build_error_observer_payload(
        self,
        *,
        mode: str,
        execution_id: str,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
        latency_ms: float,
        exc: Exception,
    ) -> dict[str, Any]:
        usage = self._empty_usage()
        payload = self._base_observer_payload(
            event="model_runtime.response",
            mode=mode,
            execution_id=execution_id,
            call=call,
            attempt=attempt,
        )
        payload.update(
            {
                "status": "error",
                "latency_ms": latency_ms,
                "usage": usage,
                "cache_hit": False,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "error": {
                    "code": type(exc).__name__,
                    "message": str(exc),
                },
                "metadata": dict(call.metadata),
                "prompt_snapshot_id": call.prompt_snapshot_id,
            }
        )
        return payload

    def _base_observer_payload(
        self,
        *,
        event: str,
        mode: str,
        execution_id: str,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "event": event,
            "mode": mode,
            "execution_id": execution_id,
            "caller": call.caller,
            "purpose": call.purpose,
            "session_id": call.session_id,
            "instance_id": call.instance_id,
            "route_id": call.route_id or "",
            "provider_id": attempt["provider"]["id"],
            "provider_type": attempt["provider"]["type"],
            "model_id": attempt["model"]["id"],
            "litellm_model": attempt["model"]["litellm_model"],
            "strategy": attempt["strategy"],
        }

    def _build_execution_metadata(
        self,
        *,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
        response: Any | None = None,
        response_payload: dict[str, Any] | None = None,
        include_response_model: bool = False,
        include_usage_raw: bool = False,
    ) -> dict[str, Any]:
        metadata = {"route_strategy": attempt["strategy"]}
        if include_response_model:
            metadata["response_model"] = maybe_get(response, "model")
        if include_usage_raw:
            payload = response_payload or {}
            metadata["usage_raw"] = payload.get("usage")
        metadata.update(call.metadata)
        return metadata

    @staticmethod
    def _empty_usage() -> dict[str, int]:
        return {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
        }

    @staticmethod
    def _build_generate_return_payload(execution: _RuntimeExecution) -> dict[str, Any]:
        return {
            "text": extract_text(execution.response),
            "tool_calls": extract_tool_calls_list(execution.response),
        }

    def _persist_generate_ai_interaction(
        self,
        execution: _RuntimeExecution,
        call: ModelRuntimeCall,
    ) -> None:
        persist_ai_interaction(
            self._database,
            AIInteractionRecord(
                execution_id=execution.execution_id,
                timestamp=execution.started_timestamp,
                latency_ms=execution.latency_ms,
                input_tokens=execution.usage["input_tokens"],
                output_tokens=execution.usage["output_tokens"],
                cache_read_tokens=execution.usage["cache_read_tokens"],
                cache_write_tokens=execution.usage["cache_write_tokens"],
                model_id=execution.model_id,
                provider_id=execution.provider_id,
                think_text=extract_think_text(execution.response),
                injected_context_json=extract_injected_context(call.messages),
                tool_calls_json=json.dumps(
                    execution.return_payload.get("tool_calls", []),
                    ensure_ascii=False,
                ),
                prompt_snapshot_id=call.prompt_snapshot_id,
            ),
        )

    @staticmethod
    def _result_context(execution: _RuntimeExecution) -> dict[str, Any]:
        return {
            "raw_response": execution.response,
            "execution_id": execution.execution_id,
            "route_id": execution.route_id,
            "provider_id": execution.provider_id,
            "model_id": execution.model_id,
        }

    def _build_generate_result(self, execution: _RuntimeExecution) -> GenerateResult:
        return GenerateResult(
            text=execution.return_payload.get("text", ""),
            tool_calls=execution.return_payload.get("tool_calls", []),
            **self._result_context(execution),
            usage=execution.usage,
        )

    def _build_embed_result(self, execution: _RuntimeExecution) -> EmbedResult:
        return EmbedResult(
            embedding=extract_embedding(execution.response),
            **self._result_context(execution),
            usage=execution.usage,
        )

    def _build_rerank_result(self, execution: _RuntimeExecution) -> RerankResult:
        return RerankResult(
            results=extract_rerank_results(execution.response),
            **self._result_context(execution),
            usage=execution.usage,
        )

    def _build_speech_result(self, execution: _RuntimeExecution) -> SpeechResult:
        return SpeechResult(
            audio_bytes=extract_speech_bytes(execution.response),
            **self._result_context(execution),
        )

    def _build_transcription_result(self, execution: _RuntimeExecution) -> TranscriptionResult:
        return TranscriptionResult(
            text=extract_transcription_text(execution.response),
            **self._result_context(execution),
            usage=execution.usage,
        )

    def _build_image_result(self, execution: _RuntimeExecution) -> ImageResult:
        return ImageResult(
            urls=extract_image_urls(execution.response),
            **self._result_context(execution),
        )

    def _build_video_result(self, execution: _RuntimeExecution) -> VideoResult:
        return VideoResult(
            urls=extract_image_urls(execution.response),
            **self._result_context(execution),
        )

    async def _notify_observers(self, payload: dict[str, Any]) -> None:
        if not self._observers:
            return
        for observer in list(self._observers):
            try:
                result = observer(payload)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Model runtime observer failed for event %s", payload.get("event"))
