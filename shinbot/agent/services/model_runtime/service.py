"""LiteLLM-backed provider/model/route runtime."""

from __future__ import annotations

import asyncio
import inspect
import json
import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar

from shinbot.persistence import AIInteractionRecord, DatabaseManager, ModelExecutionRecord
from shinbot.utils.logger import format_log_event, get_logger

from .audit_store import ModelAuditPayloadStore, sanitize_payload_for_audit
from .backends import BackendOperation, BackendRequestPlan, LiteLLMBackend, ModelBackend
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
from .planning import resolve_runtime_targets
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

logger = get_logger(__name__, source="model:runtime", color="blue")

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
    backend_name: str
    backend_model: str
    started_timestamp: float
    latency_ms: float


class ModelRuntime:
    """Unified runtime for route-based model backend calls."""

    def __init__(
        self,
        database: DatabaseManager | None,
        backend: ModelBackend | None = None,
    ) -> None:
        """Initialize the model runtime service.

        Args:
            database: Optional database manager for persisting execution
                records and AI interactions. When provided, an audit
                payload store is also initialised for request/response
                logging.
            backend: Optional model backend implementation. Defaults to
                the LiteLLM backend to preserve existing runtime behavior.
        """
        self._database = database
        self._backend = backend or LiteLLMBackend()
        data_dir = database.config.data_dir if database is not None else None
        self._audit_store = (
            ModelAuditPayloadStore(data_dir) if data_dir is not None else None
        )
        self._random = random.Random()
        self._observers: list[ModelRuntimeObserver] = []

    def register_observer(self, observer: ModelRuntimeObserver) -> None:
        """Register an observer to receive model runtime events.

        Observers are called with a payload dict for each request,
        successful response, and error event emitted during model
        execution.

        Args:
            observer: A callable (or async callable) that accepts a
                single ``dict[str, Any]`` payload and optionally
                returns an awaitable.
        """
        if observer not in self._observers:
            self._observers.append(observer)

    def unregister_observer(self, observer: ModelRuntimeObserver) -> None:
        """Unregister a previously registered observer.

        If the observer is not currently registered the call is a
        safe no-op.

        Args:
            observer: The observer instance to remove.
        """
        self._observers = [item for item in self._observers if item is not observer]

    async def generate(self, call: ModelRuntimeCall) -> GenerateResult:
        """Send a chat completion request through the model runtime.

        Routes the call to the appropriate provider via LiteLLM,
        handles retries and fallbacks, persists execution records,
        and returns the parsed text and tool calls.

        Args:
            call: A ``ModelRuntimeCall`` describing the route, messages,
                tools, and parameters for the completion request.

        Returns:
            A ``GenerateResult`` containing the response text, any
                tool calls, token usage, and execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="generate",
            mode="completion",
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
        """Generate an embedding vector for the provided input.

        Routes the request through the embedding mode of the model
        runtime, handling retries, fallbacks, and execution persistence.

        Args:
            call: A ``ModelRuntimeCall`` with ``input_data`` set to the
                text(s) to embed.

        Returns:
            An ``EmbedResult`` containing the embedding vector, token
                usage, and execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="embed",
            mode="embedding",
            failure_message="Embedding call failed",
            build_result=self._build_embed_result,
            record_usage=True,
            record_cache_metrics=True,
            include_usage_raw_metadata=True,
        )

    async def rerank(self, call: ModelRuntimeCall) -> RerankResult:
        """Rerank a set of documents against a query.

        Sends a reranking request to the configured provider, returning
        documents sorted by relevance score.

        Args:
            call: A ``ModelRuntimeCall`` with ``input_data`` containing
                the query and document list to rerank.

        Returns:
            A ``RerankResult`` containing the scored document list, token
                usage, and execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="rerank",
            mode="rerank",
            failure_message="Rerank call failed",
            build_result=self._build_rerank_result,
            record_usage=True,
        )

    async def speak(self, call: ModelRuntimeCall) -> SpeechResult:
        """Generate speech audio from text via LiteLLM.

        Args:
            call: A ``ModelRuntimeCall`` with ``input_data`` set to the
                text to synthesise.

        Returns:
            A ``SpeechResult`` containing the raw audio bytes and
                execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="speak",
            mode="speech",
            failure_message="Speech call failed",
            build_result=self._build_speech_result,
            record_estimated_cost=False,
        )

    async def transcribe(self, call: ModelRuntimeCall) -> TranscriptionResult:
        """Transcribe audio to text via LiteLLM.

        Args:
            call: A ``ModelRuntimeCall`` with ``input_data`` set to the
                audio content to transcribe.

        Returns:
            A ``TranscriptionResult`` containing the transcribed text,
                token usage, and execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="transcribe",
            mode="transcription",
            failure_message="Transcription call failed",
            build_result=self._build_transcription_result,
            record_usage=True,
        )

    async def generate_image(self, call: ModelRuntimeCall) -> ImageResult:
        """Generate images from a text prompt via LiteLLM.

        Args:
            call: A ``ModelRuntimeCall`` with ``input_data`` set to the
                image generation prompt.

        Returns:
            An ``ImageResult`` containing the list of generated image
                URLs and execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="generate_image",
            mode="image",
            failure_message="Image generation call failed",
            build_result=self._build_image_result,
        )

    async def generate_video(self, call: ModelRuntimeCall) -> VideoResult:
        """Generate video from a text prompt via LiteLLM.

        Args:
            call: A ``ModelRuntimeCall`` with ``input_data`` set to the
                video generation prompt.

        Returns:
            A ``VideoResult`` containing the list of generated video
                URLs and execution context.

        Raises:
            ModelCallError: If all retry/fallback attempts fail.
        """
        return await self._execute_with_retry(
            call,
            operation="generate_video",
            mode="video",
            failure_message="Video generation call failed",
            build_result=self._build_video_result,
        )

    async def _execute_with_retry(
        self,
        call: ModelRuntimeCall,
        *,
        operation: str,
        mode: BackendOperation,
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
            plan = self._backend.plan_request(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                operation=mode,
            )
            logger.debug(
                format_log_event(
                    "model.call.start",
                    operation=operation,
                    mode=mode,
                    execution_id=execution_id,
                    route_id=route_id,
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    purpose=call.purpose,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    trace_id=call.metadata.get("trace_id"),
                    fallback_from_model_id=previous_model_id,
                    backend=plan.backend_name,
                )
            )
            await self._notify_observers(
                self._build_request_observer_payload(
                    mode=mode,
                    execution_id=execution_id,
                    call=call,
                    attempt=attempt,
                    plan=plan,
                )
            )
            try:
                response = await asyncio.to_thread(self._backend.invoke, plan)
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
                    backend_name=plan.backend_name,
                    backend_model=plan.backend_model,
                    started_timestamp=started.timestamp(),
                    latency_ms=latency_ms,
                )
                if build_response_return is not None:
                    execution.return_payload = build_response_return(execution)
                audit_metadata = self._persist_audit_payload(
                    execution_id=execution_id,
                    operation=operation,
                    started_at=started,
                    finished_at=finished,
                    call=call,
                    attempt=attempt,
                    plan=plan,
                    response_payload=response_payload,
                    return_payload=execution.return_payload,
                    status="success",
                    error=None,
                )

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
                        audit_metadata=audit_metadata,
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
                logger.debug(
                    format_log_event(
                        "model.call.finish",
                        status="success",
                        operation=operation,
                        mode=mode,
                        execution_id=execution_id,
                        route_id=route_id,
                        provider_id=execution.provider_id,
                        model_id=execution.model_id,
                        caller=call.caller,
                        purpose=call.purpose,
                        session_id=call.session_id,
                        trace_id=call.metadata.get("trace_id"),
                        latency_ms=f"{latency_ms:.2f}",
                        backend=execution.backend_name,
                        input_tokens=usage["input_tokens"] if record_usage else 0,
                        output_tokens=usage["output_tokens"] if record_usage else 0,
                        cache_read_tokens=(
                            usage["cache_read_tokens"] if record_cache_metrics else 0
                        ),
                        cache_write_tokens=(
                            usage["cache_write_tokens"] if record_cache_metrics else 0
                        ),
                    )
                )
                return result
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                latency_ms = (finished - started).total_seconds() * 1000
                audit_metadata = self._persist_audit_payload(
                    execution_id=execution_id,
                    operation=operation,
                    started_at=started,
                    finished_at=finished,
                    call=call,
                    attempt=attempt,
                    plan=plan,
                    response_payload=None,
                    return_payload=None,
                    status="error",
                    error={
                        "code": type(exc).__name__,
                        "message": str(exc),
                    },
                )
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
                    metadata=self._build_execution_metadata(
                        call=call,
                        attempt=attempt,
                        audit_metadata=audit_metadata,
                    ),
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
                logger.debug(
                    format_log_event(
                        "model.call.finish",
                        status="error",
                        operation=operation,
                        mode=mode,
                        execution_id=execution_id,
                        route_id=route_id,
                        provider_id=attempt["provider"]["id"],
                        model_id=attempt["model"]["id"],
                        caller=call.caller,
                        purpose=call.purpose,
                        session_id=call.session_id,
                        trace_id=call.metadata.get("trace_id"),
                        latency_ms=f"{latency_ms:.2f}",
                        backend=plan.backend_name,
                        error_code=type(exc).__name__,
                    )
                )

        logger.warning(
            format_log_event(
                "model.call.failed",
                operation=operation,
                mode=mode,
                caller=call.caller,
                purpose=call.purpose,
                session_id=call.session_id,
                instance_id=call.instance_id,
                trace_id=call.metadata.get("trace_id"),
                route_id=call.route_id,
                model_id=call.model_id,
                error_code=type(last_error).__name__ if last_error is not None else "",
            )
        )
        raise ModelCallError(str(last_error) if last_error else failure_message)

    def _build_request_observer_payload(
        self,
        *,
        mode: BackendOperation,
        execution_id: str,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
        plan: BackendRequestPlan,
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
                "kwargs": plan.safe_payload,
                "backend": plan.backend_name,
                "backend_model": plan.backend_model,
                "prompt_snapshot_id": call.prompt_snapshot_id,
            }
        )
        return payload

    def _build_response_observer_payload(
        self,
        *,
        mode: BackendOperation,
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
                "backend": execution.backend_name,
                "backend_model": execution.backend_model,
                "metadata": dict(call.metadata),
                "prompt_snapshot_id": call.prompt_snapshot_id,
            }
        )
        return payload

    def _build_error_observer_payload(
        self,
        *,
        mode: BackendOperation,
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
                "backend": getattr(self._backend, "name", ""),
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
        audit_metadata: dict[str, Any] | None = None,
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
        if audit_metadata:
            metadata.update(audit_metadata)
        return metadata

    def _persist_audit_payload(
        self,
        *,
        execution_id: str,
        operation: str,
        started_at: datetime,
        finished_at: datetime,
        call: ModelRuntimeCall,
        attempt: dict[str, Any],
        plan: BackendRequestPlan,
        response_payload: dict[str, Any] | None,
        return_payload: dict[str, Any] | None,
        status: str,
        error: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if self._audit_store is None:
            return None
        try:
            payload = {
                "status": status,
                "request": {
                    "operation": operation,
                    "route_strategy": attempt["strategy"],
                    "caller": call.caller,
                    "purpose": call.purpose,
                    "session_id": call.session_id,
                    "instance_id": call.instance_id,
                    "route_id": call.route_id or "",
                    "provider_id": attempt["provider"]["id"],
                    "provider_type": attempt["provider"]["type"],
                    "model_id": attempt["model"]["id"],
                    "messages": sanitize_payload_for_audit(call.messages),
                    "input_data": sanitize_payload_for_audit(call.input_data),
                    "tools": sanitize_payload_for_audit(call.tools),
                    "response_format": sanitize_payload_for_audit(call.response_format),
                    "params": sanitize_payload_for_audit(call.params),
                    "metadata": sanitize_payload_for_audit(call.metadata),
                    "kwargs": sanitize_payload_for_audit(plan.safe_payload),
                    "backend": plan.backend_name,
                    "backend_model": plan.backend_model,
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                    "started_at": started_at.isoformat(),
                },
                "response": sanitize_payload_for_audit(response_payload),
                "return": sanitize_payload_for_audit(return_payload),
                "error": sanitize_payload_for_audit(error),
                    "meta": {
                        "execution_id": execution_id,
                        "operation": operation,
                        "attempt_started_at": started_at.isoformat(),
                        "attempt_finished_at": finished_at.isoformat(),
                        "route_id": call.route_id or "",
                    "provider_id": attempt["provider"]["id"],
                    "model_id": attempt["model"]["id"],
                    "strategy": attempt["strategy"],
                },
            }
            return self._audit_store.write(
                execution_id=execution_id,
                created_at=started_at,
                payload=payload,
            )
        except Exception:
            logger.exception("Failed to persist audit payload for execution %s", execution_id)
            return None

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
