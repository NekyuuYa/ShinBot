"""LiteLLM-backed provider/model/route runtime."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import uuid
from typing import Any

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
        if not call.route_id and not call.model_id:
            raise ValueError("generate() requires route_id or model_id")

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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "completion",
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
                    "messages": list(call.messages),
                    "tools": list(call.tools),
                    "response_format": call.response_format,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.completion, **kwargs)
                finished = utc_now()
                latency_ms = (finished - started).total_seconds() * 1000
                response_payload = response_to_dict(response)
                usage = extract_usage(response)
                text = extract_text(response)
                tool_calls = extract_tool_calls_list(response)
                think_text = extract_think_text(response)
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    first_token_at=finished.isoformat(),
                    finished_at=finished.isoformat(),
                    latency_ms=latency_ms,
                    time_to_first_token_ms=latency_ms,
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                    cache_hit=usage["cache_read_tokens"] > 0,
                    cache_read_tokens=usage["cache_read_tokens"],
                    cache_write_tokens=usage["cache_write_tokens"],
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    prompt_snapshot_id=call.prompt_snapshot_id,
                    metadata={
                        "route_strategy": attempt["strategy"],
                        "response_model": maybe_get(response, "model"),
                        "usage_raw": response_payload.get("usage"),
                        **call.metadata,
                    },
                    estimated_cost=extract_estimated_cost(response),
                    currency="USD",
                )
                persist_model_execution(self._database, record)
                persist_ai_interaction(
                    self._database,
                    AIInteractionRecord(
                        execution_id=execution_id,
                        timestamp=started.timestamp(),
                        latency_ms=latency_ms,
                        input_tokens=usage["input_tokens"],
                        output_tokens=usage["output_tokens"],
                        cache_read_tokens=usage["cache_read_tokens"],
                        cache_write_tokens=usage["cache_write_tokens"],
                        model_id=attempt["model"]["id"],
                        provider_id=attempt["provider"]["id"],
                        think_text=think_text,
                        injected_context_json=extract_injected_context(call.messages),
                        tool_calls_json=json.dumps(tool_calls, ensure_ascii=False),
                        prompt_snapshot_id=call.prompt_snapshot_id,
                    ),
                )
                await self._notify_observers(
                    {
                        "event": "model_runtime.response",
                        "mode": "completion",
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
                        "status": "success",
                        "latency_ms": latency_ms,
                        "usage": usage,
                        "cache_hit": usage["cache_read_tokens"] > 0,
                        "cache_read_tokens": usage["cache_read_tokens"],
                        "cache_write_tokens": usage["cache_write_tokens"],
                        "return": {
                            "text": text,
                            "tool_calls": tool_calls,
                        },
                        "response": response_payload,
                        "metadata": dict(call.metadata),
                        "prompt_snapshot_id": call.prompt_snapshot_id,
                    }
                )
                return GenerateResult(
                    text=text,
                    tool_calls=tool_calls,
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                latency_ms = (finished - started).total_seconds() * 1000
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
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
                    prompt_snapshot_id=call.prompt_snapshot_id,
                    metadata={
                        "route_strategy": attempt["strategy"],
                        **call.metadata,
                    },
                )
                persist_model_execution(self._database, record)
                await self._notify_observers(
                    {
                        "event": "model_runtime.response",
                        "mode": "completion",
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
                        "status": "error",
                        "latency_ms": latency_ms,
                        "usage": {
                            "input_tokens": 0,
                            "output_tokens": 0,
                            "cache_read_tokens": 0,
                            "cache_write_tokens": 0,
                        },
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
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Model call failed")

    async def embed(self, call: ModelRuntimeCall) -> EmbedResult:
        if not call.route_id and not call.model_id:
            raise ValueError("embed() requires route_id or model_id")
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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="embedding",
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "embedding",
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
                    "input_data": call.input_data,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.embedding, **kwargs)
                finished = utc_now()
                usage = extract_usage(response)
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                    cache_hit=usage["cache_read_tokens"] > 0,
                    cache_read_tokens=usage["cache_read_tokens"],
                    cache_write_tokens=usage["cache_write_tokens"],
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    metadata={
                        "route_strategy": attempt["strategy"],
                        "usage_raw": response_to_dict(response).get("usage"),
                        **call.metadata,
                    },
                    estimated_cost=extract_estimated_cost(response),
                    currency="USD",
                )
                persist_model_execution(self._database, record)
                return EmbedResult(
                    embedding=extract_embedding(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Embedding call failed")

    async def rerank(self, call: ModelRuntimeCall) -> RerankResult:
        if not call.route_id and not call.model_id:
            raise ValueError("rerank() requires route_id or model_id")
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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="rerank",
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "rerank",
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
                    "input_data": call.input_data,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.rerank, **kwargs)
                finished = utc_now()
                usage = extract_usage(response)
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                    estimated_cost=extract_estimated_cost(response),
                    currency="USD",
                )
                persist_model_execution(self._database, record)
                return RerankResult(
                    results=extract_rerank_results(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Rerank call failed")

    async def speak(self, call: ModelRuntimeCall) -> SpeechResult:
        """Text-to-speech via litellm.speech."""
        if not call.route_id and not call.model_id:
            raise ValueError("speak() requires route_id or model_id")
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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="speech",
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "speech",
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
                    "input_data": call.input_data,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.speech, **kwargs)
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                return SpeechResult(
                    audio_bytes=extract_speech_bytes(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Speech call failed")

    async def transcribe(self, call: ModelRuntimeCall) -> TranscriptionResult:
        """Speech-to-text via litellm.transcription."""
        if not call.route_id and not call.model_id:
            raise ValueError("transcribe() requires route_id or model_id")
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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="transcription",
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "transcription",
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
                    "input_data": call.input_data,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.transcription, **kwargs)
                finished = utc_now()
                usage = extract_usage(response)
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    input_tokens=usage["input_tokens"],
                    output_tokens=usage["output_tokens"],
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                    estimated_cost=extract_estimated_cost(response),
                    currency="USD",
                )
                persist_model_execution(self._database, record)
                return TranscriptionResult(
                    text=extract_transcription_text(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Transcription call failed")

    async def generate_image(self, call: ModelRuntimeCall) -> ImageResult:
        """Image generation via litellm.image_generation."""
        if not call.route_id and not call.model_id:
            raise ValueError("generate_image() requires route_id or model_id")
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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="image",
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "image",
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
                    "input_data": call.input_data,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.image_generation, **kwargs)
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                    estimated_cost=extract_estimated_cost(response),
                    currency="USD",
                )
                persist_model_execution(self._database, record)
                return ImageResult(
                    urls=extract_image_urls(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Image generation call failed")

    async def generate_video(self, call: ModelRuntimeCall) -> VideoResult:
        """Video generation via litellm.video_generation."""
        if not call.route_id and not call.model_id:
            raise ValueError("generate_video() requires route_id or model_id")
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
            kwargs = build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="video",
            )
            await self._notify_observers(
                {
                    "event": "model_runtime.request",
                    "mode": "video",
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
                    "input_data": call.input_data,
                    "params": dict(call.params),
                    "metadata": dict(call.metadata),
                    "kwargs": sanitize_litellm_kwargs(kwargs),
                    "prompt_snapshot_id": call.prompt_snapshot_id,
                }
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.video_generation, **kwargs)
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=True,
                    fallback_from_model_id=previous_model_id,
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                    estimated_cost=extract_estimated_cost(response),
                    currency="USD",
                )
                persist_model_execution(self._database, record)
                return VideoResult(
                    urls=extract_image_urls(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                )
            except Exception as exc:  # noqa: BLE001
                finished = utc_now()
                record = ModelExecutionRecord(
                    id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    caller=call.caller,
                    session_id=call.session_id,
                    instance_id=call.instance_id,
                    purpose=call.purpose,
                    started_at=started_at,
                    finished_at=finished.isoformat(),
                    latency_ms=(finished - started).total_seconds() * 1000,
                    success=False,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                    fallback_from_model_id=previous_model_id,
                    fallback_reason="provider_error" if previous_model_id else "",
                    metadata={"route_strategy": attempt["strategy"], **call.metadata},
                )
                persist_model_execution(self._database, record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Video generation call failed")

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
