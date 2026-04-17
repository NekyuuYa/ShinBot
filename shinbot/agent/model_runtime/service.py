"""LiteLLM-backed provider/model/route runtime."""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from shinbot.persistence import DatabaseManager, ModelExecutionRecord

from . import litellm_adapter

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _maybe_get(mapping: Any, key: str, default: Any = None) -> Any:
    if mapping is None:
        return default
    if isinstance(mapping, dict):
        return mapping.get(key, default)
    return getattr(mapping, key, default)


def _response_to_dict(response: Any) -> dict[str, Any]:
    if hasattr(response, "model_dump"):
        return response.model_dump()  # type: ignore[no-any-return]
    if isinstance(response, dict):
        return response
    if hasattr(response, "__dict__"):
        return dict(response.__dict__)
    return {}


def _extract_text(response: Any) -> str:
    payload = _response_to_dict(response)
    choices = payload.get("choices") or []
    if not choices:
        return ""

    message = (choices[0] or {}).get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    return ""


def _extract_embedding(response: Any) -> list[float]:
    payload = _response_to_dict(response)
    data = payload.get("data") or []
    if not data:
        return []
    embedding = (data[0] or {}).get("embedding")
    if isinstance(embedding, list):
        return [float(item) for item in embedding]
    return []


def _extract_rerank_results(response: Any) -> list[dict[str, Any]]:
    payload = _response_to_dict(response)
    results = payload.get("results") or []
    normalized = []
    for item in results:
        normalized.append(
            {
                "index": int(item.get("index", 0)),
                "relevance_score": float(item.get("relevance_score", 0.0)),
                "document": item.get("document"),
            }
        )
    return normalized


def _extract_speech_bytes(response: Any) -> bytes:
    if hasattr(response, "read"):
        return bytes(response.read())
    if hasattr(response, "content") and isinstance(response.content, bytes):
        return response.content
    if isinstance(response, bytes):
        return response
    return b""


def _extract_transcription_text(response: Any) -> str:
    if hasattr(response, "text"):
        return str(response.text)
    payload = _response_to_dict(response)
    text = payload.get("text")
    if isinstance(text, str):
        return text
    return ""


def _extract_image_urls(response: Any) -> list[str]:
    payload = _response_to_dict(response)
    data = payload.get("data") or []
    urls = []
    for item in data:
        if isinstance(item, dict):
            url = item.get("url") or item.get("b64_json")
            if url:
                urls.append(str(url))
    return urls


def _extract_usage(response: Any) -> dict[str, Any]:
    payload = _response_to_dict(response)
    usage = payload.get("usage") or {}
    prompt_details = usage.get("prompt_tokens_details") or {}
    return {
        "input_tokens": int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0),
        "output_tokens": int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
        "cache_read_tokens": int(
            prompt_details.get("cached_tokens")
            or usage.get("cache_read_input_tokens")
            or usage.get("cache_read_tokens")
            or 0
        ),
        "cache_write_tokens": int(
            usage.get("cache_creation_input_tokens")
            or usage.get("cache_write_input_tokens")
            or usage.get("cache_write_tokens")
            or 0
        ),
    }


def _extract_estimated_cost(response: Any) -> float | None:
    payload = _response_to_dict(response)
    if isinstance(payload.get("response_cost"), (int, float)):
        return float(payload["response_cost"])

    hidden = payload.get("_hidden_params")
    if isinstance(hidden, dict) and isinstance(hidden.get("response_cost"), (int, float)):
        return float(hidden["response_cost"])

    hidden_attr = _maybe_get(response, "_hidden_params")
    if isinstance(hidden_attr, dict) and isinstance(hidden_attr.get("response_cost"), (int, float)):
        return float(hidden_attr["response_cost"])
    return None


@dataclass(slots=True)
class ModelRuntimeCall:
    """Normalized runtime call input."""

    caller: str
    route_id: str | None = None
    model_id: str | None = None
    session_id: str = ""
    instance_id: str = ""
    prompt_snapshot_id: str = ""
    purpose: str = ""
    messages: list[dict[str, Any]] = field(default_factory=list)
    input_data: str | list[str] | None = None
    tools: list[dict[str, Any]] = field(default_factory=list)
    response_format: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class GenerateResult:
    text: str
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str
    usage: dict[str, Any]


@dataclass(slots=True)
class EmbedResult:
    embedding: list[float]
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str
    usage: dict[str, Any]


@dataclass(slots=True)
class RerankResult:
    results: list[dict[str, Any]]
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str
    usage: dict[str, Any]


@dataclass(slots=True)
class SpeechResult:
    audio_bytes: bytes
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str


@dataclass(slots=True)
class TranscriptionResult:
    text: str
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str
    usage: dict[str, Any]


@dataclass(slots=True)
class ImageResult:
    urls: list[str]
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str


@dataclass(slots=True)
class VideoResult:
    urls: list[str]
    raw_response: Any
    execution_id: str
    route_id: str
    provider_id: str
    model_id: str


class ModelCallError(RuntimeError):
    """Model invocation failure after route resolution/fallback."""


class ModelRuntime:
    """Unified runtime for route-based LiteLLM calls."""

    def __init__(self, database: DatabaseManager | None) -> None:
        self._database = database
        self._random = random.Random()

    async def generate(self, call: ModelRuntimeCall) -> GenerateResult:
        if not call.route_id and not call.model_id:
            raise ValueError("generate() requires route_id or model_id")

        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.completion, **kwargs)
                finished = _utc_now()
                usage = _extract_usage(response)
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
                    latency_ms=(finished - started).total_seconds() * 1000,
                    time_to_first_token_ms=(finished - started).total_seconds() * 1000,
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
                        "response_model": _maybe_get(response, "model"),
                        "usage_raw": _response_to_dict(response).get("usage"),
                        **call.metadata,
                    },
                    estimated_cost=_extract_estimated_cost(response),
                    currency="USD",
                )
                self._persist_execution(record)
                return GenerateResult(
                    text=_extract_text(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                    prompt_snapshot_id=call.prompt_snapshot_id,
                    metadata={
                        "route_strategy": attempt["strategy"],
                        **call.metadata,
                    },
                )
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Model call failed")

    async def embed(self, call: ModelRuntimeCall) -> EmbedResult:
        if not call.route_id and not call.model_id:
            raise ValueError("embed() requires route_id or model_id")
        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="embedding",
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.embedding, **kwargs)
                finished = _utc_now()
                usage = _extract_usage(response)
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
                        "usage_raw": _response_to_dict(response).get("usage"),
                        **call.metadata,
                    },
                    estimated_cost=_extract_estimated_cost(response),
                    currency="USD",
                )
                self._persist_execution(record)
                return EmbedResult(
                    embedding=_extract_embedding(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Embedding call failed")

    async def rerank(self, call: ModelRuntimeCall) -> RerankResult:
        if not call.route_id and not call.model_id:
            raise ValueError("rerank() requires route_id or model_id")
        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="rerank",
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.rerank, **kwargs)
                finished = _utc_now()
                usage = _extract_usage(response)
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
                    estimated_cost=_extract_estimated_cost(response),
                    currency="USD",
                )
                self._persist_execution(record)
                return RerankResult(
                    results=_extract_rerank_results(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Rerank call failed")

    async def speak(self, call: ModelRuntimeCall) -> SpeechResult:
        """Text-to-speech via litellm.speech."""
        if not call.route_id and not call.model_id:
            raise ValueError("speak() requires route_id or model_id")
        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="speech",
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.speech, **kwargs)
                finished = _utc_now()
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
                self._persist_execution(record)
                return SpeechResult(
                    audio_bytes=_extract_speech_bytes(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Speech call failed")

    async def transcribe(self, call: ModelRuntimeCall) -> TranscriptionResult:
        """Speech-to-text via litellm.transcription."""
        if not call.route_id and not call.model_id:
            raise ValueError("transcribe() requires route_id or model_id")
        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="transcription",
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.transcription, **kwargs)
                finished = _utc_now()
                usage = _extract_usage(response)
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
                    estimated_cost=_extract_estimated_cost(response),
                    currency="USD",
                )
                self._persist_execution(record)
                return TranscriptionResult(
                    text=_extract_transcription_text(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                    usage=usage,
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Transcription call failed")

    async def generate_image(self, call: ModelRuntimeCall) -> ImageResult:
        """Image generation via litellm.image_generation."""
        if not call.route_id and not call.model_id:
            raise ValueError("generate_image() requires route_id or model_id")
        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="image",
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.image_generation, **kwargs)
                finished = _utc_now()
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
                    estimated_cost=_extract_estimated_cost(response),
                    currency="USD",
                )
                self._persist_execution(record)
                return ImageResult(
                    urls=_extract_image_urls(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Image generation call failed")

    async def generate_video(self, call: ModelRuntimeCall) -> VideoResult:
        """Video generation via litellm.video_generation."""
        if not call.route_id and not call.model_id:
            raise ValueError("generate_video() requires route_id or model_id")
        attempts = self._resolve_targets(call)
        last_error: Exception | None = None
        previous_model_id = ""

        for attempt in attempts:
            execution_id = str(uuid.uuid4())
            started = _utc_now()
            started_at = started.isoformat()
            kwargs = self._build_litellm_kwargs(
                provider=attempt["provider"],
                model=attempt["model"],
                call=call,
                timeout_override=attempt["timeout_override"],
                mode="video",
            )
            try:
                response = await asyncio.to_thread(litellm_adapter.video_generation, **kwargs)
                finished = _utc_now()
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
                    estimated_cost=_extract_estimated_cost(response),
                    currency="USD",
                )
                self._persist_execution(record)
                return VideoResult(
                    urls=_extract_image_urls(response),
                    raw_response=response,
                    execution_id=execution_id,
                    route_id=call.route_id or attempt["model"]["id"],
                    provider_id=attempt["provider"]["id"],
                    model_id=attempt["model"]["id"],
                )
            except Exception as exc:  # noqa: BLE001
                finished = _utc_now()
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
                self._persist_execution(record)
                previous_model_id = attempt["model"]["id"]
                last_error = exc

        raise ModelCallError(str(last_error) if last_error else "Video generation call failed")

    def _resolve_targets(self, call: ModelRuntimeCall) -> list[dict[str, Any]]:
        if self._database is None:
            raise ModelCallError("Database-backed model registry is not initialized")

        registry = self._database.model_registry
        if call.model_id:
            model = registry.get_model(call.model_id)
            if model is None:
                raise ModelCallError(f"Model {call.model_id!r} not found")
            provider = registry.get_provider(model["provider_id"])
            if provider is None:
                raise ModelCallError(f"Provider {model['provider_id']!r} not found")
            if not provider["enabled"] or not model["enabled"]:
                raise ModelCallError(f"Model {call.model_id!r} is disabled")
            return [
                {
                    "provider": provider,
                    "model": model,
                    "timeout_override": None,
                    "strategy": "direct",
                }
            ]

        assert call.route_id is not None
        route = self._database.model_registry.get_route(call.route_id)
        if route is None or not route["enabled"]:
            raise ModelCallError(f"Route {call.route_id!r} not found or disabled")

        members = self._database.model_registry.list_route_members(call.route_id)
        candidates: list[dict[str, Any]] = []
        for member in members:
            if not member["enabled"]:
                continue
            model = registry.get_model(member["model_id"])
            if model is None or not model["enabled"]:
                continue
            provider = registry.get_provider(model["provider_id"])
            if provider is None or not provider["enabled"]:
                continue
            candidates.append(
                {
                    "provider": provider,
                    "model": model,
                    "timeout_override": member["timeout_override"],
                    "priority": member["priority"],
                    "weight": member["weight"],
                    "strategy": route["strategy"],
                }
            )

        if not candidates:
            raise ModelCallError(f"Route {call.route_id!r} has no available models")

        if route["strategy"] == "weighted":
            first = self._weighted_pick(candidates)
            rest = [item for item in candidates if item["model"]["id"] != first["model"]["id"]]
            rest.sort(key=lambda item: (item["priority"], -item["weight"], item["model"]["id"]))
            return [first, *rest]

        candidates.sort(key=lambda item: (item["priority"], -item["weight"], item["model"]["id"]))
        return candidates

    def _weighted_pick(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        weights = [max(float(item["weight"]), 0.0) for item in candidates]
        if all(weight == 0.0 for weight in weights):
            return sorted(
                candidates,
                key=lambda item: (item["priority"], -item["weight"], item["model"]["id"]),
            )[0]
        return self._random.choices(candidates, weights=weights, k=1)[0]

    def _build_litellm_kwargs(
        self,
        *,
        provider: dict[str, Any],
        model: dict[str, Any],
        call: ModelRuntimeCall,
        timeout_override: float | None,
        mode: str = "completion",
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if provider["base_url"]:
            kwargs["api_base"] = provider["base_url"]

        kwargs.update(provider.get("auth") or {})
        kwargs.update(provider.get("default_params") or {})
        kwargs.update(model.get("default_params") or {})
        kwargs.update(call.params)
        kwargs["model"] = model["litellm_model"]

        if timeout_override is not None:
            kwargs["timeout"] = timeout_override

        if mode == "completion":
            kwargs["messages"] = call.messages
            if call.tools:
                kwargs["tools"] = call.tools
            if call.response_format is not None:
                kwargs["response_format"] = call.response_format
        elif mode in ("embedding", "speech"):
            kwargs["input"] = call.input_data if call.input_data is not None else ""

        return kwargs

    def _persist_execution(self, record: ModelExecutionRecord) -> None:
        if self._database is None:
            return
        try:
            self._database.model_executions.insert(record)
        except Exception:
            logger.exception(
                "Failed to persist model execution %s (caller=%s, success=%s);"
                " API quota may have been consumed without a corresponding record",
                record.id,
                record.caller,
                record.success,
            )
