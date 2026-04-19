"""Shared types for the unified model runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

ModelRuntimeObserver = Callable[[dict[str, Any]], Awaitable[None] | None]


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
    tool_calls: list[dict[str, Any]]
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
