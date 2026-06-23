"""Unified model runtime service."""

from shinbot.agent.services.model_runtime.service import (
    EmbedResult,
    GenerateResult,
    ImageResult,
    ModelCallError,
    ModelRuntime,
    ModelRuntimeCall,
    ModelRuntimeObserver,
    RerankResult,
    SpeechResult,
    TranscriptionResult,
    VideoResult,
)
from shinbot.agent.services.model_runtime.types import LLMCallResult

__all__ = [
    "EmbedResult",
    "GenerateResult",
    "ImageResult",
    "LLMCallResult",
    "ModelCallError",
    "ModelRuntime",
    "ModelRuntimeCall",
    "ModelRuntimeObserver",
    "RerankResult",
    "SpeechResult",
    "TranscriptionResult",
    "VideoResult",
]
