"""Unified LiteLLM-backed model runtime."""

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

__all__ = [
    "EmbedResult",
    "GenerateResult",
    "ImageResult",
    "ModelCallError",
    "ModelRuntime",
    "ModelRuntimeCall",
    "ModelRuntimeObserver",
    "RerankResult",
    "SpeechResult",
    "TranscriptionResult",
    "VideoResult",
]
