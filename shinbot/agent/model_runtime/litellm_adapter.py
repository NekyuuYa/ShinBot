"""Lazy LiteLLM adapter wrappers."""

from __future__ import annotations

from typing import Any


class LiteLLMNotInstalledError(RuntimeError):
    """Raised when LiteLLM runtime is used without the dependency installed."""


def _import_litellm() -> Any:
    try:
        import litellm
    except ImportError as exc:  # pragma: no cover - exercised via runtime error path
        raise LiteLLMNotInstalledError(
            "LiteLLM is not installed. Add the 'litellm' dependency before using model runtime."
        ) from exc
    return litellm


def completion(**kwargs: Any) -> Any:
    """Invoke litellm.completion lazily."""
    litellm = _import_litellm()
    return litellm.completion(**kwargs)


def embedding(**kwargs: Any) -> Any:
    """Invoke litellm.embedding lazily."""
    litellm = _import_litellm()
    return litellm.embedding(**kwargs)


def rerank(**kwargs: Any) -> Any:
    """Invoke litellm.rerank lazily."""
    litellm = _import_litellm()
    return litellm.rerank(**kwargs)


def speech(**kwargs: Any) -> Any:
    """Invoke litellm.speech lazily (TTS)."""
    litellm = _import_litellm()
    return litellm.speech(**kwargs)


def transcription(**kwargs: Any) -> Any:
    """Invoke litellm.transcription lazily (STT)."""
    litellm = _import_litellm()
    return litellm.transcription(**kwargs)


def image_generation(**kwargs: Any) -> Any:
    """Invoke litellm.image_generation lazily."""
    litellm = _import_litellm()
    return litellm.image_generation(**kwargs)


def video_generation(**kwargs: Any) -> Any:
    """Invoke litellm.video_generation lazily (not yet widely supported)."""
    litellm = _import_litellm()
    if not hasattr(litellm, "video_generation"):
        raise NotImplementedError(
            "LiteLLM does not support video_generation on the installed version"
        )
    return litellm.video_generation(**kwargs)  # type: ignore[attr-defined]


def get_model_info(
    model: str,
    *,
    custom_llm_provider: str | None = None,
    api_base: str | None = None,
) -> Any:
    """Invoke litellm.get_model_info lazily."""
    litellm = _import_litellm()
    return litellm.get_model_info(
        model,
        custom_llm_provider=custom_llm_provider,
        api_base=api_base,
    )
