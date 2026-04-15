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
