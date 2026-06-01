"""Model runtime backend selection helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.agent.services.model_runtime.backends import ModelBackend

DEFAULT_MODEL_BACKEND = "litellm"
SUPPORTED_MODEL_BACKENDS = frozenset({DEFAULT_MODEL_BACKEND, "openai_compatible"})


def model_backend_name_from_config(config: dict[str, Any] | None) -> str:
    """Resolve the configured model backend name.

    Args:
        config: Application config dictionary.

    Returns:
        Backend name. Defaults to ``"litellm"`` when unset.
    """

    runtime = config.get("runtime", {}) if isinstance(config, dict) else {}
    if not isinstance(runtime, dict):
        return DEFAULT_MODEL_BACKEND
    backend_config = runtime.get("model_backend")
    if not isinstance(backend_config, dict):
        return DEFAULT_MODEL_BACKEND
    backend_name = str(backend_config.get("type") or DEFAULT_MODEL_BACKEND).strip()
    return backend_name or DEFAULT_MODEL_BACKEND


def create_model_backend(config: dict[str, Any] | None) -> ModelBackend:
    """Create a model backend from runtime config.

    Args:
        config: Application config dictionary.

    Raises:
        ValueError: If the configured backend is unsupported.
    """

    backend_name = model_backend_name_from_config(config)
    if backend_name == "litellm":
        from shinbot.agent.services.model_runtime.backends import LiteLLMBackend

        return LiteLLMBackend()
    if backend_name == "openai_compatible":
        from shinbot.agent.services.model_runtime.backends import OpenAICompatibleBackend

        return OpenAICompatibleBackend()
    raise ValueError(
        f"Unsupported model backend {backend_name!r}; "
        f"supported backends: {', '.join(sorted(SUPPORTED_MODEL_BACKENDS))}"
    )
