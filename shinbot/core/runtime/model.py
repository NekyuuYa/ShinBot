"""Model runtime installer.

Model access is a framework-level capability. Agent systems, plugins, media
inspection, and management APIs can all consume it without implying that the
full Agent runtime has been mounted.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.core.application.app import ShinBot


def create_model_runtime(database: Any, config: dict[str, Any] | None = None) -> Any:
    """Create the concrete model runtime implementation."""
    from shinbot.agent.services.model_runtime import ModelRuntime
    from shinbot.core.runtime.model_backend import create_model_backend

    return ModelRuntime(database, backend=create_model_backend(config))


def install_model_runtime(bot: ShinBot) -> Any:
    """Create and mount the default model runtime if it is not already present."""
    if bot.model_runtime is not None:
        return bot.model_runtime

    config = getattr(bot, "config", None)
    runtime = create_model_runtime(bot.database, config if isinstance(config, dict) else None)
    bot.mount_model_runtime(runtime)
    return runtime


__all__ = ["create_model_runtime", "install_model_runtime"]
