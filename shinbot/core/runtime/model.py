"""Model runtime installer.

Model access is a framework-level capability. Agent systems, plugins, media
inspection, and management APIs can all consume it without implying that the
full Agent runtime has been mounted.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.core.application.app import ShinBot


def create_model_runtime(database: Any) -> Any:
    """Create the concrete model runtime implementation."""
    from shinbot.agent.model_runtime import ModelRuntime

    return ModelRuntime(database)


def install_model_runtime(bot: ShinBot) -> Any:
    """Create and mount the default model runtime if it is not already present."""
    if bot.model_runtime is not None:
        return bot.model_runtime

    runtime = create_model_runtime(bot.database)
    bot.mount_model_runtime(runtime)
    return runtime


__all__ = ["create_model_runtime", "install_model_runtime"]
