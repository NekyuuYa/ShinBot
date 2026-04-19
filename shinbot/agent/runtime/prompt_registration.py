"""Runtime-owned prompt component registration helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.prompt_manager.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

if TYPE_CHECKING:
    from shinbot.agent.prompt_manager import PromptRegistry
    from shinbot.agent.prompt_manager.schema import PromptAssemblyRequest, PromptSource


def register_runtime_prompt_components(
    registry: "PromptRegistry",
    *,
    current_time_resolver: Callable[
        ["PromptAssemblyRequest", PromptComponent, "PromptSource"],
        dict[str, Any],
    ],
) -> None:
    """Register built-in runtime prompt components."""

    registry.register_component(
        PromptComponent(
            id=registry.BUILTIN_CURRENT_TIME_PROMPT_COMPONENT_ID,
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.RESOLVER,
            resolver_ref=registry.BUILTIN_CURRENT_TIME_PROMPT_RESOLVER,
            priority=9050,
            enabled=True,
            cache_stable=False,
            metadata={
                "builtin": True,
                "display_name": "Current Time (Dynamic)",
                "description": "Inject the current local time as a timing reference.",
            },
        )
    )
    registry.register_resolver(
        registry.BUILTIN_CURRENT_TIME_PROMPT_RESOLVER,
        current_time_resolver,
    )
