"""Identity-owned prompt component registration helpers."""

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


def register_identity_prompt_components(
    registry: PromptRegistry,
    *,
    resolver: Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], dict[str, Any]],
) -> None:
    """Register identity prompt components owned by the identity module."""

    registry.register_component(
        PromptComponent(
            id=registry.BUILTIN_IDENTITY_MAP_PROMPT_COMPONENT_ID,
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.RESOLVER,
            resolver_ref=registry.BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER,
            priority=9000,
            enabled=True,
            metadata={
                "builtin": True,
                "display_name": "Identity Map (Dynamic)",
                "description": "Inject active participant identity mapping for current context.",
            },
        )
    )
    registry.register_component(
        PromptComponent(
            id=registry.BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID,
            stage=PromptStage.CONSTRAINTS,
            kind=PromptComponentKind.STATIC_TEXT,
            content=(
                "### 行为约束\n"
                "- 严禁在输出中包含任何 【ID】 格式的字符串或原始数字 ID。\n"
                "- 称呼他人时，必须使用上述参考表中的“昵称”或“别名”。\n"
                "- 若用户 ID 未出现在上表中，请用类似于“那个人”的称呼。"
            ),
            priority=9000,
            enabled=True,
            metadata={
                "builtin": True,
                "display_name": "Identity Behavior Constraints",
                "description": "Static constraints for identity-safe assistant replies.",
            },
        )
    )
    registry.register_resolver(
        registry.BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER,
        resolver,
    )
