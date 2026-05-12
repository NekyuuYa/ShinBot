"""Identity-owned prompt component registration helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.prompt_engine.files import register_prompt_files
from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

if TYPE_CHECKING:
    from shinbot.agent.services.prompt_engine import PromptRegistry
    from shinbot.agent.services.prompt_engine.schema import PromptAssemblyRequest, PromptSource


def register_identity_prompt_components(
    registry: PromptRegistry,
    *,
    resolver: Callable[[PromptAssemblyRequest, PromptComponent, PromptSource], dict[str, Any]]
    | None = None,
    identity_store: Any | None = None,
) -> None:
    """Register identity prompt components owned by the identity module."""

    if resolver is None:
        from shinbot.agent.services.identity.prompt_runtime import resolve_identity_map_prompt

        store = identity_store if identity_store is not None else getattr(registry, "_identity_store", None)

        def resolver(
            request: PromptAssemblyRequest,
            component: PromptComponent,
            source: PromptSource,
        ) -> dict[str, Any]:
            return resolve_identity_map_prompt(
                identity_store=store,
                request=request,
                _component=component,
                _source=source,
            )

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
    register_prompt_files(
        registry,
        package=__package__,
        prompt_ids=[registry.BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID],
    )
    registry.register_resolver(
        registry.BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER,
        resolver,
    )
