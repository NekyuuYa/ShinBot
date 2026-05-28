"""Identity-owned prompt component registration helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
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
    prompt_file_config: PromptFileLoadConfig | None = None,
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
            """Resolve the identity map prompt for the current assembly request.

            Args:
                request: The prompt assembly request.
                component: The prompt component being resolved.
                source: Source metadata for the component.

            Returns:
                Dict containing the resolved prompt template variables.
            """
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
    register_identity_file_prompt_components(
        registry,
        prompt_file_config=prompt_file_config,
    )
    registry.register_resolver(
        registry.BUILTIN_IDENTITY_MAP_PROMPT_RESOLVER,
        resolver,
    )


def register_identity_file_prompt_components(
    registry: PromptRegistry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register file-backed identity prompt components."""

    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[registry.BUILTIN_IDENTITY_CONSTRAINTS_COMPONENT_ID],
    )
