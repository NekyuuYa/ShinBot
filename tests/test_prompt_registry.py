from __future__ import annotations

import pytest

from shinbot.prompting import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptProfile,
    PromptRegistry,
    PromptStage,
)


def test_prompt_component_rejects_invalid_external_stage() -> None:
    with pytest.raises(ValueError):
        PromptComponent(
            id="bad",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.EXTERNAL_INJECTION,
            content="x",
        )


def test_prompt_registry_assembles_in_fixed_stage_order() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="identity",
            stage=PromptStage.IDENTITY,
            kind=PromptComponentKind.STATIC_TEXT,
            content="identity",
        )
    )
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(
        PromptProfile(
            id="agent.default",
            base_components=["identity", "system"],
        )
    )

    result = registry.assemble(PromptAssemblyRequest(profile_id="agent.default"))

    assert [stage.stage for stage in result.stages][:2] == [
        PromptStage.SYSTEM_BASE,
        PromptStage.IDENTITY,
    ]
    assert result.final_prompt.startswith("system")
    assert "identity" in result.final_prompt


def test_prompt_registry_sorts_stage_records_stably() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system_b",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="B",
            priority=20,
        )
    )
    registry.register_component(
        PromptComponent(
            id="system_a",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="A",
            priority=10,
        )
    )
    registry.register_profile(
        PromptProfile(id="agent.default", base_components=["system_b", "system_a"])
    )

    result = registry.assemble(PromptAssemblyRequest(profile_id="agent.default"))

    system_stage = next(stage for stage in result.stages if stage.stage == PromptStage.SYSTEM_BASE)
    assert [component.component_id for component in system_stage.components] == [
        "system_a",
        "system_b",
    ]


def test_prompt_registry_supports_template_and_resolver_components() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_component(
        PromptComponent(
            id="instructions",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.TEMPLATE,
            content="task={task}",
            template_vars=["task"],
        )
    )
    registry.register_component(
        PromptComponent(
            id="context",
            stage=PromptStage.CONTEXT,
            kind=PromptComponentKind.RESOLVER,
            resolver_ref="context.short",
        )
    )
    registry.register_profile(
        PromptProfile(
            id="agent.default",
            base_components=["system", "context", "instructions"],
        )
    )
    registry.register_resolver(
        "context.short",
        lambda request, component, source: request.context_inputs.get("summary", ""),
    )

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            template_inputs={"task": "summarize"},
            context_inputs={"summary": "history"},
        )
    )

    assert "history" in result.final_prompt
    assert "task=summarize" in result.final_prompt


def test_prompt_registry_requires_system_base() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="identity",
            stage=PromptStage.IDENTITY,
            kind=PromptComponentKind.STATIC_TEXT,
            content="identity",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["identity"]))

    with pytest.raises(ValueError, match="system_base"):
        registry.assemble(PromptAssemblyRequest(profile_id="agent.default"))


def test_prompt_registry_builds_snapshot_and_log_record() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    request = PromptAssemblyRequest(
        profile_id="agent.default",
        caller="agent.runtime",
        session_id="s1",
        route_id="route.default",
    )
    result = registry.assemble(request)
    snapshot = registry.create_snapshot(result, request)
    record = registry.build_log_record(result, request)

    assert snapshot.prompt_signature == result.prompt_signature
    assert record.selected_component_count == 1
