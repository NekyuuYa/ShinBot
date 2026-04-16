from __future__ import annotations

import pytest

from shinbot.agent.prompting import (
    ContextStrategy,
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


def test_prompt_registry_supports_context_strategy_registry() -> None:
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
    registry.register_context_strategy(
        ContextStrategy(
            id="context.windowed",
            display_name="Windowed Context",
            resolver_ref="context.windowed",
        )
    )
    registry.register_context_strategy_resolver(
        "context.windowed",
        lambda request, strategy: f"history={request.context_inputs.get('summary', '')}",
    )

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            context_strategy_id="context.windowed",
            context_inputs={"summary": "recent chat"},
        )
    )

    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    assert context_stage.components[0].metadata["context_strategy_id"] == "context.windowed"
    assert "history=recent chat" in result.final_prompt


def test_prompt_registry_uses_builtin_sliding_window_fallback_strategy() -> None:
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

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            model_context_window=20,
            context_inputs={
                "summary": "short summary",
                "history_turns": [
                    {"role": "user", "content": "one two three four five six"},
                    {"role": "assistant", "content": "seven eight nine ten eleven twelve"},
                    {"role": "user", "content": "thirteen fourteen fifteen sixteen seventeen"},
                ],
            },
        )
    )

    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    component = context_stage.components[0]
    assert (
        component.metadata["context_strategy_id"]
        == PromptRegistry.BUILTIN_FALLBACK_CONTEXT_STRATEGY_ID
    )
    assert component.metadata["resolver_output"]["dropped_turns"] >= 1
    assert "one two three four five six" not in context_stage.rendered_text


def test_prompt_registry_builtin_sliding_window_ratios_are_configurable() -> None:
    registry = PromptRegistry(
        fallback_context_trigger_ratio=0.9,
        fallback_context_trim_ratio=0.5,
    )
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
        model_context_window=100,
        context_inputs={
            "history_turns": [
                {"role": "user", "content": "alpha beta gamma delta"},
                {"role": "assistant", "content": "epsilon zeta eta theta"},
                {"role": "user", "content": "iota kappa lambda mu"},
            ],
        },
    )
    result = registry.assemble(request)
    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    component = context_stage.components[0]
    assert component.metadata["budget"]["trigger_ratio"] == 0.9
    assert component.metadata["budget"]["trim_ratio"] == 0.5
    assert component.metadata["resolver_output"]["dropped_turns"] == 0
