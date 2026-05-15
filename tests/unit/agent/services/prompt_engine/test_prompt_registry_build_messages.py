from __future__ import annotations

import pytest

from shinbot.agent.services.prompt_engine import (
    PromptAssemblyRequest,
    PromptBuildRequest,
    PromptComponent,
    PromptComponentKind,
    PromptContextPolicy,
    PromptInjection,
    PromptProfile,
    PromptRegistry,
    PromptStage,
)


def _extract_message_texts(messages: list[dict[str, object]]) -> list[str]:
    texts: list[str] = []
    for message in messages:
        content = message.get("content", [])
        if isinstance(content, list):
            texts.extend(
                str(block.get("text", ""))
                for block in content
                if isinstance(block, dict) and "text" in block
            )
        else:
            texts.append(str(content))
    return texts


def test_prompt_registry_build_messages_supports_workflow_injections() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )

    result = registry.build_messages(
        PromptBuildRequest(
            caller="agent.review",
            workflow_id="review",
            stage_id="review_scan",
            component_ids_by_stage={PromptStage.SYSTEM_BASE: ["system"]},
            injections=[
                PromptInjection(
                    stage=PromptStage.INSTRUCTIONS,
                    component_id="review.scan.task",
                    content_blocks=[{"type": "text", "text": "select message ids"}],
                    priority=10,
                ),
                PromptInjection(
                    stage=PromptStage.CONSTRAINTS,
                    component_id="review.scan.contract",
                    text="return json",
                    priority=20,
                ),
            ],
            source_messages=[
                {"role": "user", "content": [{"type": "text", "text": "history"}]}
            ],
            context_policy=PromptContextPolicy.PROVIDED,
        )
    )

    assert result.workflow_id == "review"
    assert result.stage_id == "review_scan"
    assert result.messages[0]["role"] == "system"
    assert result.messages[1] == {
        "role": "user",
        "content": [{"type": "text", "text": "history"}],
    }
    final_text = " ".join(block["text"] for block in result.messages[-1]["content"])
    assert "select message ids" in final_text
    assert "return json" in final_text
    assert result.metadata["workflow_id"] == "review"
    assert result.metadata["stage_id"] == "review_scan"


def test_prompt_registry_build_messages_accepts_flat_component_ids() -> None:
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
            kind=PromptComponentKind.STATIC_TEXT,
            content="do work",
        )
    )

    result = registry.build_messages(
        PromptBuildRequest(
            caller="attention.workflow_runner",
            workflow_id="attention",
            stage_id="attention_workflow",
            component_ids=["system", "instructions"],
            context_policy=PromptContextPolicy.DISABLED,
        )
    )

    assert [component.component_id for component in result.ordered_components] == [
        "system",
        "instructions",
    ]
    assert "system" in result.messages[0]["content"][0]["text"]
    assert "do work" in result.messages[-1]["content"][0]["text"]


def test_prompt_registry_build_messages_creates_snapshot() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    request = PromptBuildRequest(
        caller="attention.workflow_runner",
        workflow_id="attention",
        stage_id="attention_workflow",
        session_id="s1",
        instance_id="inst-1",
        route_id="route-a",
        component_ids=["system"],
        context_policy=PromptContextPolicy.DISABLED,
    )
    result = registry.build_messages(request)

    snapshot = registry.create_build_snapshot(result, request)

    assert snapshot.caller == "attention.workflow_runner"
    assert snapshot.session_id == "s1"
    assert snapshot.instance_id == "inst-1"
    assert snapshot.route_id == "route-a"
    assert snapshot.prompt_signature == result.prompt_signature
    assert snapshot.full_messages == result.messages
    assert snapshot.metadata["workflow_id"] == "attention"
    assert snapshot.metadata["stage_id"] == "attention_workflow"


def test_prompt_registry_build_messages_returns_messages_only() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )

    result = registry.build_messages(
        PromptBuildRequest(
            caller="agent.review",
            workflow_id="review",
            stage_id="reply_decision",
            component_ids_by_stage={PromptStage.SYSTEM_BASE: ["system"]},
            injections=[
                PromptInjection(
                    stage=PromptStage.INSTRUCTIONS,
                    component_id="reply.tool_rules",
                    text="use send_reply when needed",
                ),
            ],
            context_policy=PromptContextPolicy.DISABLED,
        )
    )

    assert isinstance(result.messages, list)
    message_text = "\n".join(_extract_message_texts(result.messages))
    assert "use send_reply when needed" in message_text
    assert "description" not in message_text


def test_prompt_registry_build_messages_validates_stage_component_mapping() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )

    with pytest.raises(ValueError, match="belongs to stage"):
        registry.build_messages(
            PromptBuildRequest(
                caller="agent.review",
                workflow_id="review",
                stage_id="review_scan",
                component_ids_by_stage={PromptStage.INSTRUCTIONS: ["system"]},
            )
        )


def test_prompt_registry_requires_system_base() -> None:
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="instructions",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="do something",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["instructions"]))

    with pytest.raises(ValueError, match="SYSTEM_BASE or IDENTITY"):
        registry.assemble(PromptAssemblyRequest(profile_id="agent.default"))


def test_prompt_registry_allows_identity_only_system_stage() -> None:
    """Relaxed guard: IDENTITY stage alone satisfies the system-message requirement."""
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="identity_only",
            stage=PromptStage.IDENTITY,
            kind=PromptComponentKind.STATIC_TEXT,
            content="I am Shin.",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["identity_only"]))

    result = registry.assemble(PromptAssemblyRequest(profile_id="agent.default"))
    assert result.messages[0]["role"] == "system"
    system_texts = [block["text"] for block in result.messages[0]["content"]]
    assert "I am Shin." in system_texts


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
    assert isinstance(snapshot.full_messages, list)
    assert len(snapshot.full_messages) >= 1
    assert record.selected_component_count == 1


def test_prompt_registry_does_not_materialize_history_turns_without_context_manager() -> None:
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
    assert context_stage.components == []
    assert context_stage.messages == []
    assert result.messages == [{"role": "system", "content": [{"type": "text", "text": "system"}]}]


def test_prompt_registry_rejects_deprecated_fallback_context_budget_kwargs() -> None:
    with pytest.raises(TypeError, match="fallback_context_trigger_ratio"):
        PromptRegistry(
            fallback_context_trigger_ratio=0.9,
            fallback_context_trim_turns=2,
        )


def test_prompt_registry_rejects_deprecated_fallback_context_target_kwargs() -> None:
    with pytest.raises(TypeError, match="fallback_context_trigger_ratio"):
        PromptRegistry(
            fallback_context_trigger_ratio=1.0,
            fallback_context_max_tokens=15000,
            fallback_context_target_tokens=6000,
            fallback_context_trim_turns=2,
        )
