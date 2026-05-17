from __future__ import annotations

import pytest

from shinbot.agent.services.prompt_engine import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptProfile,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.services.prompt_engine.message_builder import PromptMessageBuilder


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


def test_prompt_component_rejects_invalid_external_stage() -> None:
    with pytest.raises(ValueError):
        PromptComponent(
            id="bad",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.EXTERNAL_INJECTION,
            content="x",
        )


def test_prompt_assembly_request_rejects_removed_payload_fields() -> None:
    with pytest.raises(ValueError):
        PromptAssemblyRequest(instruction_payload="should fail")
    with pytest.raises(ValueError):
        PromptAssemblyRequest(constraint_payload="should fail")
    with pytest.raises(ValueError):
        PromptAssemblyRequest(compatibility_payloads=[{"text": "should fail"}])


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

    # Stage order: SYSTEM_BASE, IDENTITY, ABILITIES, CONTEXT, ...
    assert [stage.stage for stage in result.stages][:2] == [
        PromptStage.SYSTEM_BASE,
        PromptStage.IDENTITY,
    ]
    # System message is first, with content array
    assert result.messages[0]["role"] == "system"
    system_content = result.messages[0]["content"]
    assert isinstance(system_content, list)
    # SYSTEM_BASE comes before IDENTITY in content blocks
    texts = [block["text"] for block in system_content]
    assert texts[0] == "system"
    assert "identity" in texts


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


def test_prompt_registry_exposes_stage_assembly_before_projection() -> None:
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

    stage_assembly = registry.assemble_stages(
        PromptAssemblyRequest(
            component_overrides=["system", "instructions"],
            metadata={"purpose": "test"},
        )
    )
    messages = registry.project_messages(stage_assembly)

    assert not hasattr(stage_assembly, "messages")
    assert [stage.stage for stage in stage_assembly.stages] == [
        PromptStage.SYSTEM_BASE,
        PromptStage.IDENTITY,
        PromptStage.ABILITIES,
        PromptStage.CONTEXT,
        PromptStage.COMPATIBILITY,
        PromptStage.INSTRUCTIONS,
        PromptStage.CONSTRAINTS,
    ]
    assert stage_assembly.prompt_signature
    assert stage_assembly.metadata == {"purpose": "test"}
    assert messages[0]["role"] == "system"


def test_prompt_message_builder_projects_stage_assembly() -> None:
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

    stage_assembly = registry.assemble_stages(
        PromptAssemblyRequest(component_overrides=["system", "instructions"])
    )
    messages = PromptMessageBuilder().build(stage_assembly)

    assert messages[0] == {
        "role": "system",
        "content": [{"type": "text", "text": "system"}],
    }
    assert messages[-1] == {
        "role": "user",
        "content": [{"type": "text", "text": "do work"}],
    }


def test_prompt_registry_projects_abilities_as_prompt_text_not_tools() -> None:
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
            id="abilities",
            stage=PromptStage.ABILITIES,
            kind=PromptComponentKind.STATIC_TEXT,
            content="can use chat action tools when they are provided separately",
        )
    )

    result = registry.assemble(
        PromptAssemblyRequest(component_overrides=["system", "abilities"])
    )

    abilities_stage = next(
        stage for stage in result.stages if stage.stage == PromptStage.ABILITIES
    )
    assert abilities_stage.rendered_text == (
        "can use chat action tools when they are provided separately"
    )
    system_text = "\n".join(_extract_message_texts(result.messages[:1]))
    assert "provided separately" in system_text
    ability_record = next(
        component
        for component in result.ordered_components
        if component.component_id == "abilities"
    )
    assert not hasattr(ability_record, "rendered_data")


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

    # Context resolver output appears as a user message in the context section
    context_messages = [m for m in result.messages if m != result.messages[0]]
    context_text = " ".join(
        str(m.get("content", ""))
        for m in context_messages
        if m.get("role") in ("user", "assistant")
    )
    assert "history" in context_text

    # Template instruction appears in the final user message
    final_user_msg = result.messages[-1]
    assert final_user_msg["role"] == "user"
    final_text = " ".join(block["text"] for block in final_user_msg["content"])
    assert "task=summarize" in final_text
