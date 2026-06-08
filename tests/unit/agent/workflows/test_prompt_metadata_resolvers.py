"""Unit tests for workflow prompt metadata resolvers."""

from __future__ import annotations

from shinbot.agent.runners.templates.review_instruction import resolve_review_stage_instruction
from shinbot.agent.services.prompt_engine.schema import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptSource,
    PromptStage,
)
from shinbot.agent.workflows.active_chat.prompt_registration import (
    resolve_active_chat_fast_mode_batch,
)


def _component(component_id: str, *, review_stage: str = "") -> PromptComponent:
    return PromptComponent(
        id=component_id,
        stage=PromptStage.INSTRUCTIONS,
        kind=PromptComponentKind.RESOLVER,
        resolver_ref=component_id,
        metadata={"review_stage": review_stage} if review_stage else {},
    )


def test_active_chat_fast_batch_metadata_excludes_source_payloads() -> None:
    request = PromptAssemblyRequest(
        session_id="bot:group:room",
        metadata={
            "message_log_ids": [1],
            "interest_value": 0.7,
            "active_chat_instruction_content": [{"type": "text", "text": "rendered"}],
            "active_chat_source_messages": [{"id": 1, "raw_text": "hello"}],
            "active_chat_source_messages_text": "Alice: hello",
        },
    )

    result = resolve_active_chat_fast_mode_batch(
        request,
        _component("active_chat.fast_mode.batch"),
        PromptSource(),
    )

    metadata_line = str(result["text"])
    rendered_blocks = "\n".join(str(block["text"]) for block in result["content_blocks"])
    assert "active_chat_instruction_content" not in metadata_line
    assert "active_chat_source_messages" not in metadata_line
    assert "active_chat_source_messages_text" not in metadata_line
    assert rendered_blocks.count("rendered") == 1


def test_review_stage_instruction_metadata_excludes_source_text_fallback() -> None:
    request = PromptAssemblyRequest(
        metadata={
            "stage_id": "review_scan",
            "review_stage": "review_scan",
            "review_source_messages_text": "Alice: hello",
            "review_source_messages": [{"id": 1, "raw_text": "hello"}],
        },
    )

    result = resolve_review_stage_instruction(
        request,
        _component("review.review_scan.instruction", review_stage="review_scan"),
        PromptSource(),
    )

    metadata_line = str(result["text"])
    rendered_blocks = "\n".join(str(block["text"]) for block in result["content_blocks"])
    assert "review_source_messages_text" not in metadata_line
    assert "review_source_messages" not in metadata_line
    assert "Source messages:\nAlice: hello" in rendered_blocks
