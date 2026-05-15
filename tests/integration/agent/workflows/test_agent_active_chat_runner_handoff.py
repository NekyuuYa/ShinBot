from __future__ import annotations

from active_chat_runner_support import (
    ActiveChatActionKind,
    ActiveChatDisposition,
    ActiveChatFastRunner,
    FakeMessageStore,
    FakeModelRuntime,
    FakeToolManager,
    PromptRegistry,
    ReviewHandoffContext,
    ReviewWorkflowExplanation,
    SummaryHandoffEntry,
    make_batch,
    make_result,
    make_tool_call,
    pytest,
    register_active_chat_prompt_components,
)


@pytest.mark.asyncio
async def test_active_chat_fast_runner_accepts_dataclass_review_handoff_summary() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    handoff = ReviewWorkflowExplanation(
        review_run_id="test_run",
        review_started_at=123.0,
        candidate_message_ids=[99],
        active_chat_disposition=ActiveChatDisposition.EXIT_SOON,
        active_chat_reason="low interest",
    )
    result = await runner.run(make_batch(review_result_summary=handoff))

    assert result.success is True
    assert result.action == ActiveChatActionKind.NO_REPLY
    assert model_runtime.calls[0].metadata["review_result_summary"][
        "active_chat_disposition"
    ] == "exit_soon"
    handoff_blocks = [
        block
        for message in model_runtime.calls[0].messages
        for block in (
            message.get("content", [])
            if isinstance(message.get("content"), list)
            else []
        )
        if "审查移交摘要 JSON" in str(block.get("text", ""))
    ]
    assert handoff_blocks


@pytest.mark.asyncio
async def test_active_chat_fast_runner_renders_review_handoff_context_sections() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    explanation = ReviewWorkflowExplanation(
        review_run_id="test_run",
        review_started_at=123.0,
        candidate_message_ids=[99],
    )
    handoff = ReviewHandoffContext(
        review_run_id="test_run",
        explanation=explanation,
        overflow_summaries=[
            SummaryHandoffEntry(
                content="Old messages about topic A.",
                msg_log_start=1,
                msg_log_end=10,
                msg_count=10,
            ),
            SummaryHandoffEntry(content="Old messages about topic B."),
        ],
        block_digests=[
            SummaryHandoffEntry(block_index=0, content="Discussion about X."),
            SummaryHandoffEntry(
                block_index=1,
                content="Discussion about Y.",
                msg_log_start=11,
                msg_log_end=20,
                msg_count=10,
            ),
        ],
        recent_active_chat_summary="Previously discussed Z.",
    )
    result = await runner.run(make_batch(review_result_summary=handoff))

    assert result.success is True
    all_text = " ".join(
        block.get("text", "")
        for message in model_runtime.calls[0].messages
        for block in (
            message.get("content", [])
            if isinstance(message.get("content"), list)
            else []
        )
    )
    assert "Old messages about topic A." in all_text
    assert "Old messages about topic B." in all_text
    assert "[Block 0] Discussion about X." in all_text
    assert "[Block 1; msgid 11-20; 10 messages] Discussion about Y." in all_text
    assert "Previously discussed Z." in all_text
    # Should NOT contain JSON dump fallback
    assert "审查移交摘要 JSON" not in all_text


@pytest.mark.asyncio
async def test_active_chat_fast_runner_handoff_context_fallback_when_empty() -> None:
    prompt_registry = PromptRegistry()
    register_active_chat_prompt_components(prompt_registry)
    model_runtime = FakeModelRuntime(
        [
            make_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "watching"})
                ]
            )
        ]
    )
    runner = ActiveChatFastRunner(
        model_runtime,
        prompt_registry=prompt_registry,
        tool_manager=FakeToolManager(),
        message_store=FakeMessageStore(),
    )

    explanation = ReviewWorkflowExplanation(
        review_run_id="test_run",
        review_started_at=123.0,
    )
    handoff = ReviewHandoffContext(
        review_run_id="test_run",
        explanation=explanation,
        overflow_summaries=[],
        block_digests=[],
        recent_active_chat_summary=None,
    )
    result = await runner.run(make_batch(review_result_summary=handoff))

    assert result.success is True
    all_text = " ".join(
        block.get("text", "")
        for message in model_runtime.calls[0].messages
        for block in (
            message.get("content", [])
            if isinstance(message.get("content"), list)
            else []
        )
    )
    # Empty handoff falls back to explanation JSON
    assert "审查移交摘要 JSON" in all_text
