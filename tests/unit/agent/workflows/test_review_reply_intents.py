"""Unit tests for side-effect-free review reply action planning."""

from __future__ import annotations

import json
from typing import Any

import pytest

from shinbot.agent.runners.review_reply.runner import LLMReplyDecisionStageRunner
from shinbot.agent.runners.templates import ToolCallPlanResult, parse_tool_call_payload
from shinbot.agent.runtime.session_actor.external_actions import ExternalActionKind
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.workflows.chat_actions import ExternalActionToolMode


class _PlanTemplate:
    def __init__(self, tool_calls: list[dict[str, Any]]) -> None:
        self._plan = ToolCallPlanResult(
            tool_calls=tool_calls,
            execution_id="model-run",
            reason="tool_call_plan",
        )

    async def run(self, _stage_input: ReviewStageInput) -> ToolCallPlanResult:
        return self._plan

    @staticmethod
    def parse_tool_calls(tool_calls: list[dict[str, Any]]) -> list[Any]:
        return [parse_tool_call_payload(tool_call) for tool_call in tool_calls]


def _tool_call(call_id: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": call_id,
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


def _runner(tool_calls: list[dict[str, Any]]) -> LLMReplyDecisionStageRunner:
    runner = LLMReplyDecisionStageRunner(
        object(),
        prompt_registry=PromptRegistry(),
        tool_manager=None,
        external_action_mode=ExternalActionToolMode.COLLECT_INTENTS,
    )
    runner._template = _PlanTemplate(tool_calls)  # type: ignore[assignment]
    return runner


def _stage_input() -> ReviewStageInput:
    return ReviewStageInput(
        session_id="bot:group:room",
        purpose="reply_decision",
        source_messages=[{"id": 7, "platform_msg_id": "platform-7"}],
        metadata={"candidate_message_ids": [7]},
    )


@pytest.mark.asyncio
async def test_collect_mode_returns_ordered_intents_without_tool_manager() -> None:
    runner = _runner(
        [
            _tool_call("poke-before", "send_poke", {"user_id": "alice"}),
            _tool_call(
                "reply",
                "send_reply",
                {"text": " hello ", "quote_message_log_id": 7},
            ),
            _tool_call("poke-after", "send_poke", {"user_id": "alice"}),
            _tool_call(
                "reaction",
                "send_reaction",
                {"message_log_id": 7, "emoji_id": "128077"},
            ),
        ]
    )

    result = await runner.run(_stage_input())

    assert result.replied is True
    assert result.reply_message_id is None
    assert result.reply_message_ids == []
    assert result.reason == (
        "send_reply_tool:1;send_poke_tool:1;send_reaction_tool"
    )
    assert [intent.kind for intent in result.external_action_intents] == [
        ExternalActionKind.SEND_REPLY,
        ExternalActionKind.SEND_POKE,
        ExternalActionKind.SEND_REACTION,
    ]
    assert [intent.tool_call_id for intent in result.external_action_intents] == [
        "reply",
        "poke-after",
        "reaction",
    ]
    assert [intent.action_ordinal for intent in result.external_action_intents] == [
        0,
        1,
        2,
    ]
    assert result.external_action_intents[0].payload == {
        "text": "hello",
        "quote_message_log_id": 7,
    }


@pytest.mark.asyncio
async def test_collect_mode_rejects_entire_batch_after_invalid_action() -> None:
    runner = _runner(
        [
            _tool_call(
                "reply",
                "send_reply",
                {"text": "hello", "quote_message_log_id": 7},
            ),
            _tool_call(
                "reaction",
                "send_reaction",
                {
                    "message_log_id": 7,
                    "emoji_id": "128077",
                    "idempotency_key": "model-owned",
                },
            ),
        ]
    )

    result = await runner.run(_stage_input())

    assert result.replied is False
    assert result.external_action_intents == ()
    assert result.reason == (
        "reply_external_action_invalid:send_reaction:ValueError"
    )
