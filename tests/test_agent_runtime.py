from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from shinbot.agent.coordinators.review.factory import ReviewRuntimeConfig, ReviewStageRuntimeConfig
from shinbot.agent.runners.review_scan import LLMReviewScanStageRunner
from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.scheduler import ActiveChatState, AgentScheduler, AgentState
from shinbot.agent.services.model_runtime import GenerateResult
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.persistence.records import BotConfigRecord, MessageLogRecord


class FakeModelRuntime:
    def __init__(self, responses: list[GenerateResult]) -> None:
        self.responses = list(responses)
        self.calls: list[Any] = []
        self.on_generate: Any | None = None

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        if self.on_generate is not None:
            await self.on_generate(call)
        return self.responses.pop(0)


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        **kwargs: Any,
    ) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "message_log_id": message_log_id,
                "sender_id": sender_id,
                **kwargs,
            }
        )


def make_signal(
    *,
    message_log_id: int = 123,
    instance_id: str = "test-bot",
    is_private: bool = False,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
) -> AgentEntrySignal:
    return AgentEntrySignal(
        session_id="test-bot:group:group:1",
        message_log_id=message_log_id,
        event_type="message-created",
        sender_id="user-1",
        instance_id=instance_id,
        platform="mock",
        self_id="bot-1",
        is_private=is_private,
        is_mentioned=is_mentioned,
        is_reply_to_bot=is_reply_to_bot,
    )


def make_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"call-{name}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


def make_generate_result(
    *,
    text: str = "",
    tool_calls: list[dict[str, Any]] | None = None,
) -> GenerateResult:
    return GenerateResult(
        text=text,
        tool_calls=list(tool_calls or []),
        raw_response={},
        execution_id="exec-active-chat",
        route_id="",
        provider_id="",
        model_id="",
        usage={},
    )


def test_agent_runtime_wires_review_runner_config(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        review_runtime_config=ReviewRuntimeConfig(
            review_scan=ReviewStageRuntimeConfig(
                enabled=True,
                route_id="route-a",
                model_id="model-a",
            ),
        ),
    )

    dispatcher = runtime.agent_scheduler._workflow_dispatcher
    workflow = dispatcher._review_coordinator

    assert isinstance(workflow._scan_runner, LLMReviewScanStageRunner)
    assert workflow._scan_runner._config.route_id == "route-a"
    assert workflow._scan_runner._config.model_id == "model-a"


def test_agent_runtime_accepts_review_runner_config_mapping(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        review_runtime_config={
            "review_scan": {
                "enabled": True,
                "route_id": "route-a",
            },
        },
    )

    dispatcher = runtime.agent_scheduler._workflow_dispatcher
    workflow = dispatcher._review_coordinator

    assert isinstance(workflow._scan_runner, LLMReviewScanStageRunner)
    assert workflow._scan_runner._config.route_id == "route-a"


@pytest.mark.asyncio
async def test_agent_runtime_resolves_response_profile_from_agent_boundary(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )
    bot.database.bot_configs.upsert(
        BotConfigRecord(
            uuid="cfg-group-profile",
            instance_id="test-bot",
            config={
                "response_profile_group": "passive",
                "response_profile_priority": "balanced",
                "response_profile_private": "disabled",
            },
        )
    )

    await runtime.handle_agent_entry(make_signal())
    await runtime.handle_agent_entry(make_signal(is_mentioned=True))
    await runtime.handle_agent_entry(make_signal(is_private=True))

    assert [call["response_profile"] for call in dispatcher.calls] == [
        "balanced",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_skips_unusable_agent_entry_signals(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )

    await runtime.handle_agent_entry(make_signal(is_reply_to_bot=True))
    await runtime.handle_agent_entry(make_signal(is_mentioned=True, is_private=False))
    await runtime.handle_agent_entry(
        AgentEntrySignal(
            session_id="test-bot:group:group:1",
            message_log_id=None,
            event_type="message-created",
            sender_id="user-1",
            instance_id="test-bot",
            platform="mock",
            self_id="bot-1",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    assert [call["response_profile"] for call in dispatcher.calls] == [
        "immediate",
        "immediate",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_records_ordinary_messages_without_active_reply(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )

    await runtime.handle_agent_entry(make_signal())

    assert dispatcher.calls == []
    assert [
        item.message_log_id
        for item in runtime.agent_scheduler.unread_messages("test-bot:group:group:1")
    ] == [123]


@pytest.mark.asyncio
async def test_agent_runtime_wires_active_chat_fast_runner_end_to_end(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "watching the live chat"},
                    )
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-1",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
            content_json="[]",
            role="user",
            created_at=10_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=3,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 1
        call = model_runtime.calls[0]
        assert call.purpose == "active_chat_fast"
        assert call.metadata["message_log_ids"] == [message_log_id]
        assert {
            tool["function"]["name"]
            for tool in call.tools
        } >= {"send_reply", "no_reply", "send_poke", "request_think_mode", "exit_active"}
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert state.pending_buffer == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_keeps_active_chat_pending_unread_on_exit(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "unused"}),
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-2",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot are you there?",
            content_json="[]",
            role="user",
            created_at=20_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=4,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert [message.message_log_id for message in state.pending_buffer] == [message_log_id]

        decision = runtime.agent_scheduler.adjust_active_chat_interest(
            session_id,
            force_exit=True,
            reason="test_exit_before_batch",
        )

        assert decision.returned_to_idle is True
        unread_message_ids = [
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        ]
        assert unread_message_ids == [message_log_id]
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        assert model_runtime.calls == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_repair_merges_active_chat_pending_messages(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(text="I would answer without a tool."),
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "merged live batch"},
                    )
                ]
            ),
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    first_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-3",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot first",
            content_json="[]",
            role="user",
            created_at=30_000.0,
            is_mentioned=True,
        )
    )
    second_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-4",
            sender_id="user-2",
            sender_name="User 2",
            raw_text="@bot second",
            content_json="[]",
            role="user",
            created_at=31_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=5,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    async def inject_pending_message(_call: Any) -> None:
        if len(model_runtime.calls) != 1:
            return
        await runtime.handle_agent_entry(
            make_signal(message_log_id=second_message_log_id, is_mentioned=True)
        )

    model_runtime.on_generate = inject_pending_message

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=first_message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 2
        assert model_runtime.calls[0].metadata["message_log_ids"] == [first_message_log_id]
        assert model_runtime.calls[0].metadata["repair_attempt"] == 0
        assert model_runtime.calls[1].metadata["message_log_ids"] == [
            first_message_log_id,
            second_message_log_id,
        ]
        assert model_runtime.calls[1].metadata["repair_attempt"] == 1
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert state.pending_buffer == []
        assert state.conversation_messages[0]["tool_calls"][0]["function"]["name"] == (
            "no_reply"
        )
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_exit_active_returns_idle_with_review_plan(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "exit_active",
                        {"reason": "conversation has clearly ended"},
                    )
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-5",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot bye",
            content_json="[]",
            role="user",
            created_at=40_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=6,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 1
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert runtime.agent_scheduler.active_chat_state_for(session_id) is None
        assert runtime.agent_scheduler.review_plan_for(session_id) is not None
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        assert runtime.agent_scheduler.unread_messages(session_id) == []
    finally:
        await runtime.shutdown()
