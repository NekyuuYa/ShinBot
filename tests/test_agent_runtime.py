from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from shinbot.agent.runtime import install_agent_runtime
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.persistence.records import BotConfigRecord


class RecordingScheduler:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def on_message(
        self,
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
    instance_id: str = "test-bot",
    is_private: bool = False,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
) -> AgentEntrySignal:
    return AgentEntrySignal(
        session_id="test-bot:group:group:1",
        message_log_id=123,
        event_type="message-created",
        sender_id="user-1",
        instance_id=instance_id,
        platform="mock",
        self_id="bot-1",
        is_private=is_private,
        is_mentioned=is_mentioned,
        is_reply_to_bot=is_reply_to_bot,
    )


@pytest.mark.asyncio
async def test_agent_runtime_resolves_response_profile_from_agent_boundary(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    scheduler = RecordingScheduler()
    runtime.attention_scheduler = scheduler
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

    assert [call["response_profile"] for call in scheduler.calls] == [
        "passive",
        "balanced",
        "disabled",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_skips_unusable_agent_entry_signals(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    scheduler = RecordingScheduler()
    runtime.attention_scheduler = scheduler

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

    assert [call["response_profile"] for call in scheduler.calls] == [
        "immediate",
        "immediate",
    ]
