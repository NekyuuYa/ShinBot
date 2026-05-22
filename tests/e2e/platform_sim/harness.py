"""Scenario-driven platform simulation helpers for backend E2E tests."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.message_context import MessageContext
from shinbot.core.message_routes.command import CommandDef
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, User


@dataclass(slots=True)
class SentMessage:
    session_id: str
    elements: list[MessageElement]
    message_id: str

    @property
    def text(self) -> str:
        return Message(elements=self.elements).get_text()


class SimulatedPlatformAdapter(BaseAdapter):
    """In-process adapter that records outbound traffic and can emit events."""

    def __init__(
        self,
        instance_id: str,
        platform: str = "sim",
        *,
        self_id: str = "bot-self",
    ) -> None:
        super().__init__(instance_id=instance_id, platform=platform)
        self.self_id = self_id
        self.started = False
        self.stopped = False
        self.sent: list[SentMessage] = []
        self.api_calls: list[tuple[str, dict[str, Any]]] = []

    async def start(self) -> None:
        self.started = True

    async def shutdown(self) -> None:
        self.stopped = True

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        message_id = f"sim-out-{len(self.sent) + 1}"
        sent = SentMessage(
            session_id=target_session,
            elements=list(elements),
            message_id=message_id,
        )
        self.sent.append(sent)
        return MessageHandle(
            message_id=message_id,
            adapter_ref=self,
            platform_data={"session_id": target_session},
        )

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        self.api_calls.append((method, dict(params)))
        return {"ok": True, "method": method, "params": params}

    async def get_capabilities(self) -> dict[str, Any]:
        return {
            "elements": ["text", "at", "img", "quote"],
            "actions": ["message.create", "message.delete", "message.update"],
            "limits": {},
        }

    async def emit_message(self, step: dict[str, Any]) -> None:
        if self._event_callback is None:
            raise RuntimeError("Simulated adapter has no event callback")
        result = self._event_callback(build_message_event(step, adapter=self))
        if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
            await result


def load_scenario(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


async def run_platform_scenario(
    scenario: dict[str, Any],
    *,
    data_dir: Path,
) -> tuple[ShinBot, SimulatedPlatformAdapter]:
    bot = ShinBot(data_dir=data_dir)
    bot.adapter_manager.register_adapter("sim", SimulatedPlatformAdapter)
    adapter_config = scenario.get("adapter", {})
    adapter = bot.add_adapter(
        adapter_config.get("instanceId", "sim-main"),
        adapter_config.get("platform", "sim"),
        self_id=adapter_config.get("selfId", "bot-self"),
    )
    if not isinstance(adapter, SimulatedPlatformAdapter):
        raise TypeError(f"expected SimulatedPlatformAdapter, got {type(adapter)!r}")

    register_commands(bot, scenario.get("commands", []))
    await bot.start()
    try:
        for step in scenario.get("steps", []):
            if step.get("type") != "message":
                raise ValueError(f"unsupported scenario step type: {step.get('type')!r}")
            await adapter.emit_message(step)
            await drain_route_tasks(adapter, scenario.get("expect", {}))
        assert_scenario_expectations(bot, adapter, scenario.get("expect", {}))
    finally:
        await bot.shutdown()
    return bot, adapter


def register_commands(bot: ShinBot, commands: list[dict[str, Any]]) -> None:
    for command in commands:
        name = str(command["name"])
        reply_template = str(command.get("reply", ""))
        bot.command_registry.register(
            CommandDef(
                name=name,
                handler=_make_reply_handler(reply_template),
                owner="e2e.platform_sim",
            )
        )


def _make_reply_handler(reply_template: str) -> Callable[[MessageContext, str], Awaitable[None]]:
    async def handler(ctx: MessageContext, args: str) -> None:
        await ctx.send(reply_template.format(args=args, text=ctx.text, session_id=ctx.session_id))

    return handler


def build_message_event(
    step: dict[str, Any],
    *,
    adapter: SimulatedPlatformAdapter,
) -> UnifiedEvent:
    session = step.get("session", {})
    sender = step.get("sender", {})
    session_type = str(session.get("type", "group"))
    channel_type = 1 if session_type == "private" else 0
    channel_id = str(
        session.get("channelId")
        or (sender.get("id") if channel_type == 1 else "")
        or "channel-1"
    )
    guild_id = session.get("guildId")
    guild = Guild(id=str(guild_id), name=session.get("guildName")) if guild_id else None
    channel = Channel(
        id=channel_id,
        type=channel_type,
        name=session.get("channelName"),
    )
    user = User(
        id=str(sender.get("id", "user-1")),
        name=sender.get("name"),
        nick=sender.get("nick"),
    )
    return UnifiedEvent(
        type="message-created",
        self_id=adapter.self_id,
        platform=adapter.platform,
        timestamp=int(step.get("timestamp", time.time())),
        user=user,
        channel=channel,
        guild=guild,
        message=MessagePayload(
            id=str(step.get("id", "msg-1")),
            content=str(step.get("content", "")),
        ),
    )


async def drain_route_tasks(
    adapter: SimulatedPlatformAdapter,
    expect: dict[str, Any],
    *,
    timeout: float = 1.0,
) -> None:
    expected_sent_count = len(expect.get("sent", []))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await asyncio.sleep(0)
        if len(adapter.sent) >= expected_sent_count:
            await asyncio.sleep(0)
            return
    await asyncio.sleep(0)


def assert_scenario_expectations(
    bot: ShinBot,
    adapter: SimulatedPlatformAdapter,
    expect: dict[str, Any],
) -> None:
    assert_sent_messages(adapter, expect.get("sent", []))
    assert_sessions(bot, expect.get("sessions", []))
    if "messageLogs" in expect:
        assert_message_logs(bot, expect["messageLogs"])


def assert_sent_messages(
    adapter: SimulatedPlatformAdapter,
    expected: list[dict[str, Any]],
) -> None:
    assert len(adapter.sent) >= len(expected)
    for index, item in enumerate(expected):
        sent = adapter.sent[index]
        assert sent.session_id == item["sessionId"]
        if "textContains" in item:
            assert item["textContains"] in sent.text


def assert_sessions(bot: ShinBot, expected: list[dict[str, Any]]) -> None:
    for item in expected:
        session = bot.session_manager.get(item["id"])
        assert session is not None
        if "type" in item:
            assert session.session_type == item["type"]
        if "displayName" in item:
            assert session.display_name == item["displayName"]


def assert_message_logs(bot: ShinBot, expected: dict[str, Any]) -> None:
    assert bot.database is not None
    rows = bot.database.message_logs.get_recent(
        expected["sessionId"],
        limit=int(expected.get("limit", 50)),
    )
    assert len(rows) >= int(expected.get("countAtLeast", 0))
    roles = expected.get("roles")
    if roles is not None:
        assert [row["role"] for row in rows[: len(roles)]] == roles
    routing_status = expected.get("incomingRoutingStatus")
    if routing_status is not None:
        incoming = next(row for row in rows if row["role"] == "user")
        assert incoming["routing_status"] == routing_status
