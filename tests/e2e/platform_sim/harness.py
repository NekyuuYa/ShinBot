"""Scenario-driven platform simulation helpers for backend E2E tests."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

from shinbot.agent.services.model_runtime import ModelRuntimeCall
from shinbot.core.application.app import ShinBot
from shinbot.core.application.bots_config import load_bot_service_configs
from shinbot.core.dispatch.message_context import MessageContext
from shinbot.core.message_routes.command import CommandDef
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.runtime.model import install_model_runtime
from shinbot.persistence import ModelDefinitionRecord, ModelProviderRecord
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, Member, User


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
        self.notice_events: list[str] = []

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
        await self.emit_step(step)

    async def emit_step(self, step: dict[str, Any]) -> None:
        if self._event_callback is None:
            raise RuntimeError("Simulated adapter has no event callback")
        kind = str(step.get("type", "message"))
        if kind == "message":
            event = build_message_event(step, adapter=self)
        elif kind == "notice":
            event = build_notice_event(step, adapter=self)
        else:
            raise ValueError(f"unsupported scenario step type: {kind!r}")
        result = self._event_callback(event)
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

    model_runtime = scenario.get("modelRuntime", {})
    debug_model_plugin_loaded = False
    if model_runtime or any(str(command.get("kind", "")) == "model" for command in scenario.get("commands", [])):
        install_model_runtime(bot)

    configure_bot_services(bot, scenario.get("config"), data_dir=data_dir)
    register_event_bus_handlers(bot, scenario.get("eventBusHandlers", []), adapter)
    register_commands(bot, scenario.get("commands", []), runtime_bot=bot)
    register_model_runtime_setup(bot, scenario.get("modelRuntime", {}))
    if scenario.get("debugPlugin") or model_runtime.get("debugPlugin"):
        await load_debug_model_plugin(bot)
        debug_model_plugin_loaded = True
    fake_completion = model_runtime.get("fakeCompletion") or scenario.get("fakeCompletion")
    await bot.start()
    try:
        with patch("shinbot.agent.services.model_runtime.litellm_adapter.completion", side_effect=_build_fake_model_completion(fake_completion)):
            for step in scenario.get("steps", []):
                await adapter.emit_step(step)
                await drain_route_tasks(
                    adapter,
                    scenario.get("expect", {}),
                    expected_sent_count=step.get("expectSentCount"),
                )
        await assert_scenario_expectations(bot, adapter, scenario.get("expect", {}))
    finally:
        if debug_model_plugin_loaded:
            await bot.plugin_manager.unload_plugin_async("shinbot_debug_model")
        await bot.shutdown()
    return bot, adapter


def configure_bot_services(bot: ShinBot, config: dict[str, Any] | None, *, data_dir: Path) -> None:
    if config is None:
        return
    bot.configure_bot_service_configs(load_bot_service_configs(config, data_dir=data_dir))


def register_commands(
    bot: ShinBot,
    commands: list[dict[str, Any]],
    *,
    runtime_bot: ShinBot,
) -> None:
    for command in commands:
        name = str(command["name"])
        kind = str(command.get("kind", "reply"))
        if kind == "prompt":
            handler = _make_prompt_handler(command)
        elif kind == "model":
            handler = _make_model_handler(runtime_bot, command)
        else:
            handler = _make_reply_handler(str(command.get("reply", "")))
        bot.command_registry.register(
            CommandDef(
                name=name,
                handler=handler,
                owner="e2e.platform_sim",
            )
        )


def register_event_bus_handlers(
    bot: ShinBot,
    handlers: list[dict[str, Any]],
    adapter: SimulatedPlatformAdapter,
) -> None:
    for handler in handlers:
        event_type = str(handler["eventType"])

        async def record(event: UnifiedEvent, *, _event_type: str = event_type) -> None:
            adapter.notice_events.append(getattr(event, "type", _event_type))

        bot.event_bus.on(event_type, record, owner="e2e.platform_sim")


def register_model_runtime_setup(bot: ShinBot, model_runtime: dict[str, Any]) -> None:
    if not model_runtime:
        return
    for provider in model_runtime.get("providers", []):
        bot.database.model_registry.upsert_provider(
            ModelProviderRecord(
                id=str(provider["id"]),
                type=str(provider.get("type", "")),
                display_name=str(provider.get("displayName", provider["id"])),
                enabled=bool(provider.get("enabled", True)),
                auth=dict(provider.get("auth", {})),
                default_params=dict(provider.get("defaultParams", {})),
            )
        )
    for model in model_runtime.get("models", []):
        bot.database.model_registry.upsert_model(
            ModelDefinitionRecord(
                id=str(model["id"]),
                provider_id=str(model["providerId"]),
                litellm_model=str(model.get("litellmModel", model["id"])),
                display_name=str(model.get("displayName", model["id"])),
                enabled=bool(model.get("enabled", True)),
                capabilities=list(model.get("capabilities", [])),
                default_params=dict(model.get("defaultParams", {})),
                cost_metadata=dict(model.get("costMetadata", {})),
            )
        )


def _build_fake_model_completion(fake_completion: dict[str, Any] | None):
    payload = fake_completion or {}
    response_text = str(payload.get("text", "stubbed model response"))
    input_tokens = int(payload.get("inputTokens", 4))
    output_tokens = int(payload.get("outputTokens", 2))
    cached_tokens = int(payload.get("cacheReadTokens", 1))
    cache_write_tokens = int(payload.get("cacheWriteTokens", 0))

    def fake_completion(**_kwargs: Any) -> dict[str, Any]:
        return {
            "choices": [{"message": {"content": response_text}}],
            "usage": {
                "prompt_tokens": input_tokens,
                "completion_tokens": output_tokens,
                "prompt_tokens_details": {"cached_tokens": cached_tokens},
                "cache_creation_input_tokens": cache_write_tokens,
            },
        }

    return fake_completion


async def load_debug_model_plugin(bot: ShinBot) -> None:
    metadata_path = (
        Path(__file__).resolve().parents[3]
        / "shinbot"
        / "builtin_plugins"
        / "shinbot_debug_model"
        / "metadata.json"
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    await bot.plugin_manager.load_plugin_async(
        "shinbot_debug_model",
        "shinbot.builtin_plugins.shinbot_debug_model",
        declared_metadata=metadata,
    )


def _make_reply_handler(reply_template: str) -> Callable[[MessageContext, str], Awaitable[None]]:
    async def handler(ctx: MessageContext, args: str) -> None:
        await ctx.send(reply_template.format(args=args, text=ctx.text, session_id=ctx.session_id))

    return handler


def _make_prompt_handler(command: dict[str, Any]) -> Callable[[MessageContext, str], Awaitable[None]]:
    prompt = str(command.get("prompt", ""))
    reply_template = str(command.get("replyAfterInput", "{input}"))
    timeout = float(command.get("timeout", 1.0))

    async def handler(ctx: MessageContext, args: str) -> None:
        user_input = await ctx.wait_for_input(prompt=prompt, timeout=timeout)
        await ctx.send(
            reply_template.format(
                args=args,
                input=user_input,
                text=ctx.text,
                session_id=ctx.session_id,
            )
        )

    return handler


def _make_model_handler(bot: ShinBot, command: dict[str, Any]) -> Callable[[MessageContext, str], Awaitable[None]]:
    call_config = dict(command.get("call", {}))
    reply_template = str(command.get("reply", "model: {text}"))

    async def handler(ctx: MessageContext, args: str) -> None:
        if bot.model_runtime is None:
            raise RuntimeError("Model runtime is not installed")
        prompt_text = str(call_config.get("prompt", ""))
        if not prompt_text:
            prompt_text = f"{ctx.text} {args}".strip()
        result = await bot.model_runtime.generate(
            ModelRuntimeCall(
                caller=str(call_config.get("caller", "e2e.platform_sim")),
                route_id=str(call_config.get("routeId", "")) or None,
                model_id=str(call_config.get("modelId", "")) or None,
                session_id=ctx.session_id,
                instance_id=ctx.adapter.instance_id,
                purpose=str(call_config.get("purpose", "audit")),
                messages=[
                    {
                        "role": "user",
                        "content": prompt_text.format(
                            args=args,
                            text=ctx.text,
                            session_id=ctx.session_id,
                        ),
                    }
                ],
                params=dict(call_config.get("params", {})),
                metadata=dict(call_config.get("metadata", {})),
            )
        )
        if reply_template:
            await ctx.send(
                reply_template.format(
                    args=args,
                    text=result.text,
                    session_id=ctx.session_id,
                    model_id=result.model_id,
                )
            )

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


def build_notice_event(
    step: dict[str, Any],
    *,
    adapter: SimulatedPlatformAdapter,
) -> UnifiedEvent:
    session = step.get("session", {})
    sender = step.get("sender", {})
    operator = step.get("operator", {})
    member = step.get("member", {})
    member_user = member.get("user", sender)
    channel_type = 1 if str(session.get("type", "group")) == "private" else 0
    channel_id = str(session.get("channelId") or "channel-1")
    guild_id = session.get("guildId")
    guild = Guild(id=str(guild_id), name=session.get("guildName")) if guild_id else None
    channel = Channel(
        id=channel_id,
        type=channel_type,
        name=session.get("channelName"),
    )
    return UnifiedEvent(
        id=int(step.get("id", 0)) if str(step.get("id", "")).isdigit() else None,
        type=str(step.get("eventType", step.get("noticeType", "notice"))),
        self_id=adapter.self_id,
        platform=adapter.platform,
        timestamp=int(step.get("timestamp", time.time())),
        user=User(
            id=str(sender.get("id", member_user.get("id", "user-1"))),
            name=sender.get("name"),
            nick=sender.get("nick"),
        ),
        operator=User(
            id=str(operator.get("id", "")),
            name=operator.get("name"),
            nick=operator.get("nick"),
        )
        if operator
        else None,
        member=Member(
            user=User(
                id=str(member_user.get("id", "user-1")),
                name=member_user.get("name"),
                nick=member_user.get("nick"),
            ),
            nick=member.get("nick"),
        )
        if member
        else None,
        channel=channel,
        guild=guild,
    )


async def drain_route_tasks(
    adapter: SimulatedPlatformAdapter,
    expect: dict[str, Any],
    *,
    expected_sent_count: int | None = None,
    timeout: float = 1.0,
) -> None:
    if expected_sent_count is None:
        expected_sent_count = len(expect.get("sent", []))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await asyncio.sleep(0)
        if len(adapter.sent) >= expected_sent_count:
            await asyncio.sleep(0)
            return
    await asyncio.sleep(0)


async def assert_scenario_expectations(
    bot: ShinBot,
    adapter: SimulatedPlatformAdapter,
    expect: dict[str, Any],
) -> None:
    assert_sent_messages(adapter, expect.get("sent", []))
    assert_sessions(bot, expect.get("sessions", []))
    if "messageLogs" in expect:
        assert_message_logs(bot, expect["messageLogs"])
    for expected_logs in expect.get("messageLogsBySession", []):
        assert_message_logs(bot, expected_logs)
    if "noticeEvents" in expect:
        assert adapter.notice_events == list(expect["noticeEvents"])
    if "modelRuntime" in expect:
        await assert_model_runtime_expectations(bot, expect["modelRuntime"])


def assert_sent_messages(
    adapter: SimulatedPlatformAdapter,
    expected: list[dict[str, Any]],
) -> None:
    assert len(adapter.sent) >= len(expected)
    for index, item in enumerate(expected):
        sent = adapter.sent[index]
        assert sent.session_id == item["sessionId"]
        if "text" in item:
            assert sent.text == item["text"]
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
    routing_statuses = expected.get("routingStatuses")
    if routing_statuses is not None:
        assert [row["routing_status"] for row in rows[: len(routing_statuses)]] == routing_statuses
    routing_skip_reasons = expected.get("routingSkipReasons")
    if routing_skip_reasons is not None:
        assert [
            row["routing_skip_reason"] for row in rows[: len(routing_skip_reasons)]
        ] == routing_skip_reasons
    routing_status = expected.get("incomingRoutingStatus")
    if routing_status is not None:
        incoming = next(row for row in rows if row["role"] == "user")
        assert incoming["routing_status"] == routing_status


async def assert_model_runtime_expectations(bot: ShinBot, expected: dict[str, Any]) -> None:
    assert bot.database is not None
    records = bot.database.model_executions.list_recent(limit=expected.get("limit", 20))
    assert len(records) >= int(expected.get("countAtLeast", 0))
    record = records[0]
    if "providerId" in expected:
        assert record["provider_id"] == expected["providerId"]
    if "modelId" in expected:
        assert record["model_id"] == expected["modelId"]
    if "caller" in expected:
        assert record["caller"] == expected["caller"]
    if "success" in expected:
        assert bool(record["success"]) is bool(expected["success"])
    if "promptSnapshotId" in expected:
        assert record["prompt_snapshot_id"] == expected["promptSnapshotId"]
    if "debugModelLog" in expected:
        await assert_debug_model_log(bot, expected["debugModelLog"])


async def assert_debug_model_log(bot: ShinBot, expected: dict[str, Any]) -> None:
    path = bot.data_dir / "plugin_data" / "shinbot_debug_model" / "model_requests.jsonl"
    deadline = time.monotonic() + float(expected.get("timeout", 2.0))
    lines: list[str] = []
    while time.monotonic() < deadline:
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            if content:
                lines = content.splitlines()
                if len(lines) >= int(expected.get("lineCountAtLeast", 0)):
                    break
        await asyncio.sleep(0.01)
    assert len(lines) >= int(expected.get("lineCountAtLeast", 0))
    request = json.loads(lines[0])
    response = json.loads(lines[1])
    if "requestEventType" in expected:
        assert request["event_type"] == expected["requestEventType"]
    if "responseEventType" in expected:
        assert response["event_type"] == expected["responseEventType"]
    if "requestContains" in expected:
        for key, value in expected["requestContains"].items():
            if key in request:
                assert request[key] == value
            else:
                assert request["request"][key] == value
    if "responseContains" in expected:
        for key, value in expected["responseContains"].items():
            if key in response:
                assert response[key] == value
            else:
                assert response["response"][key] == value
