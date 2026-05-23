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

from shinbot.agent.scheduler import AgentScheduler
from shinbot.agent.services.model_runtime import ModelRuntimeCall
from shinbot.agent.signals import (
    AgentMessageSignal,
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
    AgentTimerSignal,
)
from shinbot.core.application.app import ShinBot
from shinbot.core.application.bots_config import load_bot_service_configs
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.core.dispatch.message_context import MessageContext
from shinbot.core.message_routes.command import CommandDef
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.runtime.model import install_model_runtime
from shinbot.core.state.session import Session
from shinbot.persistence import ModelDefinitionRecord, ModelProviderRecord
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, Member, User
from tests.e2e.platform_sim.fixture_schema import validate_scenario


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
        self.agent_entry_signals: list[AgentEntrySignal] = []
        self.agent_scheduler: AgentScheduler | None = None
        self.agent_signal_handler: Callable[[AgentSignal], Awaitable[None] | None] | None = None

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
    scenario = json.loads(path.read_text(encoding="utf-8"))
    validate_scenario(scenario, source=path)
    return scenario


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
    register_agent_entry_probe(bot, scenario.get("agentEntryProbe"), adapter)
    register_agent_scheduler_probe(bot, scenario.get("agentSchedulerProbe"), adapter)
    register_event_bus_handlers(bot, scenario.get("eventBusHandlers", []), adapter)
    register_commands(bot, scenario.get("commands", []), runtime_bot=bot)
    register_model_runtime_setup(bot, scenario.get("modelRuntime", {}))
    configure_sessions(bot, scenario.get("sessions", []))
    if scenario.get("debugPlugin") or model_runtime.get("debugPlugin"):
        await load_debug_model_plugin(bot)
        debug_model_plugin_loaded = True
    fake_completion = model_runtime.get("fakeCompletion") or scenario.get("fakeCompletion")
    await bot.start()
    try:
        with patch("shinbot.agent.services.model_runtime.litellm_adapter.completion", side_effect=_build_fake_model_completion(fake_completion)):
            for step in scenario.get("steps", []):
                await adapter.emit_step(step)
                step_expect = step.get("expect")
                await drain_route_tasks(
                    adapter,
                    step_expect if step_expect is not None else scenario.get("expect", {}),
                    expected_sent_count=step.get("expectSentCount"),
                )
                if step_expect is not None:
                    await assert_scenario_expectations(bot, adapter, step_expect)
            for action in scenario.get("actions", []):
                await run_scenario_action(action, adapter)
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


def configure_sessions(bot: ShinBot, sessions: list[dict[str, Any]]) -> None:
    for item in sessions:
        session = bot.session_manager.get(str(item["id"]))
        if session is None:
            session = Session(
                id=str(item["id"]),
                instance_id=str(item.get("instanceId", "sim-main")),
                session_type=str(item["type"]),
                platform=str(item.get("platform", "sim")),
                guild_id=str(item["guildId"]) if item.get("guildId") else None,
                channel_id=str(item.get("channelId", "")),
                display_name=str(item.get("displayName", "")),
            )
        config = item.get("config", {})
        if "isMuted" in config:
            session.config.is_muted = bool(config["isMuted"])
        if "prefixes" in config:
            session.config.prefixes = [str(prefix) for prefix in config["prefixes"]]
        bot.session_manager.update(session)


def register_agent_entry_probe(
    bot: ShinBot,
    config: dict[str, Any] | None,
    adapter: SimulatedPlatformAdapter,
) -> None:
    if not config:
        return

    async def record(signal: AgentEntrySignal) -> None:
        adapter.agent_entry_signals.append(signal)

    bot.set_agent_entry_handler(record)


def register_agent_scheduler_probe(
    bot: ShinBot,
    config: dict[str, Any] | None,
    adapter: SimulatedPlatformAdapter,
) -> None:
    if not config:
        return
    now = float(config.get("now", time.time()))
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: str(config.get("responseProfile", "")),
        inbox=bot.database.agent_scheduler,
        state_store=bot.database.agent_scheduler,
        now=lambda: now,
    )
    adapter.agent_scheduler = scheduler

    async def dispatch(signal: AgentSignal) -> None:
        if signal.kind == AgentSignalKind.MESSAGE:
            if signal.message is None:
                return
            await scheduler.accept_signal(signal)
            return
        if signal.kind == AgentSignalKind.REVIEW_DUE:
            checked_at = (
                signal.timer.due_at
                if signal.timer is not None and signal.timer.due_at is not None
                else signal.occurred_at
            )
            await scheduler.run_due_review(signal.session_id, now=checked_at)
            return
        if signal.kind == AgentSignalKind.ACTIVE_CHAT_TICK:
            checked_at = (
                signal.timer.due_at
                if signal.timer is not None and signal.timer.due_at is not None
                else signal.occurred_at
            )
            next_review_plan = None
            preview = scheduler.preview_active_chat_tick(signal.session_id, now=checked_at)
            if preview.will_return_idle:
                next_review_plan = await scheduler.plan_idle_review_after_active_chat(
                    signal.session_id
                )
            scheduler.tick_active_chat(
                signal.session_id,
                next_review_plan=next_review_plan,
                now=checked_at,
            )
            return
        if signal.kind == AgentSignalKind.ACTIVE_CHAT_BOOTSTRAP:
            payload = signal.active_chat_bootstrap
            if payload is None:
                return
            scheduler.apply_active_chat_bootstrap(
                signal.session_id,
                disposition=payload.disposition,
                active_epoch=payload.active_epoch,
            )
            return

    adapter.agent_signal_handler = dispatch

    async def handle(signal: AgentEntrySignal) -> None:
        adapter.agent_entry_signals.append(signal)
        await dispatch(agent_signal_from_entry(signal, occurred_at=now))

    bot.set_agent_entry_handler(handle)


def agent_signal_from_entry(signal: AgentEntrySignal, *, occurred_at: float) -> AgentSignal:
    message_token = signal.message_log_id if signal.message_log_id is not None else "missing"
    return AgentSignal(
        signal_id=f"e2e-entry:{signal.session_id}:{message_token}",
        kind=AgentSignalKind.MESSAGE,
        source=AgentSignalSource.MESSAGE_INGRESS,
        session_id=signal.session_id,
        occurred_at=occurred_at,
        bot_id=signal.bot_id,
        bot_binding_id=signal.bot_binding_id,
        bot_session_id=signal.bot_session_id,
        message=AgentMessageSignal(
            message_log_id=signal.message_log_id,
            sender_id=signal.sender_id,
            instance_id=signal.instance_id,
            platform=signal.platform,
            self_id=signal.self_id,
            is_private=signal.is_private,
            is_mentioned=signal.is_mentioned,
            is_reply_to_bot=signal.is_reply_to_bot,
            is_mention_to_other=signal.is_mention_to_other,
            is_poke_to_bot=signal.is_poke_to_bot,
            is_poke_to_other=signal.is_poke_to_other,
            already_handled=signal.already_handled,
            is_stopped=signal.is_stopped,
        ),
    )


async def run_scenario_action(action: dict[str, Any], adapter: SimulatedPlatformAdapter) -> None:
    action_type = str(action.get("type", ""))
    if action_type == "agentReviewDue":
        await _emit_agent_signal(
            adapter,
            AgentSignalKind.REVIEW_DUE,
            session_id=str(action["sessionId"]),
            now=float(action.get("now", time.time())),
        )
        return
    if action_type == "agentCompleteReview":
        scheduler = adapter.agent_scheduler
        if scheduler is None:
            raise RuntimeError("agentCompleteReview action requires agentSchedulerProbe")
        scheduler.complete_review(
            str(action["sessionId"]),
            enter_active_chat=bool(action.get("enterActiveChat", False)),
            active_chat_initial_interest=_optional_float(
                action.get("activeChatInitialInterest")
            ),
            active_chat_decay_half_life_seconds=_optional_float(
                action.get("activeChatDecayHalfLifeSeconds")
            ),
            now=float(action.get("now", time.time())),
        )
        return
    if action_type == "agentActiveChatTick":
        await _emit_agent_signal(
            adapter,
            AgentSignalKind.ACTIVE_CHAT_TICK,
            session_id=str(action["sessionId"]),
            now=float(action.get("now", time.time())),
        )
        return
    raise ValueError(f"unsupported scenario action type: {action_type!r}")


async def _emit_agent_signal(
    adapter: SimulatedPlatformAdapter,
    kind: AgentSignalKind,
    *,
    session_id: str,
    now: float,
) -> None:
    handler = adapter.agent_signal_handler
    if handler is None:
        raise RuntimeError(f"{kind.value} action requires agentSchedulerProbe")
    signal = AgentSignal(
        signal_id=f"e2e-{kind.value}:{session_id}:{int(now)}",
        kind=kind,
        source=AgentSignalSource.TIMER,
        session_id=session_id,
        occurred_at=now,
        timer=AgentTimerSignal(
            trigger=kind.value,
            due_at=now,
            plan_id=f"{session_id}:{int(now)}",
        ),
    )
    result = handler(signal)
    if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
        await result


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


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
        elif kind == "api":
            handler = _make_api_handler(command)
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


def _make_api_handler(command: dict[str, Any]) -> Callable[[MessageContext, str], Awaitable[None]]:
    api_config = dict(command.get("api", {}))
    method_template = str(api_config.get("method", ""))
    params_template = dict(api_config.get("params", {}))
    reply_template = str(command.get("reply", ""))

    async def handler(ctx: MessageContext, args: str) -> None:
        method = method_template.format(
            args=args,
            text=ctx.text,
            session_id=ctx.session_id,
            user_id=ctx.user_id,
        )
        params = _format_payload(
            params_template,
            args=args,
            text=ctx.text,
            session_id=ctx.session_id,
            user_id=ctx.user_id,
        )
        result = await ctx.adapter.call_api(method, params)
        if reply_template:
            await ctx.send(
                reply_template.format(
                    args=args,
                    text=ctx.text,
                    session_id=ctx.session_id,
                    method=method,
                    result=result,
                )
            )

    return handler


def _format_payload(value: Any, **kwargs: str) -> Any:
    if isinstance(value, str):
        return value.format(**kwargs)
    if isinstance(value, list):
        return [_format_payload(item, **kwargs) for item in value]
    if isinstance(value, dict):
        return {key: _format_payload(item, **kwargs) for key, item in value.items()}
    return value


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
    message_content = str(step.get("content", ""))
    if not message_content and step.get("elements"):
        message_content = Message(
            elements=[MessageElement.model_validate(element) for element in step["elements"]]
        ).to_xml()
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
            content=message_content,
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
    if "apiCalls" in expect:
        assert_api_calls(adapter, expect["apiCalls"])
    if "agentEntrySignals" in expect:
        assert_agent_entry_signals(adapter, expect["agentEntrySignals"])
    if "agentScheduler" in expect:
        assert_agent_scheduler_state(bot, adapter, expect["agentScheduler"])
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
        if "messageId" in item:
            assert sent.message_id == item["messageId"]
        if "messageIdStartsWith" in item:
            assert sent.message_id.startswith(str(item["messageIdStartsWith"]))
        if "text" in item:
            assert sent.text == item["text"]
        if "textContains" in item:
            assert item["textContains"] in sent.text
        if "elements" in item:
            expected_elements = [
                MessageElement.model_validate(element) for element in item["elements"]
            ]
            assert sent.elements == expected_elements


def assert_api_calls(
    adapter: SimulatedPlatformAdapter,
    expected: list[dict[str, Any]],
) -> None:
    assert len(adapter.api_calls) >= len(expected)
    for index, item in enumerate(expected):
        method, params = adapter.api_calls[index]
        if "method" in item:
            assert method == item["method"]
        if "params" in item:
            assert params == item["params"]
        if "paramsContains" in item:
            for key, value in item["paramsContains"].items():
                assert params[key] == value


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
    if "countExact" in expected:
        assert len(rows) == int(expected["countExact"])
    else:
        assert len(rows) >= int(expected.get("countAtLeast", 0))
    if "ids" in expected:
        assert [row["id"] for row in rows[: len(expected["ids"])]] == list(expected["ids"])
    roles = expected.get("roles")
    if roles is not None:
        assert [row["role"] for row in rows[: len(roles)]] == roles
    sender_ids = expected.get("senderIds")
    if sender_ids is not None:
        assert [row["sender_id"] for row in rows[: len(sender_ids)]] == list(sender_ids)
    sender_names = expected.get("senderNames")
    if sender_names is not None:
        assert [row["sender_name"] for row in rows[: len(sender_names)]] == list(sender_names)
    platform_msg_ids = expected.get("platformMsgIds")
    if platform_msg_ids is not None:
        assert [row["platform_msg_id"] for row in rows[: len(platform_msg_ids)]] == list(
            platform_msg_ids
        )
    is_read = expected.get("isRead")
    if is_read is not None:
        assert [row["is_read"] for row in rows[: len(is_read)]] == list(is_read)
    is_mentioned = expected.get("isMentioned")
    if is_mentioned is not None:
        assert [row["is_mentioned"] for row in rows[: len(is_mentioned)]] == list(
            is_mentioned
        )
    raw_texts = expected.get("rawTexts")
    if raw_texts is not None:
        assert [row["raw_text"] for row in rows[: len(raw_texts)]] == list(raw_texts)
    raw_text_contains = expected.get("rawTextContains")
    if raw_text_contains is not None:
        for index, needle in enumerate(raw_text_contains):
            assert needle in rows[index]["raw_text"]
    content_elements = expected.get("contentElements")
    if content_elements is not None:
        assert [
            json.loads(row["content_json"]) for row in rows[: len(content_elements)]
        ] == list(content_elements)
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


def assert_agent_entry_signals(
    adapter: SimulatedPlatformAdapter,
    expected: list[dict[str, Any]],
) -> None:
    assert len(adapter.agent_entry_signals) >= len(expected)
    for index, item in enumerate(expected):
        signal = adapter.agent_entry_signals[index]
        if "sessionId" in item:
            assert signal.session_id == item["sessionId"]
        if "botId" in item:
            assert signal.bot_id == item["botId"]
        if "botBindingId" in item:
            assert signal.bot_binding_id == item["botBindingId"]
        if "botSessionId" in item:
            assert signal.bot_session_id == item["botSessionId"]
        if "eventType" in item:
            assert signal.event_type == item["eventType"]
        if "senderId" in item:
            assert signal.sender_id == item["senderId"]
        if "instanceId" in item:
            assert signal.instance_id == item["instanceId"]
        if "platform" in item:
            assert signal.platform == item["platform"]
        if "isPrivate" in item:
            assert signal.is_private == bool(item["isPrivate"])
        if "isMentioned" in item:
            assert signal.is_mentioned == bool(item["isMentioned"])
        if "isMentionToOther" in item:
            assert signal.is_mention_to_other == bool(item["isMentionToOther"])
        if "isReplyToBot" in item:
            assert signal.is_reply_to_bot == bool(item["isReplyToBot"])
        if "isPokeToBot" in item:
            assert signal.is_poke_to_bot == bool(item["isPokeToBot"])
        if "isPokeToOther" in item:
            assert signal.is_poke_to_other == bool(item["isPokeToOther"])
        if "alreadyHandled" in item:
            assert signal.already_handled == bool(item["alreadyHandled"])
        if "isStopped" in item:
            assert signal.is_stopped == bool(item["isStopped"])
        if item.get("messageLogId"):
            assert signal.message_log_id is not None


def assert_agent_scheduler_state(
    bot: ShinBot,
    adapter: SimulatedPlatformAdapter,
    expected: dict[str, Any],
) -> None:
    scheduler = adapter.agent_scheduler
    assert scheduler is not None
    session_id = str(expected["sessionId"])
    if "state" in expected:
        assert scheduler.state_for(session_id).value == expected["state"]
    if "unreadCount" in expected:
        assert scheduler.count_unread_messages(session_id) == int(expected["unreadCount"])
    if "knownSessionIds" in expected:
        assert scheduler.list_session_ids() == list(expected["knownSessionIds"])
    plan = scheduler.review_plan_for(session_id)
    if expected.get("reviewPlan"):
        assert plan is not None
        review_plan = expected["reviewPlan"]
        if "reason" in review_plan:
            assert plan.reason == review_plan["reason"]
        if "nextReviewAt" in review_plan:
            assert plan.next_review_at == float(review_plan["nextReviewAt"])
    if "activeChatState" in expected:
        assert_active_chat_state(scheduler, session_id, expected["activeChatState"])
    rows = bot.database.agent_scheduler.list_unread(session_id)
    if "unreadMessageLogIds" in expected:
        assert [row.message_log_id for row in rows] == list(expected["unreadMessageLogIds"])


def assert_active_chat_state(
    scheduler: AgentScheduler,
    session_id: str,
    expected: dict[str, Any] | None,
) -> None:
    active_chat_state = scheduler.active_chat_state_for(session_id)
    if expected is None or expected.get("exists") is False:
        assert active_chat_state is None
        return

    assert active_chat_state is not None
    if "interestValue" in expected:
        assert active_chat_state.interest_value == float(expected["interestValue"])
    if "decayHalfLifeSeconds" in expected:
        assert active_chat_state.decay_half_life_seconds == float(
            expected["decayHalfLifeSeconds"]
        )
    if "enteredAt" in expected:
        assert active_chat_state.entered_at == float(expected["enteredAt"])
    if "updatedAt" in expected:
        assert active_chat_state.updated_at == float(expected["updatedAt"])
    if "tickCount" in expected:
        assert active_chat_state.tick_count == int(expected["tickCount"])
    if "activeEpoch" in expected:
        assert active_chat_state.active_epoch == int(expected["activeEpoch"])


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
