from __future__ import annotations

import asyncio

import pytest

from shinbot.core.application.bot_routing import BotRuntimeRouter, session_key_for_event
from shinbot.core.application.bots_config import (
    BotAgentConfig,
    BotBindingConfig,
    BotCommandsConfig,
    BotPluginsConfig,
    BotServiceConfig,
)
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    AgentEntryDispatcher,
    AgentEntrySignal,
    make_agent_entry_fallback_route_rule,
)
from shinbot.core.dispatch.ingress import (
    ROUTING_SKIP_NO_ROUTE_MATCHED,
    MessageIngress,
    RouteTargetRegistry,
)
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode, RouteRule, RouteTable
from shinbot.core.message_routes import (
    TEXT_COMMAND_DISPATCHER_TARGET,
    TextCommandDispatcher,
    make_text_command_route_rule,
)
from shinbot.core.message_routes.command import CommandDef, CommandRegistry
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.schema.elements import MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str = "qq-main", platform: str = "mock"):
        super().__init__(instance_id=instance_id, platform=platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method: str, params: dict) -> dict:
        return {"method": method, "params": params}

    async def get_capabilities(self) -> dict:
        return {"elements": ["text"], "actions": [], "limits": {}}


def make_event(
    content: str = "hello",
    *,
    private: bool = True,
    channel_id: str = "room-1",
    user_id: str = "user-1",
) -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        self_id="bot-self",
        platform="mock",
        user=User(id=user_id),
        channel=Channel(id=channel_id, type=1 if private else 0),
        message=MessagePayload(id="msg-1", content=content),
    )


def make_bot(
    *,
    bot_id: str = "bot",
    binding_id: str = "binding",
    adapter_instance_id: str = "qq-main",
    session_patterns: tuple[str, ...] = ("private:*",),
    priority: int = 0,
    commands: BotCommandsConfig | None = None,
    plugins: BotPluginsConfig | None = None,
    agent: BotAgentConfig | None = None,
) -> BotServiceConfig:
    return BotServiceConfig(
        id=bot_id,
        display_name=bot_id,
        commands=commands or BotCommandsConfig(),
        plugins=plugins or BotPluginsConfig(),
        agent=agent or BotAgentConfig(),
        bindings=(
            BotBindingConfig(
                id=binding_id,
                adapter_instance_id=adapter_instance_id,
                session_patterns=session_patterns,
                priority=priority,
            ),
        ),
    )


def make_ingress(
    *,
    route_table: RouteTable,
    route_targets: RouteTargetRegistry,
    router: BotRuntimeRouter,
) -> MessageIngress:
    return MessageIngress(
        session_manager=SessionManager(),
        permission_engine=PermissionEngine(),
        route_table=route_table,
        route_targets=route_targets,
        bot_router=router,
    )


def test_bot_runtime_router_selects_highest_priority_binding() -> None:
    low = make_bot(bot_id="low", session_patterns=("group:room-1",), priority=10)
    high = make_bot(bot_id="high", session_patterns=("group:room-1",), priority=20)
    router = BotRuntimeRouter((low, high))

    event = make_event(private=False, channel_id="room-1")
    selection = router.resolve(adapter_instance_id="qq-main", event=event)

    assert session_key_for_event(event) == "group:room-1"
    assert selection is not None
    assert selection.bot.id == "high"
    assert selection.binding.priority == 20


@pytest.mark.asyncio
async def test_ingress_skips_unbound_bot_session() -> None:
    table = RouteTable()
    rule = RouteRule(
        id="message.recorder",
        priority=10,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target="recorder",
    )
    table.register(rule)
    targets = RouteTargetRegistry()
    calls: list[str] = []
    targets.register("recorder", lambda context, _rule: calls.append(context.message.text))
    router = BotRuntimeRouter((make_bot(session_patterns=("group:room-1",)),))
    ingress = make_ingress(route_table=table, route_targets=targets, router=router)

    result = await ingress.process_event(make_event(private=True), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == []
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    assert calls == []


@pytest.mark.asyncio
async def test_text_commands_use_selected_bot_prefixes() -> None:
    registry = CommandRegistry()
    calls: list[str] = []

    async def ping_handler(_ctx, args):
        calls.append(args)

    registry.register(CommandDef(name="ping", handler=ping_handler))
    dispatcher = TextCommandDispatcher(registry)
    command_rule = make_text_command_route_rule(dispatcher)
    table = RouteTable()
    table.register(command_rule)
    targets = RouteTargetRegistry()
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, dispatcher)
    router = BotRuntimeRouter((make_bot(commands=BotCommandsConfig(prefixes=("!",))),))
    ingress = make_ingress(route_table=table, route_targets=targets, router=router)
    adapter = MockAdapter()

    unmatched = await ingress.process_event(make_event("/ping nope"), adapter)
    matched = await ingress.process_event(make_event("!ping ok"), adapter)
    await asyncio.sleep(0)

    assert unmatched.matched_rules == []
    assert unmatched.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    assert matched.matched_rules == [command_rule]
    assert calls == ["ok"]


@pytest.mark.asyncio
async def test_plugin_owned_commands_follow_bot_plugin_policy() -> None:
    registry = CommandRegistry()
    calls: list[str] = []

    async def plugin_handler(_ctx, _args):
        calls.append("called")

    registry.register(CommandDef(name="plug", handler=plugin_handler, owner="blocked-plugin"))
    dispatcher = TextCommandDispatcher(registry)
    table = RouteTable()
    table.register(make_text_command_route_rule(dispatcher))
    targets = RouteTargetRegistry()
    targets.register(TEXT_COMMAND_DISPATCHER_TARGET, dispatcher)
    router = BotRuntimeRouter(
        (
            make_bot(
                plugins=BotPluginsConfig(
                    enabled=True,
                    enabled_plugins=("other-plugin",),
                )
            ),
        )
    )
    ingress = make_ingress(route_table=table, route_targets=targets, router=router)

    result = await ingress.process_event(make_event("/plug"), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == []
    assert result.skipped_reason == ROUTING_SKIP_NO_ROUTE_MATCHED
    assert calls == []


@pytest.mark.asyncio
async def test_plugin_route_filter_runs_before_exclusive_route_suppresses_fallback() -> None:
    table = RouteTable()
    plugin_rule = RouteRule(
        id="plugin.blocked",
        priority=100,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target="plugin.blocked",
        match_mode=RouteMatchMode.EXCLUSIVE,
        owner="blocked-plugin",
    )
    fallback_rule = make_agent_entry_fallback_route_rule()
    table.register(plugin_rule)
    table.register(fallback_rule)
    targets = RouteTargetRegistry()
    calls: list[str] = []
    targets.register("plugin.blocked", lambda _context, _rule: calls.append("plugin"))
    targets.register(AGENT_ENTRY_TARGET, lambda _context, _rule: calls.append("agent"))
    router = BotRuntimeRouter(
        (
            make_bot(
                plugins=BotPluginsConfig(enabled=True, enabled_plugins=("other-plugin",)),
                agent=BotAgentConfig(mode="full", config="agents/full-agent.toml"),
            ),
        )
    )
    ingress = make_ingress(route_table=table, route_targets=targets, router=router)

    result = await ingress.process_event(make_event("hello"), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == [fallback_rule]
    assert calls == ["agent"]


@pytest.mark.asyncio
async def test_agent_entry_signal_includes_selected_bot_identity() -> None:
    signals: list[AgentEntrySignal] = []
    dispatcher = AgentEntryDispatcher(handler=lambda signal: signals.append(signal))
    fallback_rule = make_agent_entry_fallback_route_rule()
    table = RouteTable()
    table.register(fallback_rule)
    targets = RouteTargetRegistry()
    targets.register(AGENT_ENTRY_TARGET, dispatcher)
    router = BotRuntimeRouter(
        (
            make_bot(
                bot_id="full-agent",
                binding_id="full-agent-private",
                agent=BotAgentConfig(mode="full", config="agents/full-agent.toml"),
            ),
        )
    )
    ingress = make_ingress(route_table=table, route_targets=targets, router=router)

    result = await ingress.process_event(make_event("hello"), MockAdapter())
    await asyncio.sleep(0)

    assert result.matched_rules == [fallback_rule]
    assert signals[0].bot_id == "full-agent"
    assert signals[0].bot_binding_id == "full-agent-private"
