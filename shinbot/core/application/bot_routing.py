"""Runtime routing helpers for bot service-unit configs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.core.application.bots_config import (
    PLUGIN_WILDCARD,
    WILDCARD,
    BotBindingConfig,
    BotServiceConfig,
)

if TYPE_CHECKING:
    from shinbot.core.dispatch.routing import RouteRule
    from shinbot.schema.events import UnifiedEvent

AGENT_ENTRY_TARGET_NAME = "agent_entry"


@dataclass(slots=True, frozen=True)
class BotRuntimeSelection:
    """The bot/binding selected for one incoming event."""

    bot: BotServiceConfig
    binding: BotBindingConfig
    bot_index: int
    binding_index: int

    @property
    def priority(self) -> int:
        return self.binding.priority


@dataclass(slots=True, frozen=True)
class PermissionScope:
    """Permission identity/session keys for one incoming event."""

    identity_id: str
    session_id: str


class BotRuntimeRouter:
    """Resolve incoming events to configured bot service units."""

    def __init__(self, bots: tuple[BotServiceConfig, ...] | list[BotServiceConfig]) -> None:
        self._bots = tuple(bots)

    @property
    def bots(self) -> tuple[BotServiceConfig, ...]:
        return self._bots

    def resolve(
        self,
        *,
        adapter_instance_id: str,
        event: UnifiedEvent,
    ) -> BotRuntimeSelection | None:
        candidates: list[BotRuntimeSelection] = []
        for bot_index, bot in enumerate(self._bots):
            if not bot.enabled:
                continue
            for binding_index, binding in enumerate(bot.bindings):
                if not binding.enabled:
                    continue
                if binding.adapter_instance_id != adapter_instance_id:
                    continue
                if not any(
                    session_pattern_matches_event(pattern, event)
                    for pattern in binding.session_patterns
                ):
                    continue
                candidates.append(
                    BotRuntimeSelection(
                        bot=bot,
                        binding=binding,
                        bot_index=bot_index,
                        binding_index=binding_index,
                    )
                )

        if not candidates:
            return None
        return min(candidates, key=lambda item: (-item.priority, item.bot_index, item.binding_index))


def session_key_for_event(event: UnifiedEvent) -> str:
    """Return the bot-scoped session key for an event, without adapter instance id."""

    if event.is_private:
        target = str(event.sender_id or event.channel_id or "").strip()
        if target.startswith("private:"):
            target = target[len("private:") :]
        return f"private:{target}"

    channel_id = str(event.channel_id or "").strip()
    guild_id = str(event.guild_id or "").strip()
    target = f"{guild_id}:{channel_id}" if guild_id else channel_id
    return f"group:{target}"


def session_pattern_matches_event(pattern: str, event: UnifiedEvent) -> bool:
    """Return whether a configured session pattern matches an event."""

    normalized = str(pattern or "").strip()
    if normalized == WILDCARD:
        return True

    pattern_type, separator, pattern_target = normalized.partition(":")
    if not separator:
        return False

    session_key = session_key_for_event(event)
    session_type, _separator, session_target = session_key.partition(":")
    if pattern_type != session_type:
        return False
    return pattern_target == WILDCARD or pattern_target == session_target


def bot_session_id_for_selection(
    selection: BotRuntimeSelection | None,
    *,
    event: UnifiedEvent,
) -> str:
    """Return the bot-scoped session id for a selected bot binding."""

    if selection is None:
        return ""
    return f"{selection.bot.id}:{session_key_for_event(event)}"


def permission_scope_for_event(
    selection: BotRuntimeSelection | None,
    *,
    event: UnifiedEvent,
    fallback_identity_id: str,
    fallback_session_id: str,
) -> PermissionScope:
    """Return the PermissionEngine scope for one event.

    Bot configs use keys like ``{bot_id}:{user_id}`` and
    ``{bot_id}:{session_key}.{user_id}``.  When no bot router is installed,
    existing adapter/session scoped behavior is preserved for embedded tests
    and lower-level API users.
    """

    if selection is None:
        return PermissionScope(
            identity_id=fallback_identity_id,
            session_id=fallback_session_id,
        )
    return PermissionScope(
        identity_id=selection.bot.id,
        session_id=bot_session_id_for_selection(selection, event=event),
    )


def command_prefixes_for_context(message_context: Any, fallback_prefixes: list[str]) -> list[str]:
    """Return command prefixes for the selected bot, or session defaults."""

    bot_service_config = selected_bot_service_config(message_context)
    if bot_service_config is None:
        return fallback_prefixes
    return list(bot_service_config.commands.prefixes)


def bot_commands_enabled_for_context(message_context: Any) -> bool:
    """Check if text commands are enabled for this message context."""
    bot_service_config = selected_bot_service_config(message_context)
    return bot_service_config is None or bot_service_config.commands.enabled


def bot_plugin_enabled_for_context(message_context: Any, plugin_id: str | None) -> bool:
    """Check if a specific plugin is enabled for this message context."""
    return bot_plugin_enabled(selected_bot_service_config(message_context), plugin_id)


def bot_plugin_enabled(
    bot_service_config: BotServiceConfig | None,
    plugin_id: str | None,
) -> bool:
    """Return whether a plugin-owned capability may run for a selected bot."""

    normalized_plugin_id = str(plugin_id or "").strip()
    if bot_service_config is None or not normalized_plugin_id:
        return True

    policy = bot_service_config.plugins
    if not policy.enabled:
        return False

    disabled = set(policy.disabled_plugins)
    if PLUGIN_WILDCARD in disabled or normalized_plugin_id in disabled:
        return False

    enabled = set(policy.enabled_plugins)
    return PLUGIN_WILDCARD in enabled or normalized_plugin_id in enabled


def bot_agent_enabled_for_context(message_context: Any) -> bool:
    """Check if the agent runtime is enabled for this message context."""
    bot_service_config = selected_bot_service_config(message_context)
    return bot_service_config is None or bot_service_config.agent.mode != "none"


def bot_route_rule_enabled_for_context(rule: RouteRule, message_context: Any) -> bool:
    """Return whether a matched route rule may run for the selected bot."""

    bot_service_config = selected_bot_service_config(message_context)
    if bot_service_config is None:
        return True
    if rule.target == AGENT_ENTRY_TARGET_NAME:
        return bot_agent_enabled_for_context(message_context)
    return bot_plugin_enabled(bot_service_config, rule.owner)


def selected_bot_service_config(message_context: Any) -> BotServiceConfig | None:
    return getattr(message_context, "bot_service_config", None)
