"""Built-in route target dispatchers."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from shinbot.core.bot_config import ATTENTION_DISABLED_PROFILE, select_response_profile
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.ingress import RouteDispatchContext
from shinbot.core.dispatch.keyword import KeywordRegistry
from shinbot.core.dispatch.routing import (
    RouteCondition,
    RouteMatchContext,
    RouteMatchMode,
    RouteRule,
)
from shinbot.core.security.audit import AuditLogger
from shinbot.core.state.session import SessionManager
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent

if TYPE_CHECKING:
    from shinbot.agent.attention.scheduler import AttentionScheduler
    from shinbot.agent.context import ContextManager
    from shinbot.persistence.engine import DatabaseManager

logger = logging.getLogger(__name__)

TEXT_COMMAND_DISPATCHER_TARGET = "text_command_dispatcher"
KEYWORD_DISPATCHER_TARGET = "keyword_dispatcher"
NOTICE_DISPATCHER_TARGET = "notice_dispatcher"
AGENT_ENTRY_TARGET = "agent_entry"


class TextCommandDispatcher:
    """Route target that resolves and executes registered text commands."""

    def __init__(
        self,
        command_registry: CommandRegistry,
        *,
        audit_logger: AuditLogger | None = None,
        session_manager: SessionManager | None = None,
    ) -> None:
        self._command_registry = command_registry
        self._audit_logger = audit_logger
        self._session_manager = session_manager

    def matches(
        self,
        event: UnifiedEvent,
        message: Message,
        match_context: RouteMatchContext | None = None,
    ) -> bool:
        prefixes = ["/"]
        if match_context is not None and match_context.session is not None:
            prefixes = match_context.session.config.prefixes
        plain_text = message.get_text(self_id=event.self_id)
        return self._command_registry.resolve(plain_text, prefixes) is not None

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        bot = context.require_message_context()
        match = self._command_registry.resolve(bot.text, bot.session.config.prefixes)
        if match is None:
            return

        bot.command_match = match
        permission_granted = True
        if match.command.permission:
            permission_granted = bot.has_permission(match.command.permission)
            if not permission_granted:
                logger.debug(
                    "Permission denied for %s: requires %s",
                    match.command.name,
                    match.command.permission,
                )
                await bot.send(f"权限不足：需要 {match.command.permission}")
                self._log_command_audit(
                    bot=bot,
                    command_name=match.command.name,
                    plugin_id=match.command.owner,
                    permission_required=match.command.permission,
                    permission_granted=False,
                    execution_time_ms=bot.elapsed_ms,
                    success=False,
                    error="Permission denied",
                )
                self._update_session(bot.session)
                return

        cmd_start = time.monotonic()
        cmd_time = 0.0
        success = True
        error = ""

        try:
            handler_result = await match.command.handler(bot, match.raw_args)
            if handler_result is not None:
                logger.warning(
                    "Command handler %s returned a value that was ignored; use bot.send()",
                    match.command.name,
                )
            cmd_time = (time.monotonic() - cmd_start) * 1000.0
            logger.debug(
                "Command %s (plugin=%s) executed in %.1fms",
                match.command.name,
                match.command.owner,
                cmd_time,
            )
        except Exception as e:
            cmd_time = (time.monotonic() - cmd_start) * 1000.0
            success = False
            error = str(e)
            logger.exception("Command handler error: %s", match.command.name)

        self._log_command_audit(
            bot=bot,
            command_name=match.command.name,
            plugin_id=match.command.owner,
            permission_required=match.command.permission,
            permission_granted=permission_granted,
            execution_time_ms=cmd_time,
            success=success,
            error=error,
            metadata={
                "raw_args": match.raw_args[:100] if match.raw_args else "",
                "message_count": len(bot._sent_messages),
            },
        )
        self._update_session(bot.session)

    def _log_command_audit(
        self,
        *,
        bot,
        command_name: str,
        plugin_id: str | None,
        permission_required: str,
        permission_granted: bool,
        execution_time_ms: float,
        success: bool,
        error: str,
        metadata: dict | None = None,
    ) -> None:
        if self._audit_logger is None:
            return
        self._audit_logger.log_command(
            command_name=command_name,
            plugin_id=plugin_id or "",
            user_id=bot.event.sender_id or "",
            session_id=bot.session.id,
            instance_id=bot.adapter.instance_id,
            permission_required=permission_required,
            permission_granted=permission_granted,
            execution_time_ms=execution_time_ms,
            success=success,
            error=error,
            metadata=metadata,
        )

    def _update_session(self, session) -> None:
        if self._session_manager is not None:
            self._session_manager.update(session)


def make_text_command_route_rule(
    dispatcher: TextCommandDispatcher,
    *,
    rule_id: str = "builtin.text_command_dispatcher",
    priority: int = 1000,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(
            event_types=frozenset({"message-created"}),
            custom_matcher=dispatcher.matches,
        ),
        target=TEXT_COMMAND_DISPATCHER_TARGET,
        match_mode=RouteMatchMode.EXCLUSIVE,
    )


class KeywordDispatcher:
    """Route target that executes registered keyword handlers."""

    def __init__(
        self,
        keyword_registry: KeywordRegistry,
        *,
        session_manager: SessionManager | None = None,
    ) -> None:
        self._keyword_registry = keyword_registry
        self._session_manager = session_manager

    def matches(self, event: UnifiedEvent, message: Message) -> bool:
        if not event.is_message_event:
            return False
        return bool(self._keyword_registry.match(message.get_text(self_id=event.self_id)))

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        bot = context.require_message_context()
        matches = self._keyword_registry.match(bot.text)
        for match in matches:
            if bot.is_stopped:
                break
            try:
                handler_result = await match.keyword.handler(bot, match)
                if handler_result is not None:
                    logger.warning(
                        "Keyword handler %s returned a value that was ignored; use bot.send()",
                        match.keyword.pattern,
                    )
            except Exception:
                logger.exception("Keyword handler error: %s", match.keyword.pattern)

        if self._session_manager is not None:
            self._session_manager.update(bot.session)


def make_keyword_route_rule(
    dispatcher: KeywordDispatcher,
    *,
    rule_id: str = "builtin.keyword_dispatcher",
    priority: int = 900,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(
            event_types=frozenset({"message-created"}),
            custom_matcher=dispatcher.matches,
        ),
        target=KEYWORD_DISPATCHER_TARGET,
        match_mode=RouteMatchMode.NORMAL,
    )


class NoticeDispatcher:
    """Route target that forwards notice events to the internal EventBus."""

    def __init__(self, event_bus: EventBus) -> None:
        self._event_bus = event_bus

    def matches(self, event: UnifiedEvent, _message: Message) -> bool:
        return event.is_notice_event

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        await self._event_bus.emit(context.event.type, context.event)


def make_notice_route_rule(
    dispatcher: NoticeDispatcher,
    *,
    rule_id: str = "builtin.notice_dispatcher",
    priority: int = 1000,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(custom_matcher=dispatcher.matches),
        target=NOTICE_DISPATCHER_TARGET,
        match_mode=RouteMatchMode.NORMAL,
    )


class AgentEntryDispatcher:
    """Route target that hands unmatched user messages to the Agent entry layer.

    The current Agent entry implementation delegates to the attention scheduler,
    but that is intentionally hidden behind this dispatcher so message routing
    remains stable if Agent-side triggering changes later.
    """

    def __init__(
        self,
        *,
        attention_scheduler: AttentionScheduler | None = None,
        database: DatabaseManager | None = None,
        context_manager: ContextManager | None = None,
    ) -> None:
        self._attention_scheduler = attention_scheduler
        self._database = database
        self._context_manager = context_manager

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        bot = context.require_message_context()
        response_profile = self._resolve_response_profile(bot)
        is_reply_to_bot = bot.is_reply_to_bot()

        handled_by_attention = False
        if self._attention_scheduler is not None:
            handled_by_attention = self._attention_scheduler.schedule_message(
                bot.session_id,
                context.message_log_id,
                bot.event.sender_id or "",
                response_profile=response_profile,
                message=context.message,
                self_platform_id=bot.event.self_id,
                is_reply_to_bot=is_reply_to_bot,
                already_handled=bool(bot._sent_messages),
                is_stopped=bot.is_stopped,
            )

        if not handled_by_attention:
            self._mark_trigger_read(bot.session_id, context.message_log_id)

    def _resolve_response_profile(self, bot) -> str:
        if self._database is None:
            return ATTENTION_DISABLED_PROFILE if bot.is_private else "balanced"

        bot_config = self._database.bot_configs.get_by_instance_id(bot.adapter.instance_id)
        return select_response_profile(
            bot_config,
            is_private=bot.is_private,
            is_mentioned=bot.is_mentioned,
            is_reply_to_bot=bot.is_reply_to_bot(),
        )

    def _mark_trigger_read(self, session_id: str, message_log_id: int | None) -> None:
        if self._database is None or message_log_id is None:
            return
        self._database.message_logs.mark_read(message_log_id)
        if self._context_manager is not None:
            self._context_manager.mark_read_until(session_id, message_log_id)


def make_agent_entry_fallback_route_rule(
    *,
    rule_id: str = "builtin.agent_entry_fallback",
    priority: int = -1000,
) -> RouteRule:
    return RouteRule(
        id=rule_id,
        priority=priority,
        condition=RouteCondition(event_types=frozenset({"message-created"})),
        target=AGENT_ENTRY_TARGET,
        match_mode=RouteMatchMode.FALLBACK,
    )
