"""Built-in route target dispatchers."""

from __future__ import annotations

import logging
import time

from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.ingress import RouteDispatchContext
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

logger = logging.getLogger(__name__)

TEXT_COMMAND_DISPATCHER_TARGET = "text_command_dispatcher"


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
