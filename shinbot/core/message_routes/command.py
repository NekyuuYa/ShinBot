"""Command routes — registration, resolution, and dispatch target."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

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
    from shinbot.core.dispatch.ingress import RouteDispatchContext

logger = logging.getLogger(__name__)

CommandHandler = Callable[..., Coroutine[Any, Any, Any]]
TEXT_COMMAND_DISPATCHER_TARGET = "text_command_dispatcher"


class CommandMode(Enum):
    DELEGATED = "delegated"
    MANAGED = "managed"


class CommandPriority(Enum):
    P0_PREFIX = 0
    P1_EXACT = 1
    P2_REGEX = 2


@dataclass
class CommandDef:
    """A registered command definition."""

    name: str
    handler: CommandHandler
    mode: CommandMode = CommandMode.DELEGATED
    aliases: list[str] = field(default_factory=list)
    description: str = ""
    usage: str = ""
    permission: str = ""  # Required permission node (e.g. "cmd.weather")
    priority: CommandPriority = CommandPriority.P0_PREFIX
    pattern: re.Pattern | None = None  # For P2 regex commands
    owner: str | None = None  # Plugin ID that registered this command

    @property
    def all_triggers(self) -> list[str]:
        """Command name + all aliases."""
        return [self.name] + self.aliases


@dataclass
class CommandMatch:
    """Result of command resolution."""

    command: CommandDef
    priority: CommandPriority
    raw_args: str = ""  # Remaining text after command trigger
    regex_match: re.Match | None = None  # For P2 matches


class CommandRegistry:
    """Registry and resolver for commands.

    Supports the three-tier priority resolution defined in the spec.
    """

    def __init__(self) -> None:
        self._commands: dict[str, CommandDef] = {}  # name -> CommandDef
        self._alias_map: dict[str, str] = {}  # alias -> command name
        self._regex_commands: list[CommandDef] = []  # P2 pattern commands
        self._exact_commands: dict[str, CommandDef] = {}  # P1 exact match map

    def register(self, cmd: CommandDef) -> None:
        """Register a command definition."""
        if cmd.name in self._commands:
            logger.warning("Overriding command: %s", cmd.name)

        self._commands[cmd.name] = cmd

        if cmd.priority == CommandPriority.P2_REGEX:
            if cmd.pattern is None:
                raise ValueError(f"P2 regex command {cmd.name!r} requires a pattern")
            self._regex_commands.append(cmd)
        elif cmd.priority == CommandPriority.P1_EXACT:
            for trigger in cmd.all_triggers:
                self._exact_commands[trigger] = cmd
        else:
            for alias in cmd.aliases:
                self._alias_map[alias] = cmd.name

    def unregister(self, name: str) -> CommandDef | None:
        """Remove a command by name, cleaning up all indexes."""
        cmd = self._commands.pop(name, None)
        if cmd is None:
            return None

        for alias in cmd.aliases:
            self._alias_map.pop(alias, None)

        for trigger in cmd.all_triggers:
            self._exact_commands.pop(trigger, None)

        self._regex_commands = [c for c in self._regex_commands if c.name != name]
        return cmd

    def unregister_by_owner(self, owner: str) -> int:
        """Remove all commands registered by a specific owner."""
        to_remove = [name for name, cmd in self._commands.items() if cmd.owner == owner]
        for name in to_remove:
            self.unregister(name)
        return len(to_remove)

    def get(self, name: str) -> CommandDef | None:
        """Look up a command by name or alias."""
        if name in self._commands:
            return self._commands[name]
        alias_target = self._alias_map.get(name)
        if alias_target:
            return self._commands.get(alias_target)
        return None

    @property
    def all_commands(self) -> list[CommandDef]:
        return list(self._commands.values())

    def resolve(self, text: str, prefixes: list[str]) -> CommandMatch | None:
        """Resolve a message text to a command match."""
        stripped = text.strip()
        if not stripped:
            return None

        for prefix in prefixes:
            if stripped.startswith(prefix):
                after_prefix = stripped[len(prefix) :].strip()
                if not after_prefix:
                    continue

                parts = after_prefix.split(maxsplit=1)
                cmd_word = parts[0]
                raw_args = parts[1] if len(parts) > 1 else ""

                cmd = self.get(cmd_word)
                if cmd is not None and cmd.priority == CommandPriority.P0_PREFIX:
                    return CommandMatch(
                        command=cmd,
                        priority=CommandPriority.P0_PREFIX,
                        raw_args=raw_args,
                    )
                return None

        cmd = self._exact_commands.get(stripped)
        if cmd is not None:
            return CommandMatch(
                command=cmd,
                priority=CommandPriority.P1_EXACT,
                raw_args="",
            )

        for cmd in self._regex_commands:
            if cmd.pattern is not None:
                m = cmd.pattern.search(stripped)
                if m:
                    return CommandMatch(
                        command=cmd,
                        priority=CommandPriority.P2_REGEX,
                        raw_args=stripped,
                        regex_match=m,
                    )

        return None


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


__all__ = [
    "CommandDef",
    "CommandHandler",
    "CommandMatch",
    "CommandMode",
    "CommandPriority",
    "CommandRegistry",
    "TEXT_COMMAND_DISPATCHER_TARGET",
    "TextCommandDispatcher",
    "make_text_command_route_rule",
]
