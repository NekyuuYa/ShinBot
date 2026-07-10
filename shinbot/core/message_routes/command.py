"""Command routes — registration, resolution, and dispatch target."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from shinbot.core.application.bot_routing import (
    bot_commands_enabled_for_context,
    bot_plugin_enabled_for_context,
    command_prefixes_for_context,
)
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
    from shinbot.core.dispatch.ingress import RouteDispatchContext, RouteTargetRegistry

logger = logging.getLogger(__name__)

CommandHandler = Callable[..., Coroutine[Any, Any, Any]]
TEXT_COMMAND_DISPATCHER_TARGET = "text_command_dispatcher"


class CommandMode(Enum):
    """Determine how a command handler is invoked by the dispatcher."""
    DELEGATED = "delegated"
    MANAGED = "managed"


class CommandPriority(Enum):
    """Resolution priority tiers for command matching."""
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
    pattern: re.Pattern[str] | None = None  # For P2 regex commands
    owner: str | None = None  # Plugin ID that registered this command
    enabled: bool = True

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
    prefix: str = ""  # Actual matched command prefix for P0 commands
    regex_match: re.Match[str] | None = None  # For P2 matches


@dataclass(slots=True, frozen=True)
class _CommandRegistration:
    command: CommandDef
    default_permission: str
    sequence: int


class CommandRegistry:
    """Registry and resolver for commands.

    Supports the three-tier priority resolution defined in the spec.
    """

    def __init__(self) -> None:
        """Initialize the registry with empty command and index structures."""
        self._commands: dict[str, CommandDef] = {}  # name -> CommandDef
        self._alias_map: dict[str, str] = {}  # alias -> command name
        self._regex_commands: list[CommandDef] = []  # P2 pattern commands
        self._exact_commands: dict[str, CommandDef] = {}  # P1 exact match map
        self._enabled_overrides: dict[str, bool] = {}
        self._permission_overrides: dict[str, str] = {}
        self._default_permissions: dict[str, str] = {}
        self._registration_stacks: dict[str, list[_CommandRegistration]] = {}
        self._next_registration_sequence = 0

    def register(self, cmd: CommandDef) -> None:
        """Register a command definition."""
        if cmd.priority == CommandPriority.P2_REGEX and cmd.pattern is None:
            raise ValueError(f"P2 regex command {cmd.name!r} requires a pattern")
        if cmd.name in self._commands:
            logger.warning("Overriding command: %s", cmd.name)

        registration = _CommandRegistration(
            command=cmd,
            default_permission=cmd.permission,
            sequence=self._next_registration_sequence,
        )
        self._next_registration_sequence += 1
        self._registration_stacks.setdefault(cmd.name, []).append(registration)
        self._rebuild_active_commands()

    def unregister(self, name: str) -> CommandDef | None:
        """Remove a command by name, cleaning up all indexes."""
        cmd = self._commands.get(name)
        if cmd is None:
            return None
        self._registration_stacks.pop(name, None)
        self._rebuild_active_commands()
        return cmd

    def unregister_by_owner(self, owner: str) -> int:
        """Remove all commands registered by a specific owner."""
        removed = 0
        for name, registrations in list(self._registration_stacks.items()):
            remaining = [
                registration
                for registration in registrations
                if registration.command.owner != owner
            ]
            removed += len(registrations) - len(remaining)
            if remaining:
                self._registration_stacks[name] = remaining
            else:
                self._registration_stacks.pop(name, None)
        if removed:
            self._rebuild_active_commands()
        return removed

    def _rebuild_active_commands(self) -> None:
        active_registrations = [
            registrations[-1]
            for registrations in self._registration_stacks.values()
            if registrations
        ]
        self._commands = {
            registration.command.name: registration.command
            for registration in active_registrations
        }
        self._default_permissions = {
            registration.command.name: registration.default_permission
            for registration in active_registrations
        }
        self._alias_map.clear()
        self._regex_commands.clear()
        self._exact_commands.clear()

        for registration in sorted(active_registrations, key=lambda item: item.sequence):
            cmd = registration.command
            if cmd.name in self._enabled_overrides:
                cmd.enabled = self._enabled_overrides[cmd.name]
            cmd.permission = self._permission_overrides.get(
                cmd.name,
                registration.default_permission,
            )
            if cmd.priority == CommandPriority.P2_REGEX:
                self._regex_commands.append(cmd)
            elif cmd.priority == CommandPriority.P1_EXACT:
                for trigger in cmd.all_triggers:
                    self._exact_commands[trigger] = cmd
            else:
                for alias in cmd.aliases:
                    self._alias_map[alias] = cmd.name

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
        """Return a snapshot list of all registered command definitions."""
        return list(self._commands.values())

    def set_enabled(self, name: str, enabled: bool) -> CommandDef | None:
        """Set enabled state for a command and persist the runtime override."""
        self._enabled_overrides[name] = enabled
        cmd = self._commands.get(name)
        if cmd is not None:
            cmd.enabled = enabled
        return cmd

    def is_enabled(self, name: str, *, default: bool = True) -> bool:
        """Return whether a command is enabled."""
        cmd = self._commands.get(name)
        if cmd is not None:
            return cmd.enabled
        return self._enabled_overrides.get(name, default)

    def list_enabled_overrides(self) -> dict[str, bool]:
        """Return a copy of explicit command enabled-state overrides."""
        return dict(self._enabled_overrides)

    def set_permission_override(self, name: str, permission: str) -> CommandDef | None:
        """Set the runtime permission override for a command.

        An empty permission string disables the command's permission requirement.
        """
        self._permission_overrides[name] = permission
        cmd = self._commands.get(name)
        if cmd is not None:
            cmd.permission = permission
        return cmd

    def clear_permission_override(self, name: str) -> None:
        """Remove a stored permission override and restore the plugin default."""
        self._permission_overrides.pop(name, None)
        cmd = self._commands.get(name)
        if cmd is not None:
            cmd.permission = self._default_permissions.get(name, cmd.permission)

    def list_permission_overrides(self) -> dict[str, str]:
        """Return a copy of explicit command permission overrides."""
        return dict(self._permission_overrides)

    def default_permission_for(self, name: str) -> str:
        """Return the plugin-declared default permission for a command."""
        return self._default_permissions.get(name, "")

    def has_permission_override(self, name: str) -> bool:
        """Return whether a command has an explicit permission override."""
        return name in self._permission_overrides

    def declared_permission_nodes(self) -> set[str]:
        """Return all permission nodes declared or currently required by commands."""
        permissions = {
            permission
            for permission in self._default_permissions.values()
            if permission
        }
        permissions.update(
            cmd.permission
            for cmd in self._commands.values()
            if cmd.permission
        )
        permissions.update(
            permission
            for permission in self._permission_overrides.values()
            if permission
        )
        return permissions

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
                if cmd is not None and cmd.enabled and cmd.priority == CommandPriority.P0_PREFIX:
                    return CommandMatch(
                        command=cmd,
                        priority=CommandPriority.P0_PREFIX,
                        raw_args=raw_args,
                        prefix=prefix,
                    )
                # Fallthrough: prefix matched text but command is not P0_PREFIX
                # (or not found). Continue to next prefix, then P1_EXACT/P2_REGEX.
                continue

        cmd = self._exact_commands.get(stripped)
        if cmd is not None and cmd.enabled:
            return CommandMatch(
                command=cmd,
                priority=CommandPriority.P1_EXACT,
                raw_args="",
            )

        for cmd in self._regex_commands:
            if cmd.enabled and cmd.pattern is not None:
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
        task_supervisor: RouteTargetRegistry | None = None,
    ) -> None:
        """Initialize the dispatcher with backing registries and services.

        Args:
            command_registry: Registry to resolve incoming text commands.
            audit_logger: Optional audit logger for command execution tracking.
            session_manager: Optional session manager for persisting session state.
            task_supervisor: Optional owner-aware handler task supervisor.
        """
        self._command_registry = command_registry
        self._audit_logger = audit_logger
        self._session_manager = session_manager
        self._task_supervisor = task_supervisor

    def matches(
        self,
        event: UnifiedEvent,
        message: Message,
        match_context: RouteMatchContext | None = None,
    ) -> bool:
        """Check whether the message matches a registered text command.

        Args:
            event: The unified platform event.
            message: The parsed message AST.
            match_context: Optional routing match context with session data.

        Returns:
            True if a command match is found and its owning plugin is enabled.
        """
        message_context = match_context.message_context if match_context is not None else None
        if not bot_commands_enabled_for_context(message_context):
            return False

        prefixes = ["/"]
        if match_context is not None and match_context.session is not None:
            prefixes = match_context.session.config.prefixes
        prefixes = command_prefixes_for_context(message_context, list(prefixes))
        plain_text = message.get_text(self_id=event.self_id)
        match = self._command_registry.resolve(plain_text, prefixes)
        if match is None:
            return False
        return bot_plugin_enabled_for_context(message_context, match.command.owner)

    async def __call__(self, context: RouteDispatchContext, _rule: RouteRule) -> None:
        """Resolve and execute the matched text command.

        Performs permission checks, invokes the handler with timing, logs
        audit results, and updates the session state.

        Args:
            context: The route dispatch context containing the message context.
            _rule: The route rule that triggered this dispatcher (unused).
        """
        bot = context.require_message_context()
        if not bot_commands_enabled_for_context(bot):
            return

        prefixes = command_prefixes_for_context(bot, list(bot.session.config.prefixes))
        match = self._command_registry.resolve(bot.text, prefixes)
        if match is None:
            return
        if not bot_plugin_enabled_for_context(bot, match.command.owner):
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
            if self._task_supervisor is not None and not self._task_supervisor.accepts_tasks(
                match.command.owner
            ):
                return
            handler_call = match.command.handler(bot, match.raw_args)
            if self._task_supervisor is not None:
                handler_result = await self._task_supervisor.run_owned_awaitable(
                    handler_call,
                    owner=match.command.owner,
                    name=f"command.{match.command.owner or 'core'}.{match.command.name}",
                )
            else:
                handler_result = await handler_call
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
        bot: Any,
        command_name: str,
        plugin_id: str | None,
        permission_required: str,
        permission_granted: bool,
        execution_time_ms: float,
        success: bool,
        error: str,
        metadata: dict[str, Any] | None = None,
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

    def _update_session(self, session: Any) -> None:
        if self._session_manager is not None:
            self._session_manager.update(session)


def make_text_command_route_rule(
    dispatcher: TextCommandDispatcher,
    *,
    rule_id: str = "builtin.text_command_dispatcher",
    priority: int = 1000,
) -> RouteRule:
    """Build a route rule that delegates to the text command dispatcher.

    Args:
        dispatcher: The TextCommandDispatcher instance to use for matching.
        rule_id: Unique identifier for the route rule.
        priority: Numeric priority for route ordering (lower is higher priority).

    Returns:
        A RouteRule configured for message-created events with exclusive matching.
    """
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
