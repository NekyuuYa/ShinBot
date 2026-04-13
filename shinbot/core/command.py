"""Command system — registration, resolution, and execution.

Implements the command specification (03_command_system.md).

Dual-track registration:
  - Delegated Mode: raw text passed to handler, plugin parses itself
  - Managed Mode: Pydantic schema auto-parsing + interactive prompts

Resolution priority:
  P0: Prefix + command name (session markers)
  P1: Exact match (full message == command name/alias, no prefix)
  P2: Regex match
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

CommandHandler = Callable[..., Coroutine[Any, Any, Any]]


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
        self._commands: dict[str, CommandDef] = {}  # name → CommandDef
        self._alias_map: dict[str, str] = {}  # alias → command name
        self._regex_commands: list[CommandDef] = []  # P2 pattern commands
        self._exact_commands: dict[str, CommandDef] = {}  # P1 exact match map

    # ── Registration ─────────────────────────────────────────────────

    def register(self, cmd: CommandDef) -> None:
        """Register a command definition."""
        if cmd.name in self._commands:
            logger.warning("Overriding command: %s", cmd.name)

        self._commands[cmd.name] = cmd

        # Index by priority type
        if cmd.priority == CommandPriority.P2_REGEX:
            if cmd.pattern is None:
                raise ValueError(f"P2 regex command {cmd.name!r} requires a pattern")
            self._regex_commands.append(cmd)
        elif cmd.priority == CommandPriority.P1_EXACT:
            for trigger in cmd.all_triggers:
                self._exact_commands[trigger] = cmd
        else:
            # P0 prefix commands — indexed by alias map
            for alias in cmd.aliases:
                self._alias_map[alias] = cmd.name

    def unregister(self, name: str) -> CommandDef | None:
        """Remove a command by name, cleaning up all indexes."""
        cmd = self._commands.pop(name, None)
        if cmd is None:
            return None

        # Clean alias map
        for alias in cmd.aliases:
            self._alias_map.pop(alias, None)

        # Clean exact match map
        for trigger in cmd.all_triggers:
            self._exact_commands.pop(trigger, None)

        # Clean regex list
        self._regex_commands = [c for c in self._regex_commands if c.name != name]

        return cmd

    def unregister_by_owner(self, owner: str) -> int:
        """Remove all commands registered by a specific owner (plugin).

        Returns the count of removed commands.
        """
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

    # ── Resolution ───────────────────────────────────────────────────

    def resolve(self, text: str, prefixes: list[str]) -> CommandMatch | None:
        """Resolve a message text to a command match.

        Priority order:
          P0: Prefix marker + first word matches command name/alias
          P1: Full text exact match against registered exact commands
          P2: Regex pattern match

        Args:
            text: The raw message plain text.
            prefixes: Active command prefixes for the current session.

        Returns:
            CommandMatch if a command was found, None otherwise.
        """
        stripped = text.strip()
        if not stripped:
            return None

        # ── P0: Prefix match ────────────────────────────────────────
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
                # P0 with prefix but no match → considered invalid command
                # Return a "miss" marker so the workflow can report error
                # (per spec: "directly report error, do not enter Agent")
                return None

        # ── P1: Exact match ──────────────────────────────────────────
        cmd = self._exact_commands.get(stripped)
        if cmd is not None:
            return CommandMatch(
                command=cmd,
                priority=CommandPriority.P1_EXACT,
                raw_args="",
            )

        # ── P2: Regex match ──────────────────────────────────────────
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
