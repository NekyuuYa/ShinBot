"""Tests for command dispatch system."""

import re

import pytest

from shinbot.core.message_routes.command import (
    CommandDef,
    CommandPriority,
    CommandRegistry,
)


async def noop_handler(ctx, args):
    pass


class TestCommandRegistry:
    def setup_method(self):
        self.reg = CommandRegistry()

    def test_register_and_get(self):
        cmd = CommandDef(name="ping", handler=noop_handler)
        self.reg.register(cmd)
        assert self.reg.get("ping") is cmd

    def test_get_by_alias(self):
        cmd = CommandDef(name="weather", handler=noop_handler, aliases=["w", "天气"])
        self.reg.register(cmd)
        assert self.reg.get("w") is cmd
        assert self.reg.get("天气") is cmd

    def test_get_unknown(self):
        assert self.reg.get("nonexistent") is None

    def test_unregister(self):
        cmd = CommandDef(name="ping", handler=noop_handler, aliases=["p"])
        self.reg.register(cmd)
        removed = self.reg.unregister("ping")
        assert removed is cmd
        assert self.reg.get("ping") is None
        assert self.reg.get("p") is None

    def test_unregister_by_owner(self):
        cmd1 = CommandDef(name="a", handler=noop_handler, owner="plugin-1")
        cmd2 = CommandDef(name="b", handler=noop_handler, owner="plugin-1")
        cmd3 = CommandDef(name="c", handler=noop_handler, owner="plugin-2")
        self.reg.register(cmd1)
        self.reg.register(cmd2)
        self.reg.register(cmd3)
        removed = self.reg.unregister_by_owner("plugin-1")
        assert removed == 2
        assert self.reg.get("a") is None
        assert self.reg.get("c") is not None

    def test_all_commands(self):
        self.reg.register(CommandDef(name="a", handler=noop_handler))
        self.reg.register(CommandDef(name="b", handler=noop_handler))
        assert len(self.reg.all_commands) == 2


class TestCommandResolution:
    def setup_method(self):
        self.reg = CommandRegistry()

    def test_p0_prefix_match(self):
        cmd = CommandDef(
            name="ping",
            handler=noop_handler,
            priority=CommandPriority.P0_PREFIX,
        )
        self.reg.register(cmd)
        match = self.reg.resolve("/ping", ["/"])
        assert match is not None
        assert match.command is cmd
        assert match.priority == CommandPriority.P0_PREFIX
        assert match.raw_args == ""

    def test_p0_prefix_with_args(self):
        cmd = CommandDef(name="echo", handler=noop_handler)
        self.reg.register(cmd)
        match = self.reg.resolve("/echo hello world", ["/"])
        assert match is not None
        assert match.raw_args == "hello world"

    def test_p0_prefix_no_match_returns_none(self):
        """Per spec: prefix found but command unknown → None (invalid command)."""
        match = self.reg.resolve("/unknown", ["/"])
        assert match is None

    def test_p0_multiple_prefixes(self):
        cmd = CommandDef(name="help", handler=noop_handler)
        self.reg.register(cmd)
        assert self.reg.resolve("#help", ["#", "/"]) is not None
        assert self.reg.resolve("/help", ["#", "/"]) is not None

    def test_p1_exact_match(self):
        cmd = CommandDef(
            name="菜单",
            handler=noop_handler,
            priority=CommandPriority.P1_EXACT,
        )
        self.reg.register(cmd)
        match = self.reg.resolve("菜单", ["/"])
        assert match is not None
        assert match.priority == CommandPriority.P1_EXACT

    def test_p1_no_partial_match(self):
        cmd = CommandDef(
            name="菜单",
            handler=noop_handler,
            priority=CommandPriority.P1_EXACT,
        )
        self.reg.register(cmd)
        assert self.reg.resolve("菜单 额外内容", ["/"]) is None

    def test_p2_regex_match(self):
        cmd = CommandDef(
            name="dice",
            handler=noop_handler,
            priority=CommandPriority.P2_REGEX,
            pattern=re.compile(r"^(\d+)d(\d+)$"),
        )
        self.reg.register(cmd)
        match = self.reg.resolve("3d6", ["/"])
        assert match is not None
        assert match.priority == CommandPriority.P2_REGEX
        assert match.regex_match is not None
        assert match.regex_match.group(1) == "3"
        assert match.regex_match.group(2) == "6"

    def test_p2_no_match(self):
        cmd = CommandDef(
            name="dice",
            handler=noop_handler,
            priority=CommandPriority.P2_REGEX,
            pattern=re.compile(r"^(\d+)d(\d+)$"),
        )
        self.reg.register(cmd)
        assert self.reg.resolve("hello world", ["/"]) is None

    def test_p2_requires_pattern(self):
        with pytest.raises(ValueError, match="requires a pattern"):
            self.reg.register(
                CommandDef(
                    name="bad",
                    handler=noop_handler,
                    priority=CommandPriority.P2_REGEX,
                )
            )

    def test_priority_order_p0_over_p1(self):
        """P0 prefix should be checked before P1 exact."""
        p0 = CommandDef(name="test", handler=noop_handler, priority=CommandPriority.P0_PREFIX)
        p1 = CommandDef(name="/test", handler=noop_handler, priority=CommandPriority.P1_EXACT)
        self.reg.register(p0)
        self.reg.register(p1)
        match = self.reg.resolve("/test", ["/"])
        assert match is not None
        assert match.priority == CommandPriority.P0_PREFIX

    def test_empty_text(self):
        assert self.reg.resolve("", ["/"]) is None

    def test_prefix_only(self):
        """Just a prefix with no command word."""
        assert self.reg.resolve("/", ["/"]) is None

    def test_alias_by_p0(self):
        cmd = CommandDef(name="weather", handler=noop_handler, aliases=["w"])
        self.reg.register(cmd)
        match = self.reg.resolve("/w sunny", ["/"])
        assert match is not None
        assert match.command.name == "weather"
        assert match.raw_args == "sunny"
