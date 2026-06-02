"""Builtin plugin: baseline text commands for message platforms."""

from __future__ import annotations

from collections.abc import Sequence

import shinbot
from shinbot.core.plugins.context import Plugin

__plugin_name__ = "Builtin Commands"
__plugin_description__ = "Provides baseline help, ping, and about commands for chat platforms."
__plugin_author__ = "ShinBot Team"
__plugin_version__ = shinbot.__version__


def _render_help_lines(plugin: Plugin) -> list[str]:
    commands = sorted(
        plugin._command_registry.all_commands,
        key=lambda item: (item.name, item.owner or ""),
    )
    if not commands:
        return ["当前没有可用指令"]

    lines = ["可用指令："]
    for command in commands:
        triggers = [command.name, *command.aliases]
        trigger_text = " / ".join(f"/{trigger}" for trigger in triggers)
        detail = command.description.strip() or "无描述"
        lines.append(f"{trigger_text} - {detail}")
    return lines


def _format_prefixes(prefixes: Sequence[str]) -> str:
    visible = [prefix for prefix in prefixes if prefix]
    return " ".join(visible) if visible else "/"


def setup(plg: Plugin) -> None:
    """Register the baseline builtin text commands."""

    @plg.on_command(
        "help",
        aliases=["commands"],
        description="列出当前可用的基础指令",
        usage="/help",
    )
    async def help_command(ctx, _args: str) -> None:
        await ctx.send("\n".join(_render_help_lines(plg)))

    @plg.on_command(
        "ping",
        description="检查消息命令链路是否可用",
        usage="/ping",
    )
    async def ping_command(ctx, _args: str) -> None:
        await ctx.send("pong")

    @plg.on_command(
        "about",
        aliases=["version"],
        description="显示当前 ShinBot 版本和命令前缀",
        usage="/about",
    )
    async def about_command(ctx, _args: str) -> None:
        prefixes = _format_prefixes(ctx.session.config.prefixes)
        await ctx.send(f"ShinBot {shinbot.__version__}\n命令前缀：{prefixes}")
