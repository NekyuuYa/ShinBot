"""Builtin plugin: baseline text commands for message platforms."""

from __future__ import annotations

from typing import Any

import re
import time
from collections.abc import Sequence

import shinbot
from shinbot.core.application.bot_routing import bot_plugin_enabled_for_context
from shinbot.core.plugins.context import Plugin
from shinbot.core.state.session import get_agent_pause_until

__plugin_name__ = "Builtin Commands"
__plugin_description__ = "Provides baseline help, ping, and about commands for chat platforms."
__plugin_author__ = "ShinBot Team"
__plugin_version__ = shinbot.__version__

_DURATION_PATTERN = re.compile(r"(?P<value>\d+)(?P<unit>[smhd])", re.IGNORECASE)
_DURATION_UNITS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}


def _is_command_visible(command: Any, ctx: Any) -> bool:
    return (
        command.enabled
        and (ctx is None or bot_plugin_enabled_for_context(ctx, command.owner))
        and (ctx is None or not command.permission or ctx.has_permission(command.permission))
    )


def _render_help_lines(plugin: Plugin, ctx: Any = None) -> list[str]:
    prefix = "/"
    if ctx is not None and ctx.command_match is not None and ctx.command_match.prefix:
        prefix = ctx.command_match.prefix

    commands = sorted(
        (
            command
            for command in plugin._command_registry.all_commands
            if _is_command_visible(command, ctx)
        ),
        key=lambda item: (item.name, item.owner or ""),
    )
    if not commands:
        return ["当前没有可用指令"]

    lines = ["可用指令："]
    for command in commands:
        triggers = [command.name, *command.aliases]
        trigger_text = " / ".join(f"{prefix}{trigger}" for trigger in triggers)
        detail = command.description.strip() or "无描述"
        lines.append(f"{trigger_text} - {detail}")
    return lines


def _format_prefixes(prefixes: Sequence[str]) -> str:
    visible = [prefix for prefix in prefixes if prefix]
    return " ".join(visible) if visible else "/"


def _parse_duration_spec(spec: str) -> int:
    cleaned = "".join(spec.split())
    if not cleaned:
        raise ValueError("empty duration")

    total_seconds = 0
    cursor = 0
    for match in _DURATION_PATTERN.finditer(cleaned):
        if match.start() != cursor:
            raise ValueError("invalid duration format")
        total_seconds += int(match.group("value")) * _DURATION_UNITS[match.group("unit").lower()]
        cursor = match.end()

    if cursor != len(cleaned) or total_seconds <= 0:
        raise ValueError("invalid duration format")
    return total_seconds


def _format_duration(seconds: float) -> str:
    remaining = max(int(round(seconds)), 0)
    if remaining <= 0:
        return "0s"

    parts: list[str] = []
    for suffix, unit_seconds in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        if remaining < unit_seconds:
            continue
        value, remaining = divmod(remaining, unit_seconds)
        if value > 0:
            parts.append(f"{value}{suffix}")
    return "".join(parts) or "0s"


def _permission_binding_keys(ctx: Any) -> tuple[str, str]:
    identity_id = ctx.bot_id or ctx.adapter.instance_id
    session_scope = ctx.bot_session_id or ctx.session_id
    platform_user_id = ctx.user_id
    if platform_user_id and ":" not in platform_user_id:
        platform_user_id = f"{ctx.adapter.instance_id}:{platform_user_id}"
    return (
        f"{identity_id}:{ctx.user_id}",
        f"{session_scope}.{platform_user_id}",
    )


def _current_pause_until(ctx: Any, plugin: Plugin) -> float | None:
    runtime = plugin.agent_runtime
    if runtime is not None:
        getter = getattr(runtime, "session_pause_until", None)
        if callable(getter):
            return getter(ctx.session_id)
    return get_agent_pause_until(ctx.session)


async def _clear_agent_pause(ctx: Any, plugin: Plugin) -> bool:
    """Clear the current session agent pause if runtime support is available."""
    runtime = plugin.agent_runtime
    if runtime is None:
        await ctx.send("当前未挂载 agent runtime，无法恢复 agent 活动")
        return False

    pause_clearer = getattr(runtime, "clear_session_pause", None)
    if not callable(pause_clearer):
        await ctx.send("当前 agent runtime 暂不支持提前恢复暂停")
        return False

    pause_clearer(ctx.session_id)
    await ctx.send("已恢复当前 session 的 agent 活动")
    return True


def setup(plg: Plugin) -> None:
    """Register the baseline builtin text commands."""

    @plg.on_command(
        "help",
        aliases=["commands"],
        description="列出当前可用的基础指令",
        usage="/help",
        permission="cmd.help",
    )
    async def help_command(ctx, _args: str) -> None:
        await ctx.send("\n".join(_render_help_lines(plg, ctx)))

    @plg.on_command(
        "ping",
        description="检查消息命令链路是否可用",
        usage="/ping",
        permission="cmd.ping",
    )
    async def ping_command(ctx, _args: str) -> None:
        await ctx.send("pong")

    @plg.on_command(
        "about",
        aliases=["version"],
        description="显示当前 ShinBot 版本和命令前缀",
        usage="/about",
        permission="cmd.about",
    )
    async def about_command(ctx, _args: str) -> None:
        prefixes = _format_prefixes(ctx.session.config.prefixes)
        await ctx.send(f"ShinBot {shinbot.__version__}\n命令前缀：{prefixes}")

    @plg.on_command(
        "whoami",
        description="显示自己的唯一用户标识和权限绑定 key",
        usage="/whoami",
        permission="cmd.whoami",
    )
    async def whoami_command(ctx, _args: str) -> None:
        global_key, session_key = _permission_binding_keys(ctx)
        platform_user_id = ctx.user_id
        if platform_user_id and ":" not in platform_user_id:
            platform_user_id = f"{ctx.adapter.instance_id}:{platform_user_id}"
        await ctx.send(
            "\n".join(
                [
                    f"你的唯一用户标识（user_id）：{ctx.user_id or '(unknown)'}",
                    f"平台权限标识：{platform_user_id or '(unknown)'}",
                    f"全局权限绑定 key：{global_key}",
                    f"当前会话权限绑定 key：{session_key}",
                ]
            )
        )

    @plg.on_command(
        "mute",
        description="暂停当前 session 的 agent 活动一段时间",
        usage="/mute 15m | /mute 3h5m | /mute off",
        permission="cmd.mute",
    )
    async def mute_command(ctx, args: str) -> None:
        runtime = plg.agent_runtime
        if runtime is None:
            await ctx.send("当前未挂载 agent runtime，无法暂停 agent 活动")
            return

        pause_until_getter = getattr(runtime, "session_pause_until", None)
        pause_setter = getattr(runtime, "pause_session_until", None)
        if not callable(pause_until_getter) or not callable(pause_setter):
            await ctx.send("当前 agent runtime 暂不支持 session 级暂停")
            return

        raw_args = args.strip()
        if not raw_args:
            pause_until = _current_pause_until(ctx, plg)
            if pause_until is None:
                await ctx.send("当前 session 未暂停 agent。用法：/mute 15m 或 /mute 3h5m")
                return
            await ctx.send(
                f"当前 session 的 agent 已暂停，剩余 {_format_duration(pause_until - time.time())}"
            )
            return

        normalized = "".join(raw_args.split()).lower()
        if normalized in {"off", "clear", "resume", "unmute"}:
            await _clear_agent_pause(ctx, plg)
            return

        try:
            duration_seconds = _parse_duration_spec(raw_args)
        except ValueError:
            await ctx.send("时间格式无效，请使用类似 15m、3h5m 的参数")
            return

        pause_until = time.time() + duration_seconds
        pause_setter(ctx.session_id, pause_until=pause_until)
        await ctx.send(
            "\n".join(
                [
                    f"已暂停当前 session 的 agent 活动 {_format_duration(duration_seconds)}",
                    "命令仍可使用，到期后 agent 会自动恢复。",
                ]
            )
        )

    @plg.on_command(
        "unmute",
        aliases=["resume"],
        description="恢复当前 session 的 agent 活动",
        usage="/unmute",
        permission="cmd.mute",
    )
    async def unmute_command(ctx, _args: str) -> None:
        await _clear_agent_pause(ctx, plg)
