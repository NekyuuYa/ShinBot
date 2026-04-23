"""Operator CLI command routing for ShinBot runtime control."""

from __future__ import annotations

import logging
import shlex
from dataclasses import dataclass
from typing import Any

from shinbot.core.instance_admin import (
    InstanceAdminError,
    control_instance_runtime,
    list_instance_payloads,
)
from shinbot.core.plugin_admin import (
    PluginAdminError,
    disable_plugin_or_raise,
    enable_plugin_or_raise,
    plugin_dict,
    rescan_plugins,
)
from shinbot.utils.logger import set_root_log_level


@dataclass(slots=True)
class CommandOutcome:
    message: str | None = None
    exit_requested: bool = False
    clear_screen: bool = False


class OperatorCommandRouter:
    """Parse and execute operator console commands against live runtime state."""

    def __init__(self, *, boot: Any, api_host: str, api_port: int) -> None:
        self._boot = boot
        self._api_host = api_host
        self._api_port = api_port

    @property
    def command_words(self) -> list[str]:
        return [
            "help",
            "status",
            "instances",
            "instance start",
            "instance stop",
            "plugins",
            "plugin enable",
            "plugin disable",
            "plugin rescan",
            "loglevel",
            "clear",
            "quit",
            "exit",
        ]

    async def execute(self, raw_line: str) -> CommandOutcome:
        line = raw_line.strip()
        if not line:
            return CommandOutcome()

        try:
            parts = shlex.split(line)
        except ValueError as exc:
            return CommandOutcome(f"Parse error: {exc}")

        if not parts:
            return CommandOutcome()

        head = parts[0].lower()

        if head in {"exit", "quit"}:
            return CommandOutcome("Stopping ShinBot operator console.", exit_requested=True)
        if head == "help":
            return CommandOutcome(self._help_text())
        if head == "clear":
            return CommandOutcome(clear_screen=True)
        if head == "status":
            return CommandOutcome(self._status_text())
        if head == "instances":
            return CommandOutcome(self._instances_text())
        if head == "plugins":
            return CommandOutcome(self._plugins_text())
        if head == "loglevel":
            return CommandOutcome(self._set_loglevel(parts[1:]))
        if head == "instance":
            return await self._handle_instance(parts[1:])
        if head == "plugin":
            return await self._handle_plugin(parts[1:])

        return CommandOutcome(f"Unknown command: {parts[0]}. Use 'help' to list commands.")

    def _bot(self) -> Any:
        if self._boot.bot is None:
            raise RuntimeError("ShinBot runtime is not available")
        return self._boot.bot

    def _help_text(self) -> str:
        return "\n".join(
            [
                "Available commands:",
                "  help                     Show this help message",
                "  status                   Show boot/API/runtime summary",
                "  instances                List configured instances and runtime state",
                "  instance start <id>      Start one adapter instance",
                "  instance stop <id>       Stop one adapter instance",
                "  plugins                  List loaded plugins",
                "  plugin enable <id>       Enable a disabled plugin",
                "  plugin disable <id>      Disable a loaded plugin",
                "  plugin rescan            Rescan data/plugins and load new plugins",
                "  loglevel [LEVEL]         Show or change root log level",
                "  clear                    Clear the terminal screen",
                "  exit                     Stop ShinBot and leave the console",
            ]
        )

    def _status_text(self) -> str:
        bot = self._bot()
        adapter_manager = bot.adapter_manager
        plugins = bot.plugin_manager.all_plugins
        running_instances = sum(
            1
            for adapter in adapter_manager.all_instances
            if adapter_manager.is_running(adapter.instance_id)
        )
        return "\n".join(
            [
                f"boot_state      {self._boot.state.value}",
                f"api_endpoint    http://{self._api_host}:{self._api_port}",
                f"log_level       {logging.getLevelName(logging.getLogger().level)}",
                f"instances       {len(adapter_manager.all_instances)} total / {running_instances} running",
                f"plugins         {len(plugins)} loaded",
                f"commands        {len(bot.command_registry.all_commands)} registered",
                f"data_dir        {self._boot.data_dir}",
                f"config_path     {self._boot.config_path}",
            ]
        )

    def _instances_text(self) -> str:
        bot = self._bot()
        rows = []
        for item in list_instance_payloads(bot=bot, boot=self._boot):
            rows.append(
                [
                    item["id"] or "-",
                    item["adapterType"] or "-",
                    item["status"],
                    item["name"] or "-",
                ]
            )
        return self._render_table(["ID", "PLATFORM", "STATE", "NAME"], rows)

    def _plugins_text(self) -> str:
        bot = self._bot()
        rows = []
        for meta in bot.plugin_manager.all_plugins:
            item = plugin_dict(bot, meta, self._boot)
            rows.append(
                [
                    item["id"],
                    item["role"],
                    item["state"],
                    item["version"] or "-",
                ]
            )
        return self._render_table(["ID", "ROLE", "STATE", "VERSION"], rows)

    def _set_loglevel(self, args: list[str]) -> str:
        if not args:
            return f"Current log level: {logging.getLevelName(logging.getLogger().level)}"
        try:
            normalized = set_root_log_level(args[0])
        except ValueError as exc:
            return str(exc)
        return f"Root log level set to {normalized}"

    async def _handle_instance(self, args: list[str]) -> CommandOutcome:
        if len(args) != 2 or args[0].lower() not in {"start", "stop"}:
            return CommandOutcome("Usage: instance <start|stop> <instance_id>")

        action = args[0].lower()
        instance_id = args[1]
        try:
            state = await control_instance_runtime(
                mgr=self._bot().adapter_manager,
                instance_id=instance_id,
                action=action,
            )
        except InstanceAdminError as exc:
            return CommandOutcome(exc.message)
        return CommandOutcome(f"Instance {instance_id!r} is now {state}.")

    async def _handle_plugin(self, args: list[str]) -> CommandOutcome:
        if not args:
            return CommandOutcome("Usage: plugin <enable|disable|rescan> [plugin_id]")

        action = args[0].lower()
        bot = self._bot()

        if action == "rescan":
            try:
                loaded = await rescan_plugins(bot, self._boot)
            except PluginAdminError as exc:
                return CommandOutcome(exc.message)
            return CommandOutcome(f"Rescan complete. Loaded {len(loaded)} plugin(s).")

        if len(args) != 2:
            return CommandOutcome(f"Usage: plugin {action} <plugin_id>")

        plugin_id = args[1]
        try:
            if action == "enable":
                meta = await enable_plugin_or_raise(bot, plugin_id, self._boot)
            elif action == "disable":
                meta = await disable_plugin_or_raise(bot, plugin_id, self._boot)
            else:
                return CommandOutcome(f"Unsupported plugin action: {action}")
        except PluginAdminError as exc:
            return CommandOutcome(exc.message)

        item = plugin_dict(bot, meta, self._boot)
        return CommandOutcome(f"Plugin {item['id']!r} state: {item['state']}")

    def _render_table(self, headers: list[str], rows: list[list[str]]) -> str:
        if not rows:
            return "(empty)"

        widths = [len(header) for header in headers]
        for row in rows:
            for index, cell in enumerate(row):
                widths[index] = max(widths[index], len(str(cell)))

        def render_row(row: list[str]) -> str:
            return "  ".join(str(cell).ljust(widths[index]) for index, cell in enumerate(row))

        divider = "  ".join("-" * width for width in widths)
        lines = [render_row(headers), divider]
        lines.extend(render_row(row) for row in rows)
        return "\n".join(lines)
