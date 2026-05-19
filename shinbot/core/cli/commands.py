"""Operator CLI command routing for ShinBot runtime control."""

from __future__ import annotations

import logging
import shlex
from dataclasses import dataclass
from typing import Any

from shinbot.admin.instance_admin import (
    InstanceAdminError,
    control_instance_runtime,
    list_instance_payloads,
)
from shinbot.admin.plugin_admin import (
    PluginAdminError,
    disable_plugin_or_raise,
    enable_plugin_or_raise,
    plugin_dict,
    rescan_plugins,
)
from shinbot.core.application.runtime_control import RestartReason
from shinbot.core.application.system_update import SystemUpdateError
from shinbot.utils.logger import (
    apply_logging_runtime_config,
    get_logger,
    logging_runtime_snapshot,
    set_root_log_level,
)

logger = get_logger(__name__, source="operator", color="magenta")


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
        words = [
            "help",
            "status",
            "instances",
            "instance start",
            "instance stop",
            "plugins",
            "plugin enable",
            "plugin disable",
            "plugin rescan",
            "update",
            "update status",
            "update framework",
            "build dashboard",
            "dashboard build",
            "log",
            "log level",
            "log third-party",
            "log sources",
            "loglevel",
            "restart",
            "clear",
            "quit",
            "exit",
            "?",
        ]
        return [*words, *(f"/{word}" for word in words)]

    async def execute(self, raw_line: str) -> CommandOutcome:
        line = _normalize_operator_line(raw_line)
        if not line:
            return CommandOutcome()

        try:
            parts = shlex.split(line)
        except ValueError as exc:
            return CommandOutcome(f"Parse error: {exc}")

        if not parts:
            return CommandOutcome()

        try:
            return await self._execute_parts(parts)
        except Exception as exc:
            logger.exception("Operator command failed: %s", raw_line)
            return CommandOutcome(f"Command failed: {exc}")

    async def _execute_parts(self, parts: list[str]) -> CommandOutcome:
        head = parts[0].lower()

        if head in {"exit", "quit", "q"}:
            return CommandOutcome("Stopping ShinBot.", exit_requested=True)
        if head in {"help", "?"}:
            return CommandOutcome(self._help_text())
        if head == "clear":
            return CommandOutcome(clear_screen=True)
        if head == "status":
            return CommandOutcome(self._status_text())
        if head == "instances":
            return CommandOutcome(self._instances_text())
        if head == "plugins":
            return CommandOutcome(self._plugins_text())
        if head == "update":
            return await self._handle_update(parts[1:])
        if head in {"build", "dashboard"}:
            return await self._handle_build(parts)
        if head == "log":
            return CommandOutcome(self._handle_log(parts[1:]))
        if head == "loglevel":
            return CommandOutcome(self._set_loglevel(parts[1:]))
        if head == "restart":
            return self._restart(parts[1:])
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
                "  update status            Show framework and Dashboard update state",
                "  update framework         Run configured framework update command and restart",
                "  build dashboard          Rebuild local Dashboard assets",
                "  log                      Show logging runtime state",
                "  log level [LEVEL]        Show or change root log level",
                "  log third-party [POLICY] Show or change third-party noise policy",
                "  log sources              List registered display sources",
                "  loglevel [LEVEL]         Alias for log level [LEVEL]",
                "  restart [note]           Request a process restart",
                "  clear                    Clear the terminal screen",
                "  exit                     Stop ShinBot and leave the console",
                "",
                "Tip: slash-prefixed forms also work, e.g. /status or /plugins.",
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
                f"bots            {len(getattr(bot, 'bot_service_configs', ()))} configured",
                f"model_runtime   {_yes_no(bot.model_runtime is not None)}",
                f"agent_runtime   {_yes_no(bot.agent_runtime is not None)}",
                f"instances       {len(adapter_manager.all_instances)} total / {running_instances} running",
                f"plugins         {len(plugins)} loaded",
                f"commands        {len(bot.command_registry.all_commands)} registered",
                f"database        {_database_path(bot)}",
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
                    item["adapter"] or "-",
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

    def _handle_log(self, args: list[str]) -> str:
        if not args:
            return self._logging_status_text()

        action = args[0].lower()
        if action == "level":
            return self._set_loglevel(args[1:])
        if action in {"third-party", "third_party", "noise"}:
            return self._set_third_party_noise(args[1:])
        if action == "sources":
            return self._logging_sources_text()
        return "Usage: log [level [LEVEL] | third-party [off|debug|on] | sources]"

    def _set_third_party_noise(self, args: list[str]) -> str:
        if not args:
            state = logging_runtime_snapshot()
            return f"Current third-party noise policy: {state['thirdPartyNoise']}"
        try:
            state = apply_logging_runtime_config(third_party_noise=args[0])
        except ValueError as exc:
            return str(exc)
        return f"Third-party noise policy set to {state['thirdPartyNoise']}"

    def _logging_status_text(self) -> str:
        state = logging_runtime_snapshot()
        console_handlers = sum(1 for handler in state["handlers"] if handler["console"])
        return "\n".join(
            [
                f"level                  {state['level']}",
                f"effective_level        {state['effectiveLevel']}",
                f"third_party_noise      {state['thirdPartyNoise']}",
                f"source_width           {state['sourceWidth']}",
                f"sources                {len(state['sources'])} registered",
                f"handlers               {len(state['handlers'])} total / {console_handlers} console",
            ]
        )

    def _logging_sources_text(self) -> str:
        rows = [
            [
                item["source"],
                item["color"] or "-",
                item["loggerName"],
            ]
            for item in logging_runtime_snapshot()["sources"]
        ]
        return self._render_table(["SOURCE", "COLOR", "LOGGER"], rows)

    def _restart(self, args: list[str]) -> CommandOutcome:
        runtime_control = getattr(self._bot(), "runtime_control", None)
        if runtime_control is None:
            return CommandOutcome("Runtime control is not attached; restart is unavailable.")
        note = " ".join(args).strip()
        try:
            request = runtime_control.request_restart(
                reason=RestartReason.MANUAL,
                requested_by="operator-cli",
                source="operator-cli.restart",
            )
        except RuntimeError as exc:
            return CommandOutcome(str(exc))
        suffix = f" ({note})" if note else ""
        return CommandOutcome(
            f"Restart requested at {request.requested_at}{suffix}.",
            exit_requested=True,
        )

    async def _handle_update(self, args: list[str]) -> CommandOutcome:
        action = args[0].lower() if args else "status"
        if action in {"status", "state"}:
            return CommandOutcome(await self._update_status_text())
        if action in {"framework", "app", "core"}:
            return await self._update_framework()
        if action in {"dashboard", "build"}:
            return await self._build_dashboard()
        return CommandOutcome(
            "Usage: update [status|framework|dashboard]\n"
            "Tip: use 'build dashboard' to rebuild local Dashboard assets."
        )

    async def _handle_build(self, parts: list[str]) -> CommandOutcome:
        normalized = [part.lower() for part in parts]
        if normalized in (["build", "dashboard"], ["dashboard", "build"]):
            return await self._build_dashboard()
        return CommandOutcome("Usage: build dashboard")

    async def _update_framework(self) -> CommandOutcome:
        service = getattr(self._boot, "framework_update_service", None)
        runtime_control = getattr(self._bot(), "runtime_control", None)
        if service is None or runtime_control is None:
            return CommandOutcome("Framework update service is not attached.")
        try:
            result = await service.run_and_request_restart(
                runtime_control=runtime_control,
                requested_by="operator-cli",
            )
        except SystemUpdateError as exc:
            return CommandOutcome(_system_update_error_text(exc))

        message = "\n".join(
            [
                "Framework update accepted.",
                f"workdir     {result.get('workdir') or '-'}",
                f"command     {result.get('command') or '-'}",
                f"restart     {_yes_no(bool(result.get('restartRequested')))}",
                _output_tail(str(result.get("output") or "")),
            ]
        )
        return CommandOutcome(message, exit_requested=bool(result.get("restartRequested")))

    async def _build_dashboard(self) -> CommandOutcome:
        service = getattr(self._boot, "dashboard_build_service", None)
        if service is None:
            return CommandOutcome("Dashboard build service is not attached.")
        try:
            result = await service.build()
        except SystemUpdateError as exc:
            return CommandOutcome(_system_update_error_text(exc))

        return CommandOutcome(
            "\n".join(
                [
                    "Dashboard build complete.",
                    f"path        {result.get('dashboardPath') or '-'}",
                    f"dist        {result.get('distPath') or '-'}",
                    f"command     {result.get('command') or '-'}",
                    _output_tail(str(result.get("output") or "")),
                ]
            )
        )

    async def _update_status_text(self) -> str:
        rows: list[list[str]] = []

        framework_update = getattr(self._boot, "framework_update_service", None)
        if framework_update is not None:
            try:
                state = await framework_update.inspect()
                rows.append(
                    [
                        "framework",
                        _capability_state(state, ready_key="canUpdate", busy_key="updateInProgress"),
                        str(state.get("blockCode") or "-"),
                    ]
                )
            except SystemUpdateError as exc:
                rows.append(["framework", "error", exc.code])

        dashboard_build = getattr(self._boot, "dashboard_build_service", None)
        if dashboard_build is not None:
            state = await dashboard_build.inspect()
            rows.append(
                [
                    "dashboard-build",
                    _capability_state(state, ready_key="canBuild", busy_key="buildInProgress"),
                    str(state.get("blockCode") or "-"),
                ]
            )

        return self._render_table(["TARGET", "STATE", "DETAIL"], rows)

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


def _normalize_operator_line(raw_line: str) -> str:
    line = raw_line.strip()
    if line.startswith("/"):
        line = line[1:].lstrip()
    return line


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _database_path(bot: Any) -> str:
    database = getattr(bot, "database", None)
    if database is None:
        return "-"
    config = getattr(database, "config", None)
    path = getattr(config, "sqlite_path", None)
    return str(path) if path is not None else "-"


def _capability_state(state: dict[str, Any], *, ready_key: str, busy_key: str) -> str:
    if state.get(busy_key):
        return "busy"
    if state.get(ready_key):
        return "ready"
    if state.get("blockCode") == "already_up_to_date":
        return "current"
    return "blocked"


def _system_update_error_text(exc: SystemUpdateError) -> str:
    message = f"{exc.code}: {exc.message}"
    if exc.output:
        return f"{message}\n{_output_tail(exc.output)}"
    return message


def _output_tail(output: str, *, limit: int = 1200) -> str:
    output = output.strip()
    if not output:
        return "output      -"
    if len(output) > limit:
        output = f"...\n{output[-limit:]}"
    indented = "\n".join(f"            {line}" if index else f"output      {line}" for index, line in enumerate(output.splitlines()))
    return indented
