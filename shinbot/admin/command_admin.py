"""Administrative helpers for command management flows."""

from __future__ import annotations

from typing import Any


COMMAND_OVERRIDES_SECTION = "command_overrides"
COMMAND_ENABLED_SECTION = "enabled"


class CommandAdminError(RuntimeError):
    """Structured admin-layer error for command management."""

    def __init__(self, *, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _enabled_override_store(
    config: dict[str, Any],
    *,
    create: bool = False,
) -> dict[str, bool]:
    section = config.get(COMMAND_OVERRIDES_SECTION)
    if not isinstance(section, dict):
        if not create:
            return {}
        section = {}
        config[COMMAND_OVERRIDES_SECTION] = section

    enabled = section.get(COMMAND_ENABLED_SECTION)
    if isinstance(enabled, dict):
        return enabled
    if not create:
        return {}
    enabled = {}
    section[COMMAND_ENABLED_SECTION] = enabled
    return enabled


def load_command_enabled_overrides(config: dict[str, Any]) -> dict[str, bool]:
    """Read command enabled overrides from config."""
    raw_overrides = _enabled_override_store(config, create=False)
    overrides: dict[str, bool] = {}
    for key, value in raw_overrides.items():
        if not isinstance(key, str):
            continue
        overrides[str(key)] = bool(value)
    return overrides


def apply_command_enabled_overrides(command_registry: Any, config: dict[str, Any]) -> None:
    """Apply persisted command enabled overrides to the runtime registry."""
    for name, enabled in load_command_enabled_overrides(config).items():
        command_registry.set_enabled(name, enabled)


def command_dict(definition: Any) -> dict[str, Any]:
    """Build a serialized command payload for API responses."""
    aliases = list(definition.aliases)
    triggers = [definition.name, *aliases]
    pattern = definition.pattern.pattern if definition.pattern is not None else ""

    return {
        "name": definition.name,
        "aliases": aliases,
        "triggers": triggers,
        "description": definition.description,
        "usage": definition.usage,
        "permission": definition.permission,
        "mode": definition.mode.value,
        "priority": definition.priority.value,
        "priorityLabel": definition.priority.name,
        "pattern": pattern,
        "owner": definition.owner or "",
        "enabled": bool(definition.enabled),
    }


def get_command_or_raise(command_registry: Any, name: str) -> Any:
    """Retrieve a command by primary name or raise a 404 error."""
    command = command_registry.get(name)
    if command is None or command.name != name:
        raise CommandAdminError(
            status_code=404,
            code="COMMAND_NOT_FOUND",
            message=f"Command {name!r} not found",
        )
    return command


def set_command_enabled_or_raise(
    command_registry: Any,
    boot: Any,
    name: str,
    *,
    enabled: bool,
) -> dict[str, Any]:
    """Update one command enabled state and persist it to config."""
    command = get_command_or_raise(command_registry, name)
    command_registry.set_enabled(command.name, enabled)

    store = _enabled_override_store(boot.config, create=True)
    store[command.name] = enabled
    if not boot.save_config():
        raise CommandAdminError(
            status_code=500,
            code="CONFIG_WRITE_FAILED",
            message=f"Failed to persist enabled state for command {command.name!r}",
        )
    return command_dict(command)
