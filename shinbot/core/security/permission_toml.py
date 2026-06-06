"""TOML-backed repositories for permission management state."""

from __future__ import annotations

import fcntl
import os
import tempfile
import threading
import tomllib
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

import tomli_w
from pydantic import BaseModel, Field

from shinbot.core.security.permission import PermissionGroup

_toml_transaction_lock = threading.Lock()


class PermissionGroupDefinition(BaseModel):
    """A permission group definition loaded from ``[[permissions.groups]]``."""

    id: str
    name: str = ""
    description: str = ""
    permissions: set[str] = Field(default_factory=set)
    system: bool = False
    protected: bool = False

    model_config = {"extra": "forbid"}

    @classmethod
    def from_permission_group(cls, group: PermissionGroup) -> PermissionGroupDefinition:
        """Create a TOML definition from the runtime permission group model."""
        return cls(id=group.id, name=group.name, permissions=set(group.permissions))

    def to_permission_group(self) -> PermissionGroup:
        """Convert to the runtime permission group model."""
        return PermissionGroup(id=self.id, name=self.name, permissions=set(self.permissions))


class PermissionBindingRecord(BaseModel):
    """A permission binding loaded from ``[[permissions.bindings]]``."""

    key: str
    groups: tuple[str, ...] = Field(default_factory=tuple)

    model_config = {"extra": "forbid"}


class CommandPermissionOverride(BaseModel):
    """A command permission override loaded from ``[[permissions.command_overrides]]``."""

    command: str
    permission: str

    model_config = {"extra": "forbid"}


class PermissionTomlError(RuntimeError):
    """Raised when a permission TOML repository cannot read or write its file."""


class PermissionGroupRepository:
    """Read and write permission group definitions from the main TOML config."""

    def __init__(self, config_path: Path | str) -> None:
        self.config_path = Path(config_path)

    def list(self) -> list[PermissionGroupDefinition]:
        """Load ``[[permissions.groups]]`` entries from TOML."""
        return group_definitions_from_config(_load_config(self.config_path))

    def save(self, groups: Iterable[PermissionGroup | PermissionGroupDefinition]) -> None:
        """Replace ``[[permissions.groups]]`` and persist the TOML atomically."""
        group_records = list(groups)

        def update(config: dict[str, Any]) -> None:
            set_groups_in_config(config, group_records)

        _atomic_update_toml(self.config_path, update)


class PermissionBindingRepository:
    """Read and write permission bindings from the main TOML config."""

    def __init__(self, config_path: Path | str) -> None:
        self.config_path = Path(config_path)

    def list(self) -> list[PermissionBindingRecord]:
        """Load ``[[permissions.bindings]]`` entries from TOML."""
        return bindings_from_config(_load_config(self.config_path))

    def save(self, bindings: Iterable[PermissionBindingRecord]) -> None:
        """Replace ``[[permissions.bindings]]`` and persist the TOML atomically."""
        binding_records = list(bindings)

        def update(config: dict[str, Any]) -> None:
            set_bindings_in_config(config, binding_records)

        _atomic_update_toml(self.config_path, update)


class CommandPermissionOverrideRepository:
    """Read and write command permission overrides from the main TOML config."""

    def __init__(self, config_path: Path | str) -> None:
        self.config_path = Path(config_path)

    def list(self) -> list[CommandPermissionOverride]:
        """Load ``[[permissions.command_overrides]]`` entries from TOML."""
        return command_overrides_from_config(_load_config(self.config_path))

    def save(self, overrides: Iterable[CommandPermissionOverride]) -> None:
        """Replace ``[[permissions.command_overrides]]`` and persist TOML atomically."""
        override_records = list(overrides)

        def update(config: dict[str, Any]) -> None:
            set_command_overrides_in_config(config, override_records)

        _atomic_update_toml(self.config_path, update)


def group_definitions_from_config(config: Mapping[str, Any]) -> list[PermissionGroupDefinition]:
    """Parse permission group definitions from an already-loaded TOML config dict."""
    permissions = _permissions_table(config)
    groups = permissions.get("groups", [])
    if not isinstance(groups, list):
        return []

    parsed: list[PermissionGroupDefinition] = []
    for raw in groups:
        if not isinstance(raw, Mapping):
            continue
        group_id = _str_or_empty(raw.get("id")).strip()
        if not group_id:
            continue
        name = _str_or_empty(raw.get("name"))
        permissions_value = raw.get("permissions", [])
        parsed.append(
            PermissionGroupDefinition(
                id=group_id,
                name=name,
                description=_str_or_empty(raw.get("description")),
                permissions=set(_string_values(permissions_value)),
                system=bool(raw.get("system", False)),
                protected=bool(raw.get("protected", False)),
            )
        )
    return _sort_groups(parsed)


def groups_from_config(config: Mapping[str, Any]) -> list[PermissionGroup]:
    """Parse runtime permission groups from an already-loaded TOML config dict."""
    return [group.to_permission_group() for group in group_definitions_from_config(config)]


def set_groups_in_config(
    config: dict[str, Any],
    groups: Iterable[PermissionGroup | PermissionGroupDefinition],
) -> None:
    """Replace ``permissions.groups`` in a config dict."""
    permissions = _ensure_permissions_table(config)
    permissions["groups"] = [_group_to_toml(group) for group in _sort_groups(groups)]


def bindings_from_config(config: Mapping[str, Any]) -> list[PermissionBindingRecord]:
    """Parse bindings from config, supporting legacy ``group`` and new ``groups`` fields."""
    permissions = _permissions_table(config)
    bindings = permissions.get("bindings", [])
    if isinstance(bindings, Mapping):
        return _sort_bindings(
            PermissionBindingRecord(
                key=str(key).strip(),
                groups=tuple(_unique_sorted(_string_values(value))),
            )
            for key, value in bindings.items()
            if str(key).strip()
        )
    if not isinstance(bindings, list):
        return []

    parsed: list[PermissionBindingRecord] = []
    for raw in bindings:
        if not isinstance(raw, Mapping):
            continue
        key = _str_or_empty(raw.get("key")).strip()
        if not key:
            continue

        groups = _groups_for_binding(raw)
        parsed.append(PermissionBindingRecord(key=key, groups=tuple(groups)))
    return _sort_bindings(parsed)


def set_bindings_in_config(
    config: dict[str, Any],
    bindings: Iterable[PermissionBindingRecord],
) -> None:
    """Replace ``permissions.bindings`` in a config dict using the new ``groups`` field."""
    permissions = _ensure_permissions_table(config)
    permissions["bindings"] = [
        {"key": binding.key, "groups": list(_unique_sorted(binding.groups))}
        for binding in _sort_bindings(bindings)
    ]


def command_overrides_from_config(config: Mapping[str, Any]) -> list[CommandPermissionOverride]:
    """Parse command permission overrides from an already-loaded TOML config dict."""
    permissions = _permissions_table(config)
    overrides = permissions.get("command_overrides", [])
    if not isinstance(overrides, list):
        return []

    parsed: list[CommandPermissionOverride] = []
    for raw in overrides:
        if not isinstance(raw, Mapping):
            continue
        command = _str_or_empty(raw.get("command")).strip()
        if not command:
            continue
        parsed.append(
            CommandPermissionOverride(
                command=command,
                permission=_str_or_empty(raw.get("permission")),
            )
        )
    return _sort_command_overrides(parsed)


def set_command_overrides_in_config(
    config: dict[str, Any],
    overrides: Iterable[CommandPermissionOverride],
) -> None:
    """Replace ``permissions.command_overrides`` in a config dict."""
    permissions = _ensure_permissions_table(config)
    permissions["command_overrides"] = [
        {"command": override.command, "permission": override.permission}
        for override in _sort_command_overrides(overrides)
    ]


def _load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    try:
        with config_path.open("rb") as file_obj:
            payload = tomllib.load(file_obj)
    except tomllib.TOMLDecodeError as exc:
        raise PermissionTomlError(f"Invalid TOML in {config_path}: {exc}") from exc
    if not isinstance(payload, dict):
        return {}
    return payload


def _atomic_write_toml(config_path: Path, config: Mapping[str, Any]) -> None:
    def replace_config(current_config: dict[str, Any]) -> None:
        current_config.clear()
        current_config.update(dict(config))

    _atomic_update_toml(config_path, replace_config)


def _atomic_update_toml[T](config_path: Path, update: Callable[[dict[str, Any]], T]) -> T:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _lock_path_for(config_path)
    with _toml_transaction_lock:
        with lock_path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                config = _load_config(config_path)
                result = update(config)
                _atomic_write_toml_inner(config_path, config)
                return result
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _atomic_write_toml_inner(config_path: Path, config: Mapping[str, Any]) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=config_path.parent,
            prefix=f".{config_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file_obj:
            temp_name = file_obj.name
            tomli_w.dump(dict(config), file_obj)
            file_obj.flush()
            os.fsync(file_obj.fileno())
        Path(temp_name).replace(config_path)
    except Exception as exc:
        if temp_name:
            try:
                Path(temp_name).unlink(missing_ok=True)
            except OSError:
                pass
        raise PermissionTomlError(f"Failed to persist permissions TOML {config_path}: {exc}") from exc


def _lock_path_for(config_path: Path) -> Path:
    return config_path.parent / f".{config_path.name}.lock"


def _permissions_table(config: Mapping[str, Any]) -> Mapping[str, Any]:
    permissions = config.get("permissions", {})
    if isinstance(permissions, Mapping):
        return permissions
    return {}


def _ensure_permissions_table(config: dict[str, Any]) -> dict[str, Any]:
    permissions = config.get("permissions")
    if not isinstance(permissions, dict):
        permissions = {}
        config["permissions"] = permissions
    return permissions


def _groups_for_binding(raw: Mapping[str, Any]) -> tuple[str, ...]:
    groups_value = raw.get("groups")
    if groups_value is not None:
        groups = _string_values(groups_value)
    else:
        groups = _string_values([raw.get("group")])
    return tuple(_unique_sorted(groups))


def _string_values(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, Mapping)):
        candidates = list(value)
    else:
        candidates = []
    return tuple(str(item).strip() for item in candidates if str(item).strip())


def _str_or_empty(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _unique_sorted(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(dict.fromkeys(value for value in values if value)))


def _group_to_toml(group: PermissionGroup | PermissionGroupDefinition) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": group.id,
        "name": group.name,
        "permissions": sorted(group.permissions),
    }
    if isinstance(group, PermissionGroupDefinition):
        if group.description:
            entry["description"] = group.description
        if group.system:
            entry["system"] = True
        if group.protected:
            entry["protected"] = True
    return entry


def _sort_groups(
    groups: Iterable[PermissionGroup | PermissionGroupDefinition],
) -> list[PermissionGroup | PermissionGroupDefinition]:
    return sorted(groups, key=lambda group: group.id)


def _sort_bindings(bindings: Iterable[PermissionBindingRecord]) -> list[PermissionBindingRecord]:
    return sorted(bindings, key=lambda binding: binding.key)


def _sort_command_overrides(
    overrides: Iterable[CommandPermissionOverride],
) -> list[CommandPermissionOverride]:
    return sorted(overrides, key=lambda override: override.command)
