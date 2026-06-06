"""Administrative service for permission groups, bindings, and command overrides."""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from pydantic import BaseModel, Field

from shinbot.core.security.permission import (
    ADMIN_GROUP,
    BOT_ADMIN_BINDING_PREFIX,
    DEFAULT_GROUP,
    OWNER_GROUP,
    PermissionEngine,
    PermissionGroup,
)
from shinbot.core.security.permission_toml import (
    CommandPermissionOverride,
    PermissionGroupDefinition,
    _atomic_update_toml,
    _load_config,
    bindings_from_config,
    command_overrides_from_config,
    group_definitions_from_config,
    set_bindings_in_config,
    set_command_overrides_in_config,
    set_groups_in_config,
)
from shinbot.core.security.permission_toml import (
    PermissionBindingRecord as TomlPermissionBindingRecord,
)
from shinbot.utils.logger import format_log_event

logger = logging.getLogger(__name__)
permission_audit_logger = logging.getLogger("shinbot.audit")

GROUP_ID_RE = re.compile(r"^[a-zA-Z0-9_.:-]+$")
PERMISSION_NODE_RE = re.compile(r"^-?(?:\*|[a-zA-Z0-9_:-]+(?:\.[a-zA-Z0-9_:-]+)*(?:\.\*)?)$")
BUILTIN_GROUP_IDS = frozenset({"default", "admin", "owner"})


class PermissionServiceError(ValueError):
    """Structured error for permission group administration."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class _NoopPermissionMutation(Exception):
    """Internal sentinel used to skip persistence for no-op removals."""


class PermissionGroupRecord(BaseModel):
    """Editable permission group definition."""

    id: str
    name: str = ""
    description: str = ""
    permissions: set[str] = Field(default_factory=set)
    orphan_permissions: set[str] = Field(default_factory=set, alias="orphanPermissions")
    system: bool = False
    protected: bool = False

    model_config = {"extra": "forbid", "populate_by_name": True}

    @classmethod
    def from_engine_group(cls, group: PermissionGroup) -> PermissionGroupRecord:
        """Create a service record from the runtime engine model."""
        return cls(
            id=group.id,
            name=group.name,
            permissions=set(group.permissions),
            system=group.id in BUILTIN_GROUP_IDS,
            protected=group.id in BUILTIN_GROUP_IDS,
        )

    def to_engine_group(self) -> PermissionGroup:
        """Convert to the runtime engine model."""
        return PermissionGroup(
            id=self.id,
            name=self.name,
            permissions=set(self.permissions),
        )


@dataclass(frozen=True)
class PermissionBindingRecord:
    """Maps a binding key to one or more permission group IDs."""

    key: str
    groups: tuple[str, ...]


@dataclass(frozen=True)
class CommandPermissionOverrideRecord:
    """Persisted command permission override."""

    command: str
    permission: str


@dataclass
class PermissionStoreSnapshot:
    """In-memory representation of TOML-backed permission administration state."""

    groups: dict[str, PermissionGroupRecord] = field(default_factory=dict)
    bindings: dict[str, tuple[str, ...]] = field(default_factory=dict)
    command_overrides: dict[str, str] = field(default_factory=dict)


class PermissionConfigRepository(Protocol):
    """Repository contract used by PermissionGroupService.

    Sub-agent 1 can replace the bundled TOML implementation with narrower
    repositories as long as this service-facing contract remains available.
    """

    def load(self) -> PermissionStoreSnapshot:
        """Load permission administration state."""

    def save(self, snapshot: PermissionStoreSnapshot) -> None:
        """Persist permission administration state."""

    def update[T](
        self,
        mutator: Callable[[PermissionStoreSnapshot], T],
    ) -> tuple[PermissionStoreSnapshot, T]:
        """Atomically load, mutate, and persist permission administration state."""


class TomlPermissionConfigRepository:
    """TOML repository for permission groups, bindings, and command overrides."""

    def __init__(self, config_path: Path | str) -> None:
        self.config_path = Path(config_path)

    def load(self) -> PermissionStoreSnapshot:
        """Read permission state from the configured TOML file."""
        payload = _load_config(self.config_path)
        return self._snapshot_from_payload(payload)

    def save(self, snapshot: PermissionStoreSnapshot) -> None:
        """Write permission state to TOML via temp file and atomic rename."""

        def update(payload: dict[str, Any]) -> None:
            self._write_snapshot_to_payload(payload, snapshot)

        _atomic_update_toml(self.config_path, update)

    def update[T](
        self,
        mutator: Callable[[PermissionStoreSnapshot], T],
    ) -> tuple[PermissionStoreSnapshot, T]:
        """Atomically load, mutate, and persist permission state."""

        def update(payload: dict[str, Any]) -> tuple[PermissionStoreSnapshot, T]:
            snapshot = self._snapshot_from_payload(payload)
            result = mutator(snapshot)
            self._write_snapshot_to_payload(payload, snapshot)
            return snapshot, result

        return _atomic_update_toml(self.config_path, update)

    def _snapshot_from_payload(self, payload: dict[str, Any]) -> PermissionStoreSnapshot:
        permissions = payload.get("permissions", {})
        if not isinstance(permissions, dict):
            return PermissionStoreSnapshot()

        groups: dict[str, PermissionGroupRecord] = {}
        for item in group_definitions_from_config(payload):
            group_id = item.id
            groups[group_id] = PermissionGroupRecord(
                id=group_id,
                name=item.name,
                description=item.description,
                permissions=set(item.permissions),
                system=item.system,
                protected=item.protected,
            )

        bindings: dict[str, tuple[str, ...]] = {}
        for item in bindings_from_config(payload):
            bindings[item.key] = item.groups

        command_overrides: dict[str, str] = {}
        for item in command_overrides_from_config(payload):
            command_overrides[item.command] = item.permission

        return PermissionStoreSnapshot(
            groups=groups,
            bindings=bindings,
            command_overrides=command_overrides,
        )

    def _write_snapshot_to_payload(
        self,
        payload: dict[str, Any],
        snapshot: PermissionStoreSnapshot,
    ) -> None:
        set_groups_in_config(
            payload,
            (
                PermissionGroupDefinition(
                    id=group.id,
                    name=group.name,
                    description=group.description,
                    permissions=set(group.permissions),
                    system=group.system,
                    protected=group.protected,
                )
                for group in snapshot.groups.values()
            )
        )
        set_bindings_in_config(
            payload,
            (
                TomlPermissionBindingRecord(key=key, groups=groups)
                for key, groups in snapshot.bindings.items()
            ),
        )
        set_command_overrides_in_config(
            payload,
            (
                CommandPermissionOverride(command=command, permission=permission)
                for command, permission in snapshot.command_overrides.items()
            ),
        )


class PermissionGroupService:
    """Service layer for permission group administration."""

    def __init__(
        self,
        *,
        repository: PermissionConfigRepository,
        engine: PermissionEngine | None = None,
        command_registry: Any | None = None,
        actor: str = "system",
    ) -> None:
        self._repository = repository
        self._engine = engine
        self._command_registry = command_registry
        self._actor = actor
        self._lock = threading.Lock()
        self._snapshot = repository.load()
        self._ensure_builtin_groups(self._snapshot)
        self._validate_snapshot(self._snapshot)

    @classmethod
    def from_config_path(
        cls,
        config_path: Path | str,
        *,
        engine: PermissionEngine | None = None,
        command_registry: Any | None = None,
        actor: str = "system",
    ) -> PermissionGroupService:
        """Create a service backed by ``[permissions]`` in a TOML config file."""
        return cls(
            repository=TomlPermissionConfigRepository(config_path),
            engine=engine,
            command_registry=command_registry,
            actor=actor,
        )

    def list_groups(self) -> list[PermissionGroupRecord]:
        """Return all groups sorted by ID."""
        return [
            self._group_record_copy(self._snapshot.groups[group_id])
            for group_id in sorted(self._snapshot.groups)
        ]

    def get_group(self, group_id: str) -> PermissionGroupRecord | None:
        """Return one group by ID."""
        group = self._snapshot.groups.get(group_id)
        return self._group_record_copy(group) if group else None

    def create_group(
        self,
        *,
        group_id: str,
        name: str = "",
        description: str = "",
        permissions: set[str] | list[str] | tuple[str, ...] = (),
        protected: bool = False,
    ) -> PermissionGroupRecord:
        """Create a custom permission group."""
        group_id = _validate_group_id(group_id)
        if group_id in BUILTIN_GROUP_IDS:
            raise PermissionServiceError("BUILTIN_GROUP", f"Built-in group {group_id!r} already exists")
        permission_set = _validate_permissions(permissions)

        def mutate(snapshot: PermissionStoreSnapshot) -> PermissionGroupRecord:
            if group_id in snapshot.groups:
                raise PermissionServiceError("GROUP_EXISTS", f"Permission group {group_id!r} already exists")
            group = PermissionGroupRecord(
                id=group_id,
                name=name,
                description=description,
                permissions=set(permission_set),
                protected=protected,
            )
            snapshot.groups[group_id] = group
            return group

        group = self._persist_and_refresh(mutate, "permission_group.create", group=group_id)
        return self._group_record_copy(group)

    def update_group(
        self,
        group_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        permissions: set[str] | list[str] | tuple[str, ...] | None = None,
        protected: bool | None = None,
    ) -> PermissionGroupRecord:
        """Update group metadata and permissions."""
        group_id = _validate_group_id(group_id)
        next_permissions = _validate_permissions(permissions) if permissions is not None else None

        def mutate(snapshot: PermissionStoreSnapshot) -> PermissionGroupRecord:
            group = self._require_group(group_id, snapshot)
            if name is not None:
                group.name = name
            if description is not None:
                group.description = description
            if next_permissions is not None:
                candidate_permissions = set(next_permissions)
                if group.id in BUILTIN_GROUP_IDS:
                    negative_perms = {p for p in candidate_permissions if p.startswith("-")}
                    if negative_perms:
                        raise PermissionServiceError(
                            "BUILTIN_NEGATIVE_PERMISSION",
                            (
                                f"Built-in group {group.id!r} must not contain negative permissions: "
                                f"{', '.join(sorted(negative_perms))}"
                            ),
                        )
                    missing_permissions = _builtin_group_permissions(group.id) - candidate_permissions
                    if missing_permissions:
                        raise PermissionServiceError(
                            "BUILTIN_PERMISSION_REQUIRED",
                            (
                                f"Built-in group {group.id!r} must keep permissions: "
                                f"{', '.join(sorted(missing_permissions))}"
                            ),
                        )
                group.permissions = candidate_permissions
            if protected is not None:
                if group.id in BUILTIN_GROUP_IDS and protected is False:
                    raise PermissionServiceError(
                        "BUILTIN_PROTECTION_REQUIRED",
                        f"Built-in group {group.id!r} cannot be unprotected",
                    )
                group.protected = protected
            return group

        group = self._persist_and_refresh(mutate, "permission_group.update", group=group_id)
        return self._group_record_copy(group)

    def delete_group(self, group_id: str) -> None:
        """Delete a custom permission group."""
        group_id = _validate_group_id(group_id)

        def mutate(snapshot: PermissionStoreSnapshot) -> None:
            group = self._require_group(group_id, snapshot)
            if group.id in BUILTIN_GROUP_IDS or group.system or group.protected:
                raise PermissionServiceError("GROUP_PROTECTED", f"Permission group {group.id!r} is protected")
            del snapshot.groups[group.id]
            for key, groups in list(snapshot.bindings.items()):
                remaining = tuple(candidate for candidate in groups if candidate != group.id)
                if remaining:
                    snapshot.bindings[key] = remaining
                else:
                    del snapshot.bindings[key]

        self._persist_and_refresh(mutate, "permission_group.delete", group=group_id)

    def list_bindings(
        self,
        *,
        scope_key: str | None = None,
        group_id: str | None = None,
    ) -> list[PermissionBindingRecord]:
        """List bindings, optionally filtered by key or group."""
        if group_id is not None:
            self._require_group(group_id)
        records: list[PermissionBindingRecord] = []
        for key, groups in sorted(self._snapshot.bindings.items(), key=lambda item: item[0]):
            if scope_key is not None and key != scope_key:
                continue
            if group_id is not None and group_id not in groups:
                continue
            records.append(PermissionBindingRecord(key=key, groups=groups))
        return records

    def set_binding(
        self,
        scope_key: str,
        group_ids: set[str] | list[str] | tuple[str, ...],
    ) -> PermissionBindingRecord:
        """Replace one binding key's group membership."""
        scope_key = _validate_binding_key(scope_key)
        groups = tuple(_unique_sorted(_validate_group_id(group_id) for group_id in group_ids))
        if not groups:
            raise PermissionServiceError("EMPTY_BINDING", "Binding must reference at least one group")

        def mutate(snapshot: PermissionStoreSnapshot) -> PermissionBindingRecord:
            for group_id in groups:
                self._require_group(group_id, snapshot)
            snapshot.bindings[scope_key] = groups
            return PermissionBindingRecord(key=scope_key, groups=groups)

        return self._persist_and_refresh(
            mutate,
            "permission_binding.set",
            binding=scope_key,
            groups=",".join(groups),
        )

    def remove_binding(self, scope_key: str, group_id: str | None = None) -> None:
        """Remove a whole binding key or one group from the binding."""
        scope_key = _validate_binding_key(scope_key)
        if group_id is not None:
            group_id = _validate_group_id(group_id)

        def mutate(snapshot: PermissionStoreSnapshot) -> None:
            if scope_key not in snapshot.bindings:
                raise _NoopPermissionMutation
            if group_id is None:
                del snapshot.bindings[scope_key]
                return

            groups = tuple(group for group in snapshot.bindings[scope_key] if group != group_id)
            if groups:
                snapshot.bindings[scope_key] = groups
            else:
                del snapshot.bindings[scope_key]

        if group_id is None:
            self._persist_and_refresh(mutate, "permission_binding.remove", binding=scope_key)
            return
        self._persist_and_refresh(
            mutate,
            "permission_binding.remove_group",
            binding=scope_key,
            group=group_id,
        )

    def list_command_overrides(self) -> list[CommandPermissionOverrideRecord]:
        """Return command permission overrides sorted by command name."""
        return [
            CommandPermissionOverrideRecord(command=command, permission=permission)
            for command, permission in sorted(self._snapshot.command_overrides.items())
        ]

    def set_command_override(self, command: str, permission: str) -> CommandPermissionOverrideRecord:
        """Set a command permission override."""
        command = _validate_command_name(command)
        if permission:
            _validate_permission_node(permission)

        def mutate(snapshot: PermissionStoreSnapshot) -> CommandPermissionOverrideRecord:
            snapshot.command_overrides[command] = permission
            return CommandPermissionOverrideRecord(command=command, permission=permission)

        return self._persist_and_refresh(
            mutate,
            "permission_command_override.set",
            command=command,
            permission=permission,
        )

    def remove_command_override(self, command: str) -> None:
        """Remove a command permission override."""
        command = _validate_command_name(command)

        def mutate(snapshot: PermissionStoreSnapshot) -> None:
            if command not in snapshot.command_overrides:
                raise _NoopPermissionMutation
            del snapshot.command_overrides[command]

        self._persist_and_refresh(mutate, "permission_command_override.remove", command=command)

    def detect_orphan_permissions(self, command_registry: Any) -> list[str]:
        """Find positive permission nodes in groups that no command currently requires."""
        command_permissions = _required_command_permission_nodes(command_registry)
        orphan: list[str] = []
        for group in self.list_groups():
            for permission in sorted(group.permissions):
                if permission.startswith("-"):
                    continue
                if permission not in command_permissions:
                    orphan.append(f"{group.id}:{permission}")
        return orphan

    def refresh_engine(self, engine: PermissionEngine | None = None) -> None:
        """Refresh the runtime permission engine from the current snapshot.

        Builds complete new group and binding dicts, then atomically replaces
        the engine internals to avoid mid-update inconsistent reads.
        """
        target = engine or self._engine
        if target is None:
            return

        # Build complete new snapshots
        new_groups: dict[str, PermissionGroup] = {}
        for group in self._snapshot.groups.values():
            engine_group = group.to_engine_group()
            new_groups[engine_group.id] = engine_group

        new_bindings: dict[str, set[str]] = {}
        # Preserve bot_admin bindings from the existing engine state
        for key in target.binding_keys():
            if key.startswith(BOT_ADMIN_BINDING_PREFIX):
                new_bindings[key] = set(target.groups_for_key(key))

        for key, groups in self._snapshot.bindings.items():
            if key.startswith(BOT_ADMIN_BINDING_PREFIX):
                continue
            new_bindings[key] = set(groups)

        target.replace_runtime_state(new_groups, new_bindings)

    def refresh_command_registry(self, command_registry: Any | None = None) -> None:
        """Refresh runtime command permission overrides from the current snapshot."""
        target = command_registry or self._command_registry
        if target is None:
            return

        current_overrides: dict[str, str] = {}
        list_overrides = getattr(target, "list_permission_overrides", None)
        if callable(list_overrides):
            current_overrides = dict(list_overrides())

        clear_override = getattr(target, "clear_permission_override", None)
        if callable(clear_override):
            for command in set(current_overrides) - set(self._snapshot.command_overrides):
                clear_override(command)

        set_override = getattr(target, "set_permission_override", None)
        if callable(set_override):
            for command, permission in self._snapshot.command_overrides.items():
                set_override(command, permission)

    def _persist_and_refresh[T](
        self,
        mutator: Callable[[PermissionStoreSnapshot], T],
        event: str,
        **fields: str,
    ) -> T:
        with self._lock:
            def update(snapshot: PermissionStoreSnapshot) -> T:
                self._ensure_builtin_groups(snapshot)
                result = mutator(snapshot)
                self._validate_snapshot(snapshot)
                return result

            try:
                persisted_snapshot, result = self._repository.update(update)
            except _NoopPermissionMutation:
                return cast(T, None)
            self._snapshot = persisted_snapshot
            self.refresh_engine()
            self.refresh_command_registry()
            _log_permission_admin_event(event, actor=self._actor, **fields)
            return result

    def _ensure_builtin_groups(self, snapshot: PermissionStoreSnapshot) -> None:
        for builtin in (DEFAULT_GROUP, ADMIN_GROUP, OWNER_GROUP):
            existing = snapshot.groups.get(builtin.id)
            if existing is None:
                snapshot.groups[builtin.id] = PermissionGroupRecord.from_engine_group(builtin)
                continue
            existing.system = True
            existing.protected = True
            existing.name = existing.name or builtin.name
            existing.permissions |= set(builtin.permissions)

        owner = snapshot.groups["owner"]
        owner.permissions.add("*")

    def _validate_snapshot(self, snapshot: PermissionStoreSnapshot) -> None:
        for group in snapshot.groups.values():
            _validate_group_id(group.id)
            group.permissions = _validate_permissions(group.permissions)
            if group.id == "owner" and "*" not in group.permissions:
                raise PermissionServiceError("OWNER_WILDCARD_REQUIRED", "Owner group must keep '*'")
            if group.id in BUILTIN_GROUP_IDS:
                group.system = True
                group.protected = True
                group.permissions -= {p for p in group.permissions if p.startswith("-")}
                group.permissions |= _builtin_group_permissions(group.id)

        for key, groups in list(snapshot.bindings.items()):
            _validate_binding_key(key)
            unique_groups = tuple(_unique_sorted(_validate_group_id(group_id) for group_id in groups))
            if not unique_groups:
                del snapshot.bindings[key]
                continue
            for group_id in unique_groups:
                self._require_group(group_id, snapshot)
            snapshot.bindings[key] = unique_groups

        for command, permission in snapshot.command_overrides.items():
            _validate_command_name(command)
            if permission:
                _validate_permission_node(permission)

    def _require_group(
        self,
        group_id: str,
        snapshot: PermissionStoreSnapshot | None = None,
    ) -> PermissionGroupRecord:
        group_id = _validate_group_id(group_id)
        target_snapshot = snapshot or self._snapshot
        group = target_snapshot.groups.get(group_id)
        if group is None:
            raise PermissionServiceError("GROUP_NOT_FOUND", f"Permission group {group_id!r} not found")
        return group

    def _group_record_copy(self, group: PermissionGroupRecord) -> PermissionGroupRecord:
        record = group.model_copy(deep=True)
        record.orphan_permissions = _orphan_permissions_for_group(
            record.permissions,
            _known_permission_nodes(self._command_registry),
        )
        return record


def _validate_group_id(value: str) -> str:
    value = str(value).strip()
    if not value or GROUP_ID_RE.fullmatch(value) is None:
        raise PermissionServiceError(
            "INVALID_GROUP_ID",
            "Permission group id must match [a-zA-Z0-9_.:-]+",
        )
    return value


def _validate_permission_node(value: str) -> str:
    value = str(value).strip()
    if not value or PERMISSION_NODE_RE.fullmatch(value) is None:
        raise PermissionServiceError("INVALID_PERMISSION", f"Invalid permission node: {value!r}")
    return value


def _validate_permissions(values: set[str] | list[str] | tuple[str, ...]) -> set[str]:
    return {_validate_permission_node(value) for value in values}


def _known_permission_nodes(command_registry: Any | None) -> set[str]:
    if command_registry is None:
        return set()

    declared_permission_nodes = getattr(command_registry, "declared_permission_nodes", None)
    if callable(declared_permission_nodes):
        return set(declared_permission_nodes())

    known: set[str] = set()
    commands = getattr(command_registry, "all_commands", ())
    default_permission_for = getattr(command_registry, "default_permission_for", None)
    for command in commands:
        permission = str(getattr(command, "permission", "") or "").strip()
        if permission:
            known.add(permission)
        if callable(default_permission_for):
            default_permission = str(default_permission_for(command.name) or "").strip()
            if default_permission:
                known.add(default_permission)
    list_permission_overrides = getattr(command_registry, "list_permission_overrides", None)
    if callable(list_permission_overrides):
        for permission in list_permission_overrides().values():
            permission = str(permission or "").strip()
            if permission:
                known.add(permission)
    return known


def _required_command_permission_nodes(command_registry: Any | None) -> set[str]:
    if command_registry is None:
        return set()
    return {
        str(getattr(command, "permission", "") or "").strip()
        for command in getattr(command_registry, "all_commands", ())
        if str(getattr(command, "permission", "") or "").strip()
    }


def _orphan_permissions_for_group(permissions: set[str], known_permissions: set[str]) -> set[str]:
    if not known_permissions:
        return set()

    orphan_permissions: set[str] = set()
    for permission in permissions:
        if permission.startswith("-"):
            continue
        normalized = permission[1:] if permission.startswith("-") else permission
        if normalized == "*" or normalized.endswith(".*"):
            continue
        if not normalized.startswith("cmd."):
            continue
        if normalized not in known_permissions:
            orphan_permissions.add(permission)
    return orphan_permissions


def _validate_binding_key(value: str) -> str:
    value = str(value).strip()
    if not value:
        raise PermissionServiceError("INVALID_BINDING_KEY", "Binding key cannot be empty")
    return value


def _validate_command_name(value: str) -> str:
    value = str(value).strip()
    if not value:
        raise PermissionServiceError("INVALID_COMMAND", "Command name cannot be empty")
    return value


def _builtin_group_permissions(group_id: str) -> set[str]:
    if group_id == "default":
        return set(DEFAULT_GROUP.permissions)
    if group_id == "admin":
        return set(ADMIN_GROUP.permissions)
    if group_id == "owner":
        return set(OWNER_GROUP.permissions)
    return set()


def _list_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple | set):
        return list(value)
    return [value]


def _unique_sorted(values: Any) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


def _log_permission_admin_event(event: str, *, actor: str, **fields: str) -> None:
    permission_audit_logger.info(
        format_log_event(
            event,
            actor=actor,
            **fields,
        )
    )


__all__ = [
    "BUILTIN_GROUP_IDS",
    "CommandPermissionOverrideRecord",
    "PermissionBindingRecord",
    "PermissionConfigRepository",
    "PermissionGroupRecord",
    "PermissionGroupService",
    "PermissionServiceError",
    "PermissionStoreSnapshot",
    "TomlPermissionConfigRepository",
]
