"""Permission system — RBAC with session-scoped and global-scoped bindings.

Implements the permission specification (permission_system.md).

Permission model:
  - Permissions are dot-separated tree paths: "tools.weather", "sys.reboot"
  - Wildcard "*" matches all permissions at or below a level
  - Negative permissions "-tools.weather" explicitly deny access
  - Final set = Identity-global | Session-local | Session-base
  - Explicit deny takes precedence over any grant
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

BOT_ADMIN_BINDING_PREFIX = "__bot_admin__:"


def candidate_user_keys(user_id: str) -> tuple[str, ...]:
    """Return supported user binding key variants for permission lookup."""
    normalized = str(user_id or "").strip()
    if not normalized:
        return ()
    keys = [normalized]
    if ":" in normalized:
        keys.append(normalized.rsplit(":", 1)[-1])
    return tuple(dict.fromkeys(keys))


class PermissionGroup(BaseModel):
    """A named set of permission nodes that can be assigned to users/sessions."""

    id: str
    name: str = ""
    permissions: set[str] = Field(default_factory=set)

    model_config = {"extra": "forbid"}

    def grant(self, permission: str) -> None:
        """Add a permission node to the group."""
        self.permissions.add(permission)

    def revoke(self, permission: str) -> None:
        """Remove a permission node from the group."""
        self.permissions.discard(permission)

    def deny(self, permission: str) -> None:
        """Add an explicit deny (negative permission)."""
        self.permissions.add(f"-{permission}")


# ── Built-in groups ──────────────────────────────────────────────────

DEFAULT_GROUP = PermissionGroup(
    id="default",
    name="Default",
    permissions={"cmd.help", "cmd.ping", "cmd.about", "cmd.whoami"},
)

ADMIN_GROUP = PermissionGroup(
    id="admin",
    name="Admin",
    permissions={"tools.*", "sys.*", "cmd.*"},
)

OWNER_GROUP = PermissionGroup(
    id="owner",
    name="Owner",
    permissions={"*"},
)


# ── Permission checking logic ────────────────────────────────────────


def check_permission(required: str, granted: set[str]) -> bool:
    """Check if a required permission is satisfied by a granted set.

    Algorithm:
    1. Check explicit deny: if "-{required}" or any deny wildcard matches → False
    2. Check explicit grant: if "{required}" is in set → True
    3. Check wildcard grants: walk up the tree, check for wildcards → True
    4. Otherwise → False
    """
    # Phase 1: Check for explicit denials
    if f"-{required}" in granted:
        return False

    # Check wildcard denials: -tools.* should deny tools.weather
    parts = required.split(".")
    for i in range(len(parts)):
        prefix = ".".join(parts[:i])
        deny_wildcard = f"-{prefix}.*" if prefix else "-*"
        if deny_wildcard in granted:
            return False

    # Phase 2: Check for explicit grant
    if required in granted:
        return True

    # Phase 3: Check wildcard grants
    # Global wildcard
    if "*" in granted:
        return True

    # Walk up the permission tree
    for i in range(len(parts)):
        prefix = ".".join(parts[:i])
        wildcard = f"{prefix}.*" if prefix else "*"
        if wildcard in granted:
            return True

    return False


def merge_permissions(*groups: PermissionGroup | set[str]) -> set[str]:
    """Merge multiple permission groups/sets into a single unified set.

    Per spec: FinalPermissions = Global | Session-local | Session-base
    This is a simple union of all permission nodes.
    """
    result: set[str] = set()
    for group in groups:
        if isinstance(group, PermissionGroup):
            result |= group.permissions
        else:
            result |= group
    return result


# ── User permission bindings ─────────────────────────────────────────


class PermissionBinding(BaseModel):
    """Maps a binding key to a permission group ID.

    Binding key formats:
      - Session-scoped: "{identity_id}:{session_key}.{user_id}"
      - Global-scoped:  "{identity_id}:{user_id}"
    """

    key: str
    group_id: str


class PermissionEngine:
    """Manages permission groups, bindings, and resolution.

    Resolves a user's effective permissions by merging:
    1. Global binding: {identity_id}:{user_id}
    2. Session-local binding: {identity_session_id}.{user_id}
    3. Session-base: the session's default permission group
    """

    def __init__(self) -> None:
        """Initialize the engine with built-in permission groups."""
        self._lock = threading.RLock()
        self._groups: dict[str, PermissionGroup] = {}
        self._bindings: dict[str, set[str]] = {}  # binding_key -> group_ids

        # Register built-in groups
        for group in (DEFAULT_GROUP, ADMIN_GROUP, OWNER_GROUP):
            self._groups[group.id] = group.model_copy(deep=True)

    # ── Group management ─────────────────────────────────────────────

    def add_group(self, group: PermissionGroup) -> None:
        """Register a permission group.

        Args:
            group: The permission group to register.
        """
        with self._lock:
            self._groups[group.id] = group

    def get_group(self, group_id: str) -> PermissionGroup | None:
        """Look up a permission group by ID.

        Args:
            group_id: ID of the group to retrieve.

        Returns:
            The matching PermissionGroup, or None if not found.
        """
        with self._lock:
            return self._groups.get(group_id)

    def remove_group(self, group_id: str) -> None:
        """Delete a permission group.

        Args:
            group_id: ID of the group to remove. Silently ignored if missing.
        """
        with self._lock:
            self._groups.pop(group_id, None)

    @property
    def all_groups(self) -> list[PermissionGroup]:
        """Return all registered permission groups."""
        with self._lock:
            return list(self._groups.values())

    # ── Binding management ───────────────────────────────────────────

    def bind(self, key: str, group_id: str) -> None:
        """Bind a user/session scope to one permission group.

        This compatibility API replaces any existing groups for ``key`` with a
        single group, matching the old one-key-to-one-group behavior.

        Args:
            key: Either "{session_id}.{user_id}" or "{instance_id}:{user_id}"
            group_id: ID of the permission group to assign.
        """
        with self._lock:
            if group_id not in self._groups:
                raise ValueError(f"Unknown permission group: {group_id!r}")
            self._bindings[key] = {group_id}

    def bind_group(self, key: str, group_id: str, source: str = "manual") -> None:
        """Add one permission group to a user/session scope binding."""
        with self._lock:
            if group_id not in self._groups:
                raise ValueError(f"Unknown permission group: {group_id!r}")
            self._bindings.setdefault(key, set()).add(group_id)

    def unbind(self, key: str) -> None:
        """Remove a binding between a key and a permission group.

        Args:
            key: The binding key to remove. Silently ignored if missing.
        """
        with self._lock:
            self._bindings.pop(key, None)

    def unbind_group(self, key: str, group_id: str, source: str | None = None) -> None:
        """Remove one permission group from a user/session scope binding."""
        with self._lock:
            group_ids = self._bindings.get(key)
            if not group_ids:
                return
            group_ids.discard(group_id)
            if not group_ids:
                self._bindings.pop(key, None)

    def get_binding(self, key: str) -> str | None:
        """Look up which group a binding key is mapped to.

        Args:
            key: The binding key to query.

        Returns:
            The group ID, or None if no binding exists.
        """
        groups = self.groups_for_key(key)
        return groups[0] if groups else None

    def groups_for_key(self, key: str) -> tuple[str, ...]:
        """Return all permission groups bound to a key in stable order."""
        with self._lock:
            return tuple(sorted(self._bindings.get(key, set())))

    def set_groups_for_key(self, key: str, group_ids: Iterable[str]) -> None:
        """Replace all permission groups bound to a key."""
        with self._lock:
            normalized_group_ids = set(group_ids)
            unknown_group_ids = sorted(
                group_id for group_id in normalized_group_ids if group_id not in self._groups
            )
            if unknown_group_ids:
                raise ValueError(f"Unknown permission group: {unknown_group_ids[0]!r}")
            if normalized_group_ids:
                self._bindings[key] = normalized_group_ids
            else:
                self._bindings.pop(key, None)

    def binding_keys(self) -> tuple[str, ...]:
        """Return all registered binding keys."""
        with self._lock:
            return tuple(self._bindings.keys())

    def replace_runtime_state(
        self,
        groups: dict[str, PermissionGroup],
        bindings: dict[str, set[str]],
    ) -> None:
        """Atomically replace runtime groups and bindings."""
        with self._lock:
            self._groups = groups
            self._bindings = bindings

    def _permission_layers_for_key(self, key: str) -> list[set[str]]:
        """Return permission sets for all existing groups bound to one key."""
        with self._lock:
            layers: list[set[str]] = []
            for group_id in self.groups_for_key(key):
                group = self._groups.get(group_id)
                if group is not None:
                    layers.append(group.permissions)
            return layers

    # ── Resolution ───────────────────────────────────────────────────

    def resolve(
        self,
        instance_id: str,
        session_id: str,
        user_id: str,
        session_base_group: str = "default",
    ) -> set[str]:
        """Compute the merged permission set for a user in a session context.

        Merges three layers:
        1. Global: {instance_id}:{user_id}
        2. Session-local: {session_id}.{user_id}
        3. Session-base: the session's default permission group

        The parameter is still named ``instance_id`` for API compatibility, but
        callers may pass a bot id as the permission identity.
        """
        with self._lock:
            layers: list[set[str]] = []

            for candidate_user_id in candidate_user_keys(user_id):
                global_key = f"{instance_id}:{candidate_user_id}"
                layers.extend(self._permission_layers_for_key(global_key))

                bot_admin_key = f"{BOT_ADMIN_BINDING_PREFIX}{instance_id}:{candidate_user_id}"
                layers.extend(self._permission_layers_for_key(bot_admin_key))

            # Layer 2: Session-local binding
            for candidate_user_id in candidate_user_keys(user_id):
                session_key = f"{session_id}.{candidate_user_id}"
                layers.extend(self._permission_layers_for_key(session_key))

            # Layer 3: Session-base group
            base_group = self._groups.get(session_base_group)
            if base_group:
                layers.append(base_group.permissions)

            return merge_permissions(*layers)

    def check(
        self,
        required: str,
        instance_id: str,
        session_id: str,
        user_id: str,
        session_base_group: str = "default",
    ) -> bool:
        """Check if a user has a specific permission in context."""
        merged = self.resolve(instance_id, session_id, user_id, session_base_group)
        return check_permission(required, merged)
