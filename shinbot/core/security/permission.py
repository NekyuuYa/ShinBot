"""Permission system — RBAC with session-scoped and global-scoped bindings.

Implements the permission specification (05_permission_system.md).

Permission model:
  - Permissions are dot-separated tree paths: "tools.weather", "sys.reboot"
  - Wildcard "*" matches all permissions at or below a level
  - Negative permissions "-tools.weather" explicitly deny access
  - Final set = Global | Session-local | Session-base
  - Explicit deny takes precedence over any grant
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PermissionGroup(BaseModel):
    """A named set of permission nodes that can be assigned to users/sessions."""

    id: str
    name: str = ""
    permissions: set[str] = Field(default_factory=set)

    model_config = {"extra": "forbid"}

    def grant(self, permission: str) -> None:
        self.permissions.add(permission)

    def revoke(self, permission: str) -> None:
        self.permissions.discard(permission)

    def deny(self, permission: str) -> None:
        """Add an explicit deny (negative permission)."""
        self.permissions.add(f"-{permission}")


# ── Built-in groups ──────────────────────────────────────────────────

DEFAULT_GROUP = PermissionGroup(
    id="default",
    name="Default",
    permissions={"cmd.help", "cmd.ping"},
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
      - Session-scoped: "{session_id}.{user_id}"
      - Global-scoped:  "{instance_id}:{user_id}"
    """

    key: str
    group_id: str


class PermissionEngine:
    """Manages permission groups, bindings, and resolution.

    Resolves a user's effective permissions by merging:
    1. Global binding: {instance_id}:{user_id}
    2. Session-local binding: {session_id}.{user_id}
    3. Session-base: the session's default permission group
    """

    def __init__(self) -> None:
        self._groups: dict[str, PermissionGroup] = {}
        self._bindings: dict[str, str] = {}  # binding_key → group_id

        # Register built-in groups
        for group in (DEFAULT_GROUP, ADMIN_GROUP, OWNER_GROUP):
            self._groups[group.id] = group.model_copy(deep=True)

    # ── Group management ─────────────────────────────────────────────

    def add_group(self, group: PermissionGroup) -> None:
        self._groups[group.id] = group

    def get_group(self, group_id: str) -> PermissionGroup | None:
        return self._groups.get(group_id)

    def remove_group(self, group_id: str) -> None:
        self._groups.pop(group_id, None)

    @property
    def all_groups(self) -> list[PermissionGroup]:
        return list(self._groups.values())

    # ── Binding management ───────────────────────────────────────────

    def bind(self, key: str, group_id: str) -> None:
        """Bind a user/session scope to a permission group.

        Args:
            key: Either "{session_id}.{user_id}" or "{instance_id}:{user_id}"
            group_id: ID of the permission group to assign.
        """
        if group_id not in self._groups:
            raise ValueError(f"Unknown permission group: {group_id!r}")
        self._bindings[key] = group_id

    def unbind(self, key: str) -> None:
        self._bindings.pop(key, None)

    def get_binding(self, key: str) -> str | None:
        return self._bindings.get(key)

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
        """
        layers: list[set[str]] = []

        # Layer 1: Global binding
        global_key = f"{instance_id}:{user_id}"
        global_group_id = self._bindings.get(global_key)
        if global_group_id and global_group_id in self._groups:
            layers.append(self._groups[global_group_id].permissions)

        # Layer 2: Session-local binding
        session_key = f"{session_id}.{user_id}"
        session_group_id = self._bindings.get(session_key)
        if session_group_id and session_group_id in self._groups:
            layers.append(self._groups[session_group_id].permissions)

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
