"""Permission and audit services."""

from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import (
    PermissionEngine,
    PermissionGroup,
    check_permission,
    merge_permissions,
)
from shinbot.core.security.permission_service import (
    CommandPermissionOverrideRecord as PermissionServiceCommandOverrideRecord,
)
from shinbot.core.security.permission_service import (
    PermissionBindingRecord as PermissionServiceBindingRecord,
)
from shinbot.core.security.permission_service import (
    PermissionGroupRecord,
    PermissionGroupService,
    PermissionServiceError,
    PermissionStoreSnapshot,
    TomlPermissionConfigRepository,
)
from shinbot.core.security.permission_toml import (
    CommandPermissionOverride,
    CommandPermissionOverrideRepository,
    PermissionBindingRecord,
    PermissionBindingRepository,
    PermissionGroupDefinition,
    PermissionGroupRepository,
    PermissionTomlError,
)

__all__ = [
    "AuditLogger",
    "CommandPermissionOverride",
    "CommandPermissionOverrideRepository",
    "PermissionEngine",
    "PermissionBindingRecord",
    "PermissionBindingRepository",
    "PermissionGroup",
    "PermissionGroupDefinition",
    "PermissionGroupRecord",
    "PermissionGroupRepository",
    "PermissionGroupService",
    "PermissionServiceBindingRecord",
    "PermissionServiceCommandOverrideRecord",
    "PermissionServiceError",
    "PermissionStoreSnapshot",
    "PermissionTomlError",
    "TomlPermissionConfigRepository",
    "check_permission",
    "merge_permissions",
]
