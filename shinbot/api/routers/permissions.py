"""Permission group management router: /api/v1/permissions and command permission APIs."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shinbot.admin.command_admin import CommandAdminError, get_command_or_raise
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, Envelope, ok
from shinbot.core.security.permission_service import (
    PermissionBindingRecord,
    PermissionGroupRecord,
    PermissionGroupService,
    PermissionServiceError,
)

router = APIRouter(
    tags=["permissions"],
    dependencies=AuthRequired,
)


class PermissionGroupData(BaseModel):
    id: str
    name: str
    description: str
    permissions: list[str]
    orphanPermissions: list[str]
    builtin: bool
    system: bool
    protected: bool


class PermissionGroupCreatePayload(BaseModel):
    id: str
    name: str = ""
    description: str = ""
    permissions: list[str] = Field(default_factory=list)
    protected: bool = False


class PermissionGroupUpdatePayload(BaseModel):
    name: str | None = None
    description: str | None = None
    permissions: list[str] | None = None
    protected: bool | None = None


class PermissionBindingData(BaseModel):
    scopeKey: str
    groups: list[str]
    groupIds: list[str]


class PermissionBindingPayload(BaseModel):
    groups: list[str] | None = None
    groupIds: list[str] | None = None

    def resolved_groups(self) -> list[str]:
        if self.groups is not None:
            return self.groups
        if self.groupIds is not None:
            return self.groupIds
        return []


class CommandPermissionPayload(BaseModel):
    permission: str


class CommandPermissionData(BaseModel):
    name: str
    defaultPermission: str
    permission: str
    permissionOverridden: bool


def _permission_service(bot: Any, boot: Any) -> PermissionGroupService:
    return PermissionGroupService.from_config_path(
        _permission_config_path(boot),
        engine=getattr(bot, "permission_engine", None),
        command_registry=getattr(bot, "command_registry", None),
        actor="api",
    )


def _permission_config_path(boot: Any) -> Path:
    config_path = getattr(boot, "config_path", None)
    if config_path is not None:
        return Path(config_path)
    data_dir = getattr(boot, "data_dir", None)
    if data_dir is not None:
        return Path(data_dir) / "config.toml"
    raise HTTPException(
        status_code=500,
        detail={
            "code": EC.CONFIG_WRITE_FAILED,
            "message": "Permission management requires a config path",
        },
    )


def _sync_boot_permissions_config(boot: Any) -> None:
    """Keep BootController's in-memory config aligned with TOML permission writes."""
    config = getattr(boot, "config", None)
    if not isinstance(config, dict):
        return

    config_path = _permission_config_path(boot)
    if not config_path.exists():
        config.pop("permissions", None)
        return

    with config_path.open("rb") as file_obj:
        payload = tomllib.load(file_obj)
    permissions = payload.get("permissions")
    if isinstance(permissions, dict):
        config["permissions"] = permissions
    else:
        config.pop("permissions", None)


def _group_data(record: PermissionGroupRecord) -> PermissionGroupData:
    return PermissionGroupData(
        id=record.id,
        name=record.name,
        description=record.description,
        permissions=sorted(record.permissions),
        orphanPermissions=sorted(record.orphan_permissions),
        builtin=record.system,
        system=record.system,
        protected=record.protected,
    )


def _binding_data(record: PermissionBindingRecord) -> PermissionBindingData:
    groups = list(record.groups)
    return PermissionBindingData(scopeKey=record.key, groups=groups, groupIds=groups)


def _command_permission_data(command: Any, registry: Any) -> CommandPermissionData:
    return CommandPermissionData(
        name=command.name,
        defaultPermission=registry.default_permission_for(command.name),
        permission=command.permission,
        permissionOverridden=registry.has_permission_override(command.name),
    )


def _raise_permission_http_error(exc: PermissionServiceError) -> None:
    status_code = 400
    if exc.code == "GROUP_NOT_FOUND":
        status_code = 404
    elif exc.code in {"GROUP_EXISTS", "BUILTIN_GROUP"}:
        status_code = 409
    elif exc.code == "GROUP_PROTECTED":
        status_code = 400
    raise HTTPException(
        status_code=status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


def _raise_admin_http_error(exc: CommandAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("/permissions/groups", response_model=Envelope[list[PermissionGroupData]])
async def list_permission_groups(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """List all permission groups."""
    service = _permission_service(bot, boot)
    return ok([_group_data(record).model_dump() for record in service.list_groups()])


@router.post("/permissions/groups", response_model=Envelope[PermissionGroupData])
async def create_permission_group(
    payload: PermissionGroupCreatePayload,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Create a permission group."""
    service = _permission_service(bot, boot)
    try:
        group = service.create_group(
            group_id=payload.id,
            name=payload.name,
            description=payload.description,
            permissions=payload.permissions,
            protected=payload.protected,
        )
        _sync_boot_permissions_config(boot)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok(_group_data(group).model_dump())


@router.get("/permissions/groups/{group_id}", response_model=Envelope[PermissionGroupData])
async def get_permission_group(group_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Get one permission group."""
    service = _permission_service(bot, boot)
    group = service.get_group(group_id)
    if group is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "GROUP_NOT_FOUND",
                "message": f"Permission group {group_id!r} not found",
            },
        )
    return ok(_group_data(group).model_dump())


@router.patch("/permissions/groups/{group_id}", response_model=Envelope[PermissionGroupData])
async def update_permission_group(
    group_id: str,
    payload: PermissionGroupUpdatePayload,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Update one permission group."""
    service = _permission_service(bot, boot)
    try:
        group = service.update_group(
            group_id,
            name=payload.name,
            description=payload.description,
            permissions=payload.permissions,
            protected=payload.protected,
        )
        _sync_boot_permissions_config(boot)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok(_group_data(group).model_dump())


@router.delete("/permissions/groups/{group_id}", response_model=Envelope[dict[str, bool]])
async def delete_permission_group(group_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Delete one custom permission group."""
    service = _permission_service(bot, boot)
    try:
        service.delete_group(group_id)
        _sync_boot_permissions_config(boot)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok({"deleted": True})


@router.get("/permissions/bindings", response_model=Envelope[list[PermissionBindingData]])
async def list_permission_bindings(
    scopeKey: str | None = Query(default=None),
    groupId: str | None = Query(default=None),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """List permission bindings, optionally filtered by scope key or group ID."""
    service = _permission_service(bot, boot)
    try:
        bindings = service.list_bindings(scope_key=scopeKey, group_id=groupId)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok([_binding_data(record).model_dump() for record in bindings])


@router.put("/permissions/bindings/{scope_key}", response_model=Envelope[PermissionBindingData])
async def set_permission_binding(
    scope_key: str,
    payload: PermissionBindingPayload,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Replace the groups bound to one permission scope key."""
    service = _permission_service(bot, boot)
    try:
        binding = service.set_binding(scope_key, payload.resolved_groups())
        _sync_boot_permissions_config(boot)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok(_binding_data(binding).model_dump())


@router.delete("/permissions/bindings/{scope_key}", response_model=Envelope[dict[str, bool]])
async def delete_permission_binding(
    scope_key: str,
    groupId: str | None = Query(default=None),
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Delete a binding key, or remove one group from that binding when groupId is supplied."""
    service = _permission_service(bot, boot)
    try:
        service.remove_binding(scope_key, group_id=groupId)
        _sync_boot_permissions_config(boot)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok({"deleted": True})


@router.patch("/commands/{command_name}/permission", response_model=Envelope[CommandPermissionData])
async def update_command_permission(
    command_name: str,
    payload: CommandPermissionPayload,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Override one command's required permission."""
    registry = getattr(bot, "command_registry", None)
    if registry is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "COMMAND_NOT_FOUND", "message": f"Command {command_name!r} not found"},
        )
    try:
        command = get_command_or_raise(registry, command_name)
        service = _permission_service(bot, boot)
        service.set_command_override(command.name, payload.permission)
        _sync_boot_permissions_config(boot)
        command = get_command_or_raise(registry, command.name)
    except CommandAdminError as exc:
        _raise_admin_http_error(exc)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok(_command_permission_data(command, registry).model_dump())


@router.delete("/commands/{command_name}/permission", response_model=Envelope[CommandPermissionData])
async def reset_command_permission(command_name: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Remove one command permission override and restore the plugin default."""
    registry = getattr(bot, "command_registry", None)
    if registry is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "COMMAND_NOT_FOUND", "message": f"Command {command_name!r} not found"},
        )
    try:
        command = get_command_or_raise(registry, command_name)
        service = _permission_service(bot, boot)
        service.remove_command_override(command.name)
        clear_override = getattr(registry, "clear_permission_override", None)
        has_override = getattr(registry, "has_permission_override", None)
        if callable(clear_override) and callable(has_override) and has_override(command.name):
            clear_override(command.name)
        _sync_boot_permissions_config(boot)
        command = get_command_or_raise(registry, command.name)
    except CommandAdminError as exc:
        _raise_admin_http_error(exc)
    except PermissionServiceError as exc:
        _raise_permission_http_error(exc)
    return ok(_command_permission_data(command, registry).model_dump())
