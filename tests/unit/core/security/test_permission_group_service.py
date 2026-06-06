"""Tests for permission group administration service."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from shinbot.core.message_routes.command import CommandDef, CommandRegistry
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.security.permission_service import (
    PermissionGroupService,
    PermissionServiceError,
)


def _load(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


async def _noop_command(ctx, args):
    return None


def test_create_group_persists_and_refreshes_engine(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text("[app]\nname = \"shinbot\"\n", encoding="utf-8")
    engine = PermissionEngine()

    service = PermissionGroupService.from_config_path(config_path, engine=engine, actor="admin:user")
    group = service.create_group(
        group_id="moderator",
        name="Moderator",
        description="Group managers",
        permissions=["cmd.mute", "cmd.help", "cmd.mute"],
    )

    assert group.permissions == {"cmd.help", "cmd.mute"}
    assert engine.get_group("moderator") is not None
    assert engine.get_group("moderator").permissions == {"cmd.help", "cmd.mute"}

    payload = _load(config_path)
    assert payload["app"]["name"] == "shinbot"
    group_payload = next(
        item for item in payload["permissions"]["groups"] if item["id"] == "moderator"
    )
    assert group_payload == {
        "id": "moderator",
        "name": "Moderator",
        "permissions": ["cmd.help", "cmd.mute"],
        "description": "Group managers",
    }


def test_builtin_groups_are_protected(tmp_path: Path) -> None:
    service = PermissionGroupService.from_config_path(tmp_path / "config.toml")

    with pytest.raises(PermissionServiceError, match="protected"):
        service.delete_group("default")
    with pytest.raises(PermissionServiceError, match="must keep"):
        service.update_group("owner", permissions=["cmd.help"])
    with pytest.raises(PermissionServiceError, match="cannot be unprotected"):
        service.update_group("admin", protected=False)
    with pytest.raises(PermissionServiceError, match="must keep permissions"):
        service.update_group("admin", permissions=["cmd.*"])

    owner = service.get_group("owner")
    assert owner is not None
    assert owner.system is True
    assert owner.protected is True
    assert "*" in owner.permissions


def test_validates_group_id_and_permission_nodes(tmp_path: Path) -> None:
    service = PermissionGroupService.from_config_path(tmp_path / "config.toml")

    with pytest.raises(PermissionServiceError, match="group id"):
        service.create_group(group_id="bad id", permissions=["cmd.help"])
    with pytest.raises(PermissionServiceError, match="Invalid permission"):
        service.create_group(group_id="valid", permissions=["cmd..broken"])


def test_bindings_require_existing_groups_and_refresh_multi_group_engine(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    engine = PermissionEngine()
    service = PermissionGroupService.from_config_path(config_path, engine=engine)
    service.create_group(group_id="moderator", permissions=["cmd.mute"])
    service.create_group(group_id="search_user", permissions=["tools.search"])

    binding = service.set_binding("bot-main:user-1", ["search_user", "moderator", "moderator"])

    assert binding.groups == ("moderator", "search_user")
    assert engine.groups_for_key("bot-main:user-1") == ("moderator", "search_user")
    assert "cmd.mute" in engine.resolve("bot-main", "bot-main:group:g1", "user-1")
    assert "tools.search" in engine.resolve("bot-main", "bot-main:group:g1", "user-1")
    assert _load(config_path)["permissions"]["bindings"] == [
        {"key": "bot-main:user-1", "groups": ["moderator", "search_user"]}
    ]

    with pytest.raises(PermissionServiceError, match="not found"):
        service.set_binding("bot-main:user-2", ["missing"])


def test_remove_group_cleans_bindings(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    engine = PermissionEngine()
    service = PermissionGroupService.from_config_path(config_path, engine=engine)
    service.create_group(group_id="moderator", permissions=["cmd.mute"])
    service.create_group(group_id="search_user", permissions=["tools.search"])
    service.set_binding("bot:user", ["moderator", "search_user"])

    service.delete_group("moderator")

    assert service.get_group("moderator") is None
    assert service.list_bindings(scope_key="bot:user")[0].groups == ("search_user",)
    assert engine.groups_for_key("bot:user") == ("search_user",)
    assert engine.get_group("moderator") is None


def test_command_override_crud_and_permission_validation(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    service = PermissionGroupService.from_config_path(config_path)

    override = service.set_command_override("mute", "cmd.moderation.mute")
    assert override.command == "mute"
    assert service.list_command_overrides() == [override]

    service.set_command_override("help", "")
    assert _load(config_path)["permissions"]["command_overrides"] == [
        {"command": "help", "permission": ""},
        {"command": "mute", "permission": "cmd.moderation.mute"},
    ]

    with pytest.raises(PermissionServiceError, match="Invalid permission"):
        service.set_command_override("broken", "cmd..broken")

    service.remove_command_override("mute")
    assert [item.command for item in service.list_command_overrides()] == ["help"]


def test_loads_legacy_mapping_bindings_and_preserves_on_save(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[permissions.bindings]
"bot:user-1" = "admin"
"bot:user-2" = ["default", "admin"]
""".lstrip(),
        encoding="utf-8",
    )

    service = PermissionGroupService.from_config_path(config_path)
    service.create_group(group_id="moderator", permissions=["cmd.mute"])

    assert service.list_bindings(scope_key="bot:user-1")[0].groups == ("admin",)
    assert service.list_bindings(scope_key="bot:user-2")[0].groups == ("admin", "default")
    assert _load(config_path)["permissions"]["bindings"] == [
        {"key": "bot:user-1", "groups": ["admin"]},
        {"key": "bot:user-2", "groups": ["admin", "default"]},
    ]


def test_command_overrides_refresh_runtime_registry(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    registry = CommandRegistry()
    registry.register(CommandDef(name="mute", handler=_noop_command, permission="cmd.mute"))
    service = PermissionGroupService.from_config_path(config_path, command_registry=registry)

    service.set_command_override("mute", "cmd.moderation.mute")

    assert registry.get("mute").permission == "cmd.moderation.mute"

    service.remove_command_override("mute")

    assert registry.get("mute").permission == "cmd.mute"


def test_group_records_mark_orphan_permissions_from_command_registry(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    registry = CommandRegistry()
    registry.register(CommandDef(name="help", handler=_noop_command, permission="cmd.help"))
    registry.register(CommandDef(name="mute", handler=_noop_command, permission="cmd.mute"))

    service = PermissionGroupService.from_config_path(config_path, command_registry=registry)
    service.set_command_override("mute", "cmd.moderation.mute")
    created = service.create_group(
        group_id="moderator",
        permissions=[
            "cmd.help",
            "cmd.moderation.mute",
            "cmd.missing",
            "-cmd.ghost",
            "cmd.*",
            "*",
            "tools.weather.query",
        ],
    )

    assert created.orphan_permissions == {"cmd.missing", "-cmd.ghost"}
    assert created.model_dump(by_alias=True)["orphanPermissions"] == {
        "cmd.missing",
        "-cmd.ghost",
    }

    fetched = service.get_group("moderator")
    assert fetched is not None
    assert fetched.orphan_permissions == {"cmd.missing", "-cmd.ghost"}
