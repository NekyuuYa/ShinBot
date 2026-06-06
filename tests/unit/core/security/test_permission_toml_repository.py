"""Tests for TOML-backed permission repositories."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from shinbot.core.security.permission import PermissionGroup
from shinbot.core.security.permission_toml import (
    CommandPermissionOverride,
    CommandPermissionOverrideRepository,
    PermissionBindingRecord,
    PermissionBindingRepository,
    PermissionGroupDefinition,
    PermissionGroupRepository,
    PermissionTomlError,
    bindings_from_config,
    groups_from_config,
)


def _load(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def test_group_repository_reads_and_writes_sorted_groups(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[runtime]
agent = true

[permissions]

[[permissions.groups]]
id = "zeta"
name = "Zeta"
permissions = ["cmd.z", "cmd.a", "cmd.z"]

[[permissions.groups]]
id = "alpha"
description = "Alpha group"
permissions = ["tools.search"]
system = true
protected = true

[[permissions.bindings]]
key = "bot:user"
group = "admin"
""".lstrip(),
        encoding="utf-8",
    )

    repo = PermissionGroupRepository(config_path)

    groups = repo.list()
    assert [group.id for group in groups] == ["alpha", "zeta"]
    assert groups[0].permissions == {"tools.search"}
    assert groups[0].description == "Alpha group"
    assert groups[0].system is True
    assert groups[0].protected is True
    assert groups[1].permissions == {"cmd.a", "cmd.z"}

    repo.save(
        [
            PermissionGroup(id="moderator", name="Moderator", permissions={"cmd.mute", "cmd.help"}),
            PermissionGroupDefinition(
                id="auditor",
                name="Auditor",
                description="Audit operators",
                permissions={"audit.read"},
                protected=True,
            ),
        ]
    )

    payload = _load(config_path)
    assert payload["runtime"]["agent"] is True
    assert [group["id"] for group in payload["permissions"]["groups"]] == ["auditor", "moderator"]
    assert payload["permissions"]["groups"][0] == {
        "id": "auditor",
        "name": "Auditor",
        "permissions": ["audit.read"],
        "description": "Audit operators",
        "protected": True,
    }
    assert payload["permissions"]["groups"][1] == {
        "id": "moderator",
        "name": "Moderator",
        "permissions": ["cmd.help", "cmd.mute"],
    }
    assert payload["permissions"]["bindings"] == [{"key": "bot:user", "group": "admin"}]


def test_groups_from_config_returns_runtime_permission_groups() -> None:
    groups = groups_from_config(
        {
            "permissions": {
                "groups": [
                    {
                        "id": "moderator",
                        "name": "Moderator",
                        "description": "ignored by runtime model",
                        "permissions": ["cmd.mute"],
                    }
                ]
            }
        }
    )

    assert groups == [
        PermissionGroup(id="moderator", name="Moderator", permissions={"cmd.mute"})
    ]


def test_binding_repository_supports_legacy_group_and_new_groups(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[permissions]

[[permissions.bindings]]
key = "bot:user-b"
group = "admin"

[[permissions.bindings]]
key = "bot:user-a"
group = "ignored"
groups = ["search_user", "moderator", "moderator"]
""".lstrip(),
        encoding="utf-8",
    )

    repo = PermissionBindingRepository(config_path)

    assert repo.list() == [
        PermissionBindingRecord(key="bot:user-a", groups=("moderator", "search_user")),
        PermissionBindingRecord(key="bot:user-b", groups=("admin",)),
    ]

    repo.save(
        [
            PermissionBindingRecord(key="bot:z", groups=("admin",)),
            PermissionBindingRecord(key="bot:a", groups=("moderator", "search_user", "moderator")),
        ]
    )

    payload = _load(config_path)
    assert payload["permissions"]["bindings"] == [
        {"key": "bot:a", "groups": ["moderator", "search_user"]},
        {"key": "bot:z", "groups": ["admin"]},
    ]


def test_binding_parser_prefers_groups_over_group() -> None:
    parsed = bindings_from_config(
        {
            "permissions": {
                "bindings": [
                    {
                        "key": "bot:user",
                        "group": "admin",
                        "groups": ["moderator"],
                    }
                ]
            }
        }
    )

    assert parsed == [PermissionBindingRecord(key="bot:user", groups=("moderator",))]


def test_binding_parser_supports_legacy_mapping_form() -> None:
    parsed = bindings_from_config(
        {
            "permissions": {
                "bindings": {
                    "bot:user-b": "admin",
                    "bot:user-a": ["moderator", "search_user", "moderator"],
                }
            }
        }
    )

    assert parsed == [
        PermissionBindingRecord(key="bot:user-a", groups=("moderator", "search_user")),
        PermissionBindingRecord(key="bot:user-b", groups=("admin",)),
    ]


def test_command_override_repository_reads_and_writes_sorted_overrides(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[permissions]

[[permissions.command_overrides]]
command = "zeta"
permission = "cmd.zeta"

[[permissions.command_overrides]]
command = "alpha"
permission = ""
""".lstrip(),
        encoding="utf-8",
    )

    repo = CommandPermissionOverrideRepository(config_path)

    assert repo.list() == [
        CommandPermissionOverride(command="alpha", permission=""),
        CommandPermissionOverride(command="zeta", permission="cmd.zeta"),
    ]

    repo.save(
        [
            CommandPermissionOverride(command="mute", permission="cmd.moderation.mute"),
            CommandPermissionOverride(command="help", permission=""),
        ]
    )

    payload = _load(config_path)
    assert payload["permissions"]["command_overrides"] == [
        {"command": "help", "permission": ""},
        {"command": "mute", "permission": "cmd.moderation.mute"},
    ]


def test_repository_creates_missing_config_file(tmp_path: Path) -> None:
    config_path = tmp_path / "nested" / "config.toml"

    PermissionGroupRepository(config_path).save(
        [PermissionGroup(id="moderator", name="Moderator", permissions={"cmd.mute"})]
    )

    payload = _load(config_path)
    assert payload["permissions"]["groups"] == [
        {"id": "moderator", "name": "Moderator", "permissions": ["cmd.mute"]}
    ]


def test_atomic_write_failure_keeps_original_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "config.toml"
    original = "[permissions]\n"
    config_path.write_text(original, encoding="utf-8")

    def fail_replace(self: Path, target: Path) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(Path, "replace", fail_replace)

    with pytest.raises(PermissionTomlError, match="replace failed"):
        PermissionGroupRepository(config_path).save(
            [PermissionGroup(id="moderator", name="Moderator", permissions={"cmd.mute"})]
        )

    assert config_path.read_text(encoding="utf-8") == original
    assert list(tmp_path.glob("*.tmp")) == []
