from __future__ import annotations

import tomllib
from pathlib import Path

import tomli_w
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.message_routes.command import CommandDef


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            }
        }
        self.config_path = data_dir / "config.toml"
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.save_config_calls = 0

    def save_config(self) -> bool:
        self.save_config_calls += 1
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with self.config_path.open("wb") as file_obj:
            tomli_w.dump(self.config, file_obj)
        return True


async def _noop_command(ctx, args):
    return None


def _headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def _load(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def test_permission_group_crud_and_protection(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        headers = _headers(app)
        create_resp = client.post(
            "/api/v1/permissions/groups",
            json={
                "id": "moderator",
                "name": "Moderator",
                "description": "Group managers",
                "permissions": ["cmd.mute", "cmd.help", "cmd.mute"],
            },
            headers=headers,
        )
        assert create_resp.status_code == 200
        assert create_resp.json()["data"]["permissions"] == ["cmd.help", "cmd.mute"]
        assert create_resp.json()["data"]["orphanPermissions"] == []
        assert "permissions" in boot.config
        assert bot.permission_engine.get_group("moderator") is not None

        list_resp = client.get("/api/v1/permissions/groups", headers=headers)
        assert list_resp.status_code == 200
        groups = {item["id"]: item for item in list_resp.json()["data"]}
        assert groups["default"]["protected"] is True
        assert groups["moderator"]["system"] is False

        update_resp = client.patch(
            "/api/v1/permissions/groups/moderator",
            json={"name": "Mods", "permissions": ["cmd.mute"]},
            headers=headers,
        )
        assert update_resp.status_code == 200
        assert update_resp.json()["data"]["name"] == "Mods"

        protected_resp = client.delete("/api/v1/permissions/groups/default", headers=headers)
        assert protected_resp.status_code == 403
        assert protected_resp.json()["error"]["code"] == "GROUP_PROTECTED"

        delete_resp = client.delete("/api/v1/permissions/groups/moderator", headers=headers)
        assert delete_resp.status_code == 200
        assert bot.permission_engine.get_group("moderator") is None


def test_permission_writes_sync_boot_config_before_later_save(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/permissions/groups",
            json={"id": "moderator", "permissions": ["cmd.mute"]},
            headers=_headers(app),
        )

    assert response.status_code == 200
    assert "permissions" in boot.config

    boot.save_config()

    groups = {
        item["id"]: item
        for item in _load(boot.config_path)["permissions"]["groups"]
    }
    assert groups["moderator"]["permissions"] == ["cmd.mute"]


def test_permission_bindings_support_filters_and_multi_group_runtime_refresh(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        headers = _headers(app)
        client.post(
            "/api/v1/permissions/groups",
            json={"id": "moderator", "permissions": ["cmd.mute"]},
            headers=headers,
        )
        client.post(
            "/api/v1/permissions/groups",
            json={"id": "search_user", "permissions": ["tools.search"]},
            headers=headers,
        )

        put_resp = client.put(
            "/api/v1/permissions/bindings/bot-main:user-1",
            json={"groupIds": ["search_user", "moderator", "moderator"]},
            headers=headers,
        )
        assert put_resp.status_code == 200
        assert put_resp.json()["data"] == {
            "scopeKey": "bot-main:user-1",
            "groupIds": ["moderator", "search_user"],
        }
        assert bot.permission_engine.groups_for_key("bot-main:user-1") == (
            "moderator",
            "search_user",
        )

        by_scope = client.get(
            "/api/v1/permissions/bindings",
            params={"scopeKey": "bot-main:user-1"},
            headers=headers,
        )
        assert by_scope.status_code == 200
        assert len(by_scope.json()["data"]) == 1

        by_group = client.get(
            "/api/v1/permissions/bindings",
            params={"groupId": "search_user"},
            headers=headers,
        )
        assert by_group.status_code == 200
        assert by_group.json()["data"][0]["scopeKey"] == "bot-main:user-1"

        remove_one = client.delete(
            "/api/v1/permissions/bindings/bot-main:user-1",
            params={"groupId": "moderator"},
            headers=headers,
        )
        assert remove_one.status_code == 200
        assert bot.permission_engine.groups_for_key("bot-main:user-1") == ("search_user",)


def test_command_permission_override_patch_and_delete(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    bot.command_registry.register(
        CommandDef(name="mute", handler=_noop_command, permission="cmd.mute")
    )
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        headers = _headers(app)
        patch_resp = client.patch(
            "/api/v1/commands/mute/permission",
            json={"permission": "cmd.moderation.mute"},
            headers=headers,
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["data"] == {
            "name": "mute",
            "defaultPermission": "cmd.mute",
            "permission": "cmd.moderation.mute",
            "permissionOverridden": True,
        }
        assert bot.command_registry.get("mute").permission == "cmd.moderation.mute"
        assert _load(boot.config_path)["permissions"]["command_overrides"] == [
            {"command": "mute", "permission": "cmd.moderation.mute"}
        ]

        delete_resp = client.delete("/api/v1/commands/mute/permission", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["permission"] == "cmd.mute"
        assert delete_resp.json()["data"]["permissionOverridden"] is False
        assert bot.command_registry.get("mute").permission == "cmd.mute"


def test_command_permission_override_requires_registered_command(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.patch(
            "/api/v1/commands/missing/permission",
            json={"permission": "cmd.missing"},
            headers=_headers(app),
        )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "COMMAND_NOT_FOUND"


def test_command_permission_reset_clears_runtime_only_override(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    bot.command_registry.register(
        CommandDef(name="mute", handler=_noop_command, permission="cmd.mute")
    )
    bot.command_registry.set_permission_override("mute", "cmd.moderation.mute")
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.delete("/api/v1/commands/mute/permission", headers=_headers(app))

    assert response.status_code == 200
    assert response.json()["data"] == {
        "name": "mute",
        "defaultPermission": "cmd.mute",
        "permission": "cmd.mute",
        "permissionOverridden": False,
    }
    assert bot.command_registry.get("mute").permission == "cmd.mute"
