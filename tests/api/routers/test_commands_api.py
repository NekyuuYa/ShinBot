from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot


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
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.save_config_calls = 0

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return True


def test_commands_list_route_returns_registered_commands(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    asyncio.run(bot.plugin_manager.load_all_async(tmp_path / "plugins"))
    app = create_api_app(bot, _BootStub(tmp_path))
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    with TestClient(app) as client:
        response = client.get("/api/v1/commands", headers=headers)

    assert response.status_code == 200
    payload = {item["name"]: item for item in response.json()["data"]}
    assert "help" in payload
    assert payload["help"]["aliases"] == ["commands"]
    assert payload["help"]["enabled"] is True


def test_commands_patch_route_can_disable_command_and_persist(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    asyncio.run(bot.plugin_manager.load_all_async(tmp_path / "plugins"))
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    with TestClient(app) as client:
        response = client.patch("/api/v1/commands/help", json={"enabled": False}, headers=headers)

    assert response.status_code == 200
    assert response.json()["data"]["enabled"] is False
    assert bot.command_registry.get("help") is not None
    assert bot.command_registry.get("help").enabled is False
    assert boot.config["command_overrides"]["enabled"] == {"help": False}
    assert boot.save_config_calls == 1


def test_commands_route_applies_saved_enabled_overrides_on_app_creation(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    asyncio.run(bot.plugin_manager.load_all_async(tmp_path / "plugins"))
    boot = _BootStub(tmp_path)
    boot.config["command_overrides"] = {"enabled": {"help": False}}

    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    with TestClient(app) as client:
        response = client.get("/api/v1/commands", headers=headers)

    assert response.status_code == 200
    payload = {item["name"]: item for item in response.json()["data"]}
    assert payload["help"]["enabled"] is False
