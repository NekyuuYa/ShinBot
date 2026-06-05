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

    def save_config(self) -> bool:
        return True


def test_builtin_commands_plugin_appears_in_plugins_api(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    loaded = asyncio.run(bot.plugin_manager.load_all_async(tmp_path / "plugins"))
    assert any(item.id == "shinbot_plugin_builtin_commands" for item in loaded)

    app = create_api_app(bot, _BootStub(tmp_path))
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    with TestClient(app) as client:
        response = client.get("/api/v1/plugins", headers=headers)

    assert response.status_code == 200
    payload = {item["id"]: item for item in response.json()["data"]}
    assert payload["shinbot_plugin_builtin_commands"]["commands"] == [
        "help",
        "ping",
        "about",
        "whoami",
        "mute",
    ]
