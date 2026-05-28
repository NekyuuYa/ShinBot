from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot import BootState


class _BootStub:
    def __init__(self, data_dir: Path, bot: ShinBot) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "adapter_instances": [],
        }
        self.data_dir = data_dir
        self.bot = bot
        self.state = BootState.RUNNING
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None

    def save_config(self) -> bool:
        return True


def _app(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path, bot)
    return create_api_app(bot, boot)


def test_health_returns_200(tmp_path: Path):
    app = _app(tmp_path)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/health")

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "healthy"


def test_health_requires_no_auth(tmp_path: Path):
    app = _app(tmp_path)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/health")

    assert response.status_code == 200


def test_protected_endpoint_requires_auth(tmp_path: Path):
    app = _app(tmp_path)

    with TestClient(app) as client:
        response = client.get("/api/v1/system/runtime")

    assert response.status_code == 401
