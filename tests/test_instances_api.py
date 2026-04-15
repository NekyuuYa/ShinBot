from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.app import ShinBot
from tests.conftest import MockAdapter


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "instances": [],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.save_config_calls = 0

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return True


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def test_delete_instance_route_removes_runtime_and_persisted_config(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    adapter = bot.add_adapter("inst-1", "mock")
    asyncio.run(bot.adapter_manager.start_instance("inst-1"))

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {"token": "abc"},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.delete("/api/v1/instances/inst-1", headers=_auth_headers(app))

    assert response.status_code == 200
    assert response.json()["data"] == {"id": "inst-1", "deleted": True}
    assert bot.adapter_manager.get_instance("inst-1") is None
    assert adapter.stopped is True
    assert boot.config["instances"] == []
    assert boot.save_config_calls == 1


def test_update_instance_route_returns_full_instance_payload(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    bot.add_adapter("inst-1", "mock")

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {"token": "abc"},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.patch(
            "/api/v1/instances/inst-1",
            headers=_auth_headers(app),
            json={"name": "Renamed", "config": {"token": "xyz"}},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "id": "inst-1",
        "name": "Renamed",
        "adapterType": "mock",
        "status": "stopped",
        "config": {"token": "xyz"},
        "createdAt": 1,
        "lastModified": boot.config["instances"][0]["lastModified"],
    }


def test_status_websocket_includes_instance_details(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    bot.add_adapter("inst-1", "mock")

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {},
            "createdAt": 1,
            "lastModified": 1,
        },
        {
            "id": "inst-2",
            "name": "Instance 2",
            "adapterType": "mock",
            "platform": "mock",
            "config": {},
            "createdAt": 1,
            "lastModified": 1,
        },
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        with client.websocket_connect("/ws/status") as websocket:
            payload = websocket.receive_json()

    assert payload["success"] is True
    assert payload["data"]["totalInstances"] == 2
    assert payload["data"]["runningInstances"] == 0
    assert payload["data"]["instances"] == [
        {"id": "inst-1", "running": False},
        {"id": "inst-2", "running": False},
    ]
