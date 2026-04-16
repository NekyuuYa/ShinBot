from __future__ import annotations

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


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def test_persona_crud_roundtrip(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/personas",
            headers=headers,
            json={
                "name": "Default Assistant",
                "promptText": "You are a precise assistant.",
                "enabled": True,
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["uuid"]
        assert created["name"] == "Default Assistant"
        assert created["promptText"] == "You are a precise assistant."

        persona_uuid = created["uuid"]

        get_resp = client.get(f"/api/v1/personas/{persona_uuid}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["uuid"] == persona_uuid

        patch_resp = client.patch(
            f"/api/v1/personas/{persona_uuid}",
            headers=headers,
            json={
                "name": "Default Assistant v2",
                "promptText": "You are a precise and calm assistant.",
                "enabled": False,
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["name"] == "Default Assistant v2"
        assert patched["enabled"] is False

        list_resp = client.get("/api/v1/personas", headers=headers)
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1
        assert list_resp.json()["data"][0]["uuid"] == persona_uuid

        delete_resp = client.delete(f"/api/v1/personas/{persona_uuid}", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_persona_name_must_be_unique(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        first_resp = client.post(
            "/api/v1/personas",
            headers=headers,
            json={
                "name": "Shared Name",
                "promptText": "You are persona one.",
            },
        )
        assert first_resp.status_code == 201

        second_resp = client.post(
            "/api/v1/personas",
            headers=headers,
            json={
                "name": "Shared Name",
                "promptText": "You are persona two.",
            },
        )
        assert second_resp.status_code == 409
        assert second_resp.json()["error"]["code"] == "PERSONA_ALREADY_EXISTS"
