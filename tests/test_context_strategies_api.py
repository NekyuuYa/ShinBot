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


def test_context_strategy_crud_roundtrip(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/context-strategies",
            headers=headers,
            json={
                "name": "Recent History",
                "resolverRef": "context.recent_history",
                "description": "Use recent turns.",
                "config": {"window": 8},
                "maxContextTokens": 1200,
                "maxHistoryTurns": 8,
                "memorySummaryRequired": True,
                "truncatePolicy": "head_tail",
                "triggerRatio": 0.6,
                "trimRatio": 0.2,
                "enabled": True,
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["uuid"]
        assert created["resolverRef"] == "context.recent_history"
        assert created["config"]["window"] == 8
        assert created["triggerRatio"] == 0.6
        assert created["trimRatio"] == 0.2

        strategy_uuid = created["uuid"]

        get_resp = client.get(f"/api/v1/context-strategies/{strategy_uuid}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["uuid"] == strategy_uuid

        patch_resp = client.patch(
            f"/api/v1/context-strategies/{strategy_uuid}",
            headers=headers,
            json={
                "name": "Recent History v2",
                "config": {"window": 10},
                "maxHistoryTurns": 10,
                "triggerRatio": 0.7,
                "enabled": False,
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["name"] == "Recent History v2"
        assert patched["config"]["window"] == 10
        assert patched["triggerRatio"] == 0.7
        assert patched["enabled"] is False

        list_resp = client.get("/api/v1/context-strategies", headers=headers)
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1
        assert list_resp.json()["data"][0]["uuid"] == strategy_uuid

        delete_resp = client.delete(
            f"/api/v1/context-strategies/{strategy_uuid}",
            headers=headers,
        )
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_context_strategy_name_must_be_unique(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        first_resp = client.post(
            "/api/v1/context-strategies",
            headers=headers,
            json={
                "name": "Shared Strategy",
                "resolverRef": "context.first",
            },
        )
        assert first_resp.status_code == 201

        second_resp = client.post(
            "/api/v1/context-strategies",
            headers=headers,
            json={
                "name": "Shared Strategy",
                "resolverRef": "context.second",
            },
        )
        assert second_resp.status_code == 409
        assert second_resp.json()["error"]["code"] == "CONTEXT_STRATEGY_ALREADY_EXISTS"
