from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.prompting import PromptRegistry
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
        builtin_resp = client.get(
            f"/api/v1/context-strategies/{PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID}",
            headers=headers,
        )
        assert builtin_resp.status_code == 200
        assert builtin_resp.json()["data"]["type"] == "sliding_window"
        assert builtin_resp.json()["data"]["resolverRef"] == (
            PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER
        )
        assert builtin_resp.json()["data"]["config"]["budget"]["trigger_ratio"] == 0.5
        assert builtin_resp.json()["data"]["config"]["budget"]["trim_turns"] == 2

        create_resp = client.post(
            "/api/v1/context-strategies",
            headers=headers,
            json={
                "name": "Recent History",
                "type": "recent_history",
                "resolverRef": "context.recent_history",
                "description": "Use recent turns.",
                "config": {"window": 8},
                "enabled": True,
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["uuid"]
        assert created["type"] == "recent_history"
        assert created["resolverRef"] == "context.recent_history"
        assert created["config"]["window"] == 8

        strategy_uuid = created["uuid"]

        get_resp = client.get(f"/api/v1/context-strategies/{strategy_uuid}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["uuid"] == strategy_uuid

        patch_resp = client.patch(
            f"/api/v1/context-strategies/{strategy_uuid}",
            headers=headers,
            json={
                "name": "Recent History v2",
                "type": "windowed_history",
                "config": {"window": 10},
                "enabled": False,
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["name"] == "Recent History v2"
        assert patched["type"] == "windowed_history"
        assert patched["config"]["window"] == 10
        assert patched["enabled"] is False

        list_resp = client.get("/api/v1/context-strategies", headers=headers)
        assert list_resp.status_code == 200
        items = list_resp.json()["data"]
        assert len(items) == 2
        assert {item["uuid"] for item in items} == {
            PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID,
            strategy_uuid,
        }

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
                "type": "shared",
                "resolverRef": "context.first",
            },
        )
        assert first_resp.status_code == 201

        second_resp = client.post(
            "/api/v1/context-strategies",
            headers=headers,
            json={
                "name": "Shared Strategy",
                "type": "shared",
                "resolverRef": "context.second",
            },
        )
        assert second_resp.status_code == 409
        assert second_resp.json()["error"]["code"] == "CONTEXT_STRATEGY_ALREADY_EXISTS"


def test_builtin_context_strategy_cannot_be_mutated(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        patch_resp = client.patch(
            f"/api/v1/context-strategies/{PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID}",
            headers=headers,
            json={"name": "Changed"},
        )
        assert patch_resp.status_code == 400
        assert patch_resp.json()["error"]["code"] == "INVALID_ACTION"

        delete_resp = client.delete(
            f"/api/v1/context-strategies/{PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID}",
            headers=headers,
        )
        assert delete_resp.status_code == 400
        assert delete_resp.json()["error"]["code"] == "INVALID_ACTION"
