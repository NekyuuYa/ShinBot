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


def test_prompt_definition_crud_roundtrip(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra",
                "sourceType": "agent_plugin",
                "sourceId": "plugin.identity",
                "ownerPluginId": "plugin.identity",
                "ownerModule": "shinbot.plugins.identity",
                "stage": "identity",
                "type": "static_text",
                "priority": 20,
                "description": "Additional identity prompt",
                "content": "You are calm and concise.",
                "tags": ["identity", "identity", "agent"],
                "metadata": {"display_name": "Identity Extra"},
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["uuid"]
        assert created["promptId"] == "prompt.identity.extra"
        assert created["source"]["sourceType"] == "agent_plugin"
        assert created["source"]["sourceId"] == "plugin.identity"
        assert created["tags"] == ["identity", "agent"]
        assert created["metadata"] == {}

        prompt_uuid = created["uuid"]

        get_resp = client.get(f"/api/v1/prompt-definitions/{prompt_uuid}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["uuid"] == prompt_uuid

        patch_resp = client.patch(
            f"/api/v1/prompt-definitions/{prompt_uuid}",
            headers=headers,
            json={
                "promptId": "prompt.instructions.chat",
                "name": "Chat Instructions",
                "sourceType": "builtin_system",
                "sourceId": "builtin.chat",
                "stage": "instructions",
                "type": "template",
                "content": "task={task}",
                "templateVars": ["task"],
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["promptId"] == "prompt.instructions.chat"
        assert patched["source"]["sourceType"] == "builtin_system"
        assert patched["source"]["sourceId"] == "builtin.chat"
        assert patched["type"] == "template"
        assert patched["templateVars"] == ["task"]

        list_resp = client.get("/api/v1/prompt-definitions", headers=headers)
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1
        assert list_resp.json()["data"][0]["uuid"] == prompt_uuid

        delete_resp = client.delete(f"/api/v1/prompt-definitions/{prompt_uuid}", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_prompt_definition_rejects_duplicate_id_and_invalid_shape(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        first_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra",
                "stage": "identity",
                "type": "static_text",
                "content": "hello",
            },
        )
        assert first_resp.status_code == 201

        duplicate_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=headers,
            json={
                "promptId": "prompt.identity.extra",
                "name": "Identity Extra 2",
                "stage": "identity",
                "type": "static_text",
                "content": "hello again",
            },
        )
        assert duplicate_resp.status_code == 409
        assert duplicate_resp.json()["error"]["code"] == "PROMPT_ALREADY_EXISTS"

        invalid_resp = client.post(
            "/api/v1/prompt-definitions",
            headers=headers,
            json={
                "promptId": "prompt.instructions.bad",
                "name": "Bad Template",
                "stage": "instructions",
                "type": "template",
                "content": "task={task}",
            },
        )
        assert invalid_resp.status_code == 400
        assert invalid_resp.json()["error"]["code"] == "INVALID_ACTION"
