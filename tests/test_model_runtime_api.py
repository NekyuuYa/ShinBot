from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.persistence import ModelExecutionRecord


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


def test_provider_crud_hides_auth_payload(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openai-main",
                "type": "openai",
                "displayName": "OpenAI Main",
                "baseUrl": "https://api.openai.com/v1",
                "auth": {"api_key": "secret"},
                "defaultParams": {"temperature": 0.2},
                "enabled": True,
            },
        )
        assert create_resp.status_code == 201
        assert create_resp.json()["data"]["hasAuth"] is True
        assert "auth" not in create_resp.json()["data"]

        patch_resp = client.patch(
            "/api/v1/model-runtime/providers/openai-main",
            headers=headers,
            json={"displayName": "OpenAI Primary", "enabled": False},
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["data"]["displayName"] == "OpenAI Primary"
        assert patch_resp.json()["data"]["enabled"] is False

        list_resp = client.get("/api/v1/model-runtime/providers", headers=headers)
        assert list_resp.status_code == 200
        assert list_resp.json()["data"][0]["id"] == "openai-main"

        delete_resp = client.delete("/api/v1/model-runtime/providers/openai-main", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_provider_rename_preserves_model_relationships(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openai-main",
                "type": "openai",
                "displayName": "OpenAI Main",
                "baseUrl": "https://api.openai.com/v1",
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "openai-main/gpt-fast",
                "providerId": "openai-main",
                "litellmModel": "gpt-4.1-mini",
                "displayName": "GPT Fast",
                "capabilities": ["chat"],
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201

        patch_resp = client.patch(
            "/api/v1/model-runtime/providers/openai-main",
            headers=headers,
            json={"id": "openai-stable", "displayName": "OpenAI Stable"},
        )
        assert patch_resp.status_code == 200
        assert patch_resp.json()["data"]["id"] == "openai-stable"

        models_resp = client.get("/api/v1/model-runtime/models", headers=headers)
        assert models_resp.status_code == 200
        assert models_resp.json()["data"][0]["providerId"] == "openai-stable"


def test_model_and_route_crud_roundtrip(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openrouter-main",
                "type": "openrouter",
                "displayName": "OpenRouter Main",
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "openrouter-main/claude-sonnet",
                "providerId": "openrouter-main",
                "litellmModel": "openrouter/anthropic/claude-3.7-sonnet",
                "displayName": "Claude Sonnet",
                "capabilities": ["chat", "tool_calling"],
                "contextWindow": 200000,
                "defaultParams": {"temperature": 0.1},
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201
        assert model_resp.json()["data"]["providerId"] == "openrouter-main"

        list_models_resp = client.get(
            "/api/v1/model-runtime/models",
            params={"providerId": "openrouter-main"},
            headers=headers,
        )
        assert list_models_resp.status_code == 200
        assert len(list_models_resp.json()["data"]) == 1

        route_resp = client.post(
            "/api/v1/model-runtime/routes",
            headers=headers,
            json={
                "id": "agent.default_chat",
                "purpose": "default chat",
                "strategy": "priority",
                "enabled": True,
                "stickySessions": True,
                "metadata": {"tier": "default"},
                "members": [
                    {
                        "modelId": "openrouter-main/claude-sonnet",
                        "priority": 10,
                        "weight": 1.0,
                        "conditions": {"max_input_tokens": 8000},
                        "enabled": True,
                    }
                ],
            },
        )
        assert route_resp.status_code == 201
        route_data = route_resp.json()["data"]
        assert route_data["id"] == "agent.default_chat"
        assert route_data["members"][0]["modelId"] == "openrouter-main/claude-sonnet"

        patch_route_resp = client.patch(
            "/api/v1/model-runtime/routes/agent.default_chat",
            headers=headers,
            json={"strategy": "weighted", "stickySessions": False},
        )
        assert patch_route_resp.status_code == 200
        assert patch_route_resp.json()["data"]["strategy"] == "weighted"
        assert patch_route_resp.json()["data"]["stickySessions"] is False

        rename_route_resp = client.patch(
            "/api/v1/model-runtime/routes/agent.default_chat",
            headers=headers,
            json={"id": "agent.chat.primary"},
        )
        assert rename_route_resp.status_code == 200
        assert rename_route_resp.json()["data"]["id"] == "agent.chat.primary"
        assert (
            rename_route_resp.json()["data"]["members"][0]["modelId"]
            == "openrouter-main/claude-sonnet"
        )

        delete_route_resp = client.delete(
            "/api/v1/model-runtime/routes/agent.chat.primary",
            headers=headers,
        )
        assert delete_route_resp.status_code == 200
        assert delete_route_resp.json()["data"]["deleted"] is True


def test_model_execution_list_endpoint(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.model_executions.insert(
        ModelExecutionRecord(
            id="exec-1",
            route_id="agent.default_chat",
            provider_id="openai-main",
            model_id="openai-main/gpt-4.1-mini",
            caller="agent.runtime",
            session_id="inst1:group:g1",
            instance_id="inst1",
            success=True,
            input_tokens=12,
            output_tokens=34,
            cache_hit=True,
            metadata={"trace_id": "trace-1"},
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        response = client.get("/api/v1/model-runtime/executions", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"][0]
    assert payload["id"] == "exec-1"
    assert payload["cacheHit"] is True
    assert payload["metadata"]["trace_id"] == "trace-1"


def test_provider_catalog_endpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openai-main",
                "type": "openai",
                "displayName": "OpenAI Main",
                "baseUrl": "https://api.openai.com/v1",
            },
        )
        assert create_resp.status_code == 201

        async def fake_catalog(_payload):
            return [
                {
                    "id": "gpt-4.1-mini",
                    "displayName": "GPT-4.1 Mini",
                    "litellmModel": "gpt-4.1-mini",
                }
            ]

        monkeypatch.setattr(
            "shinbot.api.routers.model_runtime._fetch_provider_catalog",
            fake_catalog,
        )

        response = client.get(
            "/api/v1/model-runtime/providers/openai-main/catalog", headers=headers
        )

    assert response.status_code == 200
    assert response.json()["data"][0]["id"] == "gpt-4.1-mini"


def test_provider_probe_endpoint_uses_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openai-main",
                "type": "openai",
                "displayName": "OpenAI Main",
                "baseUrl": "https://api.openai.com/v1",
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "openai-main/gpt-fast",
                "providerId": "openai-main",
                "litellmModel": "gpt-4.1-mini",
                "displayName": "GPT Fast",
                "capabilities": ["chat"],
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201

        async def fake_generate(call):
            return type(
                "FakeResult",
                (),
                {"execution_id": "probe-exec", "text": "pong"},
            )()

        monkeypatch.setattr(bot.model_runtime, "generate", fake_generate)

        response = client.post(
            "/api/v1/model-runtime/providers/openai-main/probe",
            headers=headers,
            json={},
        )

    assert response.status_code == 200
    assert response.json()["data"]["executionId"] == "probe-exec"
