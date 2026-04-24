from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_provider_crud_hides_auth_payload(tmp_path: Path, make_boot_stub, make_auth_headers):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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


def test_provider_rename_preserves_model_relationships(
    tmp_path: Path, make_boot_stub, make_auth_headers
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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


def test_create_model_auto_infers_context_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    monkeypatch.setattr(
        "shinbot.api.routers.model_runtime.infer_context_window",
        lambda provider, litellm_model: 128000,
    )

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
    assert model_resp.json()["data"]["contextWindow"] == 128000


def test_model_and_route_crud_roundtrip(tmp_path: Path, make_boot_stub, make_auth_headers):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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

        patch_model_resp = client.patch(
            "/api/v1/model-runtime/models/openrouter-main/claude-sonnet",
            headers=headers,
            json={"enabled": False, "displayName": "Claude Sonnet Disabled"},
        )
        assert patch_model_resp.status_code == 200
        assert patch_model_resp.json()["data"]["enabled"] is False
        assert patch_model_resp.json()["data"]["displayName"] == "Claude Sonnet Disabled"

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

        delete_model_resp = client.delete(
            "/api/v1/model-runtime/models/openrouter-main/claude-sonnet",
            headers=headers,
        )
        assert delete_model_resp.status_code == 200
        assert delete_model_resp.json()["data"]["deleted"] is True


def test_model_pricing_fields_roundtrip_and_validation(
    tmp_path: Path, make_boot_stub, make_auth_headers
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openai-main",
                "type": "openai",
                "displayName": "OpenAI Main",
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "openai-main/gpt-priced",
                "providerId": "openai-main",
                "litellmModel": "openai/gpt-4.1-mini",
                "displayName": "GPT Priced",
                "capabilities": ["chat"],
                "costMetadata": {
                    "inputPerMillionTokens": 1.25,
                    "outputPerMillionTokens": "4.5",
                    "cacheWritePerMillionTokens": 0.8,
                    "cacheReadPerMillionTokens": "",
                    "vendorTier": "standard",
                },
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201
        payload = model_resp.json()["data"]
        assert payload["costMetadata"]["inputPerMillionTokens"] == 1.25
        assert payload["costMetadata"]["outputPerMillionTokens"] == 4.5
        assert payload["costMetadata"]["cacheWritePerMillionTokens"] == 0.8
        assert payload["costMetadata"]["cacheReadPerMillionTokens"] is None
        assert payload["costMetadata"]["vendorTier"] == "standard"

        patch_resp = client.patch(
            "/api/v1/model-runtime/models/openai-main/gpt-priced",
            headers=headers,
            json={
                "costMetadata": {
                    "inputPerMillionTokens": 2,
                    "outputPerMillionTokens": 6,
                    "cacheWritePerMillionTokens": 1,
                    "cacheReadPerMillionTokens": 0.5,
                    "vendorTier": "premium",
                }
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]["costMetadata"]
        assert patched["inputPerMillionTokens"] == 2.0
        assert patched["cacheReadPerMillionTokens"] == 0.5
        assert patched["vendorTier"] == "premium"

        invalid_resp = client.patch(
            "/api/v1/model-runtime/models/openai-main/gpt-priced",
            headers=headers,
            json={"costMetadata": {"inputPerMillionTokens": -1}},
        )
        assert invalid_resp.status_code == 422


