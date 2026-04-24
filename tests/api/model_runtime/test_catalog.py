from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_provider_catalog_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
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
            },
        )
        assert create_resp.status_code == 201

        async def fake_catalog(_database, _provider_id):
            return [
                {
                    "id": "gpt-4.1-mini",
                    "displayName": "GPT-4.1 Mini",
                    "litellmModel": "gpt-4.1-mini",
                    "contextWindow": 128000,
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
    assert response.json()["data"][0]["contextWindow"] == 128000


def test_update_model_reinfers_context_window_when_model_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    inference_values = {
        "gpt-4.1-mini": 128000,
        "openrouter/anthropic/claude-3.7-sonnet": 200000,
    }
    monkeypatch.setattr(
        "shinbot.api.routers.model_runtime.infer_context_window",
        lambda provider, litellm_model: inference_values.get(litellm_model),
    )

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openrouter-main",
                "type": "openrouter",
                "displayName": "OpenRouter Main",
                "baseUrl": "https://openrouter.ai/api/v1",
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "openrouter-main/primary",
                "providerId": "openrouter-main",
                "litellmModel": "gpt-4.1-mini",
                "displayName": "Primary",
                "capabilities": ["chat"],
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201
        assert model_resp.json()["data"]["contextWindow"] == 128000

        patch_resp = client.patch(
            "/api/v1/model-runtime/models/openrouter-main/primary",
            headers=headers,
            json={
                "litellmModel": "openrouter/anthropic/claude-3.7-sonnet",
                "displayName": "Claude Primary",
            },
        )

    assert patch_resp.status_code == 200
    assert patch_resp.json()["data"]["contextWindow"] == 200000


def test_update_model_keeps_existing_context_window_when_reinference_fails(
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
        lambda provider, litellm_model: None,
    )

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "openrouter-main",
                "type": "openrouter",
                "displayName": "OpenRouter Main",
                "baseUrl": "https://openrouter.ai/api/v1",
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "openrouter-main/primary",
                "providerId": "openrouter-main",
                "litellmModel": "openrouter/anthropic/claude-3.7-sonnet",
                "displayName": "Primary",
                "capabilities": ["chat"],
                "contextWindow": 200000,
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201

        patch_resp = client.patch(
            "/api/v1/model-runtime/models/openrouter-main/primary",
            headers=headers,
            json={
                "displayName": "Claude Primary",
                "litellmModel": "openrouter/anthropic/claude-4-sonnet",
            },
        )

    assert patch_resp.status_code == 200
    assert patch_resp.json()["data"]["contextWindow"] == 200000


