from __future__ import annotations

from pathlib import Path
from typing import Any

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


def test_create_model_auto_infers_context_window(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

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
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

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
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

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

        captured: dict[str, object] = {}

        async def fake_generate(call):
            captured["params"] = dict(call.params)
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
    assert captured["params"] == {"max_tokens": 1, "drop_params": True}


def test_provider_probe_endpoint_surfaces_runtime_errors_without_unhandled_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
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
                "id": "openrouter-main/gemma",
                "providerId": "openrouter-main",
                "litellmModel": "openrouter/google/gemma-4-31b-it:free",
                "displayName": "Gemma",
                "capabilities": ["chat"],
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201

        async def fake_generate(call):
            from shinbot.agent.model_runtime import ModelCallError

            raise ModelCallError("unsupported params")

        monkeypatch.setattr(bot.model_runtime, "generate", fake_generate)

        response = client.post(
            "/api/v1/model-runtime/providers/openrouter-main/probe",
            headers=headers,
            json={},
        )

    assert response.status_code == 502
    assert response.json()["error"]["message"] == "Provider probe failed: unsupported params"


def test_provider_probe_custom_openai_uses_openai_provider_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "custom-openai-main",
                "type": "custom_openai",
                "displayName": "Custom OpenAI Main",
                "baseUrl": "https://api.example.com/v1",
                "auth": {"api_key": "secret"},
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "custom-openai-main/qwen",
                "providerId": "custom-openai-main",
                "litellmModel": "qwen3.5-plus-2026-02-15",
                "displayName": "Qwen",
                "capabilities": ["chat"],
                "enabled": True,
            },
        )
        assert model_resp.status_code == 201

        captured: dict[str, Any] = {}

        def fake_completion(**kwargs):
            captured.update(kwargs)
            return {
                "model": kwargs["model"],
                "choices": [{"message": {"content": "pong"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr("shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion)

        response = client.post(
            "/api/v1/model-runtime/providers/custom-openai-main/probe",
            headers=headers,
            json={},
        )

    assert response.status_code == 200
    assert captured["model"] == "qwen3.5-plus-2026-02-15"
    assert captured["api_base"] == "https://api.example.com/v1"
    assert captured["custom_llm_provider"] == "openai"


def test_provider_and_route_path_ids_support_nested_segments(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "tenant/openai-main",
                "type": "openai",
                "displayName": "OpenAI Main",
                "baseUrl": "https://api.openai.com/v1",
            },
        )
        assert provider_resp.status_code == 201

        patch_provider_resp = client.patch(
            "/api/v1/model-runtime/providers/tenant/openai-main",
            headers=headers,
            json={"displayName": "OpenAI Tenant Main"},
        )
        assert patch_provider_resp.status_code == 200
        assert patch_provider_resp.json()["data"]["displayName"] == "OpenAI Tenant Main"

        route_resp = client.post(
            "/api/v1/model-runtime/routes",
            headers=headers,
            json={
                "id": "agent/chat/default",
                "purpose": "default chat",
                "strategy": "priority",
                "enabled": True,
                "stickySessions": False,
                "metadata": {"domain": "chat"},
                "members": [],
            },
        )
        assert route_resp.status_code == 201

        patch_route_resp = client.patch(
            "/api/v1/model-runtime/routes/agent/chat/default",
            headers=headers,
            json={"strategy": "weighted"},
        )
        assert patch_route_resp.status_code == 200
        assert patch_route_resp.json()["data"]["strategy"] == "weighted"

        delete_route_resp = client.delete(
            "/api/v1/model-runtime/routes/agent/chat/default",
            headers=headers,
        )
        assert delete_route_resp.status_code == 200
        assert delete_route_resp.json()["data"]["deleted"] is True

        delete_provider_resp = client.delete(
            "/api/v1/model-runtime/providers/tenant/openai-main",
            headers=headers,
        )
        assert delete_provider_resp.status_code == 200
        assert delete_provider_resp.json()["data"]["deleted"] is True


def test_provider_capability_type_stored_and_returned(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        for capability_type in (
            "completion",
            "embedding",
            "rerank",
            "tts",
            "stt",
            "image",
            "video",
        ):
            resp = client.post(
                "/api/v1/model-runtime/providers",
                headers=headers,
                json={
                    "id": f"provider-{capability_type}",
                    "type": "openai",
                    "displayName": f"Provider {capability_type}",
                    "capabilityType": capability_type,
                    "baseUrl": "https://api.example.com/v1",
                },
            )
            assert resp.status_code == 201, f"Failed for {capability_type}: {resp.text}"
            assert resp.json()["data"]["capabilityType"] == capability_type

        list_resp = client.get("/api/v1/model-runtime/providers", headers=headers)
        assert list_resp.status_code == 200
        returned_types = {p["capabilityType"] for p in list_resp.json()["data"]}
        assert returned_types == {
            "completion",
            "embedding",
            "rerank",
            "tts",
            "stt",
            "image",
            "video",
        }


def test_provider_probe_embedding_type_uses_embed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "embed-provider",
                "type": "openai",
                "displayName": "Embed Provider",
                "capabilityType": "embedding",
                "baseUrl": "https://api.openai.com/v1",
            },
        )
        client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "embed-provider/text-embedding-3-small",
                "providerId": "embed-provider",
                "litellmModel": "text-embedding-3-small",
                "displayName": "Text Embedding 3 Small",
                "capabilities": ["embedding"],
                "enabled": True,
            },
        )

        async def fake_embed(call):
            return type(
                "FakeResult", (), {"execution_id": "embed-probe-exec", "embedding": [0.1]}
            )()

        monkeypatch.setattr(bot.model_runtime, "embed", fake_embed)

        resp = client.post(
            "/api/v1/model-runtime/providers/embed-provider/probe",
            headers=headers,
            json={},
        )

    assert resp.status_code == 200
    assert resp.json()["data"]["mode"] == "embedding"
    assert resp.json()["data"]["executionId"] == "embed-probe-exec"


def test_provider_probe_non_completion_types_use_catalog_or_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        for capability_type in ("rerank", "tts", "stt", "image", "video"):
            client.post(
                "/api/v1/model-runtime/providers",
                headers=headers,
                json={
                    "id": f"probe-{capability_type}",
                    "type": "openai",
                    "displayName": f"Probe {capability_type}",
                    "capabilityType": capability_type,
                    "baseUrl": "https://api.example.com/v1",
                },
            )
            client.post(
                "/api/v1/model-runtime/models",
                headers=headers,
                json={
                    "id": f"probe-{capability_type}/model-1",
                    "providerId": f"probe-{capability_type}",
                    "litellmModel": "model-1",
                    "displayName": "Model 1",
                    "capabilities": [capability_type],
                    "enabled": True,
                },
            )
            resp = client.post(
                f"/api/v1/model-runtime/providers/probe-{capability_type}/probe",
                headers=headers,
                json={},
            )
            assert resp.status_code == 200, f"Probe failed for {capability_type}: {resp.text}"
            # Should use catalog (which will fail since no real API) or skip — either is valid
            assert resp.json()["data"]["success"] is True
            assert resp.json()["data"]["mode"] in ("catalog", "skipped")
