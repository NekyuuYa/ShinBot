from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_provider_probe_endpoint_uses_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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

        monkeypatch.setattr(
            "shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion
        )

        response = client.post(
            "/api/v1/model-runtime/providers/custom-openai-main/probe",
            headers=headers,
            json={},
        )

    assert response.status_code == 200
    assert captured["model"] == "qwen3.5-plus-2026-02-15"
    assert captured["api_base"] == "https://api.example.com/v1"
    assert captured["custom_llm_provider"] == "openai"


def test_provider_probe_dashscope_uses_dashscope_provider_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

    with TestClient(app) as client:
        provider_resp = client.post(
            "/api/v1/model-runtime/providers",
            headers=headers,
            json={
                "id": "dashscope-main",
                "type": "dashscope",
                "displayName": "DashScope Main",
                "baseUrl": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "auth": {"api_key": "secret"},
            },
        )
        assert provider_resp.status_code == 201

        model_resp = client.post(
            "/api/v1/model-runtime/models",
            headers=headers,
            json={
                "id": "dashscope-main/qwen",
                "providerId": "dashscope-main",
                "litellmModel": "qwen3.5-flash",
                "displayName": "Qwen Flash",
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

        monkeypatch.setattr(
            "shinbot.agent.model_runtime.litellm_adapter.completion", fake_completion
        )

        response = client.post(
            "/api/v1/model-runtime/providers/dashscope-main/probe",
            headers=headers,
            json={},
        )

    assert response.status_code == 200
    assert captured["model"] == "qwen3.5-flash"
    assert captured["api_base"] == "https://dashscope.aliyuncs.com/compatible-mode/v1"
    assert captured["custom_llm_provider"] == "dashscope"


