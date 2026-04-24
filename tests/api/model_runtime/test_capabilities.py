from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_provider_and_route_path_ids_support_nested_segments(
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


def test_provider_capability_type_stored_and_returned(
    tmp_path: Path, make_boot_stub, make_auth_headers
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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


def test_provider_probe_embedding_type_uses_embed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    make_boot_stub,
    make_auth_headers,
):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, make_boot_stub(tmp_path))
    headers = make_auth_headers(app)

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
