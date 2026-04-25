from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.persistence import (
    AgentRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    PersonaRecord,
    PromptDefinitionRecord,
)


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "instances": [
                {
                    "id": "inst-1",
                    "name": "Instance 1",
                    "adapterType": "mock",
                    "platform": "mock",
                    "config": {},
                    "createdAt": 1,
                    "lastModified": 1,
                }
            ],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def _seed_models(bot: ShinBot) -> None:
    bot.database.model_registry.upsert_provider(
        ModelProviderRecord(
            id="openai-main",
            type="openai",
            display_name="OpenAI Main",
            base_url="https://api.openai.com/v1",
        )
    )
    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-fast",
            provider_id="openai-main",
            litellm_model="gpt-4.1-mini",
            display_name="GPT Fast",
            capabilities=["chat"],
        )
    )
    bot.database.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="openai-main/gpt-backup",
            provider_id="openai-main",
            litellm_model="gpt-4.1",
            display_name="GPT Backup",
            capabilities=["chat"],
        )
    )


def test_bot_config_crud_roundtrip(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    _seed_models(bot)
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-persona-1",
            prompt_id="persona.persona-1",
            name="Persona Prompt",
            source_type="persona",
            source_id="persona-1",
            stage="identity",
            type="static_text",
            content="You are helpful.",
        )
    )
    bot.database.personas.upsert(
        PersonaRecord(uuid="persona-1", name="Persona", prompt_definition_uuid="prompt-persona-1")
    )
    bot.database.agents.upsert(
        AgentRecord(
            uuid="agent-uuid-1",
            agent_id="agent.default",
            name="Default Agent",
            persona_uuid="persona-1",
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/bot-configs",
            headers=headers,
            json={
                "instanceId": "inst-1",
                "defaultAgentUuid": "agent-uuid-1",
                "mainLlm": "openai-main/gpt-fast",
                "explicitPromptCacheEnabled": True,
                "responseProfile": "balanced",
                "responseProfilePrivate": "immediate",
                "responseProfilePriority": "immediate",
                "responseProfileGroup": "passive",
                "config": {"replyMode": "group"},
                "tags": ["prod", "prod", "default"],
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["instanceId"] == "inst-1"
        assert created["defaultAgentUuid"] == "agent-uuid-1"
        assert created["mainLlm"] == "openai-main/gpt-fast"
        assert created["explicitPromptCacheEnabled"] is True
        assert created["responseProfile"] == "balanced"
        assert created["responseProfilePrivate"] == "immediate"
        assert created["responseProfilePriority"] == "immediate"
        assert created["responseProfileGroup"] == "passive"
        assert created["config"]["replyMode"] == "group"
        assert created["tags"] == ["prod", "default"]

        config_uuid = created["uuid"]

        patch_resp = client.patch(
            f"/api/v1/bot-configs/{config_uuid}",
            headers=headers,
            json={
                "mainLlm": "openai-main/gpt-backup",
                "explicitPromptCacheEnabled": False,
                "responseProfileGroup": "balanced",
                "config": {"replyMode": "private"},
                "tags": ["staging"],
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["mainLlm"] == "openai-main/gpt-backup"
        assert patched["explicitPromptCacheEnabled"] is False
        assert patched["responseProfile"] == "balanced"
        assert patched["responseProfilePrivate"] == "immediate"
        assert patched["responseProfilePriority"] == "immediate"
        assert patched["responseProfileGroup"] == "balanced"
        assert patched["config"]["replyMode"] == "private"
        assert patched["tags"] == ["staging"]

        list_resp = client.get("/api/v1/bot-configs", headers=headers)
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1

        delete_resp = client.delete(f"/api/v1/bot-configs/{config_uuid}", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_bot_config_validates_instance_and_uniqueness(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        missing_instance_resp = client.post(
            "/api/v1/bot-configs",
            headers=headers,
            json={"instanceId": "missing-inst"},
        )
        assert missing_instance_resp.status_code == 404
        assert missing_instance_resp.json()["error"]["code"] == "INSTANCE_NOT_FOUND"

        first_resp = client.post(
            "/api/v1/bot-configs",
            headers=headers,
            json={"instanceId": "inst-1"},
        )
        assert first_resp.status_code == 201

        duplicate_resp = client.post(
            "/api/v1/bot-configs",
            headers=headers,
            json={"instanceId": "inst-1"},
        )
        assert duplicate_resp.status_code == 409
        assert duplicate_resp.json()["error"]["code"] == "BOT_CONFIG_ALREADY_EXISTS"


def test_bot_config_rejects_litellm_model_target(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    _seed_models(bot)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/bot-configs",
            headers=headers,
            json={
                "instanceId": "inst-1",
                "mainLlm": "gpt-4.1-mini",
            },
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "MODEL_TARGET_NOT_FOUND"
    assert "openai-main/gpt-fast" in response.json()["error"]["message"]
