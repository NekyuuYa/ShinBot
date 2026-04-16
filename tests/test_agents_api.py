from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.persistence import PersonaRecord, PromptDefinitionRecord


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


def test_agent_crud_roundtrip(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-persona-1",
            prompt_id="persona.persona-1",
            name="Assistant Persona Prompt",
            source_type="persona",
            source_id="persona-1",
            stage="identity",
            type="static_text",
            content="You are helpful.",
        )
    )
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-1",
            prompt_id="prompt.identity.extra",
            name="Identity Extra",
            source_type="agent_plugin",
            source_id="plugin.identity",
            stage="identity",
            type="static_text",
            content="extra identity",
        )
    )
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-2",
            prompt_id="prompt.instructions.chat",
            name="Chat Prompt",
            source_type="agent_plugin",
            source_id="plugin.chat",
            stage="instructions",
            type="static_text",
            content="chat instructions",
        )
    )
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-3",
            prompt_id="prompt.instructions.primary",
            name="Primary Prompt",
            source_type="agent_plugin",
            source_id="plugin.chat",
            stage="instructions",
            type="static_text",
            content="primary instructions",
        )
    )
    bot.database.personas.upsert(
        PersonaRecord(
            uuid="persona-1",
            name="Assistant Persona",
            prompt_definition_uuid="prompt-persona-1",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/agents",
            headers=headers,
            json={
                "agentId": "agent.default",
                "name": "Default Agent",
                "personaUuid": "persona-1",
                "prompts": ["prompt-1", "prompt-1", "prompt-2"],
                "tools": ["tool.echo", "tool.echo", "tool.search"],
                "contextStrategy": {
                    "ref": "builtin.context.sliding_window",
                    "type": "sliding_window",
                    "params": {"triggerRatio": 0.5, "trimTurns": 2},
                },
                "config": {"modelId": "openai-main/gpt-fast"},
                "tags": ["default", "default", "chat"],
            },
        )
        assert create_resp.status_code == 201
        created = create_resp.json()["data"]
        assert created["uuid"]
        assert created["agentId"] == "agent.default"
        assert created["prompts"] == ["prompt-1", "prompt-2"]
        assert created["tools"] == ["tool.echo", "tool.search"]
        assert created["contextStrategy"] == {
            "ref": "builtin.context.sliding_window",
            "type": "sliding_window",
            "params": {"triggerRatio": 0.5, "trimTurns": 2},
        }
        assert created["config"]["modelId"] == "openai-main/gpt-fast"
        assert created["tags"] == ["default", "chat"]

        agent_uuid = created["uuid"]

        get_resp = client.get(f"/api/v1/agents/{agent_uuid}", headers=headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["data"]["uuid"] == agent_uuid

        patch_resp = client.patch(
            f"/api/v1/agents/{agent_uuid}",
            headers=headers,
            json={
                "agentId": "agent.primary",
                "name": "Primary Agent",
                "prompts": ["prompt-3"],
                "tools": ["tool.search"],
                "contextStrategy": {},
                "config": {"modelId": "openai-main/gpt-backup"},
                "tags": ["primary"],
            },
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()["data"]
        assert patched["agentId"] == "agent.primary"
        assert patched["prompts"] == ["prompt-3"]
        assert patched["tools"] == ["tool.search"]
        assert patched["contextStrategy"] == {}
        assert patched["config"]["modelId"] == "openai-main/gpt-backup"
        assert patched["tags"] == ["primary"]

        list_resp = client.get("/api/v1/agents", headers=headers)
        assert list_resp.status_code == 200
        assert len(list_resp.json()["data"]) == 1
        assert list_resp.json()["data"][0]["uuid"] == agent_uuid

        delete_resp = client.delete(f"/api/v1/agents/{agent_uuid}", headers=headers)
        assert delete_resp.status_code == 200
        assert delete_resp.json()["data"]["deleted"] is True


def test_agent_rejects_missing_persona_and_duplicate_agent_id(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-persona-1",
            prompt_id="persona.persona-1",
            name="Assistant Persona Prompt",
            source_type="persona",
            source_id="persona-1",
            stage="identity",
            type="static_text",
            content="You are helpful.",
        )
    )
    bot.database.personas.upsert(
        PersonaRecord(
            uuid="persona-1",
            name="Assistant Persona",
            prompt_definition_uuid="prompt-persona-1",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        missing_persona_resp = client.post(
            "/api/v1/agents",
            headers=headers,
            json={
                "agentId": "agent.default",
                "name": "Default Agent",
                "personaUuid": "missing-persona",
                "tools": [],
                "contextStrategy": {},
            },
        )
        assert missing_persona_resp.status_code == 404
        assert missing_persona_resp.json()["error"]["code"] == "PERSONA_NOT_FOUND"

        first_resp = client.post(
            "/api/v1/agents",
            headers=headers,
            json={
                "agentId": "agent.default",
                "name": "Default Agent",
                "personaUuid": "persona-1",
                "tools": [],
                "contextStrategy": {},
            },
        )
        assert first_resp.status_code == 201

        second_resp = client.post(
            "/api/v1/agents",
            headers=headers,
            json={
                "agentId": "agent.default",
                "name": "Default Agent 2",
                "personaUuid": "persona-1",
                "tools": [],
                "contextStrategy": {},
            },
        )
        assert second_resp.status_code == 409
        assert second_resp.json()["error"]["code"] == "AGENT_ALREADY_EXISTS"
