from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.prompt_manager import PromptComponent, PromptComponentKind, PromptStage
from shinbot.agent.runtime import install_agent_runtime
from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.persistence.records import PromptDefinitionRecord, utc_now_iso


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


def test_prompts_list_route_returns_registered_prompt_components(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    install_agent_runtime(bot)
    bot.prompt_registry.register_component(
        PromptComponent(
            id="prompt.identity.extra",
            stage=PromptStage.IDENTITY,
            kind=PromptComponentKind.STATIC_TEXT,
            content="identity text",
            priority=20,
            tags=["identity", "agent"],
            metadata={
                "display_name": "Identity Extra",
                "description": "Additional identity prompt",
                "owner_plugin_id": "plugin.identity",
                "owner_module": "shinbot.plugins.identity",
            },
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        response = client.get("/api/v1/prompts", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    payload_by_id = {item["id"]: item for item in payload}
    assert {
        "prompt.identity.extra",
        "builtin.instructions.identity_map",
        "builtin.constraints.identity_behavior",
    } <= set(payload_by_id)
    assert payload_by_id["prompt.identity.extra"] == {
        "id": "prompt.identity.extra",
        "displayName": "Identity Extra",
        "description": "Additional identity prompt",
        "stage": "identity",
        "type": "static_text",
        "version": "1.0.0",
        "priority": 20,
        "enabled": True,
        "resolverRef": "",
        "templateVars": [],
        "bundleRefs": [],
        "tags": ["identity", "agent"],
        "sourceType": "agent_plugin",
        "sourceId": "plugin.identity",
        "ownerPluginId": "plugin.identity",
        "ownerModule": "shinbot.plugins.identity",
        "modulePath": "",
        "metadata": {
            "display_name": "Identity Extra",
            "description": "Additional identity prompt",
            "owner_plugin_id": "plugin.identity",
            "owner_module": "shinbot.plugins.identity",
        },
    }


def test_prompts_list_route_includes_database_prompt_definitions(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    now = utc_now_iso()
    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid="prompt-db-1",
            prompt_id="prompt.user.custom",
            name="User Custom Prompt",
            stage="instructions",
            type="static_text",
            source_type="unknown_source",
            source_id="",
            priority=55,
            version="1.0.0",
            description="Custom prompt from database",
            enabled=True,
            content="custom prompt text",
            metadata={"display_name": "User Custom Prompt"},
            created_at=now,
            updated_at=now,
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        response = client.get("/api/v1/prompts", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    payload_by_id = {item["id"]: item for item in payload}
    assert "prompt.user.custom" in payload_by_id
    assert payload_by_id["prompt.user.custom"] == {
        "id": "prompt.user.custom",
        "displayName": "User Custom Prompt",
        "description": "Custom prompt from database",
        "stage": "instructions",
        "type": "static_text",
        "version": "1.0.0",
        "priority": 55,
        "enabled": True,
        "resolverRef": "",
        "templateVars": [],
        "bundleRefs": [],
        "tags": [],
        "sourceType": "unknown_source",
        "sourceId": "",
        "ownerPluginId": "",
        "ownerModule": "",
        "modulePath": "",
        "metadata": {"display_name": "User Custom Prompt"},
    }
