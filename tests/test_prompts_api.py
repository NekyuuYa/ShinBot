from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.prompting import PromptComponent, PromptComponentKind, PromptStage
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


def test_prompts_list_route_returns_registered_prompt_components(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
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
    assert len(payload) == 1
    assert payload[0] == {
        "id": "prompt.identity.extra",
        "displayName": "Identity Extra",
        "description": "Additional identity prompt",
        "stage": "identity",
        "type": "static_text",
        "version": "1.0.0",
        "priority": 20,
        "enabled": True,
        "cacheStable": True,
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
