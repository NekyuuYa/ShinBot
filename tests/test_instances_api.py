from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.builtin_plugins.shinbot_adapter_satori.adapter import SatoriAdapter, SatoriConfig
from shinbot.core.application.app import ShinBot
from shinbot.persistence import AgentRecord, BotConfigRecord, PersonaRecord, PromptDefinitionRecord
from tests.conftest import MockAdapter


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "instances": [],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.save_config_calls = 0

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return True


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def test_delete_instance_route_removes_runtime_and_persisted_config(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    adapter = bot.add_adapter("inst-1", "mock")
    asyncio.run(bot.adapter_manager.start_instance("inst-1"))

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {"token": "abc"},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.delete("/api/v1/instances/inst-1", headers=_auth_headers(app))

    assert response.status_code == 200
    assert response.json()["data"] == {"id": "inst-1", "deleted": True}
    assert bot.adapter_manager.get_instance("inst-1") is None
    assert adapter.stopped is True
    assert boot.config["instances"] == []
    assert boot.save_config_calls == 1


def test_update_instance_route_returns_full_instance_payload(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    bot.add_adapter("inst-1", "mock")

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {"token": "abc"},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.patch(
            "/api/v1/instances/inst-1",
            headers=_auth_headers(app),
            json={"name": "Renamed", "config": {"token": "xyz"}},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "id": "inst-1",
        "name": "Renamed",
        "adapterType": "mock",
        "status": "stopped",
        "config": {"token": "xyz"},
        "botConfig": None,
        "createdAt": 1,
        "lastModified": boot.config["instances"][0]["lastModified"],
    }


def test_instances_runtime_config_serializes_dataclass_adapter_config(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    adapter = SatoriAdapter(
        instance_id="satori-1",
        platform="satori",
        config=SatoriConfig(host="127.0.0.1:5140", token="abc"),
    )
    bot.adapter_manager._instances["satori-1"] = adapter

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "satori-1",
            "name": "Satori 1",
            "adapterType": "satori",
            "platform": "satori",
            "config": {},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.get("/api/v1/instances", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"][0]
    assert payload["config"]["host"] == "127.0.0.1:5140"
    assert payload["config"]["token"] == "abc"


def test_update_instance_updates_dataclass_adapter_runtime_config(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    adapter = SatoriAdapter(
        instance_id="satori-1",
        platform="satori",
        config=SatoriConfig(host="127.0.0.1:5140", token="abc"),
    )
    bot.adapter_manager._instances["satori-1"] = adapter

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "satori-1",
            "name": "Satori 1",
            "adapterType": "satori",
            "platform": "satori",
            "config": {"host": "127.0.0.1:5140", "token": "abc"},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.patch(
            "/api/v1/instances/satori-1",
            headers=_auth_headers(app),
            json={"config": {"token": "xyz"}},
        )

    assert response.status_code == 200
    assert adapter.config.token == "xyz"
    assert response.json()["data"]["config"]["token"] == "xyz"


def test_list_instances_includes_bot_config_summary(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
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
    bot.database.bot_configs.upsert(
        BotConfigRecord(
            uuid="bot-config-1",
            instance_id="inst-1",
            default_agent_uuid="agent-uuid-1",
            main_llm="openai-main/gpt-fast",
            config={},
            tags=["prod"],
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
        )
    )

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {},
            "createdAt": 1,
            "lastModified": 1,
        }
    ]

    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.get("/api/v1/instances", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"][0]
    assert payload["id"] == "inst-1"
    assert payload["botConfig"] == {
        "uuid": "bot-config-1",
        "defaultAgentUuid": "agent-uuid-1",
        "mainLlm": "openai-main/gpt-fast",
        "tags": ["prod"],
    }


def test_status_websocket_includes_instance_details(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    bot.add_adapter("inst-1", "mock")

    boot = _BootStub(tmp_path)
    boot.config["instances"] = [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapterType": "mock",
            "platform": "mock",
            "config": {},
            "createdAt": 1,
            "lastModified": 1,
        },
        {
            "id": "inst-2",
            "name": "Instance 2",
            "adapterType": "mock",
            "platform": "mock",
            "config": {},
            "createdAt": 1,
            "lastModified": 1,
        },
    ]

    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()

    with TestClient(app) as client:
        with client.websocket_connect(f"/ws/status?token={token}") as websocket:
            payload = websocket.receive_json()

    assert payload["success"] is True
    assert payload["data"]["totalInstances"] == 2
    assert payload["data"]["runningInstances"] == 0
    assert payload["data"]["instances"] == [
        {"id": "inst-1", "running": False},
        {"id": "inst-2", "running": False},
    ]
