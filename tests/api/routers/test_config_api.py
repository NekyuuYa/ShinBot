from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.application.bot_routing import command_prefixes_for_context
from shinbot.core.config_provider import load_provider_schema


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "runtime": {"model": False, "agent": True},
            "adapter_instances": [],
            "plugins": [],
            "bots": [],
        }
        self.config_path = data_dir / "config.toml"
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


def _schema_path(relative: str) -> Path:
    return _repo_root() / relative


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not locate repository root")


def _onebot_schema_path() -> Path:
    return _schema_path("shinbot/builtin_plugins/shinbot_adapter_onebot_v11/config.schema.toml")


def test_config_workspace_exposes_frontend_contract(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.config_provider_registry.register(load_provider_schema(_onebot_schema_path()))
    boot = _BootStub(tmp_path)
    boot.config["adapter_instances"] = [
        {
            "id": "qq-main",
            "adapter": "onebot_v11",
            "enabled": True,
            "config": {"mode": "reverse"},
        }
    ]
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.get("/api/v1/config", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["version"] == 1
    assert payload["config"]["adapter_instances"][0]["id"] == "qq-main"
    assert payload["templates"]["bot"]["administrators"] == []
    assert payload["templates"]["bot"]["agent"] == {"mode": "none", "config": ""}
    assert payload["options"]["agentModes"] == ["none", "simple", "full"]
    assert "onebot_v11" in payload["options"]["adapterPlatforms"]
    assert payload["providers"]["adapters"][0]["id"] == "onebot_v11"
    assert payload["providers"]["adapters"][0]["defaults"]["mode"] == "reverse"
    assert payload["validation"]["valid"] is True
    assert payload["runtime"]["modelEnabled"] is False
    assert payload["runtime"]["requiresRestartAfterSave"] is True
    assert payload["runtime"]["adapterInstances"] == [
        {
            "id": "qq-main",
            "running": False,
            "connected": False,
            "available": False,
        }
    ]


def test_config_workspace_marks_model_enabled_when_agent_bot_requires_it(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    boot.config.pop("runtime")
    agent_path = tmp_path / "agents" / "full-agent.toml"
    agent_path.parent.mkdir(parents=True)
    agent_path.write_text('[agent]\nid = "full-agent"\n', encoding="utf-8")
    boot.config["bots"] = [
        {
            "id": "full-agent",
            "display_name": "Full Agent",
            "enabled": True,
            "agent": {"mode": "full", "config": "agents/full-agent.toml"},
            "bindings": [],
        }
    ]
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.get("/api/v1/config", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["validation"]["valid"] is True
    assert payload["runtime"]["modelEnabled"] is True
    assert payload["runtime"]["modelMounted"] is False


def test_config_validate_reports_boot_and_provider_issues(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.config_provider_registry.register(load_provider_schema(_onebot_schema_path()))
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/config/validate",
            headers=_auth_headers(app),
            json={
                "config": {
                    "runtime": {"model": "yes"},
                    "adapter_instances": [
                        {
                            "id": "qq-main",
                            "adapter": "onebot_v11",
                            "enabled": True,
                            "config": {"mode": "bad"},
                        }
                    ],
                }
            },
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["valid"] is False
    assert {
        (issue["source"], issue["path"], issue["code"]) for issue in payload["issues"]
    } == {
        ("boot", "runtime.model", "type"),
        ("provider", "adapter_instances[0].config.mode", "choices"),
    }


def test_config_save_persists_valid_config_and_marks_restart(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    next_config = {
        "admin": dict(boot.config["admin"]),
        "runtime": {"model": True, "agent": True},
        "adapter_instances": [],
        "plugins": [],
        "bots": [],
    }

    with TestClient(app) as client:
        response = client.put(
            "/api/v1/config",
            headers=_auth_headers(app),
            json={"config": next_config},
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["saved"] is True
    assert payload["requiresRestart"] is True
    assert boot.config["runtime"] == {"model": True, "agent": True}
    assert boot.save_config_calls == 1


def test_config_save_rejects_invalid_config_with_issues(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.put(
            "/api/v1/config",
            headers=_auth_headers(app),
            json={"config": {"runtime": {"agent": "no"}}},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "CONFIG_VALIDATION_FAILED"
    assert response.json()["data"]["issues"] == [
        {
            "path": "runtime.agent",
            "message": "must be a boolean",
            "code": "type",
            "source": "boot",
        }
    ]
    assert boot.save_config_calls == 0


def test_config_save_adapter_instances_persists_section_only(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    boot.config["bots"] = [
        {
            "id": "bot-main",
            "display_name": "Bot Main",
            "enabled": True,
            "agent": {"mode": "none", "config": ""},
            "bindings": [],
        }
    ]
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.put(
            "/api/v1/config/adapter-instances",
            headers=_auth_headers(app),
            json={
                "adapterInstances": [
                    {
                        "id": "qq-main",
                        "adapter": "onebot_v11",
                        "enabled": True,
                        "config": {"mode": "reverse"},
                    }
                ],
            },
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["saved"] is True
    assert boot.config["adapter_instances"][0]["id"] == "qq-main"
    assert boot.config["bots"][0]["id"] == "bot-main"
    assert boot.save_config_calls == 1


def test_config_save_adapter_instances_roundtrip_via_workspace(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    boot.config["adapter_instances"] = []
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        save_resp = client.put(
            "/api/v1/config/adapter-instances",
            headers=_auth_headers(app),
            json={
                "adapterInstances": [
                    {
                        "id": "qq-main",
                        "adapter": "onebot_v11",
                        "enabled": True,
                        "config": {"mode": "reverse"},
                    }
                ],
            },
        )
        assert save_resp.status_code == 200

        workspace_resp = client.get("/api/v1/config", headers=_auth_headers(app))

    assert workspace_resp.status_code == 200
    payload = workspace_resp.json()["data"]
    assert payload["config"]["adapter_instances"] == [
        {
            "id": "qq-main",
            "adapter": "onebot_v11",
            "enabled": True,
            "config": {"mode": "reverse"},
        }
    ]
    assert payload["validation"]["normalized"]["adapterInstances"][0]["id"] == "qq-main"
    assert boot.save_config_calls == 1


def test_config_save_bots_persists_section_only(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    boot.config["adapter_instances"] = [
        {
            "id": "qq-main",
            "adapter": "onebot_v11",
            "enabled": True,
            "config": {"mode": "reverse"},
        }
    ]
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.put(
            "/api/v1/config/bots",
            headers=_auth_headers(app),
            json={
                "bots": [
                    {
                        "id": "bot-main",
                        "display_name": "Bot Main",
                        "enabled": True,
                        "administrators": ["qq-main:user-admin"],
                        "commands": {"enabled": True, "prefixes": ["/"]},
                        "plugins": {
                            "enabled": True,
                            "enabled_plugins": ["*"],
                            "disabled_plugins": [],
                        },
                        "agent": {"mode": "none", "config": ""},
                        "bindings": [
                            {
                                "id": "bot-main-binding-1",
                                "adapter_instance_id": "qq-main",
                                "session_patterns": ["group:*"],
                                "enabled": True,
                                "priority": 0,
                            }
                        ],
                    }
                ],
            },
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["saved"] is True
    assert boot.config["bots"][0]["id"] == "bot-main"
    assert boot.config["bots"][0]["administrators"] == ["qq-main:user-admin"]
    assert boot.config["adapter_instances"][0]["id"] == "qq-main"
    assert boot.save_config_calls == 1


def test_config_save_bots_refreshes_runtime_prefix_routing(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    boot.config["adapter_instances"] = [
        {
            "id": "qq-main",
            "adapter": "onebot_v11",
            "enabled": True,
            "config": {"mode": "reverse"},
        }
    ]
    boot.config["bots"] = [
        {
            "id": "bot-main",
            "display_name": "Bot Main",
            "enabled": True,
            "commands": {"enabled": True, "prefixes": ["/"]},
            "plugins": {
                "enabled": True,
                "enabled_plugins": ["*"],
                "disabled_plugins": [],
            },
            "agent": {"mode": "none", "config": ""},
            "bindings": [
                {
                    "id": "binding-main",
                    "adapter_instance_id": "qq-main",
                    "session_patterns": ["group:*"],
                    "enabled": True,
                    "priority": 0,
                }
            ],
        }
    ]
    bot.configure_bot_service_configs(())
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.put(
            "/api/v1/config/bots",
            headers=_auth_headers(app),
            json={
                "bots": [
                    {
                        "id": "bot-main",
                        "display_name": "Bot Main",
                        "enabled": True,
                        "administrators": ["qq-main:user-1"],
                        "commands": {"enabled": True, "prefixes": ["!"]},
                        "plugins": {
                            "enabled": True,
                            "enabled_plugins": ["*"],
                            "disabled_plugins": [],
                        },
                        "agent": {"mode": "none", "config": ""},
                        "bindings": [
                            {
                                "id": "binding-main",
                                "adapter_instance_id": "qq-main",
                                "session_patterns": ["group:*"],
                                "enabled": True,
                                "priority": 0,
                            }
                        ],
                    }
                ],
            },
        )

    assert response.status_code == 200
    assert boot.bot_service_configs[0].commands.prefixes == ("!",)
    assert boot.bot_service_configs[0].administrators == ("qq-main:user-1",)
    assert bot.bot_service_configs[0].commands.prefixes == ("!",)
    assert bot.permission_engine.check("cmd.mute", "bot-main", "bot-main:group:room-1", "qq-main:user-1")

    class _Context:
        bot_service_config = bot.bot_service_configs[0]

    assert command_prefixes_for_context(_Context(), ["/"]) == ["!"]
