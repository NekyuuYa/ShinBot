from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.scheduler import AgentState
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
            },
            "runtime": {"model": False, "agent": True},
            "adapter_instances": [],
            "plugins": [],
            "bots": [],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.bot_service_configs = ()

    def save_config(self) -> bool:
        return True


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def test_agent_runtime_overview_lists_persisted_scheduler_sessions(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    runtime.agent_profile_for_bot("bot-main").agent_scheduler._state_store.set_state(
        session_id,
        AgentState.ACTIVE_REPLY,
    )
    boot = _BootStub(tmp_path)
    boot.bot_service_configs = (
        type(
            "BotConfig",
            (),
            {
                "id": "bot-main",
                "display_name": "Bot Main",
                "enabled": True,
                "agent": type("AgentConfig", (), {"mode": "full", "config": ""})(),
                "bindings": (),
            },
        )(),
    )
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.get("/api/v1/agent-runtime", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload[0]["botId"] == "bot-main"
    assert payload[0]["sessions"][0]["sessionId"] == session_id
    assert payload[0]["sessions"][0]["state"] == "active_reply"
