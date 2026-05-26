from __future__ import annotations

import time
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


def _insert_workflow_run(bot: ShinBot, *, session_id: str) -> None:
    assert bot.database is not None
    with bot.database.connect() as conn:
        conn.execute(
            """
            INSERT INTO workflow_runs (
                id, session_id, instance_id, response_profile,
                batch_start_msg_id, batch_end_msg_id, batch_size,
                trigger_attention, effective_threshold, tool_calls_json,
                replied, response_summary, finish_reason, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-1",
                session_id,
                session_id.split(":", 1)[0],
                "balanced",
                1,
                1,
                1,
                0.0,
                0.0,
                "[]",
                0,
                "scan=selected; reply=no_reply; active_chat=observe",
                "active_chat_started",
                time.time(),
                time.time(),
            ),
        )


def test_session_overview_includes_latest_workflow_run(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    runtime.agent_profile_for_bot("bot-main").agent_scheduler._state_store.set_state(
        session_id,
        AgentState.ACTIVE_CHAT,
    )
    assert bot.database is not None
    bot.database.sessions.upsert(
        {
            "id": session_id,
            "instance_id": "bot-main",
            "session_type": "group",
            "platform": "sim",
            "channel_id": "room",
            "display_name": "Room",
        }
    )
    _insert_workflow_run(bot, session_id=session_id)
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
        response = client.get("/api/v1/session-overview", headers=_auth_headers(app))

    assert response.status_code == 200
    session = response.json()["data"][0]
    assert session["session"]["id"] == session_id
    assert session["latestWorkflowRun"]["sessionId"] == session_id
    assert session["latestWorkflowRun"]["finishReason"] == "active_chat_started"
    assert "scan=selected" in session["latestWorkflowRun"]["responseSummary"]
