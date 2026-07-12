from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.runtime.service_health import RuntimeServiceHealth
from shinbot.agent.runtime.task_manager import AgentTaskSnapshot
from shinbot.agent.scheduler import AgentState
from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.schema.elements import MessageElement


class _MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str, platform: str, **kwargs) -> None:
        super().__init__(instance_id, platform)

    async def start(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        return MessageHandle(message_id="msg-1", adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, object]) -> object:
        return {"ok": True}

    async def get_capabilities(self) -> dict[str, object]:
        return {"elements": ["text"], "actions": ["message.create"], "limits": {}}


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


def _insert_workflow_run(bot: ShinBot, *, session_id: str, response_summary: str, finish_reason: str) -> None:
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
                response_summary,
                finish_reason,
                time.time(),
                time.time(),
            ),
        )


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
    assert payload[0]["profileId"] == "bot-main"
    assert payload[0]["botId"] == "bot-main"
    assert payload[0]["sessions"][0]["sessionId"] == session_id
    # API startup reconciles transient scheduler states like ACTIVE_REPLY back to IDLE.
    assert payload[0]["sessions"][0]["state"] == "idle"


def test_agent_runtime_overview_lists_profile_background_tasks(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("bot-main")
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
    assert payload[0]["profileId"] == "bot-main"
    assert payload[0]["botId"] == "bot-main"
    assert payload[0]["tasks"] == [
        {
            "key": f"agent:{profile.bot_id or profile.profile_id}:review_due_timer:loop",
            "name": "agent-review-due-timer",
            "done": False,
            "cancelled": False,
            "error": None,
        }
    ]


def test_agent_runtime_overview_includes_timer_health_and_task_failures(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    profile = runtime.agent_profile_for_bot("bot-main")
    namespace = f"agent:{profile.bot_id or profile.profile_id}"
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
        profile.review_due_timer._health.scan_started(now=11.0)
        profile.review_due_timer._health.failed(
            RuntimeError("review timer failure"),
            now=12.0,
        )
        active_health = RuntimeServiceHealth(
            "active_chat_timer:inst-1:group:room"
        )
        active_health.start(now=20.0)
        active_health.scan_started(now=21.0)
        active_health.succeeded(now=22.0)
        profile.active_chat_timer._health["inst-1:group:room"] = active_health
        runtime.task_manager._failures[f"{namespace}:review_workflow:room"] = (
            AgentTaskSnapshot(
                key=f"{namespace}:review_workflow:room",
                name="review-workflow:room",
                done=True,
                cancelled=False,
                error="RuntimeError: workflow failed",
            )
        )

        response = client.get("/api/v1/agent-runtime", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"][0]
    assert payload["tasks"][0]["key"] == f"{namespace}:review_due_timer:loop"
    assert payload["taskFailures"] == [
        {
            "key": f"{namespace}:review_workflow:room",
            "name": "review-workflow:room",
            "done": True,
            "cancelled": False,
            "error": "RuntimeError: workflow failed",
        }
    ]
    assert payload["timerHealth"]["reviewDueTimer"]["startedAt"] > 0.0
    assert payload["timerHealth"]["reviewDueTimer"] == {
        "serviceName": "review_due_timer",
        "sessionId": "",
        "status": "degraded",
        "startedAt": payload["timerHealth"]["reviewDueTimer"]["startedAt"],
        "lastScanStartedAt": 11.0,
        "lastSuccessAt": 0.0,
        "lastErrorAt": 12.0,
        "lastErrorCode": "RuntimeError",
        "lastErrorMessage": "review timer failure",
        "consecutiveFailures": 1,
        "scanCount": 1,
        "successCount": 0,
    }
    assert payload["timerHealth"]["activeChatTimers"] == [
        {
            "serviceName": "active_chat_timer:inst-1:group:room",
            "sessionId": "inst-1:group:room",
            "status": "running",
            "startedAt": 20.0,
            "lastScanStartedAt": 21.0,
            "lastSuccessAt": 22.0,
            "lastErrorAt": 0.0,
            "lastErrorCode": "",
            "lastErrorMessage": "",
            "consecutiveFailures": 0,
            "scanCount": 1,
            "successCount": 1,
        }
    ]


def test_agent_runtime_overview_does_not_filter_session_ids_by_bot_prefix(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "inst-1:group:room"
    runtime.agent_profile_for_bot("bot-main").agent_scheduler._state_store.set_state(
        session_id,
        AgentState.ACTIVE_CHAT,
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
    assert payload[0]["sessions"][0]["sessionId"] == session_id
    assert payload[0]["sessions"][0]["state"] == "active_chat"


def test_agent_runtime_overview_includes_latest_review_run(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    runtime.agent_profile_for_bot("bot-main").agent_scheduler._state_store.set_state(
        session_id,
        AgentState.REVIEW,
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
    _insert_workflow_run(
        bot,
        session_id=session_id,
        response_summary="scan=selected; reply=no_reply; active_chat=observe",
        finish_reason="active_chat_started",
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
    session = response.json()["data"][0]["sessions"][0]
    assert session["latestReviewRun"]["sessionId"] == session_id
    assert session["latestReviewRun"]["finishReason"] == "active_chat_started"
    assert "scan=selected" in session["latestReviewRun"]["responseSummary"]


def test_agent_runtime_overview_includes_binding_and_session_platform_state(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "inst-1:group:room"
    runtime.agent_profile_for_bot("bot-main").agent_scheduler._state_store.set_state(
        session_id,
        AgentState.ACTIVE_CHAT,
    )
    bot.adapter_manager.register_adapter("mock", _MockAdapter)
    bot.adapter_manager.create_instance("inst-1", "mock")
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
                "bindings": (
                    type(
                        "BindingConfig",
                        (),
                        {
                            "adapter_instance_id": "inst-1",
                            "session_patterns": ("group:*",),
                            "enabled": True,
                            "priority": 0,
                        },
                    )(),
                ),
            },
        )(),
    )
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.get("/api/v1/agent-runtime", headers=_auth_headers(app))

    assert response.status_code == 200
    profile = response.json()["data"][0]
    assert profile["bindings"][0]["platformState"] == {
        "running": False,
        "connected": False,
        "available": False,
    }
    assert profile["sessions"][0]["adapterInstanceId"] == "inst-1"
    assert profile["sessions"][0]["platformState"] == {
        "running": False,
        "connected": False,
        "available": False,
    }
