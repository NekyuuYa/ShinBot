from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.scheduler import AgentState
from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.state.session import Session
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


def _context_state_path(tmp_path: Path, session_id: str) -> Path:
    sanitized = session_id.replace(":", "_").replace("/", "_")
    return tmp_path / "temp" / "context_state" / f"{sanitized}.json"


def _seed_session_management_records(
    bot: ShinBot,
    runtime,
    *,
    tmp_path: Path,
    session_id: str,
) -> Path:
    assert bot.database is not None
    now = time.time()
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
    session_payload = bot.database.sessions.get(session_id)
    assert session_payload is not None
    bot.session_manager.update(Session.model_validate(session_payload))

    context_state = runtime.context_manager.get_session_state(session_id)
    runtime.context_manager._session_runtime.state_store.save(context_state)
    context_state_path = _context_state_path(tmp_path, session_id)
    assert context_state_path.exists()
    raw_hash = f"hash:{session_id}"

    with bot.database.connect() as conn:
        trigger_id = int(
            conn.execute(
                """
                INSERT INTO message_logs (session_id, role, created_at, raw_text)
                VALUES (?, ?, ?, ?)
                """,
                (session_id, "user", now, "hello"),
            ).lastrowid
        )
        response_id = int(
            conn.execute(
                """
                INSERT INTO message_logs (session_id, role, created_at, raw_text)
                VALUES (?, ?, ?, ?)
                """,
                (session_id, "assistant", now + 1, "world"),
            ).lastrowid
        )
        conn.execute(
            "INSERT INTO audit_logs (timestamp, session_id) VALUES (?, ?)",
            (str(now), session_id),
        )
        conn.execute(
            "INSERT INTO agent_scheduler_states (session_id, updated_at) VALUES (?, ?)",
            (session_id, now),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_messages (session_id, message_log_id, created_at)
            VALUES (?, ?, ?)
            """,
            (session_id, trigger_id, now),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_ranges (
                session_id, start_msg_log_id, end_msg_log_id, start_at, end_at, message_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, trigger_id, response_id, now, now + 1, 2),
        )
        conn.execute(
            """
            INSERT INTO agent_review_summaries (
                session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
                message_count, summary, candidate_message_ids_json, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                trigger_id,
                response_id,
                now,
                now + 1,
                2,
                "digest",
                "[]",
                "review",
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_high_priority_events (
                session_id, message_log_id, kind, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (session_id, trigger_id, "mention", now),
        )
        conn.execute(
            "INSERT INTO agent_recent_mentions (session_id, timestamp) VALUES (?, ?)",
            (session_id, now),
        )
        conn.execute(
            """
            INSERT INTO agent_summaries (session_id, summary_type, created_at, msg_log_start, msg_log_end)
            VALUES (?, ?, ?, ?, ?)
            """,
            (session_id, "active_chat", now, trigger_id, response_id),
        )
        conn.execute(
            """
            INSERT INTO prompt_snapshots (id, session_id, created_at, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (f"snapshot:{session_id}", session_id, now, now + 3600),
        )
        conn.execute(
            """
            INSERT INTO model_execution_records (id, session_id, started_at)
            VALUES (?, ?, ?)
            """,
            (f"exec:{session_id}", session_id, str(now)),
        )
        conn.execute(
            """
            INSERT INTO workflow_runs (id, session_id, started_at)
            VALUES (?, ?, ?)
            """,
            (f"run:{session_id}", session_id, now),
        )
        conn.execute(
            """
            INSERT INTO session_attention_states (session_id, last_update_at)
            VALUES (?, ?)
            """,
            (session_id, now),
        )
        conn.execute(
            """
            INSERT INTO sender_weight_states (session_id, sender_id, last_runtime_adjust_at)
            VALUES (?, ?, ?)
            """,
            (session_id, "user-1", now),
        )
        conn.execute(
            """
            INSERT INTO media_assets (raw_hash, first_seen_at, last_seen_at, expire_at)
            VALUES (?, ?, ?, ?)
            """,
            (raw_hash, now, now, now + 3600),
        )
        conn.execute(
            """
            INSERT INTO message_media_links (message_log_id, session_id, raw_hash, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (trigger_id, session_id, raw_hash, now),
        )
        conn.execute(
            """
            INSERT INTO session_media_occurrences (
                session_id, raw_hash, first_seen_at, last_seen_at, expire_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (session_id, raw_hash, now, now, now + 3600),
        )
        conn.execute(
            """
            INSERT INTO ai_interactions (execution_id, trigger_id, response_id)
            VALUES (?, ?, ?)
            """,
            (f"exec:{session_id}", trigger_id, response_id),
        )

    return context_state_path


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


def test_session_overview_includes_agent_read_state_for_history(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    _seed_session_management_records(
        bot,
        runtime,
        tmp_path=tmp_path,
        session_id=session_id,
    )
    assert bot.database is not None
    with bot.database.connect() as conn:
        rows = conn.execute(
            """
            SELECT id
            FROM message_logs
            WHERE session_id = ?
            ORDER BY created_at ASC, id ASC
            """,
            (session_id,),
        ).fetchall()
        assert len(rows) == 2
        trigger_id = int(rows[0]["id"])
        response_id = int(rows[1]["id"])
        conn.execute(
            """
            UPDATE agent_unread_messages
            SET review_consumed = 1
            WHERE session_id = ? AND message_log_id = ?
            """,
            (session_id, trigger_id),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_messages (
                session_id, message_log_id, created_at, review_consumed, chat_consumed
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (session_id, response_id, time.time(), 0, 1),
        )

    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get("/api/v1/session-overview", headers=_auth_headers(app))

    assert response.status_code == 200
    session = response.json()["data"][0]
    history = session["history"]
    assert history[0]["agentReadState"] == "review_consumed"
    assert history[1]["agentReadState"] == "active_chat_consumed"


def test_session_overview_includes_platform_availability_state(tmp_path: Path) -> None:
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
    bot.adapter_manager.register_adapter("mock", _MockAdapter)
    bot.adapter_manager.create_instance("bot-main", "mock")
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get("/api/v1/session-overview", headers=_auth_headers(app))

    assert response.status_code == 200
    session = response.json()["data"][0]
    assert session["platformState"] == {
        "running": False,
        "connected": False,
        "available": False,
    }


def test_session_overview_delete_removes_related_session_state(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    context_state_path = _seed_session_management_records(
        bot,
        runtime,
        tmp_path=tmp_path,
        session_id=session_id,
    )

    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = client.delete(
            f"/api/v1/session-overview/{session_id}",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    assert response.json()["data"] == {"sessionId": session_id, "deleted": True}
    assert bot.session_manager.get(session_id) is None
    assert not context_state_path.exists()

    with bot.database.connect() as conn:
        for table in (
            "sessions",
            "session_configs",
            "message_logs",
            "audit_logs",
            "agent_scheduler_states",
            "agent_unread_messages",
            "agent_unread_ranges",
            "agent_review_summaries",
            "agent_high_priority_events",
            "agent_recent_mentions",
            "agent_summaries",
            "prompt_snapshots",
            "model_execution_records",
            "workflow_runs",
            "session_attention_states",
            "sender_weight_states",
            "message_media_links",
            "session_media_occurrences",
        ):
            remaining = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table} WHERE session_id = ?"
                if table != "sessions"
                else "SELECT COUNT(*) AS count FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()["count"]
            assert remaining == 0

        ai_remaining = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM ai_interactions
            WHERE execution_id = ?
            """,
            (f"exec:{session_id}",),
        ).fetchone()["count"]
        assert ai_remaining == 0


def test_session_overview_clear_history_keeps_session_shell(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    context_state_path = _seed_session_management_records(
        bot,
        runtime,
        tmp_path=tmp_path,
        session_id=session_id,
    )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.delete(
            f"/api/v1/session-overview/{session_id}/history",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "sessionId": session_id,
        "scope": "history",
        "cleared": True,
    }
    assert bot.session_manager.get(session_id) is not None
    assert not context_state_path.exists()

    assert bot.database is not None
    with bot.database.connect() as conn:
        session_count = conn.execute(
            "SELECT COUNT(*) AS count FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()["count"]
        assert session_count == 1

        session_config_count = conn.execute(
            "SELECT COUNT(*) AS count FROM session_configs WHERE session_id = ?",
            (session_id,),
        ).fetchone()["count"]
        assert session_config_count == 1

        audit_count = conn.execute(
            "SELECT COUNT(*) AS count FROM audit_logs WHERE session_id = ?",
            (session_id,),
        ).fetchone()["count"]
        assert audit_count == 1

        for table in (
            "message_logs",
            "agent_scheduler_states",
            "agent_unread_messages",
            "agent_unread_ranges",
            "agent_review_summaries",
            "agent_high_priority_events",
            "agent_recent_mentions",
            "agent_summaries",
            "prompt_snapshots",
            "model_execution_records",
            "workflow_runs",
            "session_attention_states",
            "sender_weight_states",
            "message_media_links",
            "session_media_occurrences",
        ):
            remaining = conn.execute(
                f"SELECT COUNT(*) AS count FROM {table} WHERE session_id = ?",
                (session_id,),
            ).fetchone()["count"]
            assert remaining == 0

        ai_remaining = conn.execute(
            "SELECT COUNT(*) AS count FROM ai_interactions WHERE execution_id = ?",
            (f"exec:{session_id}",),
        ).fetchone()["count"]
        assert ai_remaining == 0


def test_session_overview_clear_audit_logs_keeps_message_history(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_id = "bot-main:group:room"
    context_state_path = _seed_session_management_records(
        bot,
        runtime,
        tmp_path=tmp_path,
        session_id=session_id,
    )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.delete(
            f"/api/v1/session-overview/{session_id}/audit-logs",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "sessionId": session_id,
        "scope": "audit_logs",
        "cleared": True,
    }
    assert bot.session_manager.get(session_id) is not None
    assert context_state_path.exists()

    assert bot.database is not None
    with bot.database.connect() as conn:
        audit_count = conn.execute(
            "SELECT COUNT(*) AS count FROM audit_logs WHERE session_id = ?",
            (session_id,),
        ).fetchone()["count"]
        assert audit_count == 0

        message_count = conn.execute(
            "SELECT COUNT(*) AS count FROM message_logs WHERE session_id = ?",
            (session_id,),
        ).fetchone()["count"]
        assert message_count == 2

        session_count = conn.execute(
            "SELECT COUNT(*) AS count FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()["count"]
        assert session_count == 1


def test_session_overview_batch_clear_history_keeps_sessions(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_ids = ["bot-main:group:room-1", "bot-main:group:room-2"]
    for session_id in session_ids:
        _seed_session_management_records(
            bot,
            runtime,
            tmp_path=tmp_path,
            session_id=session_id,
        )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/session-overview/batch/history",
            headers=_auth_headers(app),
            json={"sessionIds": session_ids},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "action": "history",
        "requestedCount": 2,
        "processedCount": 2,
        "processedSessionIds": session_ids,
        "missingSessionIds": [],
    }

    assert bot.database is not None
    with bot.database.connect() as conn:
        for session_id in session_ids:
            session_count = conn.execute(
                "SELECT COUNT(*) AS count FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()["count"]
            assert session_count == 1

            message_count = conn.execute(
                "SELECT COUNT(*) AS count FROM message_logs WHERE session_id = ?",
                (session_id,),
            ).fetchone()["count"]
            assert message_count == 0

            audit_count = conn.execute(
                "SELECT COUNT(*) AS count FROM audit_logs WHERE session_id = ?",
                (session_id,),
            ).fetchone()["count"]
            assert audit_count == 1


def test_session_overview_batch_clear_audit_logs_reports_missing_ids(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    existing_session_id = "bot-main:group:room-1"
    missing_session_id = "bot-main:group:missing"
    _seed_session_management_records(
        bot,
        runtime,
        tmp_path=tmp_path,
        session_id=existing_session_id,
    )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/session-overview/batch/audit-logs",
            headers=_auth_headers(app),
            json={"sessionIds": [existing_session_id, missing_session_id]},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "action": "audit_logs",
        "requestedCount": 2,
        "processedCount": 1,
        "processedSessionIds": [existing_session_id],
        "missingSessionIds": [missing_session_id],
    }

    assert bot.database is not None
    with bot.database.connect() as conn:
        audit_count = conn.execute(
            "SELECT COUNT(*) AS count FROM audit_logs WHERE session_id = ?",
            (existing_session_id,),
        ).fetchone()["count"]
        assert audit_count == 0

        message_count = conn.execute(
            "SELECT COUNT(*) AS count FROM message_logs WHERE session_id = ?",
            (existing_session_id,),
        ).fetchone()["count"]
        assert message_count == 2


def test_session_overview_batch_delete_removes_multiple_sessions(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    session_ids = ["bot-main:group:room-1", "bot-main:group:room-2"]
    for session_id in session_ids:
        _seed_session_management_records(
            bot,
            runtime,
            tmp_path=tmp_path,
            session_id=session_id,
        )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/session-overview/batch/delete",
            headers=_auth_headers(app),
            json={"sessionIds": session_ids},
        )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "action": "delete",
        "requestedCount": 2,
        "processedCount": 2,
        "processedSessionIds": session_ids,
        "missingSessionIds": [],
    }

    assert bot.database is not None
    with bot.database.connect() as conn:
        for session_id in session_ids:
            session_count = conn.execute(
                "SELECT COUNT(*) AS count FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()["count"]
            assert session_count == 0

    for session_id in session_ids:
        assert bot.session_manager.get(session_id) is None


def test_session_overview_batch_action_returns_404_when_none_exist(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/session-overview/batch/delete",
            headers=_auth_headers(app),
            json={"sessionIds": ["bot-main:group:missing"]},
        )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "SESSION_NOT_FOUND"


def test_session_overview_delete_returns_404_for_missing_session(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.delete(
            "/api/v1/session-overview/bot-main:group:missing",
            headers=_auth_headers(app),
        )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "SESSION_NOT_FOUND"
