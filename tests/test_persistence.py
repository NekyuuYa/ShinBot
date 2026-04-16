"""Tests for the SQLite persistence layer."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from shinbot.core.security.audit import AuditLogger
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager, ModelExecutionRecord, PersonaRecord
from shinbot.schema.events import Channel, UnifiedEvent, User


def _make_event(channel_id: str = "g-1") -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        platform="mock",
        user=User(id="user-1"),
        channel=Channel(id=channel_id, type=0, name="Group"),
    )


class TestDatabaseManager:
    def test_initialize_creates_database_and_schema(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        sqlite_path = tmp_path / "db" / "shinbot.sqlite3"
        assert sqlite_path.exists()

        conn = sqlite3.connect(sqlite_path)
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        finally:
            conn.close()

        assert "sessions" in tables
        assert "audit_logs" in tables
        assert "model_execution_records" in tables
        assert "model_providers" in tables

    def test_model_execution_repository_persists_metrics(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.model_executions.insert(
            ModelExecutionRecord(
                id="exec-1",
                route_id="agent.default_chat",
                provider_id="openai",
                model_id="gpt-4.1-mini",
                caller="agent.runtime",
                session_id="inst1:group:g1",
                instance_id="inst1",
                success=True,
                latency_ms=123.4,
                input_tokens=10,
                output_tokens=20,
                cache_hit=True,
            )
        )

        rows = db.model_executions.list_recent(limit=5)
        assert len(rows) == 1
        assert rows[0]["id"] == "exec-1"
        assert rows[0]["input_tokens"] == 10

    def test_initialize_migrates_model_registry_to_provider_uuid(self, tmp_path):
        sqlite_path = tmp_path / "db" / "shinbot.sqlite3"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(sqlite_path)
        try:
            conn.execute(
                """
                CREATE TABLE model_providers (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    base_url TEXT NOT NULL DEFAULT '',
                    auth_json TEXT NOT NULL DEFAULT '{}',
                    default_params_json TEXT NOT NULL DEFAULT '{}',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE model_definitions (
                    id TEXT PRIMARY KEY,
                    provider_id TEXT NOT NULL,
                    litellm_model TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    capabilities_json TEXT NOT NULL DEFAULT '[]',
                    context_window INTEGER,
                    default_params_json TEXT NOT NULL DEFAULT '{}',
                    cost_metadata_json TEXT NOT NULL DEFAULT '{}',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(provider_id) REFERENCES model_providers(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                INSERT INTO model_providers (
                    id, type, display_name, base_url, auth_json, default_params_json,
                    enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "openai-main",
                    "openai",
                    "OpenAI Main",
                    "https://api.openai.com/v1",
                    "{}",
                    "{}",
                    1,
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-01T00:00:00+00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO model_definitions (
                    id, provider_id, litellm_model, display_name, capabilities_json,
                    context_window, default_params_json, cost_metadata_json, enabled,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "openai-main/gpt-fast",
                    "openai-main",
                    "gpt-4.1-mini",
                    "GPT Fast",
                    '["chat"]',
                    None,
                    "{}",
                    "{}",
                    1,
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-01T00:00:00+00:00",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        providers = db.model_registry.list_providers()
        models = db.model_registry.list_models(provider_id="openai-main")

        assert len(providers) == 1
        assert providers[0]["id"] == "openai-main"
        assert providers[0]["provider_uuid"]
        assert len(models) == 1
        assert models[0]["provider_id"] == "openai-main"

    def test_persona_repository_roundtrip(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.personas.upsert(
            PersonaRecord(
                uuid="persona-1",
                name="Assistant Default",
                prompt_text="You are a concise assistant.",
            )
        )

        payload = db.personas.get("persona-1")
        assert payload is not None
        assert payload["name"] == "Assistant Default"
        assert payload["prompt_text"] == "You are a concise assistant."

        items = db.personas.list()
        assert len(items) == 1
        assert items[0]["uuid"] == "persona-1"


class TestDatabaseBackedSessionManager:
    def test_session_roundtrip_via_database(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        manager = SessionManager(session_repo=db.sessions)
        session = manager.get_or_create("inst1", _make_event())
        session.config.prefixes = ["/", "#"]
        manager.update(session)

        second_manager = SessionManager(session_repo=db.sessions)
        restored = second_manager.get_or_create("inst1", _make_event())

        assert restored.id == session.id
        assert restored.display_name == "Group"
        assert restored.config.prefixes == ["/", "#"]


class TestDatabaseBackedAuditLogger:
    def test_audit_log_is_persisted_to_database(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        audit = AuditLogger(tmp_path, audit_repo=db.audit)

        entry = audit.log_command(
            command_name="ping",
            plugin_id="plugin.test",
            user_id="user-1",
            session_id="inst1:group:g1",
            instance_id="inst1",
            success=True,
            execution_time_ms=12.5,
        )

        sqlite_path = Path(tmp_path) / "db" / "shinbot.sqlite3"
        conn = sqlite3.connect(sqlite_path)
        try:
            row = conn.execute(
                """
                SELECT command_name, plugin_id, session_id, success
                FROM audit_logs
                WHERE id = (
                    SELECT MAX(id) FROM audit_logs
                )
                """
            ).fetchone()
        finally:
            conn.close()

        assert entry.command_name == "ping"
        assert row == ("ping", "plugin.test", "inst1:group:g1", 1)
