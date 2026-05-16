"""Tests for the SQLite persistence layer."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from shinbot.core.security.audit import AuditLogger
from shinbot.core.state.session import SessionManager
from shinbot.persistence import (
    AIInteractionRecord,
    DatabaseManager,
    InstanceConfigRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PromptDefinitionRecord,
    PromptSnapshotRecord,
)
from shinbot.persistence.repositories.model_definitions import ModelDefinitionRepositoryMixin
from shinbot.persistence.repositories.model_providers import ModelProviderRepositoryMixin
from shinbot.persistence.repositories.model_registry import ModelRegistryRepository
from shinbot.persistence.repositories.model_routes import ModelRouteRepositoryMixin
from shinbot.persistence.repositories.model_usage_hourly import ModelUsageHourlyRepositoryMixin
from shinbot.schema.events import UnifiedEvent
from shinbot.schema.resources import Channel, User
from shinbot.schema.routing import MessageRoutingSkipReason, MessageRoutingStatus


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
        assert "model_usage_hourly" in tables
        assert "model_providers" in tables
        assert "message_logs" in tables
        assert "ai_interactions" in tables
        assert "prompt_snapshots" in tables
        assert "agents" not in tables
        assert "context_strategies" not in tables
        assert "personas" not in tables

        conn = sqlite3.connect(sqlite_path)
        try:
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(message_logs)").fetchall()
            }
        finally:
            conn.close()

        assert {"routing_status", "routed_at", "routing_skip_reason"} <= columns

    def test_initialize_migrates_message_log_routing_columns(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        with db.connect() as conn:
            conn.execute(
                """
                CREATE TABLE message_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    platform_msg_id TEXT NOT NULL DEFAULT '',
                    sender_id TEXT NOT NULL DEFAULT '',
                    sender_name TEXT NOT NULL DEFAULT '',
                    content_json TEXT NOT NULL DEFAULT '[]',
                    raw_text TEXT NOT NULL DEFAULT '',
                    role TEXT NOT NULL,
                    is_read INTEGER NOT NULL DEFAULT 0,
                    is_mentioned INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO message_logs (session_id, role, created_at)
                VALUES ('s-1', 'user', 1234)
                """
            )

        db.initialize()

        with db.connect() as conn:
            row = conn.execute("SELECT * FROM message_logs WHERE session_id = 's-1'").fetchone()

        assert row["routing_status"] == MessageRoutingStatus.PENDING.value
        assert row["routed_at"] is None
        assert row["routing_skip_reason"] is None

    def test_initialize_drops_legacy_agents_table(self, tmp_path):
        sqlite_path = tmp_path / "db" / "shinbot.sqlite3"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(sqlite_path)
        try:
            conn.execute(
                """
                CREATE TABLE agents (
                    uuid TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    persona_uuid TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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

        assert "agents" not in tables

    def test_initialize_drops_legacy_context_strategies_table(self, tmp_path):
        sqlite_path = tmp_path / "db" / "shinbot.sqlite3"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(sqlite_path)
        try:
            conn.execute(
                """
                CREATE TABLE context_strategies (
                    uuid TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    resolver_ref TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO context_strategies (
                    uuid, name, resolver_ref, created_at, updated_at
                ) VALUES (
                    'ctx-1', 'Legacy Context', 'context.legacy', '2025-01-01', '2025-01-01'
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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

        assert "context_strategies" not in tables

    def test_initialize_drops_legacy_personas_table(self, tmp_path):
        sqlite_path = tmp_path / "db" / "shinbot.sqlite3"
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(sqlite_path)
        try:
            conn.execute(
                """
                CREATE TABLE personas (
                    uuid TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    prompt_definition_uuid TEXT NOT NULL,
                    tags_json TEXT NOT NULL DEFAULT '[]',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO personas (
                    uuid, name, prompt_definition_uuid, created_at, updated_at
                ) VALUES (
                    'persona-1', 'Legacy Persona', 'prompt-persona-1',
                    '2025-01-01', '2025-01-01'
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

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

        assert "personas" not in tables

    def test_initialize_drops_legacy_persona_prompt_definitions(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-persona-1",
                prompt_id="persona.persona-1",
                name="Legacy Persona Prompt",
                source_type="persona",
                source_id="persona-1",
                stage="identity",
                type="static_text",
                content="Old persona text.",
            )
        )
        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-custom-1",
                prompt_id="prompt.identity.extra",
                name="Custom Identity Prompt",
                source_type="agent_plugin",
                source_id="plugin.identity",
                stage="identity",
                type="static_text",
                content="Keep this prompt.",
            )
        )

        db.initialize()

        assert db.prompt_definitions.get("prompt-persona-1") is None
        assert db.prompt_definitions.get("prompt-custom-1") is not None

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

        with db.connect() as conn:
            usage_rows = conn.execute(
                """
                SELECT *
                FROM model_usage_hourly
                WHERE provider_id = ? AND model_id = ?
                """,
                ("openai", "gpt-4.1-mini"),
            ).fetchall()

        assert len(usage_rows) == 1
        assert usage_rows[0]["total_calls"] == 1
        assert usage_rows[0]["input_tokens"] == 10
        assert usage_rows[0]["output_tokens"] == 20

    def test_cost_analysis_backfills_legacy_execution_records(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        started_at = datetime.now(UTC).replace(minute=12, second=0, microsecond=0)
        with db.connect() as conn:
            conn.execute(
                """
                INSERT INTO model_execution_records (
                    id, provider_id, model_id, started_at, success,
                    input_tokens, output_tokens, cache_hit,
                    cache_read_tokens, cache_write_tokens, latency_ms,
                    time_to_first_token_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "legacy-exec-1",
                    "openai-main",
                    "openai-main/gpt-fast",
                    started_at.isoformat(),
                    1,
                    17,
                    23,
                    1,
                    5,
                    3,
                    640,
                    120,
                ),
            )

        with db.connect() as conn:
            usage_total = conn.execute(
                "SELECT COALESCE(SUM(total_calls), 0) AS total FROM model_usage_hourly"
            ).fetchone()
        assert usage_total["total"] == 0

        analysis = db.model_executions.analyze_costs(
            since=(started_at - timedelta(days=1)).replace(hour=0).isoformat(),
            hourly_since=(started_at - timedelta(hours=23)).replace(minute=0).isoformat(),
        )

        assert analysis["summary"]["total_calls"] == 1
        assert analysis["summary"]["total_tokens"] == 40
        assert sum(bucket["total_calls"] for bucket in analysis["timeline"]["hourly"]) == 1

        with db.connect() as conn:
            usage_row = conn.execute(
                """
                SELECT *
                FROM model_usage_hourly
                WHERE provider_id = ? AND model_id = ?
                """,
                ("openai-main", "openai-main/gpt-fast"),
            ).fetchone()
        assert usage_row is not None
        assert usage_row["total_calls"] == 1
        assert usage_row["input_tokens"] == 17
        assert usage_row["output_tokens"] == 23

    def test_model_repository_mixins_can_be_instantiated_directly(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        providers = ModelProviderRepositoryMixin(db)
        models = ModelDefinitionRepositoryMixin(db)
        routes = ModelRouteRepositoryMixin(db)

        providers.upsert_provider(
            ModelProviderRecord(
                id="openai-main",
                type="openai",
                display_name="OpenAI Main",
            )
        )
        models.upsert_model(
            ModelDefinitionRecord(
                id="openai-main/gpt-fast",
                provider_id="openai-main",
                litellm_model="gpt-4.1-mini",
                display_name="GPT Fast",
                capabilities=["chat"],
            )
        )
        routes.upsert_route(
            ModelRouteRecord(id="agent.default_chat", purpose="chat"),
            members=[
                ModelRouteMemberRecord(
                    route_id="agent.default_chat",
                    model_id="openai-main/gpt-fast",
                )
            ],
        )

        assert providers.list_providers()[0]["id"] == "openai-main"
        assert models.get_model("openai-main/gpt-fast")["capabilities"] == ["chat"]
        assert routes.list_route_members("agent.default_chat")[0]["model_id"] == (
            "openai-main/gpt-fast"
        )

    def test_model_usage_mixin_uses_explicit_registry_dependency(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        registry = ModelRegistryRepository(db)
        registry.upsert_provider(
            ModelProviderRecord(
                id="openai-main",
                type="openai",
                display_name="OpenAI Main",
            )
        )
        registry.upsert_model(
            ModelDefinitionRecord(
                id="openai-main/gpt-fast",
                provider_id="openai-main",
                litellm_model="gpt-4.1-mini",
                display_name="GPT Fast",
                cost_metadata={
                    "input_per_million_tokens": 1.0,
                    "output_per_million_tokens": 2.0,
                },
            )
        )

        started_at = datetime.now(UTC).replace(minute=12, second=0, microsecond=0)
        db.model_executions.insert(
            ModelExecutionRecord(
                id="exec-usage-mixin",
                provider_id="openai-main",
                model_id="openai-main/gpt-fast",
                started_at=started_at.isoformat(),
                success=True,
                input_tokens=1000,
                output_tokens=2000,
            )
        )

        usage = ModelUsageHourlyRepositoryMixin(db, model_registry=registry)
        analysis = usage.analyze_costs(
            since=(started_at - timedelta(days=1)).replace(hour=0).isoformat(),
            hourly_since=(started_at - timedelta(hours=23)).replace(minute=0).isoformat(),
        )

        assert analysis["summary"]["total_calls"] == 1
        assert analysis["models"][0]["model_display_name"] == "GPT Fast"
        assert analysis["models"][0]["estimated_cost"] > 0

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

    def test_message_log_repository_supports_standard_context_queries(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        first = MessageLogRecord(
            session_id="s-1",
            role="user",
            raw_text="hello there",
            created_at=1000,
        )
        second = MessageLogRecord(
            session_id="s-1",
            role="assistant",
            raw_text="general kenobi",
            created_at=2000,
        )
        third = MessageLogRecord(
            session_id="s-1",
            role="user",
            raw_text="searchable needle",
            created_at=3000,
        )
        db.message_logs.insert(first)
        db.message_logs.insert(second)
        db.message_logs.insert(third)

        recent = db.message_logs.get_recent("s-1", limit=2)
        assert [item["raw_text"] for item in recent] == ["general kenobi", "searchable needle"]

        ranged = db.message_logs.get_by_time("s-1", start=1500, end=3500, limit=10)
        assert [item["raw_text"] for item in ranged] == ["general kenobi", "searchable needle"]

        searched = db.message_logs.search_context("s-1", "needle", limit=10)
        assert [item["raw_text"] for item in searched] == ["searchable needle"]

    def test_prompt_definition_repository_roundtrip(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-1",
                prompt_id="prompt.identity.extra",
                name="Identity Extra",
                source_type="agent_plugin",
                source_id="plugin.identity",
                owner_plugin_id="plugin.identity",
                owner_module="shinbot.plugins.identity",
                stage="identity",
                type="static_text",
                priority=20,
                description="Additional identity prompt",
                content="You are calm and concise.",
                tags=["identity", "agent"],
                metadata={"display_name": "Identity Extra"},
            )
        )

        payload = db.prompt_definitions.get("prompt-1")
        assert payload is not None
        assert payload["prompt_id"] == "prompt.identity.extra"
        assert payload["source_type"] == "agent_plugin"
        assert payload["source_id"] == "plugin.identity"
        assert payload["content"] == "You are calm and concise."
        assert payload["tags"] == ["identity", "agent"]

        items = db.prompt_definitions.list()
        assert len(items) == 1
        assert items[0]["uuid"] == "prompt-1"

    def test_instance_config_repository_roundtrip(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.instance_configs.upsert(
            InstanceConfigRecord(
                uuid="instance-config-1",
                instance_id="inst-1",
                main_llm="openai-main/gpt-fast",
                config={"reply_mode": "group"},
                tags=["prod", "default"],
            )
        )

        payload = db.instance_configs.get("instance-config-1")
        assert payload is not None
        assert payload["instance_id"] == "inst-1"
        assert payload["main_llm"] == "openai-main/gpt-fast"
        assert payload["config"]["reply_mode"] == "group"
        assert payload["tags"] == ["prod", "default"]

        items = db.instance_configs.list()
        assert len(items) == 1
        assert items[0]["uuid"] == "instance-config-1"

    def test_prompt_snapshot_repository_roundtrip(self, tmp_path):
        import time
        import uuid

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        snapshot_id = str(uuid.uuid4())
        now = time.time()
        record = PromptSnapshotRecord(
            id=snapshot_id,
            profile_id="agent.default",
            caller="agent.runtime",
            session_id="inst1:group:g1",
            instance_id="inst1",
            messages=[
                {"role": "system", "content": [{"type": "text", "text": "You are helpful."}]},
                {"role": "user", "content": [{"type": "text", "text": "Hello"}]},
            ],
            tools=[{"type": "function", "function": {"name": "search"}}],
            created_at=now,
            expires_at=now + 10800,
        )
        db.prompt_snapshots.insert(record)

        result = db.prompt_snapshots.get(snapshot_id)
        assert result is not None
        assert result["profile_id"] == "agent.default"
        assert len(result["messages"]) == 2
        assert result["messages"][0]["role"] == "system"
        assert len(result["tools"]) == 1
        assert result["tools"][0]["type"] == "function"

        # Expired snapshot is not returned
        expired_id = str(uuid.uuid4())
        expired_record = PromptSnapshotRecord(
            id=expired_id,
            created_at=now - 10801,
            expires_at=now - 1,
        )
        db.prompt_snapshots.insert(expired_record)
        assert db.prompt_snapshots.get(expired_id) is None

    def test_prompt_snapshot_repository_uses_injected_ttl(self, tmp_path):
        import time
        import uuid

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path, snapshot_ttl=2)
        db.initialize()

        now = time.time()
        snapshot_id = str(uuid.uuid4())
        db.prompt_snapshots.insert(
            PromptSnapshotRecord(
                id=snapshot_id,
                created_at=now,
                expires_at=None,
            )
        )

        with db.connect() as conn:
            row = conn.execute(
                "SELECT expires_at FROM prompt_snapshots WHERE id = ?",
                (snapshot_id,),
            ).fetchone()

        assert row["expires_at"] == now + 2

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

    def test_message_log_repository_roundtrip(self, tmp_path):
        import time

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        created_at = time.time() * 1000
        msg_id = db.message_logs.insert(
            MessageLogRecord(
                session_id="inst1:group:g1",
                platform_msg_id="pm-001",
                sender_id="QQ:12345",
                sender_name="Alice",
                content_json='[{"type":"text","attrs":{"content":"hi"},"children":[]}]',
                raw_text="hi",
                role="user",
                is_read=False,
                is_mentioned=True,
                created_at=created_at,
            )
        )
        assert isinstance(msg_id, int)

        row = db.message_logs.get(msg_id)
        assert row is not None
        assert row["session_id"] == "inst1:group:g1"
        assert row["role"] == "user"
        assert row["raw_text"] == "hi"
        assert row["is_read"] is False
        assert row["is_mentioned"] is True
        assert row["routing_status"] == MessageRoutingStatus.PENDING.value
        assert row["routed_at"] is None
        assert row["routing_skip_reason"] is None

        db.message_logs.mark_read(msg_id)
        row2 = db.message_logs.get(msg_id)
        assert row2 is not None
        assert row2["is_read"] is True

        listing = db.message_logs.list_by_session("inst1:group:g1")
        assert len(listing) == 1
        assert listing[0]["id"] == msg_id

    def test_message_log_repository_updates_routing_status(self, tmp_path):
        import time

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        msg_id = db.message_logs.insert(
            MessageLogRecord(
                session_id="inst1:group:g1",
                role="user",
                created_at=time.time() * 1000,
            )
        )

        db.message_logs.mark_routing_dispatched(msg_id, routed_at=12345.0)
        row = db.message_logs.get(msg_id)
        assert row is not None
        assert row["routing_status"] == MessageRoutingStatus.DISPATCHED.value
        assert row["routed_at"] == 12345.0
        assert row["routing_skip_reason"] is None

        db.message_logs.mark_routing_skipped(
            msg_id,
            reason=MessageRoutingSkipReason.EXPIRED_MESSAGE,
            routed_at=23456.0,
        )
        row2 = db.message_logs.get(msg_id)
        assert row2 is not None
        assert row2["routing_status"] == MessageRoutingStatus.SKIPPED.value
        assert row2["routed_at"] == 23456.0
        assert row2["routing_skip_reason"] == MessageRoutingSkipReason.EXPIRED_MESSAGE.value

    def test_ai_interaction_repository_roundtrip(self, tmp_path):
        import time

        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        trigger_id = db.message_logs.insert(
            MessageLogRecord(
                session_id="inst1:group:g1",
                role="user",
                created_at=time.time() * 1000,
            )
        )
        response_id = db.message_logs.insert(
            MessageLogRecord(
                session_id="inst1:group:g1",
                role="assistant",
                created_at=time.time() * 1000,
            )
        )

        ia_id = db.ai_interactions.insert(
            AIInteractionRecord(
                execution_id="exec-42",
                trigger_id=trigger_id,
                response_id=response_id,
                model_id="claude-3-haiku",
                provider_id="anthropic",
                input_tokens=100,
                output_tokens=50,
                think_text="reasoning here",
                injected_context_json='[{"type":"text","text":"summarize"}]',
            )
        )
        assert isinstance(ia_id, int)

        result = db.ai_interactions.get_by_execution("exec-42")
        assert result is not None
        assert result["execution_id"] == "exec-42"
        assert result["trigger_id"] == trigger_id
        assert result["response_id"] == response_id
        assert result["model_id"] == "claude-3-haiku"
        assert result["provider_id"] == "anthropic"
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50
        assert result["think_text"] == "reasoning here"

        by_session = db.ai_interactions.list_by_session("inst1:group:g1")
        assert len(by_session) == 1
        assert by_session[0]["id"] == ia_id
