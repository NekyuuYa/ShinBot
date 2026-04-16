"""Tests for the SQLite persistence layer."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from shinbot.agent.prompting import PromptRegistry
from shinbot.core.security.audit import AuditLogger
from shinbot.core.state.session import SessionManager
from shinbot.persistence import (
    AgentRecord,
    BotConfigRecord,
    ContextStrategyRecord,
    DatabaseManager,
    ModelExecutionRecord,
    PersonaRecord,
    PromptDefinitionRecord,
)
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

    def test_initialize_seeds_builtin_context_strategy(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        payload = db.context_strategies.get(PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID)
        assert payload is not None
        assert payload["type"] == "sliding_window"
        assert payload["resolver_ref"] == PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_RESOLVER
        assert payload["config"] == {"builtin": True, "default": True}

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
        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-persona-1",
                prompt_id="persona.persona-1",
                name="Assistant Default Persona Prompt",
                source_type="persona",
                source_id="persona-1",
                stage="identity",
                type="static_text",
                content="You are a concise assistant.",
            )
        )

        db.personas.upsert(
            PersonaRecord(
                uuid="persona-1",
                name="Assistant Default",
                prompt_definition_uuid="prompt-persona-1",
            )
        )

        payload = db.personas.get("persona-1")
        assert payload is not None
        assert payload["name"] == "Assistant Default"
        assert payload["prompt_definition_uuid"] == "prompt-persona-1"
        assert payload["prompt_text"] == "You are a concise assistant."

        items = db.personas.list()
        assert len(items) == 1
        assert items[0]["uuid"] == "persona-1"

    def test_context_strategy_repository_roundtrip(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.context_strategies.upsert(
            ContextStrategyRecord(
                uuid="ctx-1",
                name="Recent History",
                type="recent_history",
                resolver_ref="context.recent_history",
                description="Use the latest conversation turns.",
                config={"window": 12},
            )
        )

        payload = db.context_strategies.get("ctx-1")
        assert payload is not None
        assert payload["type"] == "recent_history"
        assert payload["resolver_ref"] == "context.recent_history"
        assert payload["config"]["window"] == 12

        items = db.context_strategies.list()
        assert len(items) == 2
        assert {item["uuid"] for item in items} == {
            "ctx-1",
            PromptRegistry.BUILTIN_SLIDING_WINDOW_CONTEXT_STRATEGY_ID,
        }

    def test_agent_repository_roundtrip(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()
        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-persona-1",
                prompt_id="persona.persona-1",
                name="Assistant Default Persona Prompt",
                source_type="persona",
                source_id="persona-1",
                stage="identity",
                type="static_text",
                content="You are a concise assistant.",
            )
        )
        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-1",
                prompt_id="prompt.identity.extra",
                name="Identity Extra",
                source_type="agent_plugin",
                source_id="plugin.identity",
                stage="identity",
                type="static_text",
                content="extra identity",
            )
        )
        db.prompt_definitions.upsert(
            PromptDefinitionRecord(
                uuid="prompt-2",
                prompt_id="prompt.instructions.chat",
                name="Chat Instructions",
                source_type="agent_plugin",
                source_id="plugin.chat",
                stage="instructions",
                type="static_text",
                content="chat instructions",
            )
        )
        db.personas.upsert(
            PersonaRecord(
                uuid="persona-1",
                name="Assistant Default",
                prompt_definition_uuid="prompt-persona-1",
            )
        )

        db.agents.upsert(
            AgentRecord(
                uuid="agent-uuid-1",
                agent_id="agent.default",
                name="Default Agent",
                persona_uuid="persona-1",
                prompts=["prompt-1", "prompt-2"],
                tools=["tool.echo", "tool.search"],
                context_strategy={
                    "ref": "builtin.context.sliding_window",
                    "type": "sliding_window",
                    "params": {"triggerRatio": 0.5, "trimTurns": 2},
                },
                config={"model_id": "openai-main/gpt-fast"},
                tags=["default", "chat"],
            )
        )

        payload = db.agents.get("agent-uuid-1")
        assert payload is not None
        assert payload["agent_id"] == "agent.default"
        assert payload["prompts"] == ["prompt-1", "prompt-2"]
        assert payload["tools"] == ["tool.echo", "tool.search"]
        assert payload["context_strategy"] == {
            "ref": "builtin.context.sliding_window",
            "type": "sliding_window",
            "params": {"triggerRatio": 0.5, "trimTurns": 2},
        }
        assert payload["config"]["model_id"] == "openai-main/gpt-fast"
        assert payload["tags"] == ["default", "chat"]

        items = db.agents.list()
        assert len(items) == 1
        assert items[0]["uuid"] == "agent-uuid-1"

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

    def test_bot_config_repository_roundtrip(self, tmp_path):
        db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
        db.initialize()

        db.bot_configs.upsert(
            BotConfigRecord(
                uuid="bot-config-1",
                instance_id="inst-1",
                default_agent_uuid="agent-uuid-1",
                main_llm="openai-main/gpt-fast",
                config={"reply_mode": "group"},
                tags=["prod", "default"],
            )
        )

        payload = db.bot_configs.get("bot-config-1")
        assert payload is not None
        assert payload["instance_id"] == "inst-1"
        assert payload["main_llm"] == "openai-main/gpt-fast"
        assert payload["config"]["reply_mode"] == "group"
        assert payload["tags"] == ["prod", "default"]

        items = db.bot_configs.list()
        assert len(items) == 1
        assert items[0]["uuid"] == "bot-config-1"


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
