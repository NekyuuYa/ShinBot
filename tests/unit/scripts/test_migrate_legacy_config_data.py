from __future__ import annotations

import json
import sqlite3
import tomllib
from pathlib import Path

import pytest

from scripts.migrate_legacy_config_data import (
    LegacyConfigMigrationError,
    apply_migration,
    build_migration_plan,
)


def test_exports_legacy_db_config_tables_to_file_layout(tmp_path: Path) -> None:
    _write_legacy_main_config(tmp_path / "config.toml")
    data_dir = tmp_path / "data"
    db_path = data_dir / "db" / "shinbot.sqlite3"
    db_path.parent.mkdir(parents=True)
    _seed_legacy_config_db(db_path)

    plan = build_migration_plan(data_dir=data_dir)

    assert plan.models_payload["providers"] == [
        {
            "id": "openai-main",
            "type": "openai",
            "display_name": "OpenAI Main",
            "capability_type": "embedding",
            "base_url": "https://api.openai.com/v1",
            "auth": {"api_key": "secret"},
            "default_params": {"temperature": 0.2},
            "enabled": True,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-02T00:00:00+00:00",
        }
    ]
    assert plan.models_payload["models"][0]["provider_id"] == "openai-main"
    assert plan.models_payload["models"][0]["capabilities"] == ["chat"]
    assert plan.models_payload["routes"][0]["members"][0]["model_id"] == (
        "openai-main/gpt-fast"
    )

    assert plan.instance_configs_payload["configs"] == [
        {
            "uuid": "instance-config-1",
            "instance_id": "inst-1",
            "main_llm": "chat.default",
            "config": {"response_profile": "balanced"},
            "tags": ["prod"],
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-02T00:00:00+00:00",
        }
    ]
    assert plan.main_config_payload["adapter_instances"] == [
        {
            "id": "inst-1",
            "name": "Instance 1",
            "adapter": "onebot_v11",
            "enabled": True,
            "config": {
                "mode": "reverse",
                "reverse_port": 3001,
                "access_token": "token",
                "auto_download_media": True,
                "download_file_resources": False,
                "resource_cache_dir": "data/temp/resources",
                "reconnect_delay": 5,
                "max_reconnects": -1,
                "request_timeout": 20,
                "forward_max_depth": 3,
                "silent_reconnect": True,
                "reconnect_log_interval": 30,
            },
            "createdAt": 10,
            "lastModified": 20,
        }
    ]
    assert plan.main_config_payload["plugins"] == [
        {
            "id": "shinbot_debug_message",
            "enabled": False,
            "config": {},
        },
        {
            "id": "shinbot_plugin_search",
            "enabled": True,
            "config": {"tavily_api_key": "tavily", "default_max_results": 5},
        },
    ]
    assert plan.main_config_payload["bots"][0]["agent"] == {
        "mode": "full",
        "config": "agents/demo-agent.toml",
    }
    agent_path = data_dir / "agents" / "demo-agent.toml"
    assert agent_path in plan.agent_files
    assert 'persona_id = "companion"' in plan.agent_files[agent_path]
    assert 'llm = "[route]chat.default"' in plan.agent_files[agent_path]
    persona_path = data_dir / "personas" / "companion.md"
    assert persona_path in plan.persona_files
    assert "Please be helpful." in plan.persona_files[persona_path]
    prompt_path = data_dir / "prompts" / "custom" / "custom.reply.md"
    assert prompt_path in plan.prompt_definition_files
    assert "Reply with care." in plan.prompt_definition_files[prompt_path]

    apply_migration(plan)

    assert json.loads((data_dir / "models.json").read_text(encoding="utf-8"))[
        "providers"
    ][0]["id"] == "openai-main"
    assert json.loads((data_dir / "instance-configs.json").read_text(encoding="utf-8"))[
        "configs"
    ][0]["instance_id"] == "inst-1"
    main_config = tomllib.loads((data_dir / "config.toml").read_text(encoding="utf-8"))
    assert main_config["adapter_instances"][0]["id"] == "inst-1"
    assert main_config["bots"][0]["agent"]["config"] == "agents/demo-agent.toml"
    assert tomllib.loads(agent_path.read_text(encoding="utf-8"))["agent"]["persona_id"] == (
        "companion"
    )
    assert "Please be helpful." in persona_path.read_text(encoding="utf-8")
    assert "Reply with care." in prompt_path.read_text(encoding="utf-8")


def test_apply_refuses_to_overwrite_non_empty_generated_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db_path = data_dir / "db" / "shinbot.sqlite3"
    db_path.parent.mkdir(parents=True)
    _seed_legacy_config_db(db_path)
    (data_dir / "models.json").write_text(
        json.dumps({"version": 1, "providers": [{"id": "existing"}], "models": [], "routes": []}),
        encoding="utf-8",
    )

    plan = build_migration_plan(data_dir=data_dir)

    with pytest.raises(LegacyConfigMigrationError):
        apply_migration(plan)

    apply_migration(plan, overwrite=True)
    assert json.loads((data_dir / "models.json").read_text(encoding="utf-8"))[
        "providers"
    ][0]["id"] == "openai-main"


def test_apply_refuses_to_overwrite_non_empty_main_config(tmp_path: Path) -> None:
    _write_legacy_main_config(tmp_path / "config.toml")
    data_dir = tmp_path / "data"
    db_path = data_dir / "db" / "shinbot.sqlite3"
    db_path.parent.mkdir(parents=True)
    _seed_legacy_config_db(db_path)
    (data_dir / "config.toml").write_text("[admin]\nusername = \"existing\"\n", encoding="utf-8")

    plan = build_migration_plan(data_dir=data_dir)

    with pytest.raises(LegacyConfigMigrationError):
        apply_migration(plan)


def test_supports_older_model_schema_with_provider_id(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db_path = data_dir / "db" / "shinbot.sqlite3"
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
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
            );
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
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO model_providers (
                id, type, display_name, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                "legacy-provider",
                "openai",
                "Legacy Provider",
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO model_definitions (
                id, provider_id, litellm_model, display_name,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-provider/model",
                "legacy-provider",
                "openai/gpt-4.1-mini",
                "Legacy Model",
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    plan = build_migration_plan(data_dir=data_dir)

    assert plan.models_payload["models"][0]["provider_id"] == "legacy-provider"


def _seed_legacy_config_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE model_providers (
                provider_uuid TEXT PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                capability_type TEXT NOT NULL DEFAULT 'completion',
                base_url TEXT NOT NULL DEFAULT '',
                auth_json TEXT NOT NULL DEFAULT '{}',
                default_params_json TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE model_definitions (
                id TEXT PRIMARY KEY,
                provider_uuid TEXT NOT NULL,
                litellm_model TEXT NOT NULL,
                display_name TEXT NOT NULL,
                capabilities_json TEXT NOT NULL DEFAULT '[]',
                context_window INTEGER,
                default_params_json TEXT NOT NULL DEFAULT '{}',
                cost_metadata_json TEXT NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE model_routes (
                id TEXT PRIMARY KEY,
                purpose TEXT NOT NULL DEFAULT '',
                strategy TEXT NOT NULL DEFAULT 'priority',
                enabled INTEGER NOT NULL DEFAULT 1,
                sticky_sessions INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE model_route_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                route_id TEXT NOT NULL,
                model_id TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                weight REAL NOT NULL DEFAULT 1.0,
                conditions_json TEXT NOT NULL DEFAULT '{}',
                timeout_override REAL,
                enabled INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE bot_configs (
                uuid TEXT PRIMARY KEY,
                instance_id TEXT NOT NULL,
                default_agent_uuid TEXT,
                main_llm TEXT NOT NULL DEFAULT '',
                config_json TEXT NOT NULL DEFAULT '{}',
                tags_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE agents (
                uuid TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                persona_uuid TEXT NOT NULL,
                prompts_json TEXT NOT NULL DEFAULT '[]',
                tools_json TEXT NOT NULL DEFAULT '[]',
                context_strategy_json TEXT NOT NULL DEFAULT '{}',
                config_json TEXT NOT NULL DEFAULT '{}',
                tags_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE prompt_definitions (
                uuid TEXT PRIMARY KEY,
                prompt_id TEXT,
                name TEXT,
                source_type TEXT,
                source_id TEXT,
                owner_plugin_id TEXT,
                owner_module TEXT,
                module_path TEXT,
                stage TEXT,
                type TEXT,
                priority INTEGER,
                version TEXT,
                description TEXT,
                enabled INTEGER,
                content TEXT,
                template_vars_json TEXT,
                resolver_ref TEXT,
                bundle_refs_json TEXT,
                config_json TEXT,
                tags_json TEXT,
                metadata_json TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE personas (
                uuid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                prompt_definition_uuid TEXT NOT NULL,
                tags_json TEXT NOT NULL DEFAULT '[]',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO model_providers (
                provider_uuid, id, type, display_name, base_url, auth_json,
                default_params_json, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "provider-uuid-1",
                "openai-main",
                "openai",
                "OpenAI Main",
                "https://api.openai.com/v1",
                json.dumps({"api_key": "secret"}),
                json.dumps({"_tab": "embedding", "temperature": 0.2}),
                1,
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO model_definitions (
                id, provider_uuid, litellm_model, display_name, capabilities_json,
                context_window, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "openai-main/gpt-fast",
                "provider-uuid-1",
                "openai/gpt-4.1-mini",
                "GPT Fast",
                json.dumps(["chat"]),
                128000,
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO model_routes (
                id, purpose, strategy, enabled, sticky_sessions, metadata_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "chat.default",
                "default chat",
                "priority",
                1,
                0,
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO model_route_members (
                route_id, model_id, priority, weight, conditions_json, enabled
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("chat.default", "openai-main/gpt-fast", 10, 1.5, json.dumps({"tier": "fast"}), 1),
        )
        conn.execute(
            """
            INSERT INTO bot_configs (
                uuid, instance_id, default_agent_uuid, main_llm, config_json, tags_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "instance-config-1",
                "inst-1",
                "agent-1",
                "chat.default",
                json.dumps({"response_profile": "balanced"}),
                json.dumps(["prod"]),
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO agents (
                uuid, agent_id, name, persona_uuid, prompts_json, tools_json,
                context_strategy_json, config_json, tags_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "agent-1",
                "demo-agent",
                "Demo Agent",
                "companion",
                "[]",
                "[]",
                "{}",
                "{}",
                "[]",
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO prompt_definitions (
                uuid, content, version, description
            ) VALUES (?, ?, ?, ?)
            """,
            ("prompt-persona-1", "Please be helpful.", "1.0.0", "Companion persona"),
        )
        conn.execute(
            """
            INSERT INTO prompt_definitions (
                uuid, prompt_id, name, source_type, stage, type, priority, version,
                enabled, content, template_vars_json, bundle_refs_json, config_json,
                tags_json, metadata_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "prompt-custom-1",
                "custom.reply",
                "Custom Reply",
                "admin",
                "instructions",
                "static_text",
                100,
                "1.0.0",
                1,
                "Reply with care.",
                "[]",
                "[]",
                "{}",
                json.dumps(["custom"]),
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO personas (
                uuid, name, prompt_definition_uuid, tags_json, enabled, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "companion",
                "Companion",
                "prompt-persona-1",
                json.dumps(["main"]),
                1,
                "2026-01-01T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _write_legacy_main_config(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "secret"',
                "jwt_expire_hours = 24",
                "",
                "[[instances]]",
                'id = "inst-1"',
                'name = "Instance 1"',
                'adapterType = "onebot_v11"',
                'platform = "onebot_v11"',
                "createdAt = 10",
                "lastModified = 20",
                "",
                "[instances.config]",
                'mode = "reverse"',
                "reverse_port = 3001",
                'access_token = "token"',
                "download_resources = true",
                "",
                "[plugin_configs.shinbot_plugin_search]",
                'tavily_api_key = "tavily"',
                "default_max_results = 5",
                "",
                "[plugin_states.shinbot_debug_message]",
                "enabled = false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
