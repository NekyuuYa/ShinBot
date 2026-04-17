"""SQLite schema bootstrap for ShinBot persistence."""

from __future__ import annotations

import sqlite3
import uuid

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS model_providers (
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
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS model_definitions (
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
        updated_at TEXT NOT NULL,
        FOREIGN KEY(provider_uuid) REFERENCES model_providers(provider_uuid) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_definitions_provider_id
    ON model_definitions(provider_uuid)
    """,
    """
    CREATE TABLE IF NOT EXISTS model_routes (
        id TEXT PRIMARY KEY,
        purpose TEXT NOT NULL DEFAULT '',
        strategy TEXT NOT NULL DEFAULT 'priority',
        enabled INTEGER NOT NULL DEFAULT 1,
        sticky_sessions INTEGER NOT NULL DEFAULT 0,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS model_route_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        route_id TEXT NOT NULL,
        model_id TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        weight REAL NOT NULL DEFAULT 1.0,
        conditions_json TEXT NOT NULL DEFAULT '{}',
        timeout_override REAL,
        enabled INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(route_id) REFERENCES model_routes(id) ON DELETE CASCADE,
        FOREIGN KEY(model_id) REFERENCES model_definitions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_route_members_route_id
    ON model_route_members(route_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS model_execution_records (
        id TEXT PRIMARY KEY,
        route_id TEXT NOT NULL DEFAULT '',
        provider_id TEXT NOT NULL DEFAULT '',
        model_id TEXT NOT NULL DEFAULT '',
        caller TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        instance_id TEXT NOT NULL DEFAULT '',
        purpose TEXT NOT NULL DEFAULT '',
        started_at TEXT NOT NULL,
        first_token_at TEXT,
        finished_at TEXT,
        latency_ms REAL NOT NULL DEFAULT 0,
        time_to_first_token_ms REAL,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cache_hit INTEGER NOT NULL DEFAULT 0,
        cache_read_tokens INTEGER NOT NULL DEFAULT 0,
        cache_write_tokens INTEGER NOT NULL DEFAULT 0,
        success INTEGER NOT NULL DEFAULT 0,
        error_code TEXT NOT NULL DEFAULT '',
        error_message TEXT NOT NULL DEFAULT '',
        fallback_from_model_id TEXT NOT NULL DEFAULT '',
        fallback_reason TEXT NOT NULL DEFAULT '',
        estimated_cost REAL,
        currency TEXT NOT NULL DEFAULT '',
        prompt_snapshot_id TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_execution_records_started_at
    ON model_execution_records(started_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_execution_records_session_id
    ON model_execution_records(session_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        instance_id TEXT NOT NULL,
        session_type TEXT NOT NULL,
        platform TEXT NOT NULL DEFAULT '',
        guild_id TEXT,
        channel_id TEXT NOT NULL DEFAULT '',
        display_name TEXT NOT NULL DEFAULT '',
        permission_group TEXT NOT NULL DEFAULT 'default',
        created_at REAL NOT NULL,
        last_active REAL NOT NULL,
        state_json TEXT NOT NULL DEFAULT '{}',
        plugin_data_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sessions_instance_id
    ON sessions(instance_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS session_configs (
        session_id TEXT PRIMARY KEY,
        prefixes_json TEXT NOT NULL DEFAULT '["/"]',
        llm_enabled INTEGER NOT NULL DEFAULT 1,
        is_muted INTEGER NOT NULL DEFAULT 0,
        audit_enabled INTEGER NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL,
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        entry_type TEXT NOT NULL DEFAULT 'command',
        command_name TEXT NOT NULL DEFAULT '',
        plugin_id TEXT NOT NULL DEFAULT '',
        user_id TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        instance_id TEXT NOT NULL DEFAULT '',
        permission_required TEXT NOT NULL DEFAULT '',
        permission_granted INTEGER NOT NULL DEFAULT 0,
        execution_time_ms REAL NOT NULL DEFAULT 0,
        success INTEGER NOT NULL DEFAULT 0,
        error TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp
    ON audit_logs(timestamp)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_audit_logs_session_id
    ON audit_logs(session_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS personas (
        uuid TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        prompt_definition_uuid TEXT NOT NULL,
        tags_json TEXT NOT NULL DEFAULT '[]',
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(prompt_definition_uuid) REFERENCES prompt_definitions(uuid) ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS context_strategies (
        uuid TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        type TEXT NOT NULL DEFAULT 'custom',
        resolver_ref TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        config_json TEXT NOT NULL DEFAULT '{}',
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agents (
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
        updated_at TEXT NOT NULL,
        FOREIGN KEY(persona_uuid) REFERENCES personas(uuid) ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS prompt_definitions (
        uuid TEXT PRIMARY KEY,
        prompt_id TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        source_type TEXT NOT NULL DEFAULT 'unknown_source',
        source_id TEXT NOT NULL DEFAULT '',
        owner_plugin_id TEXT NOT NULL DEFAULT '',
        owner_module TEXT NOT NULL DEFAULT '',
        module_path TEXT NOT NULL DEFAULT '',
        stage TEXT NOT NULL,
        type TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 100,
        version TEXT NOT NULL DEFAULT '1.0.0',
        description TEXT NOT NULL DEFAULT '',
        enabled INTEGER NOT NULL DEFAULT 1,
        content TEXT NOT NULL DEFAULT '',
        template_vars_json TEXT NOT NULL DEFAULT '[]',
        resolver_ref TEXT NOT NULL DEFAULT '',
        bundle_refs_json TEXT NOT NULL DEFAULT '[]',
        config_json TEXT NOT NULL DEFAULT '{}',
        tags_json TEXT NOT NULL DEFAULT '[]',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bot_configs (
        uuid TEXT PRIMARY KEY,
        instance_id TEXT NOT NULL UNIQUE,
        default_agent_uuid TEXT NOT NULL DEFAULT '',
        main_llm TEXT NOT NULL DEFAULT '',
        config_json TEXT NOT NULL DEFAULT '{}',
        tags_json TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    # ── Message logs & AI interactions ───────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS message_logs (
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
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_logs_session_id
    ON message_logs(session_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_logs_created_at
    ON message_logs(created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_logs_sender_id
    ON message_logs(sender_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id TEXT NOT NULL DEFAULT '',
        trigger_id INTEGER,
        response_id INTEGER,
        full_prompt_json TEXT NOT NULL DEFAULT '[]',
        think_text TEXT NOT NULL DEFAULT '',
        injected_context_json TEXT NOT NULL DEFAULT '[]',
        tool_calls_json TEXT NOT NULL DEFAULT '[]',
        model_id TEXT NOT NULL DEFAULT '',
        usage_json TEXT NOT NULL DEFAULT '{}',
        prompt_snapshot_id TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(trigger_id) REFERENCES message_logs(id),
        FOREIGN KEY(response_id) REFERENCES message_logs(id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ai_interactions_execution_id
    ON ai_interactions(execution_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ai_interactions_trigger_id
    ON ai_interactions(trigger_id)
    """,
    # ── Prompt snapshots (TTL-based full prompt storage) ────────────────
    """
    CREATE TABLE IF NOT EXISTS prompt_snapshots (
        id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL DEFAULT '',
        caller TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        instance_id TEXT NOT NULL DEFAULT '',
        route_id TEXT NOT NULL DEFAULT '',
        model_id TEXT NOT NULL DEFAULT '',
        prompt_signature TEXT NOT NULL DEFAULT '',
        cache_key TEXT NOT NULL DEFAULT '',
        messages_json TEXT NOT NULL DEFAULT '[]',
        tools_json TEXT NOT NULL DEFAULT '[]',
        compatibility_used INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL,
        expires_at REAL NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_prompt_snapshots_expires_at
    ON prompt_snapshots(expires_at)
    """,
)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _migrate_model_registry_schema(conn: sqlite3.Connection) -> None:
    provider_columns = _table_columns(conn, "model_providers")
    model_columns = _table_columns(conn, "model_definitions")

    if not provider_columns or not model_columns:
        return
    if "provider_uuid" in provider_columns and "provider_uuid" in model_columns:
        return

    provider_rows = conn.execute("SELECT * FROM model_providers").fetchall()
    provider_uuid_by_id = {str(row["id"]): str(uuid.uuid4()) for row in provider_rows}
    model_rows = conn.execute("SELECT * FROM model_definitions").fetchall()

    foreign_keys_before = conn.execute("PRAGMA foreign_keys").fetchone()
    had_foreign_keys = bool(foreign_keys_before[0]) if foreign_keys_before else True
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute(
            """
            CREATE TABLE model_providers__new (
                provider_uuid TEXT PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
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
            CREATE TABLE model_definitions__new (
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
                updated_at TEXT NOT NULL,
                FOREIGN KEY(provider_uuid) REFERENCES model_providers__new(provider_uuid) ON DELETE CASCADE
            )
            """
        )

        for row in provider_rows:
            conn.execute(
                """
                INSERT INTO model_providers__new (
                    provider_uuid, id, type, display_name, base_url, auth_json,
                    default_params_json, enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider_uuid_by_id[str(row["id"])],
                    row["id"],
                    row["type"],
                    row["display_name"],
                    row["base_url"],
                    row["auth_json"],
                    row["default_params_json"],
                    row["enabled"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )

        for row in model_rows:
            provider_uuid = provider_uuid_by_id.get(str(row["provider_id"]))
            if provider_uuid is None:
                continue
            conn.execute(
                """
                INSERT INTO model_definitions__new (
                    id, provider_uuid, litellm_model, display_name, capabilities_json,
                    context_window, default_params_json, cost_metadata_json,
                    enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    provider_uuid,
                    row["litellm_model"],
                    row["display_name"],
                    row["capabilities_json"],
                    row["context_window"],
                    row["default_params_json"],
                    row["cost_metadata_json"],
                    row["enabled"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )

        conn.execute("DROP TABLE model_definitions")
        conn.execute("DROP TABLE model_providers")
        conn.execute("ALTER TABLE model_providers__new RENAME TO model_providers")
        conn.execute("ALTER TABLE model_definitions__new RENAME TO model_definitions")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_model_definitions_provider_id
            ON model_definitions(provider_uuid)
            """
        )
    finally:
        conn.execute(f"PRAGMA foreign_keys = {'ON' if had_foreign_keys else 'OFF'}")


def _migrate_context_strategies_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "context_strategies")
    if not columns:
        return

    if "type" not in columns:
        conn.execute(
            """
            ALTER TABLE context_strategies
            ADD COLUMN type TEXT NOT NULL DEFAULT 'custom'
            """
        )
    if "trigger_ratio" not in columns:
        conn.execute(
            """
            ALTER TABLE context_strategies
            ADD COLUMN trigger_ratio REAL NOT NULL DEFAULT 0.5
            """
        )
    if "trim_turns" not in columns:
        conn.execute(
            """
            ALTER TABLE context_strategies
            ADD COLUMN trim_turns INTEGER NOT NULL DEFAULT 2
            """
        )


def _migrate_agents_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "agents")
    if not columns:
        return

    if "prompts_json" not in columns:
        conn.execute(
            """
            ALTER TABLE agents
            ADD COLUMN prompts_json TEXT NOT NULL DEFAULT '[]'
            """
        )
    if "context_strategy_json" not in columns:
        conn.execute(
            """
            ALTER TABLE agents
            ADD COLUMN context_strategy_json TEXT NOT NULL DEFAULT '{}'
            """
        )
    if "context_strategy_ref" in columns:
        conn.execute(
            """
            UPDATE agents
            SET context_strategy_json = json_object(
                'ref', context_strategy_ref,
                'type',
                CASE
                    WHEN context_strategy_ref = 'builtin.context.sliding_window' THEN 'sliding_window'
                    ELSE 'custom'
                END,
                'params', json('{}')
            )
            WHERE context_strategy_ref != ''
              AND context_strategy_json = '{}'
            """
        )
    if "config_json" not in columns:
        conn.execute(
            """
            ALTER TABLE agents
            ADD COLUMN config_json TEXT NOT NULL DEFAULT '{}'
            """
        )
    if "tags_json" not in columns:
        conn.execute(
            """
            ALTER TABLE agents
            ADD COLUMN tags_json TEXT NOT NULL DEFAULT '[]'
            """
        )


def _migrate_personas_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "personas")
    if not columns:
        return
    if "prompt_definition_uuid" not in columns:
        conn.execute(
            """
            ALTER TABLE personas
            ADD COLUMN prompt_definition_uuid TEXT NOT NULL DEFAULT ''
            """
        )
    if "tags_json" not in columns:
        conn.execute(
            """
            ALTER TABLE personas
            ADD COLUMN tags_json TEXT NOT NULL DEFAULT '[]'
            """
        )


def _migrate_prompt_definitions_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "prompt_definitions")
    if not columns:
        return

    column_defaults = {
        "source_type": "TEXT NOT NULL DEFAULT 'unknown_source'",
        "source_id": "TEXT NOT NULL DEFAULT ''",
        "owner_plugin_id": "TEXT NOT NULL DEFAULT ''",
        "owner_module": "TEXT NOT NULL DEFAULT ''",
        "module_path": "TEXT NOT NULL DEFAULT ''",
        "type": "TEXT NOT NULL DEFAULT 'static_text'",
        "description": "TEXT NOT NULL DEFAULT ''",
        "content": "TEXT NOT NULL DEFAULT ''",
        "template_vars_json": "TEXT NOT NULL DEFAULT '[]'",
        "resolver_ref": "TEXT NOT NULL DEFAULT ''",
        "bundle_refs_json": "TEXT NOT NULL DEFAULT '[]'",
        "config_json": "TEXT NOT NULL DEFAULT '{}'",
        "tags_json": "TEXT NOT NULL DEFAULT '[]'",
        "metadata_json": "TEXT NOT NULL DEFAULT '{}'",
    }
    for column_name, column_spec in column_defaults.items():
        if column_name in columns:
            continue
        conn.execute(
            f"""
            ALTER TABLE prompt_definitions
            ADD COLUMN {column_name} {column_spec}
            """
        )


def _migrate_bot_configs_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "bot_configs")
    if not columns:
        return

    column_defaults = {
        "default_agent_uuid": "TEXT NOT NULL DEFAULT ''",
        "main_llm": "TEXT NOT NULL DEFAULT ''",
        "config_json": "TEXT NOT NULL DEFAULT '{}'",
        "tags_json": "TEXT NOT NULL DEFAULT '[]'",
    }
    for column_name, column_spec in column_defaults.items():
        if column_name in columns:
            continue
        conn.execute(
            f"""
            ALTER TABLE bot_configs
            ADD COLUMN {column_name} {column_spec}
            """
        )


def _migrate_model_execution_records_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "model_execution_records")
    if not columns:
        return
    if "prompt_snapshot_id" not in columns:
        conn.execute(
            """
            ALTER TABLE model_execution_records
            ADD COLUMN prompt_snapshot_id TEXT NOT NULL DEFAULT ''
            """
        )


def _migrate_ai_interactions_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "ai_interactions")
    if not columns:
        return
    if "prompt_snapshot_id" not in columns:
        conn.execute(
            """
            ALTER TABLE ai_interactions
            ADD COLUMN prompt_snapshot_id TEXT NOT NULL DEFAULT ''
            """
        )


def _migrate_provider_capability_type(conn: sqlite3.Connection) -> None:
    """Add capability_type column and migrate data from defaultParams._tab."""
    import json

    columns = _table_columns(conn, "model_providers")
    if not columns:
        return

    if "capability_type" not in columns:
        conn.execute(
            """
            ALTER TABLE model_providers
            ADD COLUMN capability_type TEXT NOT NULL DEFAULT 'completion'
            """
        )

    # Migrate _tab from default_params_json → capability_type, then clean up _tab.
    _TAB_TO_CAPABILITY = {"chat": "completion", "embedding": "embedding", "other": "rerank"}
    rows = conn.execute(
        "SELECT provider_uuid, default_params_json, capability_type FROM model_providers"
    ).fetchall()
    for row in rows:
        try:
            params = json.loads(row["default_params_json"] or "{}")
        except Exception:
            params = {}
        tab = params.get("_tab")
        if not tab:
            continue
        capability_type = _TAB_TO_CAPABILITY.get(str(tab), "completion")
        params.pop("_tab", None)
        conn.execute(
            "UPDATE model_providers SET capability_type = ?, default_params_json = ? WHERE provider_uuid = ?",
            (capability_type, json.dumps(params), row["provider_uuid"]),
        )


def apply_schema(conn: sqlite3.Connection) -> None:
    """Create all persistence tables if they do not exist yet."""
    _migrate_model_registry_schema(conn)
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
    _migrate_provider_capability_type(conn)
    _migrate_context_strategies_schema(conn)
    _migrate_agents_schema(conn)
    _migrate_personas_schema(conn)
    _migrate_prompt_definitions_schema(conn)
    _migrate_bot_configs_schema(conn)
    _migrate_model_execution_records_schema(conn)
    _migrate_ai_interactions_schema(conn)
