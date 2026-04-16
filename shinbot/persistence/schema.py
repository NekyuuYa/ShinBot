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
        prompt_text TEXT NOT NULL DEFAULT '',
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
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


def apply_schema(conn: sqlite3.Connection) -> None:
    """Create all persistence tables if they do not exist yet."""
    _migrate_model_registry_schema(conn)
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
