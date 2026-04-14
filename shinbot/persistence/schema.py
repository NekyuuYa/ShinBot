"""SQLite schema bootstrap for ShinBot persistence."""

from __future__ import annotations

import sqlite3

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS model_providers (
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
    """,
    """
    CREATE TABLE IF NOT EXISTS model_definitions (
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
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_definitions_provider_id
    ON model_definitions(provider_id)
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
)


def apply_schema(conn: sqlite3.Connection) -> None:
    """Create all persistence tables if they do not exist yet."""
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
