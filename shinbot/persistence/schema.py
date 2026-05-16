"""SQLite schema bootstrap for ShinBot persistence."""

from __future__ import annotations

import sqlite3

SCHEMA_STATEMENTS: tuple[str, ...] = (
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
    CREATE TABLE IF NOT EXISTS model_usage_hourly (
        bucket_start TEXT NOT NULL,
        provider_id TEXT NOT NULL DEFAULT '',
        model_id TEXT NOT NULL DEFAULT '',
        total_calls INTEGER NOT NULL DEFAULT 0,
        successful_calls INTEGER NOT NULL DEFAULT 0,
        failed_calls INTEGER NOT NULL DEFAULT 0,
        cache_hits INTEGER NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cache_read_tokens INTEGER NOT NULL DEFAULT 0,
        cache_write_tokens INTEGER NOT NULL DEFAULT 0,
        total_latency_ms REAL NOT NULL DEFAULT 0,
        latency_sample_count INTEGER NOT NULL DEFAULT 0,
        total_ttft_ms REAL NOT NULL DEFAULT 0,
        ttft_sample_count INTEGER NOT NULL DEFAULT 0,
        last_seen_at TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(bucket_start, provider_id, model_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_usage_hourly_bucket_start
    ON model_usage_hourly(bucket_start)
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
        created_at REAL NOT NULL,
        routing_status TEXT NOT NULL DEFAULT 'pending',
        routed_at REAL,
        routing_skip_reason TEXT
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
    # ── Agent scheduler state ───────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS agent_scheduler_states (
        session_id TEXT PRIMARY KEY,
        state TEXT NOT NULL DEFAULT 'idle',
        next_review_at REAL,
        review_reason TEXT NOT NULL DEFAULT '',
        mention_sensitivity TEXT NOT NULL DEFAULT 'normal',
        active_reply_threshold_json TEXT NOT NULL DEFAULT '{}',
        active_chat_state_json TEXT NOT NULL DEFAULT '{}',
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_unread_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        message_log_id INTEGER NOT NULL,
        sender_id TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        review_consumed INTEGER NOT NULL DEFAULT 0,
        chat_consumed INTEGER NOT NULL DEFAULT 0,
        UNIQUE(session_id, message_log_id),
        FOREIGN KEY(message_log_id) REFERENCES message_logs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_unread_messages_session
    ON agent_unread_messages(session_id, review_consumed, created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_unread_ranges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        start_msg_log_id INTEGER NOT NULL,
        end_msg_log_id INTEGER NOT NULL,
        start_at REAL NOT NULL,
        end_at REAL NOT NULL,
        message_count INTEGER NOT NULL DEFAULT 0,
        review_consumed INTEGER NOT NULL DEFAULT 0,
        chat_consumed INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(start_msg_log_id) REFERENCES message_logs(id) ON DELETE CASCADE,
        FOREIGN KEY(end_msg_log_id) REFERENCES message_logs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_unread_ranges_session
    ON agent_unread_ranges(session_id, review_consumed, start_at, start_msg_log_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_review_summaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        start_msg_log_id INTEGER NOT NULL,
        end_msg_log_id INTEGER NOT NULL,
        start_at REAL NOT NULL,
        end_at REAL NOT NULL,
        message_count INTEGER NOT NULL DEFAULT 0,
        summary TEXT NOT NULL DEFAULT '',
        candidate_message_ids_json TEXT NOT NULL DEFAULT '[]',
        reason TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        FOREIGN KEY(start_msg_log_id) REFERENCES message_logs(id) ON DELETE CASCADE,
        FOREIGN KEY(end_msg_log_id) REFERENCES message_logs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_summaries_session
    ON agent_review_summaries(session_id, start_at, start_msg_log_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_high_priority_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        message_log_id INTEGER NOT NULL,
        sender_id TEXT NOT NULL DEFAULT '',
        kind TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        handled INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(message_log_id) REFERENCES message_logs(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_high_priority_events_session
    ON agent_high_priority_events(session_id, handled, created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_recent_mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        timestamp REAL NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_recent_mentions_session
    ON agent_recent_mentions(session_id, timestamp)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_summaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        summary_type TEXT NOT NULL,
        content TEXT NOT NULL DEFAULT '',
        source_run_id TEXT NOT NULL DEFAULT '',
        msg_log_start INTEGER,
        msg_log_end INTEGER,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        FOREIGN KEY(msg_log_start) REFERENCES message_logs(id) ON DELETE SET NULL,
        FOREIGN KEY(msg_log_end) REFERENCES message_logs(id) ON DELETE SET NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_summaries_session
    ON agent_summaries(session_id, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_summaries_run
    ON agent_summaries(source_run_id, created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_interactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id TEXT NOT NULL DEFAULT '',
        trigger_id INTEGER,
        response_id INTEGER,
        timestamp REAL NOT NULL DEFAULT 0,
        latency_ms REAL NOT NULL DEFAULT 0,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        cache_read_tokens INTEGER NOT NULL DEFAULT 0,
        cache_write_tokens INTEGER NOT NULL DEFAULT 0,
        model_id TEXT NOT NULL DEFAULT '',
        provider_id TEXT NOT NULL DEFAULT '',
        think_text TEXT NOT NULL DEFAULT '',
        injected_context_json TEXT NOT NULL DEFAULT '[]',
        tool_calls_json TEXT NOT NULL DEFAULT '[]',
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
    # ── Attention-driven conversation workflow ──────────────────────────
    """
    CREATE TABLE IF NOT EXISTS session_attention_states (
        session_id TEXT PRIMARY KEY,
        attention_value REAL NOT NULL DEFAULT 0.0,
        base_threshold REAL NOT NULL DEFAULT 5.0,
        runtime_threshold_offset REAL NOT NULL DEFAULT 0.0,
        cooldown_until REAL NOT NULL DEFAULT 0.0,
        last_update_at REAL NOT NULL DEFAULT 0.0,
        last_consumed_msg_log_id INTEGER,
        last_trigger_msg_log_id INTEGER,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sender_weight_states (
        session_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        stable_weight REAL NOT NULL DEFAULT 0.0,
        runtime_weight REAL NOT NULL DEFAULT 0.0,
        last_runtime_adjust_at REAL NOT NULL DEFAULT 0.0,
        PRIMARY KEY(session_id, sender_id),
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sender_weight_states_session_id
    ON sender_weight_states(session_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_runs (
        id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        instance_id TEXT NOT NULL DEFAULT '',
        response_profile TEXT NOT NULL DEFAULT 'balanced',
        batch_start_msg_id INTEGER,
        batch_end_msg_id INTEGER,
        batch_size INTEGER NOT NULL DEFAULT 0,
        trigger_attention REAL NOT NULL DEFAULT 0.0,
        effective_threshold REAL NOT NULL DEFAULT 0.0,
        tool_calls_json TEXT NOT NULL DEFAULT '[]',
        replied INTEGER NOT NULL DEFAULT 0,
        response_summary TEXT NOT NULL DEFAULT '',
        finish_reason TEXT NOT NULL DEFAULT '',
        started_at REAL NOT NULL,
        finished_at REAL,
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_workflow_runs_session_id
    ON workflow_runs(session_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_workflow_runs_started_at
    ON workflow_runs(started_at)
    """,
    # ── Media semantics & meme handling ───────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS media_assets (
        raw_hash TEXT PRIMARY KEY,
        element_type TEXT NOT NULL DEFAULT 'img',
        storage_path TEXT NOT NULL DEFAULT '',
        mime_type TEXT NOT NULL DEFAULT '',
        file_size INTEGER NOT NULL DEFAULT 0,
        strict_dhash TEXT NOT NULL DEFAULT '',
        width INTEGER,
        height INTEGER,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        first_seen_at REAL NOT NULL,
        last_seen_at REAL NOT NULL,
        expire_at REAL NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_media_assets_expire_at
    ON media_assets(expire_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS message_media_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_log_id INTEGER NOT NULL,
        session_id TEXT NOT NULL,
        platform_msg_id TEXT NOT NULL DEFAULT '',
        raw_hash TEXT NOT NULL,
        media_index INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL,
        FOREIGN KEY(message_log_id) REFERENCES message_logs(id) ON DELETE CASCADE,
        FOREIGN KEY(raw_hash) REFERENCES media_assets(raw_hash) ON DELETE CASCADE
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_media_links_message_log_id
    ON message_media_links(message_log_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_media_links_session_id_message_log_id
    ON message_media_links(session_id, message_log_id DESC, media_index ASC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_media_links_session_id_platform_msg_id
    ON message_media_links(session_id, platform_msg_id, message_log_id DESC, media_index ASC)
    """,
    """
    CREATE TABLE IF NOT EXISTS session_media_occurrences (
        session_id TEXT NOT NULL,
        raw_hash TEXT NOT NULL,
        strict_dhash TEXT NOT NULL DEFAULT '',
        last_sender_id TEXT NOT NULL DEFAULT '',
        last_platform_msg_id TEXT NOT NULL DEFAULT '',
        recent_timestamps_json TEXT NOT NULL DEFAULT '[]',
        occurrence_count INTEGER NOT NULL DEFAULT 0,
        first_seen_at REAL NOT NULL,
        last_seen_at REAL NOT NULL,
        expire_at REAL NOT NULL,
        PRIMARY KEY(session_id, raw_hash)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_session_media_occurrences_expire_at
    ON session_media_occurrences(expire_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS media_semantics (
        raw_hash TEXT PRIMARY KEY,
        kind TEXT NOT NULL DEFAULT '',
        digest TEXT NOT NULL DEFAULT '',
        verified_by_model INTEGER NOT NULL DEFAULT 0,
        inspection_agent_ref TEXT NOT NULL DEFAULT '',
        inspection_llm_ref TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        first_seen_at REAL NOT NULL,
        last_seen_at REAL NOT NULL,
        expire_at REAL NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_media_semantics_expire_at
    ON media_semantics(expire_at)
    """,
)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _drop_legacy_model_registry_tables(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS model_route_members")
    conn.execute("DROP TABLE IF EXISTS model_definitions")
    conn.execute("DROP TABLE IF EXISTS model_routes")
    conn.execute("DROP TABLE IF EXISTS model_providers")


def _drop_legacy_agents_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS agents")


def _drop_legacy_context_strategies_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS context_strategies")


def _drop_legacy_personas_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS personas")


def _drop_legacy_prompt_definitions_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS prompt_definitions")


def _drop_legacy_bot_configs_table(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS bot_configs")


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
    new_columns = {
        "timestamp": "REAL NOT NULL DEFAULT 0",
        "latency_ms": "REAL NOT NULL DEFAULT 0",
        "input_tokens": "INTEGER NOT NULL DEFAULT 0",
        "output_tokens": "INTEGER NOT NULL DEFAULT 0",
        "cache_read_tokens": "INTEGER NOT NULL DEFAULT 0",
        "cache_write_tokens": "INTEGER NOT NULL DEFAULT 0",
        "provider_id": "TEXT NOT NULL DEFAULT ''",
        "prompt_snapshot_id": "TEXT NOT NULL DEFAULT ''",
    }
    for col, spec in new_columns.items():
        if col not in columns:
            conn.execute(f"ALTER TABLE ai_interactions ADD COLUMN {col} {spec}")


def _migrate_message_logs_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "message_logs")
    if not columns:
        return
    new_columns = {
        "routing_status": "TEXT NOT NULL DEFAULT 'pending'",
        "routed_at": "REAL",
        "routing_skip_reason": "TEXT",
    }
    for col, spec in new_columns.items():
        if col not in columns:
            conn.execute(f"ALTER TABLE message_logs ADD COLUMN {col} {spec}")


def _migrate_agent_scheduler_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "agent_scheduler_states")
    if not columns:
        return
    new_columns = {
        "next_review_at": "REAL",
        "review_reason": "TEXT NOT NULL DEFAULT ''",
        "mention_sensitivity": "TEXT NOT NULL DEFAULT 'normal'",
        "active_reply_threshold_json": "TEXT NOT NULL DEFAULT '{}'",
        "active_chat_state_json": "TEXT NOT NULL DEFAULT '{}'",
    }
    for col, spec in new_columns.items():
        if col not in columns:
            conn.execute(f"ALTER TABLE agent_scheduler_states ADD COLUMN {col} {spec}")


def _migrate_agent_unread_ranges(conn: sqlite3.Connection) -> None:
    range_columns = _table_columns(conn, "agent_unread_ranges")
    message_columns = _table_columns(conn, "agent_unread_messages")
    if not range_columns or not message_columns:
        return
    existing = conn.execute("SELECT COUNT(*) AS cnt FROM agent_unread_ranges").fetchone()
    if existing is not None and int(existing["cnt"] or 0) > 0:
        return
    rows = conn.execute(
        """
        SELECT session_id, message_log_id, created_at, review_consumed, chat_consumed
        FROM agent_unread_messages
        ORDER BY session_id ASC, created_at ASC, message_log_id ASC
        """
    ).fetchall()
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO agent_unread_ranges (
            session_id, start_msg_log_id, end_msg_log_id, start_at, end_at,
            message_count, review_consumed, chat_consumed
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        """,
        [
            (
                row["session_id"],
                row["message_log_id"],
                row["message_log_id"],
                row["created_at"],
                row["created_at"],
                row["review_consumed"],
                row["chat_consumed"],
            )
            for row in rows
        ],
    )


def _migrate_workflow_runs_schema(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "workflow_runs")
    if not columns:
        return
    if "response_profile" not in columns:
        conn.execute(
            """
            ALTER TABLE workflow_runs
            ADD COLUMN response_profile TEXT NOT NULL DEFAULT 'balanced'
            """
        )
    if "finish_reason" not in columns:
        conn.execute(
            """
            ALTER TABLE workflow_runs
            ADD COLUMN finish_reason TEXT NOT NULL DEFAULT ''
            """
        )


def apply_schema(conn: sqlite3.Connection) -> None:
    """Create all persistence tables if they do not exist yet."""
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
    _drop_legacy_model_registry_tables(conn)
    _drop_legacy_agents_table(conn)
    _drop_legacy_context_strategies_table(conn)
    _drop_legacy_personas_table(conn)
    _drop_legacy_prompt_definitions_table(conn)
    _drop_legacy_bot_configs_table(conn)
    _migrate_model_execution_records_schema(conn)
    _migrate_ai_interactions_schema(conn)
    _migrate_message_logs_schema(conn)
    _migrate_agent_scheduler_schema(conn)
    _migrate_agent_unread_ranges(conn)
    _migrate_workflow_runs_schema(conn)
