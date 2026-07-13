"""SQLite schema bootstrap for ShinBot persistence."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time

from shinbot.persistence.canonical_json import (
    MAX_CANONICAL_JSON_BYTES,
    validate_canonical_json_object,
)
from shinbot.persistence.sqlite_raw import (
    RawSQLiteValue,
    bounded_raw_sqlite_projection,
    complete_truncated_raw_sqlite_value,
    decode_raw_sqlite_values,
    raw_sqlite_values,
)

_SQLITE_INT64_MAX = (1 << 63) - 1


# These indexes deliberately remain non-unique. Raw-key preflight must be
# able to observe TEXT/BLOB storage aliases as conflicting persisted rows
# instead of making them impossible to inspect or recover from.
_ACTOR_RAW_LOGICAL_KEY_INDEXES: tuple[tuple[str, str, str], ...] = (
    (
        "idx_agent_session_mailbox_raw_logical_key",
        "agent_session_mailbox",
        """
        CREATE INDEX IF NOT EXISTS idx_agent_session_mailbox_raw_logical_key
        ON agent_session_mailbox(
            CAST(profile_id AS BLOB),
            CAST(session_id AS BLOB),
            CAST(event_id AS BLOB)
        )
        """,
    ),
    (
        "idx_agent_review_schedule_events_raw_logical_key",
        "agent_review_schedule_events",
        """
        CREATE INDEX IF NOT EXISTS
            idx_agent_review_schedule_events_raw_logical_key
        ON agent_review_schedule_events(CAST(schedule_event_id AS BLOB))
        """,
    ),
)


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
        state_resume_json TEXT NOT NULL DEFAULT '{}',
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
        response_profile TEXT NOT NULL DEFAULT '',
        is_mentioned INTEGER NOT NULL DEFAULT 0,
        is_reply_to_bot INTEGER NOT NULL DEFAULT 0,
        is_mention_to_other INTEGER NOT NULL DEFAULT 0,
        is_poke_to_bot INTEGER NOT NULL DEFAULT 0,
        is_poke_to_other INTEGER NOT NULL DEFAULT 0,
        self_platform_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
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
        strict_dhash TEXT NOT NULL DEFAULT '',
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
    """
    CREATE INDEX IF NOT EXISTS idx_media_semantics_strict_dhash
    ON media_semantics(strict_dhash)
    """,
    # -- Recoverable core message routing --------------------------------
    """
    CREATE TABLE IF NOT EXISTS message_routing_jobs (
        routing_job_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        routing_job_id TEXT NOT NULL UNIQUE,
        idempotency_key TEXT NOT NULL UNIQUE,
        message_log_id INTEGER NOT NULL UNIQUE,
        version INTEGER NOT NULL,
        profile_id TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        message_fingerprint TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        trace_id TEXT NOT NULL,
        correlation_id TEXT NOT NULL,
        causation_id TEXT NOT NULL DEFAULT '',
        occurred_at REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        claim_id TEXT NOT NULL DEFAULT '',
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        decision_version INTEGER,
        decision_kind TEXT NOT NULL DEFAULT '',
        decision_id TEXT NOT NULL DEFAULT '',
        decision_payload_json TEXT NOT NULL DEFAULT '{}',
        decision_payload_digest TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        completed_at REAL,
        failed_at REAL,
        last_error_code TEXT NOT NULL DEFAULT '',
        last_error_message TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(message_log_id) REFERENCES message_logs(id) ON DELETE CASCADE,
        CHECK(version >= 1),
        CHECK(ownership_generation >= 0),
        CHECK(
            (
                profile_id = ''
                AND session_id = ''
                AND ownership_generation = 0
            )
            OR
            (
                profile_id != ''
                AND session_id != ''
                AND ownership_generation >= 1
            )
        ),
        CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
        CHECK(attempt_count >= 0),
        CHECK(decision_version IS NULL OR decision_version >= 1)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_routing_jobs_pending
    ON message_routing_jobs(status, available_at, routing_job_seq)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_routing_jobs_leases
    ON message_routing_jobs(status, lease_until)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_message_routing_jobs_trace
    ON message_routing_jobs(trace_id, routing_job_seq)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_route_outbox (
        outbox_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_id TEXT NOT NULL UNIQUE,
        idempotency_key TEXT NOT NULL UNIQUE,
        routing_job_id TEXT NOT NULL,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        message_log_id INTEGER NOT NULL,
        route_rule_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        ownership_generation INTEGER NOT NULL,
        event_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        trace_id TEXT NOT NULL,
        correlation_id TEXT NOT NULL,
        causation_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        claim_id TEXT NOT NULL DEFAULT '',
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        completed_at REAL,
        failed_at REAL,
        last_error_code TEXT NOT NULL DEFAULT '',
        last_error_message TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(routing_job_id)
            REFERENCES message_routing_jobs(routing_job_id) ON DELETE CASCADE,
        FOREIGN KEY(message_log_id) REFERENCES message_logs(id) ON DELETE CASCADE,
        UNIQUE(profile_id, session_id, message_log_id, route_rule_id),
        CHECK(version >= 1),
        CHECK(ownership_generation >= 1),
        CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
        CHECK(attempt_count >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_route_outbox_pending
    ON agent_route_outbox(status, available_at, outbox_seq)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_route_outbox_leases
    ON agent_route_outbox(status, lease_until)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_route_outbox_session
    ON agent_route_outbox(profile_id, session_id, status, outbox_seq)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_route_outbox_trace
    ON agent_route_outbox(trace_id, outbox_seq)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_route_outbox_event
    ON agent_route_outbox(profile_id, session_id, event_id)
    """,
    # -- Durable Agent runtime ownership gate ---------------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_runtime_ownership (
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        legacy_session_id TEXT NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        pending_mode TEXT NOT NULL DEFAULT '',
        generation INTEGER NOT NULL,
        selection_reason TEXT NOT NULL,
        migration_reason TEXT NOT NULL DEFAULT '',
        requested_by TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY(profile_id, session_id),
        CHECK(mode IN ('legacy', 'actor_v2')),
        CHECK(status IN ('active', 'migrating')),
        CHECK(pending_mode IN ('', 'legacy', 'actor_v2')),
        CHECK(generation >= 1),
        CHECK(
            (status = 'active' AND pending_mode = '')
            OR
            (
                status = 'migrating'
                AND pending_mode != ''
                AND pending_mode != mode
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_runtime_ownership_mode
    ON agent_session_runtime_ownership(mode, status, updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_runtime_ownership_legacy
    ON agent_session_runtime_ownership(legacy_session_id, mode, status)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_runtime_ownership_events (
        event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        generation INTEGER NOT NULL,
        from_mode TEXT NOT NULL DEFAULT '',
        to_mode TEXT NOT NULL,
        status TEXT NOT NULL,
        reason TEXT NOT NULL,
        requested_by TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_runtime_ownership(profile_id, session_id)
            ON DELETE CASCADE,
        CHECK(
            event_type IN (
                'claimed', 'migration_started',
                'migration_completed', 'migration_aborted'
            )
        ),
        CHECK(from_mode IN ('', 'legacy', 'actor_v2')),
        CHECK(to_mode IN ('legacy', 'actor_v2')),
        CHECK(status IN ('active', 'migrating')),
        CHECK(generation >= 1)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_runtime_ownership_events_key
    ON agent_session_runtime_ownership_events(
        profile_id, session_id, event_seq
    )
    """,
    # -- Durable Agent session actors (v2) -------------------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_aggregates (
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'idle',
        state_revision INTEGER NOT NULL DEFAULT 0,
        event_sequence INTEGER NOT NULL DEFAULT 0,
        activity_generation INTEGER NOT NULL DEFAULT 0,
        active_epoch INTEGER NOT NULL DEFAULT 0,
        review_plan_json TEXT NOT NULL DEFAULT '{}',
        current_plan_id TEXT NOT NULL DEFAULT '',
        review_plan_revision INTEGER NOT NULL DEFAULT 0,
        active_reply_resume_json TEXT NOT NULL DEFAULT '{}',
        active_chat_state_json TEXT NOT NULL DEFAULT '{}',
        review_operation_id TEXT NOT NULL DEFAULT '',
        active_reply_operation_id TEXT NOT NULL DEFAULT '',
        active_chat_round_operation_id TEXT NOT NULL DEFAULT '',
        idle_planning_operation_id TEXT NOT NULL DEFAULT '',
        data_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY(profile_id, session_id),
        CHECK(state_revision >= 0),
        CHECK(event_sequence >= 0),
        CHECK(review_plan_revision >= 0),
        CHECK(ownership_generation >= 0),
        CHECK(activity_generation >= 0),
        CHECK(active_epoch >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_aggregates_state
    ON agent_session_aggregates(profile_id, state, updated_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_mailbox (
        mailbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        kind TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        occurred_at REAL NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        causation_id TEXT NOT NULL DEFAULT '',
        correlation_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        claim_id TEXT NOT NULL DEFAULT '',
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        created_at REAL NOT NULL,
        handled_at REAL,
        last_error TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(profile_id, session_id, event_id),
        CHECK(ownership_generation >= 0),
        CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
        CHECK(attempt_count >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_mailbox_pending
    ON agent_session_mailbox(
        profile_id, session_id, status, available_at, mailbox_id
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_mailbox_leases
    ON agent_session_mailbox(status, lease_until)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_operations (
        operation_id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        kind TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        launched_by_event_id TEXT NOT NULL DEFAULT '',
        state_revision INTEGER NOT NULL DEFAULT 0,
        active_epoch INTEGER NOT NULL DEFAULT 0,
        activity_generation INTEGER NOT NULL DEFAULT 0,
        input_watermark INTEGER,
        input_ledger_sequence INTEGER,
        started_at REAL NOT NULL,
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        superseded_at REAL,
        finished_at REAL,
        failure_code TEXT NOT NULL DEFAULT '',
        failure_message TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        CHECK(
            status IN (
                'pending', 'running', 'completed', 'failed',
                'superseded', 'cancelled'
            )
        ),
        CHECK(state_revision >= 0),
        CHECK(ownership_generation >= 0),
        CHECK(active_epoch >= 0),
        CHECK(activity_generation >= 0),
        CHECK(input_watermark IS NULL OR input_watermark >= 0),
        CHECK(input_ledger_sequence IS NULL OR input_ledger_sequence >= 0),
        CHECK(
            (input_watermark IS NULL AND input_ledger_sequence IS NULL)
            OR
            (input_watermark IS NOT NULL AND input_ledger_sequence IS NOT NULL)
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_operations_live
    ON agent_session_operations(profile_id, session_id, status, kind)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_operations_leases
    ON agent_session_operations(status, lease_until)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_message_ledger_consumptions (
        consumption_id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        kind TEXT NOT NULL,
        selection TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        source_event_id TEXT NOT NULL,
        input_watermark INTEGER NOT NULL,
        input_ledger_sequence INTEGER NOT NULL DEFAULT 0,
        explicit_message_log_ids_json TEXT NOT NULL DEFAULT '[]',
        canonical_json TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        occurred_at REAL NOT NULL,
        committed_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        FOREIGN KEY(operation_id)
            REFERENCES agent_session_operations(operation_id)
            ON DELETE RESTRICT,
        UNIQUE(profile_id, session_id, kind, idempotency_key),
        CHECK(ownership_generation >= 1),
        CHECK(kind IN ('review', 'chat', 'high_priority')),
        CHECK(selection IN ('all_through_watermark', 'explicit_ids')),
        CHECK(input_watermark >= 0),
        CHECK(input_ledger_sequence >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_message_consumptions_operation
    ON agent_message_ledger_consumptions(
        profile_id, session_id, operation_id, kind, committed_at
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_message_consumptions_watermark
    ON agent_message_ledger_consumptions(
        profile_id, session_id, ownership_generation,
        selection, input_watermark, committed_at
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_message_ledger (
        ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ledger_sequence INTEGER NOT NULL,
        message_log_id INTEGER NOT NULL,
        ownership_generation INTEGER NOT NULL,
        source_event_id TEXT NOT NULL,
        actor_event_id TEXT NOT NULL,
        delivery_version INTEGER NOT NULL,
        event_source TEXT NOT NULL,
        sender_id TEXT NOT NULL DEFAULT '',
        instance_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        bot_id TEXT NOT NULL DEFAULT '',
        bot_binding_id TEXT NOT NULL DEFAULT '',
        base_session_id TEXT NOT NULL DEFAULT '',
        bot_session_id TEXT NOT NULL DEFAULT '',
        platform TEXT NOT NULL DEFAULT '',
        self_id TEXT NOT NULL DEFAULT '',
        is_private INTEGER NOT NULL,
        is_mentioned INTEGER NOT NULL,
        is_mention_to_other INTEGER NOT NULL,
        is_reply_to_bot INTEGER NOT NULL,
        is_poke_to_bot INTEGER NOT NULL,
        is_poke_to_other INTEGER NOT NULL,
        already_handled INTEGER NOT NULL,
        is_stopped INTEGER NOT NULL,
        is_self_message INTEGER NOT NULL,
        eligible_for_work INTEGER NOT NULL,
        suppression_reason TEXT NOT NULL DEFAULT '',
        response_profile TEXT NOT NULL DEFAULT '',
        priority_mention INTEGER NOT NULL,
        priority_reply_to_bot INTEGER NOT NULL,
        priority_repeated_mention INTEGER NOT NULL,
        priority_poke_to_bot INTEGER NOT NULL,
        priority_should_wake INTEGER NOT NULL,
        priority_reasons_json TEXT NOT NULL DEFAULT '{}',
        causation_id TEXT NOT NULL DEFAULT '',
        correlation_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        observed_at REAL NOT NULL,
        occurred_at REAL NOT NULL,
        event_created_at REAL NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        canonical_json TEXT NOT NULL,
        review_consumption_id TEXT,
        chat_consumption_id TEXT,
        high_priority_consumption_id TEXT,
        recorded_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        FOREIGN KEY(message_log_id)
            REFERENCES message_logs(id)
            ON DELETE CASCADE,
        FOREIGN KEY(review_consumption_id)
            REFERENCES agent_message_ledger_consumptions(consumption_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(chat_consumption_id)
            REFERENCES agent_message_ledger_consumptions(consumption_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(high_priority_consumption_id)
            REFERENCES agent_message_ledger_consumptions(consumption_id)
            ON DELETE RESTRICT,
        UNIQUE(profile_id, session_id, ledger_sequence),
        UNIQUE(profile_id, session_id, message_log_id),
        UNIQUE(profile_id, session_id, source_event_id),
        CHECK(ledger_sequence >= 1),
        CHECK(ownership_generation >= 1),
        CHECK(delivery_version >= 1),
        CHECK(is_private IN (0, 1)),
        CHECK(is_mentioned IN (0, 1)),
        CHECK(is_mention_to_other IN (0, 1)),
        CHECK(is_reply_to_bot IN (0, 1)),
        CHECK(is_poke_to_bot IN (0, 1)),
        CHECK(is_poke_to_other IN (0, 1)),
        CHECK(already_handled IN (0, 1)),
        CHECK(is_stopped IN (0, 1)),
        CHECK(is_self_message IN (0, 1)),
        CHECK(eligible_for_work IN (0, 1)),
        CHECK(
            (eligible_for_work = 1 AND suppression_reason = '')
            OR (eligible_for_work = 0 AND length(suppression_reason) > 0)
        ),
        CHECK(priority_mention IN (0, 1)),
        CHECK(priority_reply_to_bot IN (0, 1)),
        CHECK(priority_repeated_mention IN (0, 1)),
        CHECK(priority_poke_to_bot IN (0, 1)),
        CHECK(priority_should_wake IN (0, 1))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_message_ledger_unread
    ON agent_message_ledger(
        profile_id, session_id,
        review_consumption_id, chat_consumption_id, ledger_sequence
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_message_ledger_priority
    ON agent_message_ledger(
        profile_id, session_id,
        high_priority_consumption_id, ledger_sequence
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_review_schedules (
        plan_id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        plan_revision INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'scheduled',
        trigger TEXT NOT NULL DEFAULT '',
        outcome TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT '',
        requested_delay_seconds REAL,
        applied_delay_seconds REAL NOT NULL,
        scheduled_from REAL NOT NULL,
        next_review_at REAL NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        fallback_reason TEXT NOT NULL DEFAULT '',
        mention_sensitivity TEXT NOT NULL DEFAULT 'normal',
        active_reply_threshold_json TEXT NOT NULL DEFAULT '{}',
        model_execution_id TEXT NOT NULL DEFAULT '',
        prompt_signature TEXT NOT NULL DEFAULT '',
        expected_active_epoch INTEGER,
        expected_activity_generation INTEGER,
        committed_state_revision INTEGER NOT NULL,
        available_at REAL NOT NULL,
        claim_owner TEXT NOT NULL DEFAULT '',
        claim_until REAL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        delivery_cycle INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(profile_id, session_id, plan_revision),
        CHECK(status IN ('scheduled', 'claimed', 'completed', 'failed', 'superseded')),
        CHECK(ownership_generation >= 0),
        CHECK(plan_revision >= 0),
        CHECK(applied_delay_seconds >= 0),
        CHECK(committed_state_revision >= 0),
        CHECK(attempt_count >= 0),
        CHECK(delivery_cycle >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_schedules_due
    ON agent_review_schedules(status, available_at, next_review_at, claim_until)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_schedules_session
    ON agent_review_schedules(profile_id, session_id, plan_revision DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_state_transitions (
        transition_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        transition_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        event_id TEXT NOT NULL,
        from_state TEXT NOT NULL,
        to_state TEXT NOT NULL,
        trigger TEXT NOT NULL DEFAULT '',
        disposition TEXT NOT NULL DEFAULT 'applied',
        state_revision INTEGER NOT NULL,
        event_sequence INTEGER NOT NULL,
        operation_id TEXT NOT NULL DEFAULT '',
        plan_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(profile_id, session_id, event_sequence),
        CHECK(ownership_generation >= 0),
        CHECK(state_revision >= 0),
        CHECK(event_sequence >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_state_transitions_session
    ON agent_state_transitions(profile_id, session_id, transition_seq DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_review_schedule_events (
        schedule_event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        schedule_event_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        event_id TEXT NOT NULL,
        plan_id TEXT NOT NULL DEFAULT '',
        previous_plan_id TEXT NOT NULL DEFAULT '',
        event_type TEXT NOT NULL,
        trigger TEXT NOT NULL DEFAULT '',
        outcome TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT '',
        requested_delay_seconds REAL,
        applied_delay_seconds REAL,
        scheduled_from REAL,
        next_review_at REAL,
        reason TEXT NOT NULL DEFAULT '',
        fallback_reason TEXT NOT NULL DEFAULT '',
        model_execution_id TEXT NOT NULL DEFAULT '',
        prompt_signature TEXT NOT NULL DEFAULT '',
        expected_active_epoch INTEGER,
        expected_activity_generation INTEGER,
        committed_state_revision INTEGER,
        operation_id TEXT NOT NULL DEFAULT '',
        trace_id TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        CHECK(ownership_generation >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_schedule_events_session
    ON agent_review_schedule_events(
        profile_id, session_id, schedule_event_seq DESC
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_schedule_events_execution
    ON agent_review_schedule_events(model_execution_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_effect_outbox (
        effect_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        effect_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        event_id TEXT NOT NULL,
        operation_id TEXT NOT NULL DEFAULT '',
        kind TEXT NOT NULL,
        contract_version INTEGER NOT NULL,
        contract_signature TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        claim_id TEXT NOT NULL DEFAULT '',
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        completed_at REAL,
        last_error TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(profile_id, session_id, effect_id),
        UNIQUE(profile_id, session_id, idempotency_key),
        CHECK(
            typeof(effect_id) = 'text'
            AND length(
                trim(effect_id, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND effect_id = trim(
                effect_id,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(idempotency_key) = 'text'
            AND length(
                trim(idempotency_key, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND idempotency_key = trim(
                idempotency_key,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(profile_id) = 'text'
            AND length(
                trim(profile_id, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND profile_id = trim(
                profile_id,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(session_id) = 'text'
            AND length(
                trim(session_id, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND session_id = trim(
                session_id,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(event_id) = 'text'
            AND length(
                trim(event_id, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND event_id = trim(
                event_id,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(operation_id) = 'text'
            AND operation_id = trim(
                operation_id,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(kind) = 'text'
            AND length(
                trim(kind, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND kind = trim(
                kind,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(contract_signature) = 'text'
            AND length(
                trim(contract_signature, char(9) || char(10) || char(11) ||
                     char(12) || char(13) || ' ')
            ) > 0
            AND contract_signature = trim(
                contract_signature,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            CASE
                WHEN typeof(payload_json) = 'text' AND json_valid(payload_json)
                THEN json_type(payload_json) = 'object'
                ELSE 0
            END
        ),
        CHECK(
            CASE
                WHEN typeof(payload_json) = 'text' AND json_valid(payload_json)
                THEN payload_json = json(payload_json)
                ELSE 0
            END
        ),
        CHECK(typeof(status) = 'text'),
        CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 0),
        CHECK(typeof(contract_version) = 'integer'),
        CHECK(contract_version >= 1),
        CHECK(typeof(attempt_count) = 'integer'),
        CHECK(attempt_count >= 0),
        CHECK(attempt_count <= 9223372036854775807),
        CHECK(
            typeof(claim_id) = 'text'
            AND claim_id = trim(
                claim_id,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(
            typeof(lease_owner) = 'text'
            AND lease_owner = trim(
                lease_owner,
                char(9) || char(10) || char(11) || char(12) || char(13) || ' '
            )
        ),
        CHECK(typeof(last_error) = 'text'),
        CHECK(
            typeof(available_at) IN ('integer', 'real')
            AND available_at >= 0
            AND available_at <= 1.7976931348623157e308
        ),
        CHECK(
            typeof(created_at) IN ('integer', 'real')
            AND created_at >= 0
            AND created_at <= 1.7976931348623157e308
        ),
        CHECK(
            typeof(updated_at) IN ('integer', 'real')
            AND updated_at >= 0
            AND updated_at <= 1.7976931348623157e308
        ),
        CHECK(
            lease_until IS NULL
            OR (
                typeof(lease_until) IN ('integer', 'real')
                AND lease_until >= 0
                AND lease_until <= 1.7976931348623157e308
            )
        ),
        CHECK(
            completed_at IS NULL
            OR (
                typeof(completed_at) IN ('integer', 'real')
                AND completed_at >= 0
                AND completed_at <= 1.7976931348623157e308
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_effect_outbox_pending
    ON agent_effect_outbox(status, available_at, effect_seq)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_effect_outbox_session
    ON agent_effect_outbox(profile_id, session_id, status, effect_seq)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_effect_scrub_state (
        cursor_name TEXT PRIMARY KEY,
        last_effect_seq INTEGER NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL DEFAULT 0,
        CHECK(typeof(cursor_name) = 'text'),
        CHECK(cursor_name = 'claimable'),
        CHECK(typeof(last_effect_seq) = 'integer'),
        CHECK(last_effect_seq >= 0),
        CHECK(typeof(updated_at) IN ('integer', 'real')),
        CHECK(updated_at >= 0),
        CHECK(updated_at <= 1.7976931348623157e308)
    )
    """,
    """
    INSERT OR IGNORE INTO agent_effect_scrub_state (
        cursor_name, last_effect_seq, updated_at
    ) VALUES ('claimable', 0, 0)
    """,
    # -- Durable session-actor recovery cases ----------------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_recovery_cases (
        case_id TEXT PRIMARY KEY,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        certificate_version INTEGER NOT NULL,
        policy_version INTEGER NOT NULL,
        work_graph_digest TEXT NOT NULL,
        latest_certificate_digest TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        next_delivery_cycle INTEGER NOT NULL DEFAULT 0,
        delivery_count INTEGER NOT NULL DEFAULT 0,
        last_event_id TEXT NOT NULL DEFAULT '',
        last_error TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(
            profile_id, session_id, ownership_generation,
            policy_version, work_graph_digest
        ),
        CHECK(typeof(case_id) = 'text'),
        CHECK(length(case_id) = 81),
        CHECK(substr(case_id, 1, 17) = 'recovery-case:v1:'),
        CHECK(substr(case_id, 18) NOT GLOB '*[^0-9a-f]*'),
        CHECK(typeof(profile_id) = 'text'),
        CHECK(
            length(
                trim(
                    profile_id,
                    char(9) || char(10) || char(11) ||
                    char(12) || char(13) || ' '
                )
            ) > 0
        ),
        CHECK(
            profile_id = trim(
                profile_id,
                char(9) || char(10) || char(11) ||
                char(12) || char(13) || ' '
            )
        ),
        CHECK(typeof(session_id) = 'text'),
        CHECK(
            length(
                trim(
                    session_id,
                    char(9) || char(10) || char(11) ||
                    char(12) || char(13) || ' '
                )
            ) > 0
        ),
        CHECK(
            session_id = trim(
                session_id,
                char(9) || char(10) || char(11) ||
                char(12) || char(13) || ' '
            )
        ),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(typeof(certificate_version) = 'integer'),
        CHECK(certificate_version = 1),
        CHECK(typeof(policy_version) = 'integer'),
        CHECK(policy_version >= 1),
        CHECK(typeof(work_graph_digest) = 'text'),
        CHECK(length(work_graph_digest) = 64),
        CHECK(work_graph_digest NOT GLOB '*[^0-9a-f]*'),
        CHECK(typeof(latest_certificate_digest) = 'text'),
        CHECK(length(latest_certificate_digest) = 64),
        CHECK(latest_certificate_digest NOT GLOB '*[^0-9a-f]*'),
        CHECK(typeof(status) = 'text'),
        CHECK(
            status IN (
                'open', 'applied', 'superseded',
                'delivery_exhausted', 'scanner_blocked'
            )
        ),
        CHECK(
            status != 'scanner_blocked'
            OR length(
                trim(
                    last_error,
                    char(9) || char(10) || char(11) ||
                    char(12) || char(13) || ' '
                )
            ) > 0
        ),
        CHECK(typeof(next_delivery_cycle) = 'integer'),
        CHECK(typeof(delivery_count) = 'integer'),
        CHECK(next_delivery_cycle >= 0),
        CHECK(delivery_count >= 0),
        CHECK(next_delivery_cycle = delivery_count),
        CHECK(typeof(last_event_id) = 'text'),
        CHECK(typeof(last_error) = 'text'),
        CHECK(
            (delivery_count = 0 AND last_event_id = '')
            OR (
                delivery_count >= 1
                AND last_event_id =
                    'recovery-requested:v1:'
                    || substr(case_id, 18)
                    || ':'
                    || CAST(delivery_count - 1 AS TEXT)
            )
        ),
        CHECK(typeof(created_at) IN ('integer', 'real')),
        CHECK(typeof(updated_at) IN ('integer', 'real')),
        CHECK(created_at >= 0),
        CHECK(created_at <= 1.7976931348623157e308),
        CHECK(updated_at >= created_at),
        CHECK(updated_at <= 1.7976931348623157e308),
        CHECK(
            status NOT IN ('applied', 'delivery_exhausted')
            OR delivery_count >= 1
        )
    )
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_insert_guard
    BEFORE INSERT ON agent_session_recovery_cases
    BEGIN
        SELECT CASE
            WHEN EXISTS (
                SELECT 1
                FROM agent_session_recovery_cases AS existing
                WHERE existing.case_id = NEW.case_id
                   OR (
                       existing.profile_id = NEW.profile_id
                       AND existing.session_id = NEW.session_id
                       AND existing.ownership_generation =
                           NEW.ownership_generation
                       AND existing.policy_version = NEW.policy_version
                       AND existing.work_graph_digest = NEW.work_graph_digest
                   )
            )
            THEN RAISE(
                ABORT,
                'recovery case identity already exists; select and validate it'
            )
        END;
        SELECT CASE
            WHEN NOT (
                NEW.status IN ('open', 'scanner_blocked')
                AND NEW.next_delivery_cycle = 0
                AND NEW.delivery_count = 0
                AND NEW.last_event_id = ''
                AND NEW.created_at = NEW.updated_at
                AND (
                    (NEW.status = 'open' AND NEW.last_error = '')
                    OR (
                        NEW.status = 'scanner_blocked'
                        AND typeof(NEW.last_error) = 'text'
                        AND length(
                            trim(
                                NEW.last_error,
                                char(9) || char(10) || char(11) ||
                                char(12) || char(13) || ' '
                            )
                        ) > 0
                    )
                )
            )
            THEN RAISE(
                ABORT,
                'recovery case initial state must start without delivery progress'
            )
        END;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_generation_insert
    BEFORE INSERT ON agent_session_recovery_cases
    WHEN NOT EXISTS (
        SELECT 1
        FROM agent_session_aggregates AS aggregate
        WHERE aggregate.profile_id = NEW.profile_id
          AND aggregate.session_id = NEW.session_id
          AND aggregate.ownership_generation = NEW.ownership_generation
    )
    BEGIN
        SELECT RAISE(
            ABORT,
            'recovery case ownership generation does not match aggregate'
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_generation_update
    BEFORE UPDATE OF profile_id, session_id, ownership_generation
    ON agent_session_recovery_cases
    WHEN (
        NEW.profile_id != OLD.profile_id
        OR NEW.session_id != OLD.session_id
        OR NEW.ownership_generation != OLD.ownership_generation
    )
    AND NOT EXISTS (
        SELECT 1
        FROM agent_session_aggregates AS aggregate
        WHERE aggregate.profile_id = NEW.profile_id
          AND aggregate.session_id = NEW.session_id
          AND aggregate.ownership_generation = NEW.ownership_generation
    )
    BEGIN
        SELECT RAISE(
            ABORT,
            'recovery case ownership generation does not match aggregate'
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_current_authority_update
    BEFORE UPDATE OF
        next_delivery_cycle, delivery_count,
        latest_certificate_digest, status
    ON agent_session_recovery_cases
    WHEN NOT EXISTS (
        SELECT 1
        FROM agent_session_aggregates AS aggregate
        WHERE aggregate.profile_id = NEW.profile_id
          AND aggregate.session_id = NEW.session_id
          AND aggregate.ownership_generation = NEW.ownership_generation
    )
    AND (
        NEW.delivery_count > OLD.delivery_count
        OR NEW.latest_certificate_digest != OLD.latest_certificate_digest
        OR (
            OLD.status = 'scanner_blocked'
            AND NEW.status = 'open'
        )
        OR (OLD.status != 'applied' AND NEW.status = 'applied')
    )
    BEGIN
        SELECT RAISE(
            ABORT,
            'recovery case no longer has current aggregate authority'
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_identity_immutable
    BEFORE UPDATE OF
        case_id, profile_id, session_id, ownership_generation,
        certificate_version, policy_version, work_graph_digest, created_at
    ON agent_session_recovery_cases
    WHEN NEW.case_id != OLD.case_id
      OR NEW.profile_id != OLD.profile_id
      OR NEW.session_id != OLD.session_id
      OR NEW.ownership_generation != OLD.ownership_generation
      OR NEW.certificate_version != OLD.certificate_version
      OR NEW.policy_version != OLD.policy_version
      OR NEW.work_graph_digest != OLD.work_graph_digest
      OR NEW.created_at != OLD.created_at
    BEGIN
        SELECT RAISE(ABORT, 'recovery case identity is immutable');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_time_monotonic
    BEFORE UPDATE ON agent_session_recovery_cases
    WHEN NEW.updated_at < OLD.updated_at
      OR (
          NEW.updated_at <= OLD.updated_at
          AND (
              NEW.latest_certificate_digest != OLD.latest_certificate_digest
              OR NEW.status != OLD.status
              OR NEW.next_delivery_cycle != OLD.next_delivery_cycle
              OR NEW.delivery_count != OLD.delivery_count
              OR NEW.last_event_id != OLD.last_event_id
              OR NEW.last_error != OLD.last_error
          )
      )
    BEGIN
        SELECT RAISE(
            ABORT,
            'recovery case semantic updates must advance updated_at'
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_progress_monotonic
    BEFORE UPDATE OF next_delivery_cycle, delivery_count
    ON agent_session_recovery_cases
    WHEN NEW.next_delivery_cycle < OLD.next_delivery_cycle
      OR NEW.delivery_count < OLD.delivery_count
      OR NEW.next_delivery_cycle > OLD.next_delivery_cycle + 1
      OR NEW.delivery_count > OLD.delivery_count + 1
    BEGIN
        SELECT RAISE(
            ABORT,
            'recovery case delivery progress must advance by at most one cycle'
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_progress_evidence
    BEFORE UPDATE OF
        next_delivery_cycle, delivery_count, last_event_id,
        latest_certificate_digest, status
    ON agent_session_recovery_cases
    WHEN NEW.delivery_count > 0
      AND NEW.delivery_count != (
          SELECT COUNT(*)
          FROM agent_session_mailbox AS mailbox
          WHERE mailbox.profile_id = NEW.profile_id
            AND mailbox.session_id = NEW.session_id
            AND mailbox.ownership_generation = NEW.ownership_generation
            AND mailbox.kind = 'RecoveryRequested'
            AND mailbox.source = 'durable_session_recovery_scanner'
            AND CASE
                WHEN typeof(mailbox.payload_json) = 'text'
                 AND json_valid(mailbox.payload_json)
                 AND json_type(mailbox.payload_json) = 'object'
                THEN
                    json_extract(
                        mailbox.payload_json,
                        '$.schema'
                    ) = 'shinbot.agent.session.recovery-delivery'
                    AND json_type(
                        mailbox.payload_json,
                        '$.version'
                    ) = 'integer'
                    AND json_extract(
                        mailbox.payload_json,
                        '$.version'
                    ) = 1
                    AND json_type(
                        mailbox.payload_json,
                        '$.delivery_cycle'
                    ) = 'integer'
                    AND json_extract(
                        mailbox.payload_json,
                        '$.delivery_cycle'
                    ) >= 0
                    AND json_extract(
                        mailbox.payload_json,
                        '$.delivery_cycle'
                    ) < NEW.delivery_count
                    AND json_extract(
                        mailbox.payload_json,
                        '$.case_id'
                    ) = NEW.case_id
                    AND json_type(
                        mailbox.payload_json,
                        '$.certificate'
                    ) = 'object'
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.schema'
                    ) = 'shinbot.agent.session.recovery-certificate'
                    AND json_type(
                        mailbox.payload_json,
                        '$.certificate.version'
                    ) = 'integer'
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.version'
                    ) = NEW.certificate_version
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.case_id'
                    ) = NEW.case_id
                    AND typeof(
                        json_extract(
                            mailbox.payload_json,
                            '$.certificate.certificate_digest'
                        )
                    ) = 'text'
                    AND length(
                        json_extract(
                            mailbox.payload_json,
                            '$.certificate.certificate_digest'
                        )
                    ) = 64
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.certificate_digest'
                    ) NOT GLOB '*[^0-9a-f]*'
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.work_graph_digest'
                    ) = NEW.work_graph_digest
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.policy_version'
                    ) = NEW.policy_version
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.subject.profile_id'
                    ) = NEW.profile_id
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.subject.session_id'
                    ) = NEW.session_id
                    AND json_extract(
                        mailbox.payload_json,
                        '$.certificate.subject.ownership_generation'
                    ) = NEW.ownership_generation
                    AND (
                        json_extract(
                            mailbox.payload_json,
                            '$.delivery_cycle'
                        ) != NEW.delivery_count - 1
                        OR json_extract(
                            mailbox.payload_json,
                            '$.certificate.certificate_digest'
                        ) = NEW.latest_certificate_digest
                    )
                    AND mailbox.event_id =
                        'recovery-requested:v1:'
                        || substr(NEW.case_id, 18)
                        || ':'
                        || CAST(
                            json_extract(
                                mailbox.payload_json,
                                '$.delivery_cycle'
                            ) AS TEXT
                        )
                ELSE 0
            END
      )
    BEGIN
        SELECT RAISE(
            ABORT,
            'recovery case delivery progress requires matching RecoveryRequested mailbox'
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_status_transition
    BEFORE UPDATE OF status ON agent_session_recovery_cases
    WHEN NOT (
        NEW.status = OLD.status
        OR OLD.status = 'open'
        OR (
            OLD.status = 'scanner_blocked'
            AND NEW.status IN (
                'open', 'applied', 'superseded', 'delivery_exhausted'
            )
        )
    )
    BEGIN
        SELECT RAISE(ABORT, 'invalid recovery case status transition');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_terminal_immutable
    BEFORE UPDATE ON agent_session_recovery_cases
    WHEN OLD.status IN ('applied', 'superseded', 'delivery_exhausted')
      AND (
          NEW.latest_certificate_digest != OLD.latest_certificate_digest
          OR NEW.status != OLD.status
          OR NEW.next_delivery_cycle != OLD.next_delivery_cycle
          OR NEW.delivery_count != OLD.delivery_count
          OR NEW.last_event_id != OLD.last_event_id
          OR NEW.last_error != OLD.last_error
          OR NEW.updated_at != OLD.updated_at
      )
    BEGIN
        SELECT RAISE(ABORT, 'terminal recovery case is immutable');
    END
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_recovery_cases_status
    ON agent_session_recovery_cases(status, updated_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_recovery_cases_session
    ON agent_session_recovery_cases(
        profile_id, session_id, ownership_generation, status
    )
    """,
    # -- Durable external-action receipts -------------------------------
    """
    CREATE TABLE IF NOT EXISTS agent_external_action_receipts (
        receipt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key TEXT NOT NULL UNIQUE,
        effect_id TEXT NOT NULL UNIQUE,
        operation_id TEXT NOT NULL,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        action_ordinal INTEGER NOT NULL,
        action_kind TEXT NOT NULL,
        contract_version INTEGER NOT NULL,
        request_digest TEXT NOT NULL,
        request_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'prepared',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        claim_id TEXT NOT NULL DEFAULT '',
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        platform_result_json TEXT NOT NULL DEFAULT '{}',
        rejection_json TEXT NOT NULL DEFAULT '{}',
        unknown_json TEXT NOT NULL DEFAULT '{}',
        assistant_message_log_id INTEGER,
        prepared_at REAL NOT NULL,
        execution_started_at REAL,
        settled_at REAL,
        updated_at REAL NOT NULL,
        FOREIGN KEY(assistant_message_log_id) REFERENCES message_logs(id),
        CHECK(length(idempotency_key) > 0),
        CHECK(length(effect_id) > 0),
        CHECK(length(operation_id) > 0),
        CHECK(length(profile_id) > 0),
        CHECK(length(session_id) > 0),
        CHECK(ownership_generation >= 1),
        CHECK(action_ordinal >= 0),
        CHECK(contract_version >= 1),
        CHECK(length(request_digest) = 64),
        CHECK(json_valid(request_json)),
        CHECK(json_valid(platform_result_json)),
        CHECK(json_valid(rejection_json)),
        CHECK(json_valid(unknown_json)),
        CHECK(action_kind IN ('send_reply', 'send_poke', 'send_reaction')),
        CHECK(
            status IN (
                'prepared', 'executing', 'succeeded',
                'rejected_before_dispatch', 'abandoned_before_dispatch',
                'unknown'
            )
        ),
        CHECK(attempt_count >= 0),
        CHECK(
            (
                status = 'prepared'
                AND attempt_count = 0
                AND claim_id = ''
                AND lease_owner = ''
                AND lease_until IS NULL
                AND execution_started_at IS NULL
                AND settled_at IS NULL
            )
            OR
            (
                status = 'abandoned_before_dispatch'
                AND lease_until IS NULL
                AND settled_at IS NOT NULL
                AND (
                    (
                        attempt_count = 0
                        AND claim_id = ''
                        AND lease_owner = ''
                        AND execution_started_at IS NULL
                    )
                    OR
                    (
                        attempt_count >= 1
                        AND claim_id != ''
                        AND lease_owner != ''
                        AND execution_started_at IS NOT NULL
                    )
                )
            )
            OR
            (
                status NOT IN ('prepared', 'abandoned_before_dispatch')
                AND attempt_count >= 1
                AND claim_id != ''
                AND lease_owner != ''
                AND execution_started_at IS NOT NULL
                AND (
                    (
                        status = 'executing'
                        AND lease_until IS NOT NULL
                        AND settled_at IS NULL
                    )
                    OR
                    (
                        status != 'executing'
                        AND lease_until IS NULL
                        AND settled_at IS NOT NULL
                    )
                )
            )
        ),
        CHECK(
            assistant_message_log_id IS NULL OR status = 'succeeded'
        ),
        CHECK(
            action_kind != 'send_reply'
            OR status != 'succeeded'
            OR assistant_message_log_id IS NOT NULL
        ),
        CHECK(
            action_kind = 'send_reply'
            OR assistant_message_log_id IS NULL
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_external_action_receipts_owner_status
    ON agent_external_action_receipts(
        profile_id, session_id, status, receipt_seq DESC
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_external_action_receipts_claim
    ON agent_external_action_receipts(claim_id)
    WHERE claim_id != ''
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_external_action_attempts (
        attempt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        idempotency_key TEXT NOT NULL,
        attempt_count INTEGER NOT NULL,
        claim_id TEXT NOT NULL UNIQUE,
        lease_owner TEXT NOT NULL,
        claimed_at REAL NOT NULL,
        lease_until REAL NOT NULL,
        status TEXT NOT NULL,
        platform_result_json TEXT NOT NULL DEFAULT '{}',
        rejection_json TEXT NOT NULL DEFAULT '{}',
        unknown_json TEXT NOT NULL DEFAULT '{}',
        assistant_message_log_id INTEGER,
        settled_at REAL,
        FOREIGN KEY(idempotency_key)
            REFERENCES agent_external_action_receipts(idempotency_key)
            ON DELETE CASCADE,
        FOREIGN KEY(assistant_message_log_id) REFERENCES message_logs(id),
        UNIQUE(idempotency_key, attempt_count),
        CHECK(length(idempotency_key) > 0),
        CHECK(attempt_count >= 1),
        CHECK(length(claim_id) > 0),
        CHECK(length(lease_owner) > 0),
        CHECK(
            status IN (
                'executing', 'succeeded',
                'rejected_before_dispatch', 'unknown'
            )
        ),
        CHECK(json_valid(platform_result_json)),
        CHECK(json_valid(rejection_json)),
        CHECK(json_valid(unknown_json)),
        CHECK(
            (status = 'executing' AND settled_at IS NULL)
            OR (status != 'executing' AND settled_at IS NOT NULL)
        ),
        CHECK(
            assistant_message_log_id IS NULL OR status = 'succeeded'
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_external_action_attempts_receipt
    ON agent_external_action_attempts(
        idempotency_key, attempt_count DESC
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_external_action_attempts_status
    ON agent_external_action_attempts(status, attempt_seq DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_runtime_service_health (
        profile_id TEXT NOT NULL,
        service_name TEXT NOT NULL,
        runtime_id TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'stopped',
        expected INTEGER NOT NULL DEFAULT 1,
        started_at REAL,
        heartbeat_at REAL,
        last_scan_started_at REAL,
        last_scan_finished_at REAL,
        last_success_at REAL,
        last_error_at REAL,
        last_error_code TEXT NOT NULL DEFAULT '',
        last_error_message TEXT NOT NULL DEFAULT '',
        consecutive_failures INTEGER NOT NULL DEFAULT 0,
        restart_count INTEGER NOT NULL DEFAULT 0,
        scan_count INTEGER NOT NULL DEFAULT 0,
        due_seen_count INTEGER NOT NULL DEFAULT 0,
        dispatch_count INTEGER NOT NULL DEFAULT 0,
        skip_count INTEGER NOT NULL DEFAULT 0,
        in_flight_count INTEGER NOT NULL DEFAULT 0,
        lease_owner TEXT NOT NULL DEFAULT '',
        updated_at REAL NOT NULL,
        PRIMARY KEY(profile_id, service_name),
        CHECK(expected IN (0, 1)),
        CHECK(consecutive_failures >= 0),
        CHECK(restart_count >= 0),
        CHECK(scan_count >= 0),
        CHECK(due_seen_count >= 0),
        CHECK(dispatch_count >= 0),
        CHECK(skip_count >= 0),
        CHECK(in_flight_count >= 0)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_runtime_service_health_status
    ON agent_runtime_service_health(status, heartbeat_at)
    """,
) + tuple(
    statement
    for _index_name, _table_name, statement in _ACTOR_RAW_LOGICAL_KEY_INDEXES
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
        "state_resume_json": "TEXT NOT NULL DEFAULT '{}'",
    }
    for col, spec in new_columns.items():
        if col not in columns:
            conn.execute(f"ALTER TABLE agent_scheduler_states ADD COLUMN {col} {spec}")


def _migrate_agent_unread_messages(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "agent_unread_messages")
    if not columns:
        return
    new_columns = {
        "response_profile": "TEXT NOT NULL DEFAULT ''",
        "is_mentioned": "INTEGER NOT NULL DEFAULT 0",
        "is_reply_to_bot": "INTEGER NOT NULL DEFAULT 0",
        "is_mention_to_other": "INTEGER NOT NULL DEFAULT 0",
        "is_poke_to_bot": "INTEGER NOT NULL DEFAULT 0",
        "is_poke_to_other": "INTEGER NOT NULL DEFAULT 0",
        "self_platform_id": "TEXT NOT NULL DEFAULT ''",
        "trace_id": "TEXT NOT NULL DEFAULT ''",
    }
    for col, spec in new_columns.items():
        if col not in columns:
            conn.execute(f"ALTER TABLE agent_unread_messages ADD COLUMN {col} {spec}")

    if "is_mentioned" not in columns:
        conn.execute(
            """
            UPDATE agent_unread_messages
            SET is_mentioned = COALESCE(
                (
                    SELECT m.is_mentioned
                    FROM message_logs AS m
                    WHERE m.id = agent_unread_messages.message_log_id
                ),
                0
            )
            """
        )


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


def _migrate_media_semantics_schema(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(
        conn,
        "media_assets",
        "strict_dhash",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "session_media_occurrences",
        "strict_dhash",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "media_semantics",
        "strict_dhash",
        "TEXT NOT NULL DEFAULT ''",
    )


def _migrate_durable_routing_schema(conn: sqlite3.Connection) -> None:
    """Add fail-closed columns introduced during durable routing rollout."""

    _add_column_if_missing(
        conn,
        "message_routing_jobs",
        "occurred_at",
        "REAL NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "message_routing_jobs",
        "profile_id",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "message_routing_jobs",
        "session_id",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "message_routing_jobs",
        "ownership_generation",
        "INTEGER NOT NULL DEFAULT 0",
    )
    conn.execute(
        """
        UPDATE message_routing_jobs
        SET profile_id = COALESCE(
                NULLIF(json_extract(payload_json, '$.bot_id'), ''),
                '__default_agent_profile__'
            ),
            session_id = COALESCE(
                NULLIF(json_extract(payload_json, '$.bot_session_id'), ''),
                COALESCE(
                    NULLIF(json_extract(payload_json, '$.bot_id'), ''),
                    '__default_agent_profile__'
                ) || ':' || json_extract(payload_json, '$.base_session_id')
            ),
            ownership_generation = CAST(
                json_extract(payload_json, '$.ownership_generation') AS INTEGER
            )
        WHERE profile_id = ''
          AND session_id = ''
          AND ownership_generation = 0
          AND json_valid(payload_json)
          AND json_type(payload_json, '$.base_session_id') = 'text'
          AND COALESCE(
                CAST(json_extract(payload_json, '$.ownership_generation') AS INTEGER),
                0
              ) >= 1
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_routing_jobs_session
        ON message_routing_jobs(
            profile_id, session_id, status, routing_job_seq
        )
        """
    )
    _add_column_if_missing(
        conn,
        "agent_route_outbox",
        "ownership_generation",
        "INTEGER NOT NULL DEFAULT 0",
    )


def _migrate_external_action_receipts_schema(conn: sqlite3.Connection) -> None:
    """Migrate durable receipt ordering and terminal lifecycle constraints."""

    columns = _table_columns(conn, "agent_external_action_receipts")
    if not columns:
        return
    action_ordinal_missing = "action_ordinal" not in columns
    if action_ordinal_missing:
        _add_column_if_missing(
            conn,
            "agent_external_action_receipts",
            "action_ordinal",
            "INTEGER NOT NULL DEFAULT 0 CHECK(action_ordinal >= 0)",
        )
        conn.execute(
            """
            UPDATE agent_external_action_receipts
            SET action_ordinal = CASE
                WHEN json_valid(request_json) THEN CASE
                    WHEN json_type(request_json, '$.action_ordinal') = 'integer'
                     AND CAST(json_extract(request_json, '$.action_ordinal') AS INTEGER) >= 0
                    THEN CAST(json_extract(request_json, '$.action_ordinal') AS INTEGER)
                    ELSE action_ordinal
                END
                ELSE action_ordinal
            END
            """
        )
    _rebuild_external_action_receipt_constraints(conn)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_external_action_receipts_operation_ordinal
        ON agent_external_action_receipts(
            profile_id, session_id, ownership_generation, operation_id, action_ordinal
        )
        """
    )


def _rebuild_external_action_receipt_constraints(conn: sqlite3.Connection) -> None:
    """Rebuild the receipt/attempt pair when SQLite cannot ALTER its CHECKs.

    ``agent_external_action_attempts`` references receipt idempotency keys, so
    the two tables must move together. Rebuilding only the parent would leave
    the child foreign key attached to the renamed legacy table.
    """

    row = conn.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type = 'table' AND name = 'agent_external_action_receipts'
        """
    ).fetchone()
    if row is None:
        return
    normalized_sql = " ".join(str(row["sql"] or "").lower().split())
    required_fragments = (
        "action_ordinal integer not null",
        "'abandoned_before_dispatch'",
        "status = 'abandoned_before_dispatch'",
    )
    order_index_rows = conn.execute(
        "PRAGMA index_list('agent_external_action_receipts')"
    ).fetchall()
    order_index_is_unique = any(
        str(index["name"]) == "idx_external_action_receipts_operation_ordinal"
        and int(index["unique"]) == 1
        for index in order_index_rows
    )
    order_index_is_absent = not any(
        str(index["name"]) == "idx_external_action_receipts_operation_ordinal"
        for index in order_index_rows
    )
    if all(fragment in normalized_sql for fragment in required_fragments) and (
        order_index_is_unique or order_index_is_absent
    ):
        return

    for index_name in (
        "idx_external_action_receipts_owner_status",
        "idx_external_action_receipts_claim",
        "idx_external_action_receipts_operation_ordinal",
        "idx_external_action_attempts_receipt",
        "idx_external_action_attempts_status",
    ):
        conn.execute(f"DROP INDEX IF EXISTS {index_name}")
    conn.execute(
        "ALTER TABLE agent_external_action_attempts "
        "RENAME TO agent_external_action_attempts_legacy"
    )
    conn.execute(
        "ALTER TABLE agent_external_action_receipts "
        "RENAME TO agent_external_action_receipts_legacy"
    )
    _create_external_action_receipts_table(conn)
    _create_external_action_attempts_table(conn)
    conn.execute(
        """
        INSERT INTO agent_external_action_receipts (
            receipt_seq, idempotency_key, effect_id, operation_id, profile_id,
            session_id, ownership_generation, action_ordinal, action_kind,
            contract_version, request_digest, request_json, status,
            attempt_count, claim_id, lease_owner, lease_until,
            platform_result_json, rejection_json, unknown_json,
            assistant_message_log_id, prepared_at, execution_started_at,
            settled_at, updated_at
        )
        SELECT
            receipt_seq, idempotency_key, effect_id, operation_id, profile_id,
            session_id, ownership_generation, action_ordinal, action_kind,
            contract_version, request_digest, request_json, status,
            attempt_count, claim_id, lease_owner, lease_until,
            platform_result_json, rejection_json, unknown_json,
            assistant_message_log_id, prepared_at, execution_started_at,
            settled_at, updated_at
        FROM agent_external_action_receipts_legacy
        """
    )
    conn.execute(
        """
        INSERT INTO agent_external_action_attempts (
            attempt_seq, idempotency_key, attempt_count, claim_id, lease_owner,
            claimed_at, lease_until, status, platform_result_json,
            rejection_json, unknown_json, assistant_message_log_id, settled_at
        )
        SELECT
            attempt_seq, idempotency_key, attempt_count, claim_id, lease_owner,
            claimed_at, lease_until, status, platform_result_json,
            rejection_json, unknown_json, assistant_message_log_id, settled_at
        FROM agent_external_action_attempts_legacy
        """
    )
    conn.execute("DROP TABLE agent_external_action_attempts_legacy")
    conn.execute("DROP TABLE agent_external_action_receipts_legacy")
    _create_external_action_receipt_indexes(conn)


def _create_external_action_receipts_table(conn: sqlite3.Connection) -> None:
    """Create the canonical durable external-action receipt table."""

    conn.execute(
        """
        CREATE TABLE agent_external_action_receipts (
            receipt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
            idempotency_key TEXT NOT NULL UNIQUE,
            effect_id TEXT NOT NULL UNIQUE,
            operation_id TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            ownership_generation INTEGER NOT NULL,
            action_ordinal INTEGER NOT NULL,
            action_kind TEXT NOT NULL,
            contract_version INTEGER NOT NULL,
            request_digest TEXT NOT NULL,
            request_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'prepared',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            claim_id TEXT NOT NULL DEFAULT '',
            lease_owner TEXT NOT NULL DEFAULT '',
            lease_until REAL,
            platform_result_json TEXT NOT NULL DEFAULT '{}',
            rejection_json TEXT NOT NULL DEFAULT '{}',
            unknown_json TEXT NOT NULL DEFAULT '{}',
            assistant_message_log_id INTEGER,
            prepared_at REAL NOT NULL,
            execution_started_at REAL,
            settled_at REAL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(assistant_message_log_id) REFERENCES message_logs(id),
            CHECK(length(idempotency_key) > 0),
            CHECK(length(effect_id) > 0),
            CHECK(length(operation_id) > 0),
            CHECK(length(profile_id) > 0),
            CHECK(length(session_id) > 0),
            CHECK(ownership_generation >= 1),
            CHECK(action_ordinal >= 0),
            CHECK(contract_version >= 1),
            CHECK(length(request_digest) = 64),
            CHECK(json_valid(request_json)),
            CHECK(json_valid(platform_result_json)),
            CHECK(json_valid(rejection_json)),
            CHECK(json_valid(unknown_json)),
            CHECK(action_kind IN ('send_reply', 'send_poke', 'send_reaction')),
            CHECK(
                status IN (
                    'prepared', 'executing', 'succeeded',
                    'rejected_before_dispatch', 'abandoned_before_dispatch',
                    'unknown'
                )
            ),
            CHECK(attempt_count >= 0),
            CHECK(
                (
                    status = 'prepared'
                    AND attempt_count = 0
                    AND claim_id = ''
                    AND lease_owner = ''
                    AND lease_until IS NULL
                    AND execution_started_at IS NULL
                    AND settled_at IS NULL
                )
                OR
                (
                    status = 'abandoned_before_dispatch'
                    AND lease_until IS NULL
                    AND settled_at IS NOT NULL
                    AND (
                        (
                            attempt_count = 0
                            AND claim_id = ''
                            AND lease_owner = ''
                            AND execution_started_at IS NULL
                        )
                        OR
                        (
                            attempt_count >= 1
                            AND claim_id != ''
                            AND lease_owner != ''
                            AND execution_started_at IS NOT NULL
                        )
                    )
                )
                OR
                (
                    status NOT IN ('prepared', 'abandoned_before_dispatch')
                    AND attempt_count >= 1
                    AND claim_id != ''
                    AND lease_owner != ''
                    AND execution_started_at IS NOT NULL
                    AND (
                        (
                            status = 'executing'
                            AND lease_until IS NOT NULL
                            AND settled_at IS NULL
                        )
                        OR
                        (
                            status != 'executing'
                            AND lease_until IS NULL
                            AND settled_at IS NOT NULL
                        )
                    )
                )
            ),
            CHECK(assistant_message_log_id IS NULL OR status = 'succeeded'),
            CHECK(
                action_kind != 'send_reply'
                OR status != 'succeeded'
                OR assistant_message_log_id IS NOT NULL
            ),
            CHECK(action_kind = 'send_reply' OR assistant_message_log_id IS NULL)
        )
        """
    )


def _create_external_action_attempts_table(conn: sqlite3.Connection) -> None:
    """Create the receipt attempt journal with its canonical foreign key."""

    conn.execute(
        """
        CREATE TABLE agent_external_action_attempts (
            attempt_seq INTEGER PRIMARY KEY AUTOINCREMENT,
            idempotency_key TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            claim_id TEXT NOT NULL UNIQUE,
            lease_owner TEXT NOT NULL,
            claimed_at REAL NOT NULL,
            lease_until REAL NOT NULL,
            status TEXT NOT NULL,
            platform_result_json TEXT NOT NULL DEFAULT '{}',
            rejection_json TEXT NOT NULL DEFAULT '{}',
            unknown_json TEXT NOT NULL DEFAULT '{}',
            assistant_message_log_id INTEGER,
            settled_at REAL,
            FOREIGN KEY(idempotency_key)
                REFERENCES agent_external_action_receipts(idempotency_key)
                ON DELETE CASCADE,
            FOREIGN KEY(assistant_message_log_id) REFERENCES message_logs(id),
            UNIQUE(idempotency_key, attempt_count),
            CHECK(length(idempotency_key) > 0),
            CHECK(attempt_count >= 1),
            CHECK(length(claim_id) > 0),
            CHECK(length(lease_owner) > 0),
            CHECK(
                status IN (
                    'executing', 'succeeded',
                    'rejected_before_dispatch', 'unknown'
                )
            ),
            CHECK(json_valid(platform_result_json)),
            CHECK(json_valid(rejection_json)),
            CHECK(json_valid(unknown_json)),
            CHECK(
                (status = 'executing' AND settled_at IS NULL)
                OR (status != 'executing' AND settled_at IS NOT NULL)
            ),
            CHECK(assistant_message_log_id IS NULL OR status = 'succeeded')
        )
        """
    )


def _create_external_action_receipt_indexes(conn: sqlite3.Connection) -> None:
    """Create indexes shared by fresh and rebuilt external-action tables."""

    conn.execute(
        """
        CREATE INDEX idx_external_action_receipts_owner_status
        ON agent_external_action_receipts(
            profile_id, session_id, status, receipt_seq DESC
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX idx_external_action_receipts_claim
        ON agent_external_action_receipts(claim_id)
        WHERE claim_id != ''
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX idx_external_action_receipts_operation_ordinal
        ON agent_external_action_receipts(
            profile_id, session_id, ownership_generation, operation_id, action_ordinal
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_external_action_attempts_receipt
        ON agent_external_action_attempts(idempotency_key, attempt_count DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_external_action_attempts_status
        ON agent_external_action_attempts(status, attempt_seq DESC)
        """
    )


def _ensure_actor_raw_logical_key_indexes(conn: sqlite3.Connection) -> None:
    """Ensure raw logical-key expression indexes retain their exact contract.

    ``CREATE INDEX IF NOT EXISTS`` only checks an index name. A same-name
    ordinary TEXT index would otherwise silently survive startup and turn the
    storage-aware ReviewDue preflight back into a table scan.
    """

    for index_name, expected_table, create_statement in (
        _ACTOR_RAW_LOGICAL_KEY_INDEXES
    ):
        row = conn.execute(
            """
            SELECT type, tbl_name, sql
            FROM sqlite_master
            WHERE name = ?
            """,
            (index_name,),
        ).fetchone()
        if row is None:
            conn.execute(create_statement)
            continue
        if str(row["type"]) != "index":
            raise sqlite3.IntegrityError(
                "actor raw logical-key index name is occupied by another "
                f"schema object: {index_name}"
            )
        if str(row["tbl_name"]) != expected_table:
            raise sqlite3.IntegrityError(
                "actor raw logical-key index belongs to an unexpected table: "
                f"{index_name}"
            )
        if row["sql"] is None:
            raise sqlite3.IntegrityError(
                "actor raw logical-key index has no mutable SQL definition: "
                f"{index_name}"
            )
        if _normalized_create_index_sql(row["sql"]) == _normalized_create_index_sql(
            create_statement
        ):
            continue
        escaped_index_name = index_name.replace('"', '""')
        conn.execute(f'DROP INDEX "{escaped_index_name}"')
        conn.execute(create_statement)


def _migrate_session_actor_schema(conn: sqlite3.Connection) -> None:
    """Add durable contract and ownership fences to actor foundation tables."""

    review_schedule_columns = _table_columns(conn, "agent_review_schedules")
    if review_schedule_columns and "delivery_cycle" not in review_schedule_columns:
        conn.execute(
            """
            ALTER TABLE agent_review_schedules
            ADD COLUMN delivery_cycle INTEGER NOT NULL DEFAULT 0
                CHECK (delivery_cycle >= 0)
            """
        )
        conn.execute(
            """
            UPDATE agent_review_schedules AS schedule
            SET delivery_cycle = CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM agent_session_mailbox AS mailbox
                    WHERE mailbox.profile_id = schedule.profile_id
                      AND mailbox.session_id = schedule.session_id
                      AND mailbox.ownership_generation =
                          schedule.ownership_generation
                      AND mailbox.kind = 'ReviewDue'
                      AND mailbox.source = 'durable_review_due_scanner'
                      AND mailbox.causation_id = schedule.plan_id
                )
                THEN 1
                ELSE 0
            END
            """
        )

    _add_column_if_missing(
        conn,
        "agent_session_operations",
        "input_ledger_sequence",
        "INTEGER CHECK (input_ledger_sequence IS NULL OR input_ledger_sequence >= 0)",
    )
    _add_column_if_missing(
        conn,
        "agent_message_ledger_consumptions",
        "input_ledger_sequence",
        "INTEGER NOT NULL DEFAULT 0 CHECK (input_ledger_sequence >= 0)",
    )
    conn.execute(
        """
        UPDATE agent_session_operations AS operation
        SET input_ledger_sequence = COALESCE(
            (
                SELECT MAX(ledger.ledger_sequence)
                FROM agent_message_ledger AS ledger
                WHERE ledger.profile_id = operation.profile_id
                  AND ledger.session_id = operation.session_id
                  AND ledger.message_log_id <= operation.input_watermark
                  AND ledger.recorded_at <= operation.started_at
            ),
            0
        )
        WHERE input_watermark IS NOT NULL
          AND input_ledger_sequence IS NULL
        """
    )
    invalid_input_fence = conn.execute(
        """
        SELECT operation_id
        FROM agent_session_operations
        WHERE (input_watermark IS NULL) != (input_ledger_sequence IS NULL)
           OR (input_watermark IS NOT NULL AND input_watermark < 0)
           OR (
               input_ledger_sequence IS NOT NULL
               AND input_ledger_sequence < 0
           )
        LIMIT 1
        """
    ).fetchone()
    if invalid_input_fence is not None:
        raise sqlite3.IntegrityError(
            "invalid operation input fence: "
            f"{invalid_input_fence['operation_id']}"
        )
    _ensure_agent_operation_input_fence_triggers(conn)
    _add_column_if_missing(
        conn,
        "agent_effect_outbox",
        "contract_version",
        "INTEGER NOT NULL DEFAULT 1",
    )
    _add_column_if_missing(
        conn,
        "agent_effect_outbox",
        "contract_signature",
        "TEXT NOT NULL DEFAULT 'legacy-unsigned-v1'",
    )
    actor_tables = (
        "agent_session_aggregates",
        "agent_session_mailbox",
        "agent_session_operations",
        "agent_review_schedules",
        "agent_state_transitions",
        "agent_review_schedule_events",
        "agent_effect_outbox",
    )
    added_generation_to: list[str] = []
    for table_name in actor_tables:
        columns = _table_columns(conn, table_name)
        if columns and "ownership_generation" not in columns:
            _add_column_if_missing(
                conn,
                table_name,
                "ownership_generation",
                "INTEGER NOT NULL DEFAULT 0",
            )
            added_generation_to.append(table_name)
    for table_name in added_generation_to:
        conn.execute(
            f"""
            UPDATE {table_name}
            SET ownership_generation = COALESCE(
                (
                    SELECT ownership.generation
                    FROM agent_session_runtime_ownership AS ownership
                    WHERE ownership.profile_id = {table_name}.profile_id
                      AND ownership.session_id = {table_name}.session_id
                      AND ownership.mode = 'actor_v2'
                      AND ownership.status = 'active'
                ),
                0
            )
            """
        )
    _rebuild_agent_effect_outbox_constraints(conn)


def _ensure_agent_operation_input_fence_triggers(
    conn: sqlite3.Connection,
) -> None:
    """Enforce paired operation input fences on ALTER-migrated databases."""

    mismatch = """
        (NEW.input_watermark IS NULL) !=
            (NEW.input_ledger_sequence IS NULL)
        OR (NEW.input_watermark IS NOT NULL AND NEW.input_watermark < 0)
        OR (
            NEW.input_ledger_sequence IS NOT NULL
            AND NEW.input_ledger_sequence < 0
        )
    """
    for name, timing in (
        ("trg_agent_operation_input_fence_insert", "BEFORE INSERT"),
        (
            "trg_agent_operation_input_fence_update",
            "BEFORE UPDATE OF input_watermark, input_ledger_sequence",
        ),
    ):
        conn.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {name}
            {timing} ON agent_session_operations
            WHEN {mismatch}
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'operation input watermark and ledger sequence must be paired'
                );
            END
            """
        )


def _rebuild_agent_effect_outbox_constraints(conn: sqlite3.Connection) -> None:
    """Rebuild legacy effect outboxes so ALTER-added columns gain constraints."""

    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_effect_outbox" in statement
    )
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'agent_effect_outbox'
        """
    ).fetchone()
    if row is None:
        return
    if _normalized_create_table_sql(row["sql"]) == _normalized_create_table_sql(
        table_statement
    ):
        return

    conn.execute("SAVEPOINT rebuild_agent_effect_outbox")
    try:
        conn.execute(
            "ALTER TABLE agent_effect_outbox RENAME TO agent_effect_outbox_legacy"
        )
        conn.execute(table_statement)
        migration_now = time.time()
        effect_projection = bounded_raw_sqlite_projection(
            "effect",
            _EFFECT_OUTBOX_COLUMNS,
            byte_limits=_EFFECT_OUTBOX_RAW_BYTE_LIMITS,
        )
        trace_projection = bounded_raw_sqlite_projection(
            "source",
            ("trace_id",),
            byte_limits={"trace_id": _EFFECT_OUTBOX_METADATA_FIELD_BYTE_LIMIT},
            output_prefix="source_",
        )
        last_legacy_rowid = 0
        while True:
            legacy_row = conn.execute(
                f"""
                SELECT effect.rowid AS legacy_rowid,
                       {effect_projection}, {trace_projection}
                FROM agent_effect_outbox_legacy AS effect
                LEFT JOIN agent_session_mailbox AS source
                  ON source.profile_id = effect.profile_id
                 AND source.session_id = effect.session_id
                 AND source.event_id = effect.event_id
                WHERE effect.rowid > ?
                ORDER BY effect.rowid
                LIMIT 1
                """,
                (last_legacy_rowid,),
            ).fetchone()
            if legacy_row is None:
                break
            last_legacy_rowid = int(legacy_row["legacy_rowid"])
            raw_values = raw_sqlite_values(
                legacy_row,
                _EFFECT_OUTBOX_COLUMNS,
            )
            raw_values["source_trace_id"] = raw_sqlite_values(
                legacy_row,
                ("trace_id",),
                output_prefix="source_",
            )["trace_id"]
            _complete_legacy_effect_raw_values(
                conn,
                raw_values,
                legacy_rowid=last_legacy_rowid,
            )
            decoded, decoding_violations = decode_raw_sqlite_values(raw_values)
            violations = (
                *decoding_violations,
                *_effect_outbox_row_violations(decoded),
            )
            if violations:
                _migrate_malformed_effect_outbox_row(
                    conn,
                    raw_values,
                    decoded,
                    violations=violations,
                    now=migration_now,
                )
            else:
                _insert_effect_outbox_row(conn, decoded)
        conn.execute("DROP TABLE agent_effect_outbox_legacy")
        conn.execute(
            """
            CREATE INDEX idx_agent_effect_outbox_pending
            ON agent_effect_outbox(status, available_at, effect_seq)
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_agent_effect_outbox_session
            ON agent_effect_outbox(profile_id, session_id, status, effect_seq)
            """
        )
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT rebuild_agent_effect_outbox")
        conn.execute("RELEASE SAVEPOINT rebuild_agent_effect_outbox")
        raise
    conn.execute("RELEASE SAVEPOINT rebuild_agent_effect_outbox")


_EFFECT_OUTBOX_COLUMNS = (
    "effect_seq",
    "effect_id",
    "idempotency_key",
    "profile_id",
    "session_id",
    "ownership_generation",
    "event_id",
    "operation_id",
    "kind",
    "contract_version",
    "contract_signature",
    "payload_json",
    "status",
    "attempt_count",
    "available_at",
    "claim_id",
    "lease_owner",
    "lease_until",
    "created_at",
    "updated_at",
    "completed_at",
    "last_error",
)
_EFFECT_OUTBOX_METADATA_FIELD_BYTE_LIMIT = 65_536
_EFFECT_OUTBOX_RAW_BYTE_LIMITS = {
    field_name: (
        MAX_CANONICAL_JSON_BYTES
        if field_name == "payload_json"
        else _EFFECT_OUTBOX_METADATA_FIELD_BYTE_LIMIT
    )
    for field_name in _EFFECT_OUTBOX_COLUMNS
}


def _complete_legacy_effect_raw_values(
    conn: sqlite3.Connection,
    raw_values: dict[str, RawSQLiteValue],
    *,
    legacy_rowid: int,
) -> None:
    """Finish oversized legacy evidence with a fixed-memory chunked digest."""

    for field_name, raw_value in tuple(raw_values.items()):
        if not raw_value.projection_truncated:
            continue
        expression = (
            "source.trace_id"
            if field_name == "source_trace_id"
            else f"effect.{field_name}"
        )
        raw_values[field_name] = complete_truncated_raw_sqlite_value(
            raw_value,
            chunk_reader=lambda offset, length, expression=expression: (
                _read_legacy_effect_raw_chunk(
                    conn,
                    legacy_rowid=legacy_rowid,
                    expression=expression,
                    offset=offset,
                    length=length,
                )
            ),
        )


def _read_legacy_effect_raw_chunk(
    conn: sqlite3.Connection,
    *,
    legacy_rowid: int,
    expression: str,
    offset: int,
    length: int,
) -> object:
    row = conn.execute(
        f"""
        SELECT substr(CAST({expression} AS BLOB), ?, ?) AS raw_chunk
        FROM agent_effect_outbox_legacy AS effect
        LEFT JOIN agent_session_mailbox AS source
          ON source.profile_id = effect.profile_id
         AND source.session_id = effect.session_id
         AND source.event_id = effect.event_id
        WHERE effect.rowid = ?
        """,
        (offset, length, legacy_rowid),
    ).fetchone()
    return None if row is None else row["raw_chunk"]


def _effect_outbox_row_violations(row: dict[str, object]) -> tuple[str, ...]:
    """Return every strict outbox contract violated by one legacy row."""

    violations: list[str] = []
    for field_name in (
        "effect_id",
        "idempotency_key",
        "profile_id",
        "session_id",
        "event_id",
        "kind",
        "contract_signature",
    ):
        if not _is_canonical_nonempty_text(row[field_name]):
            violations.append(f"{field_name}_invalid")
    for field_name in ("operation_id", "claim_id", "lease_owner"):
        if not _is_canonical_text(row[field_name]):
            violations.append(f"{field_name}_invalid")
    if not isinstance(row["last_error"], str):
        violations.append("last_error_not_text")
    if str(row["status"]) not in {
        "pending",
        "processing",
        "completed",
        "failed",
    } or not isinstance(row["status"], str):
        violations.append("status_invalid")
    for field_name, minimum in (
        ("effect_seq", 1),
        ("ownership_generation", 0),
        ("contract_version", 1),
        ("attempt_count", 0),
    ):
        if not _is_integer_at_least(row[field_name], minimum):
            violations.append(f"{field_name}_not_integer")
    if (
        row["status"] in {"pending", "processing"}
        and _is_integer_at_least(row["attempt_count"], _SQLITE_INT64_MAX)
    ):
        violations.append("attempt_count_not_claimable")
    for field_name in ("available_at", "created_at", "updated_at"):
        if not _is_nonnegative_finite_number(row[field_name]):
            violations.append(f"{field_name}_invalid")
    for field_name in ("lease_until", "completed_at"):
        value = row[field_name]
        if value is not None and not _is_nonnegative_finite_number(value):
            violations.append(f"{field_name}_invalid")
    trace_id = row["source_trace_id"]
    if trace_id is not None and not isinstance(trace_id, str):
        violations.append("source_trace_id_invalid")

    payload_json = row["payload_json"]
    if not isinstance(payload_json, str):
        violations.append("payload_json_not_text")
    else:
        violations.extend(validate_canonical_json_object(payload_json).violations)
    return tuple(violations)


def _insert_effect_outbox_row(
    conn: sqlite3.Connection,
    row: dict[str, object],
) -> None:
    placeholders = ", ".join("?" for _column in _EFFECT_OUTBOX_COLUMNS)
    columns = ", ".join(_EFFECT_OUTBOX_COLUMNS)
    conn.execute(
        f"INSERT INTO agent_effect_outbox ({columns}) VALUES ({placeholders})",
        tuple(row[column] for column in _EFFECT_OUTBOX_COLUMNS),
    )


def _normalized_create_table_sql(value: object) -> str:
    normalized = _collapse_sql_whitespace(str(value or "").strip().rstrip(";"))
    optional_prefix = "CREATE TABLE IF NOT EXISTS "
    required_prefix = "CREATE TABLE "
    if normalized[: len(optional_prefix)].upper() == optional_prefix:
        return required_prefix + normalized[len(optional_prefix) :]
    if normalized[: len(required_prefix)].upper() == required_prefix:
        return required_prefix + normalized[len(required_prefix) :]
    return normalized


def _normalized_create_index_sql(value: object) -> str:
    """Normalize equivalent SQLite index DDL for startup schema auditing."""

    normalized = _collapse_sql_whitespace(str(value or "").strip().rstrip(";"))
    normalized = normalized.upper()
    optional_prefix = "CREATE INDEX IF NOT EXISTS "
    required_prefix = "CREATE INDEX "
    if normalized.startswith(optional_prefix):
        return required_prefix + normalized[len(optional_prefix) :]
    return normalized


def _collapse_sql_whitespace(value: str) -> str:
    collapsed: list[str] = []
    quote_end = ""
    pending_space = False
    index = 0
    while index < len(value):
        character = value[index]
        if quote_end:
            collapsed.append(character)
            if character == quote_end:
                if index + 1 < len(value) and value[index + 1] == quote_end:
                    index += 1
                    collapsed.append(value[index])
                else:
                    quote_end = ""
            index += 1
            continue
        if character.isspace():
            pending_space = True
            index += 1
            continue
        if pending_space and collapsed:
            collapsed.append(" ")
        pending_space = False
        collapsed.append(character)
        if character in {"'", '"', "`"}:
            quote_end = character
        elif character == "[":
            quote_end = "]"
        index += 1
    return "".join(collapsed)


def _migrate_malformed_effect_outbox_row(
    conn: sqlite3.Connection,
    raw_values: dict[str, RawSQLiteValue],
    row: dict[str, object],
    *,
    violations: tuple[str, ...],
    now: float,
) -> None:
    profile_id = row["profile_id"]
    session_id = row["session_id"]
    if not _is_canonical_nonempty_text(profile_id) or not (
        _is_canonical_nonempty_text(session_id)
    ):
        raise sqlite3.IntegrityError(
            "malformed effect row cannot be assigned to a canonical session"
        )
    ownership = conn.execute(
        """
        SELECT ownership.generation
        FROM agent_session_runtime_ownership AS ownership
        JOIN agent_session_aggregates AS aggregate
          ON aggregate.profile_id = ownership.profile_id
         AND aggregate.session_id = ownership.session_id
         AND aggregate.ownership_generation = ownership.generation
        WHERE ownership.profile_id = ?
          AND ownership.session_id = ?
          AND ownership.mode = 'actor_v2'
          AND ownership.status = 'active'
        """,
        (profile_id, session_id),
    ).fetchone()
    if ownership is None or not _is_integer_at_least(ownership["generation"], 1):
        raise sqlite3.IntegrityError(
            "malformed effect row has no active actor ownership"
        )

    evidence = {
        column: raw_values[column].evidence()
        for column in (*_EFFECT_OUTBOX_COLUMNS, "source_trace_id")
    }
    evidence_json = json.dumps(
        evidence,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    evidence_digest = hashlib.sha256(evidence_json.encode("ascii")).hexdigest()
    effect_seq = int(row["effect_seq"])
    normalized_effect_id = (
        str(row["effect_id"])
        if _is_canonical_nonempty_text(row["effect_id"])
        else f"malformed-effect:{effect_seq}:{evidence_digest[:16]}"
    )
    normalized_idempotency_key = (
        str(row["idempotency_key"])
        if _is_canonical_nonempty_text(row["idempotency_key"])
        else f"malformed-idempotency:{effect_seq}:{evidence_digest[:16]}"
    )
    normalized_source_event_id = (
        str(row["event_id"])
        if _is_canonical_nonempty_text(row["event_id"])
        else f"malformed-source:{effect_seq}:{evidence_digest[:16]}"
    )
    normalized_operation_id = (
        str(row["operation_id"])
        if _is_canonical_text(row["operation_id"])
        else ""
    )
    identity = "\x1f".join(
        (profile_id, session_id, normalized_effect_id, "quarantined")
    )
    quarantine_event_id = (
        "effect-quarantined:"
        + hashlib.sha256(identity.encode("utf-8")).hexdigest()
    )
    failure_message = "durable effect row failed validation: " + ", ".join(
        violations
    )
    sentinel_payload_json = json.dumps(
        {
            "quarantine_event_id": quarantine_event_id,
            "quarantined": True,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    normalized_attempt_count = (
        int(row["attempt_count"])
        if _is_integer_at_least(row["attempt_count"], 0)
        else 0
    )
    normalized_created_at = (
        float(row["created_at"])
        if _is_nonnegative_finite_number(row["created_at"])
        else now
    )
    conn.execute(
        """
        INSERT INTO agent_effect_outbox (
            effect_seq, effect_id, idempotency_key, profile_id, session_id,
            ownership_generation, event_id, operation_id, kind,
            contract_version, contract_signature, payload_json, status,
            attempt_count, available_at, claim_id, lease_owner, lease_until,
            created_at, updated_at, completed_at, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '__malformed_persisted_effect__',
                  1, 'schema-quarantine-v1', ?, 'failed', ?, ?, '', '', NULL,
                  ?, ?, ?, ?)
        """,
        (
            effect_seq,
            normalized_effect_id,
            normalized_idempotency_key,
            profile_id,
            session_id,
            int(ownership["generation"]),
            normalized_source_event_id,
            normalized_operation_id,
            sentinel_payload_json,
            normalized_attempt_count,
            now,
            normalized_created_at,
            now,
            now,
            f"malformed_effect_row: {failure_message}",
        ),
    )
    trace_id = (
        str(row["source_trace_id"])
        if isinstance(row["source_trace_id"], str)
        else ""
    )
    diagnostic_payload_json = json.dumps(
        {
            "attempt_count": normalized_attempt_count,
            "contract_signature": "schema-quarantine-v1",
            "contract_version": 1,
            "effect_id": normalized_effect_id,
            "effect_kind": "__malformed_persisted_effect__",
            "failure_code": "malformed_effect_row",
            "failure_message": failure_message,
            "idempotency_key": normalized_idempotency_key,
            "operation_id": normalized_operation_id,
            "raw_row": evidence,
            "reason_code": "malformed_effect_row",
            "reason_message": failure_message,
            "violations": list(violations),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO agent_session_mailbox (
            event_id, profile_id, session_id, ownership_generation,
            kind, source, occurred_at, payload_json,
            causation_id, correlation_id, trace_id,
            status, attempt_count, available_at,
            claim_id, lease_owner, lease_until,
            created_at, handled_at, last_error
        ) VALUES (?, ?, ?, ?, 'EffectQuarantined', 'effect_store', ?, ?,
                  ?, ?, ?, 'pending', 0, ?, '', '', NULL, ?, NULL, '')
        """,
        (
            quarantine_event_id,
            profile_id,
            session_id,
            int(ownership["generation"]),
            now,
            diagnostic_payload_json,
            normalized_source_event_id,
            normalized_operation_id or normalized_effect_id,
            trace_id,
            now,
            now,
        ),
    )
    if inserted.rowcount == 1:
        return
    existing = conn.execute(
        """
        SELECT kind, source, ownership_generation, payload_json,
               causation_id, correlation_id, trace_id
        FROM agent_session_mailbox
        WHERE profile_id = ? AND session_id = ? AND event_id = ?
        """,
        (profile_id, session_id, quarantine_event_id),
    ).fetchone()
    expected = (
        "EffectQuarantined",
        "effect_store",
        int(ownership["generation"]),
        diagnostic_payload_json,
        normalized_source_event_id,
        normalized_operation_id or normalized_effect_id,
        trace_id,
    )
    if existing is None or tuple(existing) != expected:
        raise sqlite3.IntegrityError(
            "malformed effect quarantine event changed diagnostic identity"
        )


def _is_canonical_nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value == value.strip()


def _is_canonical_text(value: object) -> bool:
    return isinstance(value, str) and value == value.strip()


def _is_integer_at_least(value: object, minimum: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _is_nonnegative_finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0
    )


def _sqlite_value_evidence(value: object) -> dict[str, str | None]:
    if value is None:
        return {"encoding": "null", "storage_class": "null", "value": None}
    if isinstance(value, bytes):
        return {"encoding": "hex", "storage_class": "blob", "value": value.hex()}
    if isinstance(value, str):
        return {"encoding": "text", "storage_class": "text", "value": value}
    if isinstance(value, int):
        return {
            "encoding": "decimal",
            "storage_class": "integer",
            "value": str(value),
        }
    if isinstance(value, float):
        return {
            "encoding": "float.hex",
            "storage_class": "real",
            "value": value.hex(),
        }
    raise TypeError(f"unsupported SQLite value type: {type(value)!r}")


_RECOVERY_CASE_COLUMNS = (
    "case_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "certificate_version",
    "policy_version",
    "work_graph_digest",
    "latest_certificate_digest",
    "status",
    "next_delivery_cycle",
    "delivery_count",
    "last_event_id",
    "last_error",
    "created_at",
    "updated_at",
)

_RECOVERY_CASE_TRIGGER_NAMES = (
    "trg_agent_recovery_case_insert_guard",
    "trg_agent_recovery_case_generation_insert",
    "trg_agent_recovery_case_generation_update",
    "trg_agent_recovery_case_current_authority_update",
    "trg_agent_recovery_case_identity_immutable",
    "trg_agent_recovery_case_time_monotonic",
    "trg_agent_recovery_case_progress_monotonic",
    "trg_agent_recovery_case_progress_evidence",
    "trg_agent_recovery_case_status_transition",
    "trg_agent_recovery_case_terminal_immutable",
)


def _ensure_agent_recovery_case_schema(conn: sqlite3.Connection) -> None:
    """Rebuild weak recovery ledgers and reinstall their authority triggers."""

    conn.execute("SAVEPOINT ensure_agent_session_recovery_cases")
    try:
        row = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_session_recovery_cases'
            """
        ).fetchone()
        if row is None:
            conn.execute("RELEASE SAVEPOINT ensure_agent_session_recovery_cases")
            return
        table_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if "CREATE TABLE IF NOT EXISTS agent_session_recovery_cases" in statement
        )
        actual_sql = _normalize_recovery_table_sql(str(row["sql"] or ""))
        canonical_sql = _normalize_recovery_table_sql(table_statement)
        if actual_sql != canonical_sql:
            _rebuild_agent_recovery_case_schema(
                conn,
                table_statement=table_statement,
            )
        _replace_agent_recovery_case_triggers(conn)
        _validate_agent_recovery_case_authority(conn)
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT ensure_agent_session_recovery_cases")
        conn.execute("RELEASE SAVEPOINT ensure_agent_session_recovery_cases")
        raise
    conn.execute("RELEASE SAVEPOINT ensure_agent_session_recovery_cases")


def _rebuild_agent_recovery_case_schema(
    conn: sqlite3.Connection,
    *,
    table_statement: str,
) -> None:
    """Copy valid recovery authority exactly or retain the weak table on error."""

    columns = tuple(
        str(row["name"])
        for row in conn.execute(
            "PRAGMA table_info(agent_session_recovery_cases)"
        ).fetchall()
    )
    column_mismatch = set(columns) != set(_RECOVERY_CASE_COLUMNS)
    row_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM agent_session_recovery_cases"
        ).fetchone()[0]
    )
    if column_mismatch and row_count > 0:
        raise sqlite3.IntegrityError(
            "invalid recovery case authority: legacy columns do not match "
            "the canonical recovery ledger"
        )

    conn.execute("SAVEPOINT rebuild_agent_session_recovery_cases")
    try:
        conn.execute(
            "ALTER TABLE agent_session_recovery_cases "
            "RENAME TO agent_session_recovery_cases_legacy"
        )
        conn.execute(table_statement)
        placeholders = ", ".join("?" for _column in _RECOVERY_CASE_COLUMNS)
        column_list = ", ".join(_RECOVERY_CASE_COLUMNS)
        legacy_rows = (
            ()
            if column_mismatch
            else conn.execute(
                "SELECT * FROM agent_session_recovery_cases_legacy ORDER BY case_id"
            ).fetchall()
        )
        for legacy_row in legacy_rows:
            try:
                inserted = conn.execute(
                    "INSERT INTO agent_session_recovery_cases "
                    f"({column_list}) VALUES ({placeholders})",
                    tuple(legacy_row[column] for column in _RECOVERY_CASE_COLUMNS),
                )
                migrated_row = conn.execute(
                    "SELECT * FROM agent_session_recovery_cases WHERE rowid = ?",
                    (inserted.lastrowid,),
                ).fetchone()
                if migrated_row is None or any(
                    _sqlite_value_evidence(migrated_row[column])
                    != _sqlite_value_evidence(legacy_row[column])
                    for column in _RECOVERY_CASE_COLUMNS
                ):
                    raise sqlite3.IntegrityError(
                        "canonical storage would coerce one or more fields"
                    )
            except sqlite3.IntegrityError as exc:
                raise sqlite3.IntegrityError(
                    "invalid recovery case authority: canonical constraints "
                    f"rejected {legacy_row['case_id']!r}"
                ) from exc

        invalid_generation = conn.execute(
            """
            SELECT recovery.case_id
            FROM agent_session_recovery_cases AS recovery
            LEFT JOIN agent_session_aggregates AS aggregate
              ON aggregate.profile_id = recovery.profile_id
             AND aggregate.session_id = recovery.session_id
            WHERE aggregate.profile_id IS NULL
               OR recovery.ownership_generation > aggregate.ownership_generation
            LIMIT 1
            """
        ).fetchone()
        if invalid_generation is not None:
            raise sqlite3.IntegrityError(
                "invalid recovery case authority: ownership generation does not "
                f"match aggregate for {invalid_generation['case_id']!r}"
            )

        invalid_delivery = conn.execute(
            """
            SELECT recovery.case_id
            FROM agent_session_recovery_cases AS recovery
            WHERE recovery.delivery_count != (
                SELECT COUNT(*)
                FROM agent_session_mailbox AS mailbox
                WHERE mailbox.profile_id = recovery.profile_id
                  AND mailbox.session_id = recovery.session_id
                  AND mailbox.ownership_generation = recovery.ownership_generation
                  AND mailbox.kind = 'RecoveryRequested'
                  AND mailbox.source = 'durable_session_recovery_scanner'
                  AND CASE
                      WHEN typeof(mailbox.payload_json) = 'text'
                       AND json_valid(mailbox.payload_json)
                       AND json_type(mailbox.payload_json) = 'object'
                      THEN
                          json_extract(
                              mailbox.payload_json,
                              '$.schema'
                          ) = 'shinbot.agent.session.recovery-delivery'
                          AND json_type(
                              mailbox.payload_json,
                              '$.version'
                          ) = 'integer'
                          AND json_extract(
                              mailbox.payload_json,
                              '$.version'
                          ) = 1
                          AND json_type(
                              mailbox.payload_json,
                              '$.delivery_cycle'
                          ) = 'integer'
                          AND json_extract(
                              mailbox.payload_json,
                              '$.delivery_cycle'
                          ) >= 0
                          AND json_extract(
                              mailbox.payload_json,
                              '$.delivery_cycle'
                          ) < recovery.delivery_count
                          AND json_extract(
                              mailbox.payload_json,
                              '$.case_id'
                          ) = recovery.case_id
                          AND json_type(
                              mailbox.payload_json,
                              '$.certificate'
                          ) = 'object'
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.schema'
                          ) = 'shinbot.agent.session.recovery-certificate'
                          AND json_type(
                              mailbox.payload_json,
                              '$.certificate.version'
                          ) = 'integer'
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.version'
                          ) = recovery.certificate_version
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.case_id'
                          ) = recovery.case_id
                          AND typeof(
                              json_extract(
                                  mailbox.payload_json,
                                  '$.certificate.certificate_digest'
                              )
                          ) = 'text'
                          AND length(
                              json_extract(
                                  mailbox.payload_json,
                                  '$.certificate.certificate_digest'
                              )
                          ) = 64
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.certificate_digest'
                          ) NOT GLOB '*[^0-9a-f]*'
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.work_graph_digest'
                          ) = recovery.work_graph_digest
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.policy_version'
                          ) = recovery.policy_version
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.subject.profile_id'
                          ) = recovery.profile_id
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.subject.session_id'
                          ) = recovery.session_id
                          AND json_extract(
                              mailbox.payload_json,
                              '$.certificate.subject.ownership_generation'
                          ) = recovery.ownership_generation
                          AND (
                              json_extract(
                                  mailbox.payload_json,
                                  '$.delivery_cycle'
                              ) != recovery.delivery_count - 1
                              OR json_extract(
                                  mailbox.payload_json,
                                  '$.certificate.certificate_digest'
                              ) = recovery.latest_certificate_digest
                          )
                          AND mailbox.event_id =
                              'recovery-requested:v1:'
                              || substr(recovery.case_id, 18)
                              || ':'
                              || CAST(
                                  json_extract(
                                      mailbox.payload_json,
                                      '$.delivery_cycle'
                                  ) AS TEXT
                              )
                      ELSE 0
                  END
            )
            LIMIT 1
            """
        ).fetchone()
        if invalid_delivery is not None:
            raise sqlite3.IntegrityError(
                "invalid recovery case authority: delivery progress has no "
                "matching RecoveryRequested mailbox for "
                f"{invalid_delivery['case_id']!r}"
            )

        conn.execute("DROP TABLE agent_session_recovery_cases_legacy")
        for statement in SCHEMA_STATEMENTS:
            if (
                "CREATE INDEX IF NOT EXISTS idx_agent_session_recovery_cases_"
                in statement
            ):
                conn.execute(statement)
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT rebuild_agent_session_recovery_cases")
        conn.execute("RELEASE SAVEPOINT rebuild_agent_session_recovery_cases")
        raise
    conn.execute("RELEASE SAVEPOINT rebuild_agent_session_recovery_cases")


def _validate_agent_recovery_case_authority(conn: sqlite3.Connection) -> None:
    """Fail closed when persisted cases cannot be justified by current authority."""

    invalid_generation = conn.execute(
        """
        SELECT recovery.case_id
        FROM agent_session_recovery_cases AS recovery
        LEFT JOIN agent_session_aggregates AS aggregate
          ON aggregate.profile_id = recovery.profile_id
         AND aggregate.session_id = recovery.session_id
        WHERE aggregate.profile_id IS NULL
           OR recovery.ownership_generation > aggregate.ownership_generation
        LIMIT 1
        """
    ).fetchone()
    if invalid_generation is not None:
        raise sqlite3.IntegrityError(
            "invalid recovery case authority: ownership generation does not "
            f"match aggregate for {invalid_generation['case_id']!r}"
        )
    try:
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = next_delivery_cycle,
                delivery_count = delivery_count,
                last_event_id = last_event_id,
                latest_certificate_digest = latest_certificate_digest,
                status = status
            """
        )
    except sqlite3.IntegrityError as exc:
        raise sqlite3.IntegrityError(
            "invalid recovery case authority: persisted delivery or status "
            "has no matching RecoveryRequested mailbox"
        ) from exc


def _replace_agent_recovery_case_triggers(conn: sqlite3.Connection) -> None:
    """Replace same-name weak triggers with the canonical recovery protocol."""

    for trigger_name in _RECOVERY_CASE_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    for statement in SCHEMA_STATEMENTS:
        if "CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_" in statement:
            conn.execute(statement)


def _normalize_recovery_table_sql(value: str) -> str:
    normalized = " ".join(value.strip().rstrip(";").split())
    return normalized.replace(
        "CREATE TABLE IF NOT EXISTS agent_session_recovery_cases",
        "CREATE TABLE agent_session_recovery_cases",
        1,
    )


def _add_column_if_missing(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_spec: str,
) -> None:
    columns = _table_columns(conn, table_name)
    if columns and column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_spec}")


def apply_schema(conn: sqlite3.Connection) -> None:
    """Create all persistence tables if they do not exist yet."""
    for statement in SCHEMA_STATEMENTS:
        if "idx_media_semantics_strict_dhash" in statement:
            _migrate_media_semantics_schema(conn)
        conn.execute(statement)
        if "CREATE TABLE IF NOT EXISTS agent_session_recovery_cases" in statement:
            _ensure_agent_recovery_case_schema(conn)
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
    _migrate_agent_unread_messages(conn)
    _migrate_agent_unread_ranges(conn)
    _migrate_workflow_runs_schema(conn)
    _migrate_media_semantics_schema(conn)
    _migrate_durable_routing_schema(conn)
    _migrate_external_action_receipts_schema(conn)
    _migrate_session_actor_schema(conn)
    _ensure_actor_raw_logical_key_indexes(conn)
