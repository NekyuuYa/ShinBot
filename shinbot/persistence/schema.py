"""SQLite schema bootstrap for ShinBot persistence."""

from __future__ import annotations

import hashlib
import json
import math
import re
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

# Manual review is an explicit admission protocol rather than a generic
# mailbox event. Unlike ReviewDue's inspectable deterministic-key preflight,
# its caller-owned request id is a hard idempotency boundary. Cast every key
# to BLOB so a TEXT/BLOB storage alias cannot create a second admission.
_MANUAL_REVIEW_REQUEST_UNIQUE_INDEX: tuple[str, str, str] = (
    "idx_agent_session_mailbox_manual_review_request_unique",
    "agent_session_mailbox",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS
        idx_agent_session_mailbox_manual_review_request_unique
    ON agent_session_mailbox(
        CAST(profile_id AS BLOB),
        CAST(session_id AS BLOB),
        CAST(causation_id AS BLOB)
    )
    WHERE CAST(kind AS BLOB) = X'4D616E75616C526576696577526571756573746564'
      AND CAST(source AS BLOB) = X'6D616E75616C5F7265766965775F61646D697373696F6E'
    """,
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
    """
    CREATE INDEX IF NOT EXISTS idx_audit_logs_session_command_timestamp
    ON audit_logs(session_id, command_name, timestamp DESC, id DESC)
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
        admission_fence_id TEXT NOT NULL DEFAULT '',
        admission_fence_generation INTEGER NOT NULL DEFAULT 0,
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
                AND admission_fence_id = ''
                AND admission_fence_generation = 0
            )
            OR
            (
                profile_id != ''
                AND session_id != ''
                AND (
                    (
                        ownership_generation >= 1
                        AND admission_fence_id = ''
                        AND admission_fence_generation = 0
                    )
                    OR
                    (
                        ownership_generation >= 0
                        AND admission_fence_id != ''
                        AND admission_fence_generation >= 1
                    )
                )
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
        admission_fence_id TEXT NOT NULL DEFAULT '',
        admission_fence_generation INTEGER NOT NULL DEFAULT 0,
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
        CHECK(
            (admission_fence_id = '' AND admission_fence_generation = 0)
            OR
            (admission_fence_id != '' AND admission_fence_generation >= 1)
        ),
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
        admission_fence_id TEXT NOT NULL DEFAULT '',
        admission_fence_generation INTEGER NOT NULL DEFAULT 0,
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
            (admission_fence_id = '' AND admission_fence_generation = 0)
            OR
            (
                mode = 'actor_v2'
                AND admission_fence_id != ''
                AND admission_fence_generation >= 1
            )
        ),
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
    # -- Future Actor v2 admission fence ---------------------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_admission_fences (
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        fence_id TEXT NOT NULL UNIQUE,
        generation INTEGER NOT NULL,
        holder_token_digest TEXT NOT NULL,
        holder_id TEXT NOT NULL,
        status TEXT NOT NULL,
        expires_at REAL NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        committed_at REAL,
        revoked_at REAL,
        revocation_reason TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(profile_id, session_id),
        CHECK(generation >= 1),
        CHECK(status IN ('reserved', 'committed', 'revoked')),
        CHECK(
            (status = 'reserved' AND committed_at IS NULL AND revoked_at IS NULL)
            OR
            (status = 'committed' AND committed_at IS NOT NULL AND revoked_at IS NULL)
            OR
            (status = 'revoked' AND revoked_at IS NOT NULL)
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_actor_v2_admission_fences_status
    ON agent_session_actor_v2_admission_fences(status, expires_at)
    """,
    # -- Future Actor v2 production cutover journal ---------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_cutover_journal (
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        cutover_epoch INTEGER NOT NULL,
        cutover_id TEXT NOT NULL UNIQUE,
        legacy_session_id TEXT NOT NULL,
        adapter_instance_ids_json TEXT NOT NULL,
        phase TEXT NOT NULL,
        initiated_by TEXT NOT NULL,
        admission_fence_id TEXT NOT NULL DEFAULT '',
        admission_fence_generation INTEGER NOT NULL DEFAULT 0,
        ownership_generation INTEGER NOT NULL DEFAULT 0,
        target_id TEXT NOT NULL DEFAULT '',
        target_incarnation_id TEXT NOT NULL DEFAULT '',
        target_lease_epoch INTEGER NOT NULL DEFAULT 0,
        blocked_code TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY(profile_id, session_id, cutover_epoch),
        UNIQUE(profile_id, session_id),
        CHECK(cutover_epoch >= 1),
        CHECK(phase IN (
            'preflighted', 'admission_reserved', 'legacy_quiesced',
            'actor_owner_committed', 'target_published',
            'ingress_resumed', 'blocked'
        )),
        CHECK(initiated_by != ''),
        CHECK(legacy_session_id != ''),
        CHECK(
            (admission_fence_id = '' AND admission_fence_generation = 0)
            OR
            (admission_fence_id != '' AND admission_fence_generation >= 1)
        ),
        CHECK(ownership_generation >= 0),
        CHECK(
            (target_id = '' AND target_incarnation_id = '' AND target_lease_epoch = 0)
            OR
            (target_id != '' AND target_incarnation_id != '' AND target_lease_epoch >= 1)
        ),
        CHECK(updated_at >= created_at),
        CHECK(
            (phase = 'preflighted'
             AND admission_fence_id = ''
             AND admission_fence_generation = 0
             AND ownership_generation = 0
             AND target_id = ''
             AND target_incarnation_id = ''
             AND target_lease_epoch = 0
             AND blocked_code = '')
            OR
            (phase IN ('admission_reserved', 'legacy_quiesced')
             AND admission_fence_id != ''
             AND admission_fence_generation >= 1
             AND ownership_generation = 0
             AND target_id = ''
             AND target_incarnation_id = ''
             AND target_lease_epoch = 0
             AND blocked_code = '')
            OR
            (phase = 'actor_owner_committed'
             AND admission_fence_id != ''
             AND admission_fence_generation >= 1
             AND ownership_generation >= 1
             AND target_id = ''
             AND target_incarnation_id = ''
             AND target_lease_epoch = 0
             AND blocked_code = '')
            OR
            (phase IN ('target_published', 'ingress_resumed')
             AND admission_fence_id != ''
             AND admission_fence_generation >= 1
             AND ownership_generation >= 1
             AND target_id != ''
             AND target_incarnation_id != ''
             AND target_lease_epoch >= 1
             AND blocked_code = '')
            OR
            (phase = 'blocked' AND blocked_code != '')
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_actor_v2_cutover_journal_phase
    ON agent_session_actor_v2_cutover_journal(phase, updated_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_cutover_events (
        event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        cutover_id TEXT NOT NULL,
        phase TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        occurred_at REAL NOT NULL,
        UNIQUE(cutover_id, phase),
        FOREIGN KEY(cutover_id)
            REFERENCES agent_session_actor_v2_cutover_journal(cutover_id)
            ON DELETE RESTRICT,
        CHECK(phase IN (
            'preflighted', 'admission_reserved', 'legacy_quiesced',
            'actor_owner_committed', 'target_published',
            'ingress_resumed', 'blocked'
        )),
        CHECK(json_valid(evidence_json))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_actor_v2_cutover_events_cutover
    ON agent_session_actor_v2_cutover_events(cutover_id, event_seq)
    """,
    # -- Future fenced legacy-to-Actor ownership migration barrier -------
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_migration_barriers (
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        barrier_id TEXT NOT NULL UNIQUE,
        legacy_session_id TEXT NOT NULL,
        adapter_instance_ids_json TEXT NOT NULL,
        source_generation INTEGER NOT NULL,
        migration_generation INTEGER NOT NULL,
        holder_id TEXT NOT NULL,
        holder_token_digest TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        aborted_at REAL,
        abort_reason TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(profile_id, session_id),
        CHECK(source_generation >= 1),
        CHECK(legacy_session_id != ''),
        CHECK(json_valid(adapter_instance_ids_json)),
        CHECK(migration_generation = source_generation + 1),
        CHECK(holder_id != ''),
        CHECK(holder_token_digest != ''),
        CHECK(status IN ('migrating', 'aborted')),
        CHECK(updated_at >= created_at),
        CHECK(
            (status = 'migrating' AND aborted_at IS NULL AND abort_reason = '')
            OR
            (status = 'aborted'
             AND aborted_at IS NOT NULL
             AND aborted_at = updated_at
             AND abort_reason != '')
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_migration_barriers_status
    ON agent_session_actor_v2_migration_barriers(status, updated_at)
    """,
    # -- Future Actor v2 cross-process ingress drain --------------------
    """
    CREATE TABLE IF NOT EXISTS agent_runtime_actor_v2_ingress_participants (
        member_id TEXT PRIMARY KEY,
        adapter_instance_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        participant_epoch INTEGER NOT NULL,
        holder_token_digest TEXT NOT NULL,
        status TEXT NOT NULL,
        registered_at REAL NOT NULL,
        last_heartbeat_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        retired_at REAL,
        revoked_at REAL,
        stop_proof_issuer_id TEXT NOT NULL DEFAULT '',
        stop_proof_epoch INTEGER NOT NULL DEFAULT 0,
        stop_proof_digest TEXT NOT NULL DEFAULT '',
        stop_proof_summary_code TEXT NOT NULL DEFAULT '',
        UNIQUE(adapter_instance_id, participant_id, participant_epoch),
        CHECK(participant_epoch >= 1),
        CHECK(holder_token_digest != ''),
        CHECK(status IN ('active', 'retired', 'revoked')),
        CHECK(last_heartbeat_at >= registered_at),
        CHECK(updated_at >= last_heartbeat_at),
        CHECK(
            (status = 'active'
             AND retired_at IS NULL
             AND revoked_at IS NULL
             AND stop_proof_issuer_id = ''
             AND stop_proof_epoch = 0
             AND stop_proof_digest = ''
             AND stop_proof_summary_code = '')
            OR
            (status = 'retired'
             AND retired_at IS NOT NULL
             AND retired_at = updated_at
             AND revoked_at IS NULL
             AND stop_proof_issuer_id = ''
             AND stop_proof_epoch = 0
             AND stop_proof_digest = ''
             AND stop_proof_summary_code = '')
            OR
            (status = 'revoked'
             AND retired_at IS NULL
             AND revoked_at IS NOT NULL
             AND revoked_at = updated_at
             AND stop_proof_issuer_id != ''
             AND stop_proof_epoch >= 1
             AND stop_proof_digest != ''
             AND stop_proof_summary_code != '')
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_ingress_participants_adapter_status
    ON agent_runtime_actor_v2_ingress_participants(
        adapter_instance_id, status, participant_id, participant_epoch
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_ingress_drain_requests (
        request_id TEXT PRIMARY KEY,
        cutover_id TEXT NOT NULL UNIQUE,
        cutover_epoch INTEGER NOT NULL,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        legacy_session_id TEXT NOT NULL,
        adapter_instance_ids_json TEXT NOT NULL,
        admission_fence_id TEXT NOT NULL,
        admission_fence_generation INTEGER NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        drained_at REAL,
        FOREIGN KEY(cutover_id)
            REFERENCES agent_session_actor_v2_cutover_journal(cutover_id)
            ON DELETE RESTRICT,
        CHECK(cutover_epoch >= 1),
        CHECK(admission_fence_id != ''),
        CHECK(admission_fence_generation >= 1),
        CHECK(status IN ('assembling', 'open', 'drained')),
        CHECK(updated_at >= created_at),
        CHECK(
            (status IN ('assembling', 'open') AND drained_at IS NULL)
            OR
            (status = 'drained' AND drained_at IS NOT NULL AND drained_at = updated_at)
        ),
        CHECK(json_valid(adapter_instance_ids_json))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_ingress_drain_requests_status
    ON agent_session_actor_v2_ingress_drain_requests(status, updated_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_ingress_drain_members (
        request_id TEXT NOT NULL,
        member_id TEXT NOT NULL,
        adapter_instance_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        participant_epoch INTEGER NOT NULL,
        PRIMARY KEY(request_id, member_id),
        FOREIGN KEY(request_id)
            REFERENCES agent_session_actor_v2_ingress_drain_requests(request_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(member_id)
            REFERENCES agent_runtime_actor_v2_ingress_participants(member_id)
            ON DELETE RESTRICT,
        CHECK(participant_epoch >= 1)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_ingress_drain_members_adapter
    ON agent_session_actor_v2_ingress_drain_members(
        adapter_instance_id, request_id, member_id
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_ingress_drain_acknowledgements (
        request_id TEXT NOT NULL,
        member_id TEXT NOT NULL,
        adapter_pause_digest TEXT NOT NULL,
        legacy_quiescence_digest TEXT NOT NULL,
        proof_epoch INTEGER NOT NULL,
        summary_code TEXT NOT NULL,
        acknowledged_at REAL NOT NULL,
        PRIMARY KEY(request_id, member_id),
        FOREIGN KEY(request_id, member_id)
            REFERENCES agent_session_actor_v2_ingress_drain_members(request_id, member_id)
            ON DELETE RESTRICT,
        CHECK(length(adapter_pause_digest) = 64),
        CHECK(length(legacy_quiescence_digest) = 64),
        CHECK(proof_epoch >= 1),
        CHECK(summary_code != '')
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_ingress_drain_acknowledgements_request
    ON agent_session_actor_v2_ingress_drain_acknowledgements(request_id, member_id)
    """,
    # -- Barrier-bound Actor v2 core-ingress drain ---------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_core_ingress_drain_requests (
        request_id TEXT PRIMARY KEY,
        barrier_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        legacy_session_id TEXT NOT NULL,
        adapter_instance_ids_json TEXT NOT NULL,
        source_generation INTEGER NOT NULL,
        migration_generation INTEGER NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        drained_at REAL,
        FOREIGN KEY(barrier_id)
            REFERENCES agent_session_actor_v2_migration_barriers(barrier_id)
            ON DELETE RESTRICT,
        CHECK(legacy_session_id != ''),
        CHECK(source_generation >= 1),
        CHECK(migration_generation = source_generation + 1),
        CHECK(status IN ('assembling', 'open', 'drained')),
        CHECK(updated_at >= created_at),
        CHECK(
            (status IN ('assembling', 'open') AND drained_at IS NULL)
            OR
            (status = 'drained' AND drained_at IS NOT NULL AND drained_at = updated_at)
        ),
        CHECK(json_valid(adapter_instance_ids_json))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_core_ingress_drain_requests_status
    ON agent_session_actor_v2_core_ingress_drain_requests(status, updated_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_core_ingress_drain_members (
        request_id TEXT NOT NULL,
        member_id TEXT NOT NULL,
        adapter_instance_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        participant_epoch INTEGER NOT NULL,
        PRIMARY KEY(request_id, member_id),
        FOREIGN KEY(request_id)
            REFERENCES agent_session_actor_v2_core_ingress_drain_requests(request_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(member_id)
            REFERENCES agent_runtime_actor_v2_ingress_participants(member_id)
            ON DELETE RESTRICT,
        CHECK(participant_epoch >= 1)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_core_ingress_drain_members_adapter
    ON agent_session_actor_v2_core_ingress_drain_members(
        adapter_instance_id, request_id, member_id
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_core_ingress_drain_members_participant
    ON agent_session_actor_v2_core_ingress_drain_members(
        participant_id, request_id, member_id
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_core_ingress_drain_acknowledgements (
        request_id TEXT NOT NULL,
        member_id TEXT NOT NULL,
        core_ingress_digest TEXT NOT NULL,
        legacy_quiescence_digest TEXT NOT NULL,
        proof_epoch INTEGER NOT NULL,
        summary_code TEXT NOT NULL,
        acknowledged_at REAL NOT NULL,
        PRIMARY KEY(request_id, member_id),
        FOREIGN KEY(request_id, member_id)
            REFERENCES agent_session_actor_v2_core_ingress_drain_members(request_id, member_id)
            ON DELETE RESTRICT,
        CHECK(length(core_ingress_digest) = 64),
        CHECK(length(legacy_quiescence_digest) = 64),
        CHECK(proof_epoch >= 1),
        CHECK(summary_code != '')
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_core_ingress_drain_acknowledgements_request
    ON agent_session_actor_v2_core_ingress_drain_acknowledgements(request_id, member_id)
    """,
    # -- Frozen legacy source-state handoff for future Actor v2 cutover --
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_legacy_state_handoff_manifests (
        manifest_id TEXT PRIMARY KEY,
        barrier_id TEXT NOT NULL UNIQUE,
        core_ingress_drain_request_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        legacy_session_id TEXT NOT NULL,
        source_generation INTEGER NOT NULL,
        migration_generation INTEGER NOT NULL,
        manifest_version INTEGER NOT NULL,
        scope_json TEXT NOT NULL,
        source_payload_json TEXT NOT NULL,
        core_ingress_digest TEXT NOT NULL,
        legacy_quiescence_digest TEXT NOT NULL,
        source_digest TEXT NOT NULL,
        captured_at REAL NOT NULL,
        FOREIGN KEY(barrier_id)
            REFERENCES agent_session_actor_v2_migration_barriers(barrier_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(core_ingress_drain_request_id)
            REFERENCES agent_session_actor_v2_core_ingress_drain_requests(request_id)
            ON DELETE RESTRICT,
        CHECK(legacy_session_id != ''),
        CHECK(source_generation >= 1),
        CHECK(migration_generation = source_generation + 1),
        CHECK(manifest_version >= 1),
        CHECK(length(core_ingress_digest) = 64),
        CHECK(length(legacy_quiescence_digest) = 64),
        CHECK(length(source_digest) = 64),
        CHECK(json_valid(scope_json)),
        CHECK(json_valid(source_payload_json))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_legacy_state_handoff_manifest_source
    ON agent_session_actor_v2_legacy_state_handoff_manifests(
        profile_id, session_id, migration_generation
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_legacy_state_handoff_materializations (
        manifest_id TEXT NOT NULL,
        materializer_id TEXT NOT NULL,
        materializer_version INTEGER NOT NULL,
        target_schema_version INTEGER NOT NULL,
        source_digest TEXT NOT NULL,
        target_payload_json TEXT NOT NULL,
        target_digest TEXT NOT NULL,
        materialized_at REAL NOT NULL,
        PRIMARY KEY(manifest_id, materializer_id, materializer_version),
        FOREIGN KEY(manifest_id)
            REFERENCES agent_session_actor_v2_legacy_state_handoff_manifests(manifest_id)
            ON DELETE RESTRICT,
        CHECK(materializer_id != ''),
        CHECK(materializer_version >= 1),
        CHECK(target_schema_version >= 1),
        CHECK(length(source_digest) = 64),
        CHECK(length(target_digest) = 64),
        CHECK(json_valid(target_payload_json))
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_legacy_state_handoff_materializations_manifest
    ON agent_session_actor_v2_legacy_state_handoff_materializations(
        manifest_id, materializer_id, materializer_version
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_legacy_state_handoff_finalizations (
        barrier_id TEXT PRIMARY KEY,
        manifest_id TEXT NOT NULL UNIQUE,
        materializer_id TEXT NOT NULL,
        materializer_version INTEGER NOT NULL,
        target_schema_version INTEGER NOT NULL,
        source_digest TEXT NOT NULL,
        target_digest TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        completion_reason TEXT NOT NULL,
        requested_by TEXT NOT NULL DEFAULT '',
        completed_at REAL NOT NULL,
        FOREIGN KEY(barrier_id)
            REFERENCES agent_session_actor_v2_migration_barriers(barrier_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(manifest_id)
            REFERENCES agent_session_actor_v2_legacy_state_handoff_manifests(manifest_id)
            ON DELETE RESTRICT,
        FOREIGN KEY(manifest_id, materializer_id, materializer_version)
            REFERENCES agent_session_actor_v2_legacy_state_handoff_materializations(
                manifest_id, materializer_id, materializer_version
            )
            ON DELETE RESTRICT,
        CHECK(materializer_id != ''),
        CHECK(materializer_version >= 1),
        CHECK(target_schema_version >= 1),
        CHECK(length(source_digest) = 64),
        CHECK(length(target_digest) = 64),
        CHECK(ownership_generation >= 1),
        CHECK(completion_reason != '')
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_legacy_state_handoff_finalizations_owner
    ON agent_session_actor_v2_legacy_state_handoff_finalizations(
        ownership_generation, completed_at
    )
    """,
    # -- Legacy broad-recovery / Actor v2 admission interlock -----------
    """
    CREATE TABLE IF NOT EXISTS agent_runtime_legacy_recovery_gate (
        gate_id INTEGER PRIMARY KEY,
        mode TEXT NOT NULL,
        epoch INTEGER NOT NULL,
        holder_id TEXT NOT NULL DEFAULT '',
        holder_token_digest TEXT NOT NULL DEFAULT '',
        activated_at REAL,
        updated_at REAL NOT NULL,
        CHECK(gate_id = 1),
        CHECK(mode IN ('legacy_open', 'legacy_recovery_active', 'fenced_only')),
        CHECK(epoch >= 0),
        CHECK(
            (mode = 'legacy_recovery_active'
             AND holder_id != ''
             AND holder_token_digest != ''
             AND activated_at IS NOT NULL)
            OR
            (mode IN ('legacy_open', 'fenced_only')
             AND holder_id = ''
             AND holder_token_digest = ''
             AND activated_at IS NULL)
        )
    )
    """,
    # -- Dormant Actor v2 clean-canary isolation slot ------------------
    """
    CREATE TABLE IF NOT EXISTS agent_runtime_actor_v2_canary_isolation_leases (
        lease_id INTEGER PRIMARY KEY,
        lease_epoch INTEGER NOT NULL,
        holder_id TEXT NOT NULL,
        holder_token_digest TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        released_at REAL,
        revoked_at REAL,
        revocation_reason TEXT NOT NULL DEFAULT '',
        CHECK(lease_id = 1),
        CHECK(lease_epoch >= 1),
        CHECK(holder_id != ''),
        CHECK(holder_token_digest != ''),
        CHECK(status IN ('active', 'released', 'revoked')),
        CHECK(updated_at >= created_at),
        CHECK(
            (status = 'active'
             AND released_at IS NULL
             AND revoked_at IS NULL
             AND revocation_reason = '')
            OR
            (status = 'released'
             AND released_at IS NOT NULL
             AND released_at = updated_at
             AND revoked_at IS NULL
             AND revocation_reason = '')
            OR
            (status = 'revoked'
             AND released_at IS NULL
             AND revoked_at IS NOT NULL
             AND revoked_at = updated_at
             AND revocation_reason != '')
        )
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
    CREATE INDEX IF NOT EXISTS idx_agent_session_mailbox_manual_review_request
    ON agent_session_mailbox(
        CAST(profile_id AS BLOB), CAST(session_id AS BLOB),
        CAST(kind AS BLOB), CAST(source AS BLOB), CAST(causation_id AS BLOB)
    )
    """,
    # -- Dormant Actor v2 mailbox handoff evidence ----------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_mailbox_handoffs (
        mailbox_id INTEGER PRIMARY KEY,
        handoff_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        evidence_state TEXT NOT NULL,
        admission_fence_id TEXT NOT NULL DEFAULT '',
        admission_fence_generation INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'blocked',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        available_at REAL NOT NULL,
        claim_id TEXT NOT NULL DEFAULT '',
        lease_owner TEXT NOT NULL DEFAULT '',
        lease_until REAL,
        target_id TEXT NOT NULL DEFAULT '',
        target_incarnation_id TEXT NOT NULL DEFAULT '',
        target_disposition TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        claimed_at REAL,
        settled_at REAL,
        last_error TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(mailbox_id)
            REFERENCES agent_session_mailbox(mailbox_id)
            ON DELETE RESTRICT,
        CHECK(ownership_generation >= 0),
        CHECK(attempt_count >= 0),
        CHECK(evidence_state IN ('fenced', 'unfenced_legacy', 'unknown')),
        CHECK(state IN ('pending', 'claimed', 'settled', 'blocked')),
        CHECK(
            (
                evidence_state = 'fenced'
                AND ownership_generation >= 1
                AND admission_fence_id != ''
                AND admission_fence_generation >= 1
            )
            OR (
                evidence_state IN ('unfenced_legacy', 'unknown')
                AND admission_fence_id = ''
                AND admission_fence_generation = 0
            )
        ),
        CHECK(
            (
                state = 'pending'
                AND evidence_state = 'fenced'
                AND claim_id = ''
                AND lease_owner = ''
                AND lease_until IS NULL
                AND target_id = ''
                AND target_incarnation_id = ''
                AND target_disposition = ''
                AND claimed_at IS NULL
                AND settled_at IS NULL
            )
            OR (
                state = 'claimed'
                AND evidence_state = 'fenced'
                AND claim_id != ''
                AND lease_owner != ''
                AND lease_until IS NOT NULL
                AND target_id != ''
                AND target_incarnation_id != ''
                AND target_disposition = ''
                AND claimed_at IS NOT NULL
                AND lease_until > claimed_at
                AND lease_until <= claimed_at + 300.0
                AND settled_at IS NULL
            )
            OR (
                state = 'settled'
                AND evidence_state = 'fenced'
                AND claim_id = ''
                AND lease_owner = ''
                AND lease_until IS NULL
                AND target_id != ''
                AND target_incarnation_id != ''
                AND target_disposition IN ('accepted', 'stale')
                AND claimed_at IS NULL
                AND settled_at IS NOT NULL
            )
            OR (
                state = 'blocked'
                AND evidence_state IN ('unfenced_legacy', 'unknown')
                AND claim_id = ''
                AND lease_owner = ''
                AND lease_until IS NULL
                AND target_id = ''
                AND target_incarnation_id = ''
                AND target_disposition = ''
                AND claimed_at IS NULL
                AND settled_at IS NULL
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_mailbox_handoffs_pending
    ON agent_session_mailbox_handoffs(
        evidence_state, state, available_at, mailbox_id
    )
    """,
    # -- Dormant fenced Actor wake-target publication -------------------
    """
    CREATE TABLE IF NOT EXISTS agent_session_actor_v2_fenced_wake_target_leases (
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        admission_fence_id TEXT NOT NULL,
        admission_fence_generation INTEGER NOT NULL,
        lease_epoch INTEGER NOT NULL,
        target_id TEXT NOT NULL,
        target_incarnation_id TEXT NOT NULL,
        holder_token_digest TEXT NOT NULL,
        status TEXT NOT NULL,
        expires_at REAL NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        released_at REAL,
        PRIMARY KEY(profile_id, session_id),
        CHECK(ownership_generation >= 1),
        CHECK(admission_fence_id != ''),
        CHECK(admission_fence_generation >= 1),
        CHECK(lease_epoch >= 1),
        CHECK(target_id != ''),
        CHECK(target_incarnation_id != ''),
        CHECK(holder_token_digest != ''),
        CHECK(status IN ('active', 'released')),
        CHECK(expires_at > created_at),
        CHECK(updated_at >= created_at),
        CHECK(
            (status = 'active' AND released_at IS NULL)
            OR
            (
                status = 'released'
                AND released_at IS NOT NULL
                AND released_at = updated_at
                AND released_at >= created_at
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_actor_v2_fenced_wake_target_leases_expiry
    ON agent_session_actor_v2_fenced_wake_target_leases(status, expires_at)
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
        CHECK(status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')),
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
    CREATE TABLE IF NOT EXISTS agent_historical_effect_terminalizations (
        terminalization_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        audit_id TEXT NOT NULL UNIQUE,
        effect_seq INTEGER NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        effect_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        effect_kind TEXT NOT NULL,
        contract_version INTEGER NOT NULL,
        contract_signature TEXT NOT NULL,
        effect_payload_sha256 TEXT NOT NULL,
        failure_code TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        terminalized_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(profile_id, session_id, effect_id),
        CHECK(effect_seq >= 1),
        CHECK(typeof(audit_id) = 'text'),
        CHECK(length(audit_id) = 101),
        CHECK(substr(audit_id, 1, 37) = 'historical-effect-terminalization:v1:'),
        CHECK(substr(audit_id, 38) NOT GLOB '*[^0-9a-f]*'),
        CHECK(typeof(profile_id) = 'text'),
        CHECK(length(trim(profile_id)) > 0),
        CHECK(profile_id = trim(profile_id)),
        CHECK(typeof(session_id) = 'text'),
        CHECK(length(trim(session_id)) > 0),
        CHECK(session_id = trim(session_id)),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(typeof(effect_id) = 'text'),
        CHECK(length(trim(effect_id)) > 0),
        CHECK(effect_id = trim(effect_id)),
        CHECK(typeof(idempotency_key) = 'text'),
        CHECK(length(trim(idempotency_key)) > 0),
        CHECK(idempotency_key = trim(idempotency_key)),
        CHECK(typeof(operation_id) = 'text'),
        CHECK(length(trim(operation_id)) > 0),
        CHECK(operation_id = trim(operation_id)),
        CHECK(effect_kind IN (
            'run_active_chat_bootstrap',
            'run_active_chat_round',
            'active_chat_runtime_reconciliation',
            'stop_active_chat_runtime',
            'cancel_idle_review_planning',
            'idle_review_planning_cancellation_reconciliation'
        )),
        CHECK(contract_version IN (1, 2)),
        CHECK(typeof(contract_signature) = 'text'),
        CHECK(length(trim(contract_signature)) > 0),
        CHECK(contract_signature = trim(contract_signature)),
        CHECK(length(effect_payload_sha256) = 64),
        CHECK(effect_payload_sha256 NOT GLOB '*[^0-9a-f]*'),
        CHECK(failure_code = 'historical_effect_never_claimed_terminalized'),
        CHECK(
            CASE
                WHEN typeof(evidence_json) = 'text' AND json_valid(evidence_json)
                THEN json_type(evidence_json) = 'object'
                ELSE 0
            END
        ),
        CHECK(
            CASE
                WHEN typeof(evidence_json) = 'text' AND json_valid(evidence_json)
                THEN evidence_json = json(evidence_json)
                ELSE 0
            END
        ),
        CHECK(
            typeof(terminalized_at) IN ('integer', 'real')
            AND terminalized_at >= 0
            AND terminalized_at <= 1.7976931348623157e308
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_historical_effect_terminalizations_session
    ON agent_historical_effect_terminalizations(
        profile_id, session_id, terminalization_seq DESC
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_review_cancellation_gates (
        gate_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        cancellation_effect_id TEXT NOT NULL,
        request_event_id TEXT NOT NULL,
        review_operation_id TEXT NOT NULL,
        review_effect_id TEXT NOT NULL,
        review_effect_kind TEXT NOT NULL,
        review_contract_version INTEGER NOT NULL,
        review_contract_signature TEXT NOT NULL,
        gate_status TEXT NOT NULL,
        target_effect_status TEXT NOT NULL,
        target_effect_claim_id TEXT NOT NULL DEFAULT '',
        target_effect_attempt_count INTEGER NOT NULL DEFAULT 0,
        target_effect_terminal_at REAL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(
            profile_id, session_id, ownership_generation,
            cancellation_effect_id
        ),
        UNIQUE(
            profile_id, session_id, ownership_generation, review_effect_id
        ),
        CHECK(
            typeof(profile_id) = 'text'
            AND length(trim(profile_id)) > 0
            AND profile_id = trim(profile_id)
        ),
        CHECK(
            typeof(session_id) = 'text'
            AND length(trim(session_id)) > 0
            AND session_id = trim(session_id)
        ),
        CHECK(
            typeof(cancellation_effect_id) = 'text'
            AND length(trim(cancellation_effect_id)) > 0
            AND cancellation_effect_id = trim(cancellation_effect_id)
        ),
        CHECK(
            typeof(request_event_id) = 'text'
            AND length(trim(request_event_id)) > 0
            AND request_event_id = trim(request_event_id)
        ),
        CHECK(
            typeof(review_operation_id) = 'text'
            AND length(trim(review_operation_id)) > 0
            AND review_operation_id = trim(review_operation_id)
        ),
        CHECK(
            typeof(review_effect_id) = 'text'
            AND length(trim(review_effect_id)) > 0
            AND review_effect_id = trim(review_effect_id)
        ),
        CHECK(
            review_effect_kind = 'run_review_workflow'
        ),
        CHECK(
            typeof(review_contract_version) = 'integer'
            AND review_contract_version >= 1
        ),
        CHECK(
            typeof(review_contract_signature) = 'text'
            AND length(trim(review_contract_signature)) > 0
            AND review_contract_signature = trim(review_contract_signature)
        ),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(gate_status IN ('requested', 'cancelled', 'terminal')),
        CHECK(
            target_effect_status IN (
                'pending', 'processing', 'completed', 'failed', 'cancelled'
            )
        ),
        CHECK(
            (gate_status = 'requested' AND target_effect_status = 'processing')
            OR (gate_status = 'cancelled' AND target_effect_status = 'cancelled')
            OR (
                gate_status = 'terminal'
                AND target_effect_status IN ('completed', 'failed', 'cancelled')
            )
        ),
        CHECK(
            typeof(target_effect_claim_id) = 'text'
            AND target_effect_claim_id = trim(target_effect_claim_id)
        ),
        CHECK(
            typeof(target_effect_attempt_count) = 'integer'
            AND target_effect_attempt_count >= 0
        ),
        CHECK(
            target_effect_terminal_at IS NULL
            OR (
                typeof(target_effect_terminal_at) IN ('integer', 'real')
                AND target_effect_terminal_at >= 0
                AND target_effect_terminal_at <= 1.7976931348623157e308
            )
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
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_cancellation_gates_target
    ON agent_review_cancellation_gates(
        profile_id, session_id, ownership_generation, review_effect_id
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_review_execution_runs (
        run_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        review_effect_id TEXT NOT NULL,
        review_operation_id TEXT NOT NULL,
        review_effect_kind TEXT NOT NULL,
        review_contract_version INTEGER NOT NULL,
        review_contract_signature TEXT NOT NULL,
        claim_id TEXT NOT NULL,
        worker_id TEXT NOT NULL,
        execution_status TEXT NOT NULL,
        started_at REAL NOT NULL,
        finished_at REAL,
        unknown_at REAL,
        unknown_reason TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(
            profile_id, session_id, ownership_generation,
            review_effect_id, claim_id
        ),
        CHECK(
            typeof(profile_id) = 'text'
            AND length(trim(profile_id)) > 0
            AND profile_id = trim(profile_id)
        ),
        CHECK(
            typeof(session_id) = 'text'
            AND length(trim(session_id)) > 0
            AND session_id = trim(session_id)
        ),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(
            typeof(review_effect_id) = 'text'
            AND length(trim(review_effect_id)) > 0
            AND review_effect_id = trim(review_effect_id)
        ),
        CHECK(
            typeof(review_operation_id) = 'text'
            AND length(trim(review_operation_id)) > 0
            AND review_operation_id = trim(review_operation_id)
        ),
        CHECK(review_effect_kind = 'run_review_workflow'),
        CHECK(
            typeof(review_contract_version) = 'integer'
            AND review_contract_version >= 1
        ),
        CHECK(
            typeof(review_contract_signature) = 'text'
            AND length(trim(review_contract_signature)) > 0
            AND review_contract_signature = trim(review_contract_signature)
        ),
        CHECK(
            typeof(claim_id) = 'text'
            AND length(trim(claim_id)) > 0
            AND claim_id = trim(claim_id)
        ),
        CHECK(
            typeof(worker_id) = 'text'
            AND length(trim(worker_id)) > 0
            AND worker_id = trim(worker_id)
        ),
        CHECK(execution_status IN ('running', 'finished', 'cancelled', 'unknown')),
        CHECK(
            typeof(started_at) IN ('integer', 'real')
            AND started_at >= 0
            AND started_at <= 1.7976931348623157e308
        ),
        CHECK(
            finished_at IS NULL
            OR (
                typeof(finished_at) IN ('integer', 'real')
                AND finished_at >= 0
                AND finished_at <= 1.7976931348623157e308
            )
        ),
        CHECK(
            unknown_at IS NULL
            OR (
                typeof(unknown_at) IN ('integer', 'real')
                AND unknown_at >= 0
                AND unknown_at <= 1.7976931348623157e308
            )
        ),
        CHECK(typeof(unknown_reason) = 'text'),
        CHECK(
            (
                execution_status = 'running'
                AND finished_at IS NULL
                AND unknown_at IS NULL
                AND unknown_reason = ''
            )
            OR (
                execution_status IN ('finished', 'cancelled')
                AND finished_at IS NOT NULL
                AND unknown_at IS NULL
                AND unknown_reason = ''
            )
            OR (
                execution_status = 'unknown'
                AND finished_at IS NULL
                AND unknown_at IS NOT NULL
                AND length(trim(unknown_reason)) > 0
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_review_execution_runs_live
    ON agent_review_execution_runs(
        profile_id, session_id, ownership_generation,
        review_effect_id, execution_status
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_model_execution_runs (
        run_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        effect_id TEXT NOT NULL,
        operation_id TEXT NOT NULL,
        effect_kind TEXT NOT NULL,
        contract_version INTEGER NOT NULL,
        contract_signature TEXT NOT NULL,
        claim_id TEXT NOT NULL,
        worker_id TEXT NOT NULL,
        execution_status TEXT NOT NULL,
        started_at REAL NOT NULL,
        finished_at REAL,
        unknown_at REAL,
        unknown_reason TEXT NOT NULL DEFAULT '',
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(
            profile_id, session_id, ownership_generation,
            effect_id, claim_id
        ),
        CHECK(
            typeof(profile_id) = 'text'
            AND length(trim(profile_id)) > 0
            AND profile_id = trim(profile_id)
        ),
        CHECK(
            typeof(session_id) = 'text'
            AND length(trim(session_id)) > 0
            AND session_id = trim(session_id)
        ),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(
            typeof(effect_id) = 'text'
            AND length(trim(effect_id)) > 0
            AND effect_id = trim(effect_id)
        ),
        CHECK(
            typeof(operation_id) = 'text'
            AND length(trim(operation_id)) > 0
            AND operation_id = trim(operation_id)
        ),
        CHECK(effect_kind IN (
            'run_active_reply_workflow',
            'run_active_chat_bootstrap',
            'run_active_chat_round',
            'run_idle_review_planning'
        )),
        CHECK(
            typeof(contract_version) = 'integer'
            AND contract_version >= 1
        ),
        CHECK(
            typeof(contract_signature) = 'text'
            AND length(trim(contract_signature)) > 0
            AND contract_signature = trim(contract_signature)
        ),
        CHECK(
            typeof(claim_id) = 'text'
            AND length(trim(claim_id)) > 0
            AND claim_id = trim(claim_id)
        ),
        CHECK(
            typeof(worker_id) = 'text'
            AND length(trim(worker_id)) > 0
            AND worker_id = trim(worker_id)
        ),
        CHECK(execution_status IN ('running', 'finished', 'unknown')),
        CHECK(
            typeof(started_at) IN ('integer', 'real')
            AND started_at >= 0
            AND started_at <= 1.7976931348623157e308
        ),
        CHECK(
            finished_at IS NULL
            OR (
                typeof(finished_at) IN ('integer', 'real')
                AND finished_at >= 0
                AND finished_at <= 1.7976931348623157e308
            )
        ),
        CHECK(
            unknown_at IS NULL
            OR (
                typeof(unknown_at) IN ('integer', 'real')
                AND unknown_at >= 0
                AND unknown_at <= 1.7976931348623157e308
            )
        ),
        CHECK(typeof(unknown_reason) = 'text'),
        CHECK(
            (
                execution_status = 'running'
                AND finished_at IS NULL
                AND unknown_at IS NULL
                AND unknown_reason = ''
            )
            OR (
                execution_status = 'finished'
                AND finished_at IS NOT NULL
                AND unknown_at IS NULL
                AND unknown_reason = ''
            )
            OR (
                execution_status = 'unknown'
                AND finished_at IS NULL
                AND unknown_at IS NOT NULL
                AND length(trim(unknown_reason)) > 0
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_model_execution_runs_live
    ON agent_model_execution_runs(
        profile_id, session_id, ownership_generation,
        effect_id, execution_status
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_model_execution_cancellation_gates (
        gate_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        cancellation_effect_id TEXT NOT NULL,
        request_event_id TEXT NOT NULL,
        target_operation_id TEXT NOT NULL,
        target_effect_id TEXT NOT NULL,
        target_effect_kind TEXT NOT NULL,
        target_contract_version INTEGER NOT NULL,
        target_contract_signature TEXT NOT NULL,
        target_effect_status TEXT NOT NULL,
        target_claim_id TEXT NOT NULL DEFAULT '',
        target_worker_id TEXT NOT NULL DEFAULT '',
        target_effect_attempt_count INTEGER NOT NULL DEFAULT 0,
        target_execution_status TEXT NOT NULL,
        gate_status TEXT NOT NULL,
        target_effect_terminal_at REAL,
        blocker_code TEXT NOT NULL DEFAULT '',
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(
            profile_id, session_id, ownership_generation,
            cancellation_effect_id
        ),
        UNIQUE(
            profile_id, session_id, ownership_generation, target_effect_id
        ),
        CHECK(
            typeof(profile_id) = 'text'
            AND length(trim(profile_id)) > 0
            AND profile_id = trim(profile_id)
        ),
        CHECK(
            typeof(session_id) = 'text'
            AND length(trim(session_id)) > 0
            AND session_id = trim(session_id)
        ),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(
            typeof(cancellation_effect_id) = 'text'
            AND length(trim(cancellation_effect_id)) > 0
            AND cancellation_effect_id = trim(cancellation_effect_id)
        ),
        CHECK(
            typeof(request_event_id) = 'text'
            AND length(trim(request_event_id)) > 0
            AND request_event_id = trim(request_event_id)
        ),
        CHECK(
            typeof(target_operation_id) = 'text'
            AND length(trim(target_operation_id)) > 0
            AND target_operation_id = trim(target_operation_id)
        ),
        CHECK(
            typeof(target_effect_id) = 'text'
            AND length(trim(target_effect_id)) > 0
            AND target_effect_id = trim(target_effect_id)
        ),
        CHECK(target_effect_kind = 'run_idle_review_planning'),
        CHECK(target_contract_version = 3),
        CHECK(
            typeof(target_contract_signature) = 'text'
            AND length(trim(target_contract_signature)) > 0
            AND target_contract_signature = trim(target_contract_signature)
        ),
        CHECK(target_effect_status IN (
            'pending', 'processing', 'completed', 'failed', 'cancelled'
        )),
        CHECK(
            typeof(target_claim_id) = 'text'
            AND target_claim_id = trim(target_claim_id)
        ),
        CHECK(
            typeof(target_worker_id) = 'text'
            AND target_worker_id = trim(target_worker_id)
        ),
        CHECK(
            typeof(target_effect_attempt_count) = 'integer'
            AND target_effect_attempt_count >= 0
        ),
        CHECK(target_execution_status IN ('none', 'running', 'finished', 'unknown')),
        CHECK(gate_status IN ('requested', 'cancelled', 'terminal', 'blocked')),
        CHECK(
            target_effect_terminal_at IS NULL
            OR (
                typeof(target_effect_terminal_at) IN ('integer', 'real')
                AND target_effect_terminal_at >= 0
                AND target_effect_terminal_at <= 1.7976931348623157e308
            )
        ),
        CHECK(typeof(blocker_code) = 'text'),
        CHECK(
            (gate_status = 'requested'
                AND target_effect_status = 'processing'
                AND length(target_claim_id) > 0
                AND length(target_worker_id) > 0
                AND target_execution_status IN ('none', 'running')
                AND target_effect_terminal_at IS NULL
                AND blocker_code = '')
            OR (gate_status = 'cancelled'
                AND target_effect_status = 'cancelled'
                AND length(target_claim_id) > 0
                AND length(target_worker_id) > 0
                AND target_execution_status = 'running'
                AND target_effect_terminal_at IS NOT NULL
                AND blocker_code = '')
            OR (gate_status = 'terminal'
                AND target_effect_status IN ('completed', 'failed', 'cancelled')
                AND target_execution_status IN ('none', 'finished')
                AND target_effect_terminal_at IS NOT NULL
                AND blocker_code = '')
            OR (gate_status = 'blocked'
                AND target_effect_status = 'processing'
                AND length(target_claim_id) > 0
                AND length(target_worker_id) > 0
                AND target_execution_status = 'unknown'
                AND target_effect_terminal_at IS NULL
                AND length(trim(blocker_code)) > 0)
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
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_model_execution_cancellation_gates_target
    ON agent_model_execution_cancellation_gates(
        profile_id, session_id, ownership_generation, target_effect_id
    )
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
    CREATE TRIGGER IF NOT EXISTS trg_agent_recovery_case_terminal_delivery_state
    BEFORE UPDATE OF status ON agent_session_recovery_cases
    WHEN NEW.status IN (
            'applied', 'superseded', 'delivery_exhausted', 'scanner_blocked'
         )
      AND NEW.delivery_count > 0
      AND (
          EXISTS (
              SELECT 1
              FROM agent_session_mailbox AS mailbox
              WHERE mailbox.profile_id = NEW.profile_id
                AND mailbox.session_id = NEW.session_id
                AND mailbox.ownership_generation = NEW.ownership_generation
                AND mailbox.kind = 'RecoveryRequested'
                AND mailbox.source = 'durable_session_recovery_scanner'
                AND mailbox.causation_id = NEW.case_id
                AND mailbox.status IN ('pending', 'processing')
          )
          OR (
              NEW.status IN ('applied', 'superseded', 'scanner_blocked')
              AND NOT EXISTS (
                  SELECT 1
                  FROM agent_session_mailbox AS mailbox
                  WHERE mailbox.profile_id = NEW.profile_id
                    AND mailbox.session_id = NEW.session_id
                    AND mailbox.ownership_generation = NEW.ownership_generation
                    AND mailbox.event_id = NEW.last_event_id
                    AND mailbox.kind = 'RecoveryRequested'
                    AND mailbox.source = 'durable_session_recovery_scanner'
                    AND mailbox.status = 'completed'
              )
          )
          OR (
              NEW.status = 'delivery_exhausted'
              AND NOT EXISTS (
                  SELECT 1
                  FROM agent_session_mailbox AS mailbox
                  WHERE mailbox.profile_id = NEW.profile_id
                    AND mailbox.session_id = NEW.session_id
                    AND mailbox.ownership_generation = NEW.ownership_generation
                    AND mailbox.event_id = NEW.last_event_id
                    AND mailbox.kind = 'RecoveryRequested'
                    AND mailbox.source = 'durable_session_recovery_scanner'
                    AND mailbox.status = 'failed'
              )
          )
      )
    BEGIN
        SELECT RAISE(
            ABORT,
            'terminal recovery case requires settled typed delivery evidence'
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
    """
    CREATE TABLE IF NOT EXISTS agent_session_recovery_findings (
        finding_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        finding_id TEXT NOT NULL UNIQUE,
        profile_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        ownership_generation INTEGER NOT NULL,
        code TEXT NOT NULL,
        evidence_digest TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        occurrence_count INTEGER NOT NULL DEFAULT 1,
        first_seen_at REAL NOT NULL,
        last_seen_at REAL NOT NULL,
        resolved_at REAL,
        FOREIGN KEY(profile_id, session_id)
            REFERENCES agent_session_aggregates(profile_id, session_id)
            ON DELETE CASCADE,
        UNIQUE(
            profile_id, session_id, ownership_generation, code, evidence_digest
        ),
        CHECK(typeof(finding_id) = 'text'),
        CHECK(length(finding_id) = 84),
        CHECK(substr(finding_id, 1, 20) = 'recovery-finding:v1:'),
        CHECK(substr(finding_id, 21) NOT GLOB '*[^0-9a-f]*'),
        CHECK(typeof(profile_id) = 'text'),
        CHECK(length(trim(profile_id)) > 0),
        CHECK(profile_id = trim(profile_id)),
        CHECK(typeof(session_id) = 'text'),
        CHECK(length(trim(session_id)) > 0),
        CHECK(session_id = trim(session_id)),
        CHECK(typeof(ownership_generation) = 'integer'),
        CHECK(ownership_generation >= 1),
        CHECK(typeof(code) = 'text'),
        CHECK(length(trim(code)) > 0),
        CHECK(code = trim(code)),
        CHECK(typeof(evidence_digest) = 'text'),
        CHECK(length(evidence_digest) = 64),
        CHECK(evidence_digest NOT GLOB '*[^0-9a-f]*'),
        CHECK(typeof(evidence_json) = 'text'),
        CHECK(json_valid(evidence_json)),
        CHECK(typeof(status) = 'text'),
        CHECK(status IN ('open', 'resolved')),
        CHECK(typeof(occurrence_count) = 'integer'),
        CHECK(occurrence_count >= 1),
        CHECK(typeof(first_seen_at) IN ('integer', 'real')),
        CHECK(typeof(last_seen_at) IN ('integer', 'real')),
        CHECK(first_seen_at >= 0),
        CHECK(last_seen_at >= first_seen_at),
        CHECK(
            (status = 'open' AND resolved_at IS NULL)
            OR (
                status = 'resolved'
                AND typeof(resolved_at) IN ('integer', 'real')
                AND resolved_at >= first_seen_at
                AND resolved_at <= last_seen_at
            )
        )
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_agent_session_recovery_findings_open
    ON agent_session_recovery_findings(
        profile_id, session_id, ownership_generation, status, last_seen_at
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
) + tuple(statement for _index_name, _table_name, statement in _ACTOR_RAW_LOGICAL_KEY_INDEXES)


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
    _add_column_if_missing(
        conn,
        "message_routing_jobs",
        "admission_fence_id",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "message_routing_jobs",
        "admission_fence_generation",
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
    _add_column_if_missing(
        conn,
        "agent_route_outbox",
        "admission_fence_id",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "agent_route_outbox",
        "admission_fence_generation",
        "INTEGER NOT NULL DEFAULT 0",
    )


def _migrate_actor_v2_admission_schema(conn: sqlite3.Connection) -> None:
    """Add durable admission-fence columns to pre-fence ownership rows."""

    _add_column_if_missing(
        conn,
        "agent_session_runtime_ownership",
        "admission_fence_id",
        "TEXT NOT NULL DEFAULT ''",
    )
    _add_column_if_missing(
        conn,
        "agent_session_runtime_ownership",
        "admission_fence_generation",
        "INTEGER NOT NULL DEFAULT 0",
    )


_CUTOVER_JOURNAL_REQUIRED_COLUMNS = frozenset(
    {
        "profile_id",
        "session_id",
        "cutover_epoch",
        "cutover_id",
        "legacy_session_id",
        "adapter_instance_ids_json",
        "phase",
        "initiated_by",
        "admission_fence_id",
        "admission_fence_generation",
        "ownership_generation",
        "target_id",
        "target_incarnation_id",
        "target_lease_epoch",
        "blocked_code",
        "created_at",
        "updated_at",
    }
)

_CUTOVER_EVENT_REQUIRED_COLUMNS = frozenset(
    {"event_seq", "cutover_id", "phase", "evidence_json", "occurred_at"}
)

_CUTOVER_EVIDENCE_REQUIRED_KEYS = frozenset(
    {"digest", "issuer_id", "kind", "proof_epoch", "summary_code"}
)
_CUTOVER_EVIDENCE_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_CUTOVER_EVIDENCE_SUMMARY_CODE_PATTERN = re.compile(
    r"[a-z][a-z0-9_.:-]{0,127}"
)
_CUTOVER_EVIDENCE_KIND_SETS_BY_PHASE: dict[str, frozenset[frozenset[str]]] = {
    "preflighted": frozenset({frozenset({"clean_preflight"})}),
    "admission_reserved": frozenset({frozenset({"admission_reservation"})}),
    "legacy_quiesced": frozenset(
        {
            frozenset({"adapter_pause_drain", "legacy_quiescence"}),
            frozenset({"core_ingress_drain", "legacy_quiescence"}),
        }
    ),
    "actor_owner_committed": frozenset({frozenset({"actor_owner_commit"})}),
    "target_published": frozenset({frozenset({"target_publication"})}),
    "ingress_resumed": frozenset({frozenset({"ingress_resume"})}),
    "blocked": frozenset({frozenset({"blocked"})}),
}

_CUTOVER_JOURNAL_TRIGGER_NAMES = (
    "trg_actor_v2_cutover_journal_initial_state",
    "trg_actor_v2_cutover_journal_identity_immutable",
    "trg_actor_v2_cutover_journal_phase_lifecycle",
    "trg_actor_v2_cutover_journal_delete_forbidden",
    "trg_actor_v2_cutover_event_insert_order",
    "trg_actor_v2_cutover_event_immutable",
    "trg_actor_v2_cutover_event_delete_forbidden",
)


def _migrate_actor_v2_cutover_journal_schema(conn: sqlite3.Connection) -> None:
    """Validate the immutable, no-skip Actor v2 cutover journal contract.

    The journal is a durable evidence boundary for a future controller. It is
    intentionally not a feature flag: malformed history fails database
    initialization rather than being repaired into an apparent authorization.
    """

    journal_columns = _table_columns(conn, "agent_session_actor_v2_cutover_journal")
    event_columns = _table_columns(conn, "agent_session_actor_v2_cutover_events")
    if not journal_columns or not event_columns:
        raise sqlite3.IntegrityError("Actor v2 cutover journal tables are missing")
    journal_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_session_actor_v2_cutover_journal"
        in statement
    )
    event_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_session_actor_v2_cutover_events"
        in statement
    )
    _require_exact_table_schema(
        conn,
        table_name="agent_session_actor_v2_cutover_journal",
        create_statement=journal_statement,
        label="Actor v2 cutover journal",
    )
    _require_exact_table_schema(
        conn,
        table_name="agent_session_actor_v2_cutover_events",
        create_statement=event_statement,
        label="Actor v2 cutover events",
    )
    missing_journal_columns = _CUTOVER_JOURNAL_REQUIRED_COLUMNS.difference(
        journal_columns
    )
    if missing_journal_columns:
        raise sqlite3.IntegrityError(
            "Actor v2 cutover journal lacks required columns: "
            + ", ".join(sorted(missing_journal_columns))
        )
    missing_event_columns = _CUTOVER_EVENT_REQUIRED_COLUMNS.difference(event_columns)
    if missing_event_columns:
        raise sqlite3.IntegrityError(
            "Actor v2 cutover events lack required columns: "
            + ", ".join(sorted(missing_event_columns))
        )
    journal_index_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE INDEX IF NOT EXISTS idx_agent_session_actor_v2_cutover_journal_phase"
        in statement
    )
    event_index_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE INDEX IF NOT EXISTS idx_agent_session_actor_v2_cutover_events_cutover"
        in statement
    )
    _ensure_exact_index(
        conn,
        index_name="idx_agent_session_actor_v2_cutover_journal_phase",
        expected_table="agent_session_actor_v2_cutover_journal",
        create_statement=journal_index_statement,
        label="Actor v2 cutover journal phase",
    )
    _ensure_exact_index(
        conn,
        index_name="idx_agent_session_actor_v2_cutover_events_cutover",
        expected_table="agent_session_actor_v2_cutover_events",
        create_statement=event_index_statement,
        label="Actor v2 cutover event",
    )
    _validate_cutover_journal_rows(conn)
    _replace_cutover_journal_triggers(conn)


def _require_exact_table_schema(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    create_statement: str,
    label: str,
) -> None:
    """Require a durable table to retain the exact immutable schema shape."""

    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if row is None or _normalized_create_table_sql(
        row["sql"]
    ) != _normalized_create_table_sql(create_statement):
        raise sqlite3.IntegrityError(f"{label} table does not match its immutable contract")


def _validate_cutover_journal_rows(conn: sqlite3.Connection) -> None:
    """Reject partial, skipped, or non-canonical journal evidence at startup."""

    rows = conn.execute(
        "SELECT * FROM agent_session_actor_v2_cutover_journal ORDER BY cutover_id"
    ).fetchall()
    for row in rows:
        _validate_cutover_journal_row(conn, row)


def _validate_cutover_journal_row(conn: sqlite3.Connection, row: sqlite3.Row) -> None:
    """Validate one persisted journal's phase and append-only event sequence."""

    try:
        phase = str(row["phase"])
        cutover_id = str(row["cutover_id"])
        created_at = float(row["created_at"])
        updated_at = float(row["updated_at"])
        cutover_epoch = row["cutover_epoch"]
        admission_generation = row["admission_fence_generation"]
        ownership_generation = row["ownership_generation"]
        target_epoch = row["target_lease_epoch"]
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError("Actor v2 cutover journal has malformed state") from exc
    if (
        isinstance(cutover_epoch, bool)
        or not isinstance(cutover_epoch, int)
        or cutover_epoch != 1
        or not cutover_id
        or not str(row["legacy_session_id"])
        or not str(row["initiated_by"])
        or phase
        not in {
            "preflighted",
            "admission_reserved",
            "legacy_quiesced",
            "actor_owner_committed",
            "target_published",
            "ingress_resumed",
            "blocked",
        }
        or not math.isfinite(created_at)
        or not math.isfinite(updated_at)
        or updated_at < created_at
        or isinstance(admission_generation, bool)
        or not isinstance(admission_generation, int)
        or isinstance(ownership_generation, bool)
        or not isinstance(ownership_generation, int)
        or isinstance(target_epoch, bool)
        or not isinstance(target_epoch, int)
    ):
        raise sqlite3.IntegrityError("Actor v2 cutover journal has invalid state")
    try:
        adapter_instances = json.loads(str(row["adapter_instance_ids_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise sqlite3.IntegrityError(
            "Actor v2 cutover journal adapter identities are invalid"
        ) from exc
    if (
        not isinstance(adapter_instances, list)
        or not adapter_instances
        or any(not isinstance(item, str) or not item for item in adapter_instances)
        or adapter_instances != sorted(set(adapter_instances))
    ):
        raise sqlite3.IntegrityError(
            "Actor v2 cutover journal adapter identities are non-canonical"
        )
    event_rows = conn.execute(
        """
        SELECT phase, evidence_json, occurred_at
        FROM agent_session_actor_v2_cutover_events
        WHERE cutover_id = ?
        ORDER BY event_seq ASC
        """,
        (cutover_id,),
    ).fetchall()
    event_phases = [str(event_row["phase"]) for event_row in event_rows]
    successful = [
        "preflighted",
        "admission_reserved",
        "legacy_quiesced",
        "actor_owner_committed",
        "target_published",
        "ingress_resumed",
    ]
    if phase == "blocked":
        valid_chain = (
            len(event_phases) >= 2
            and event_phases[-1] == "blocked"
            and event_phases[:-1] == successful[: len(event_phases) - 1]
        )
    else:
        expected_count = successful.index(phase) + 1
        valid_chain = event_phases == successful[:expected_count]
    if not valid_chain:
        raise sqlite3.IntegrityError(
            "Actor v2 cutover journal has a skipped or incomplete event chain"
        )
    last_occurred_at = created_at
    for event_row in event_rows:
        try:
            occurred_at = float(event_row["occurred_at"])
        except (TypeError, ValueError) as exc:
            raise sqlite3.IntegrityError(
                "Actor v2 cutover journal event has invalid timestamp"
            ) from exc
        if (
            not math.isfinite(occurred_at)
            or occurred_at < last_occurred_at
            or occurred_at > updated_at
            or not _is_typed_cutover_evidence_json(
                str(event_row["phase"]),
                event_row["evidence_json"],
            )
        ):
            raise sqlite3.IntegrityError(
                "Actor v2 cutover journal event has invalid evidence"
            )
        last_occurred_at = occurred_at


def _is_typed_cutover_evidence_json(phase: str, value: object) -> bool:
    """Validate one phase's exact token-free proof bundle.

    This mirrors the typed core contract without importing dispatch code from
    the schema layer. Startup must reject an otherwise valid JSON envelope
    whose proof kind, digest, or stable summary could not have been produced
    by the journal repository.
    """

    allowed_kind_sets = _CUTOVER_EVIDENCE_KIND_SETS_BY_PHASE.get(phase)
    if allowed_kind_sets is None or not isinstance(value, str):
        return False
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(decoded, list):
        return False
    kinds: list[str] = []
    for item in decoded:
        if not isinstance(item, dict) or set(item) != _CUTOVER_EVIDENCE_REQUIRED_KEYS:
            return False
        digest = item["digest"]
        issuer_id = item["issuer_id"]
        kind = item["kind"]
        proof_epoch = item["proof_epoch"]
        summary_code = item["summary_code"]
        if (
            not isinstance(digest, str)
            or _CUTOVER_EVIDENCE_DIGEST_PATTERN.fullmatch(digest) is None
            or not isinstance(issuer_id, str)
            or not issuer_id
            or issuer_id != issuer_id.strip()
            or not isinstance(kind, str)
            or not kind
            or not isinstance(proof_epoch, int)
            or isinstance(proof_epoch, bool)
            or proof_epoch < 1
            or not isinstance(summary_code, str)
            or _CUTOVER_EVIDENCE_SUMMARY_CODE_PATTERN.fullmatch(summary_code)
            is None
        ):
            return False
        kinds.append(kind)
    return (
        len(set(kinds)) == len(kinds)
        and frozenset(kinds) in allowed_kind_sets
        and kinds == sorted(kinds)
    )


def _replace_cutover_journal_triggers(conn: sqlite3.Connection) -> None:
    """Install SQL guards against skipped, mutable, or erasable cutover history."""

    for trigger_name in _CUTOVER_JOURNAL_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_journal_initial_state
        BEFORE INSERT ON agent_session_actor_v2_cutover_journal
        WHEN NEW.cutover_epoch != 1
          OR NEW.phase != 'preflighted'
          OR NEW.admission_fence_id != ''
          OR NEW.admission_fence_generation != 0
          OR NEW.ownership_generation != 0
          OR NEW.target_id != ''
          OR NEW.target_incarnation_id != ''
          OR NEW.target_lease_epoch != 0
          OR NEW.blocked_code != ''
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 cutover journal must begin preflighted without later references'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_journal_identity_immutable
        BEFORE UPDATE OF profile_id, session_id, cutover_epoch, cutover_id,
                         legacy_session_id, adapter_instance_ids_json,
                         initiated_by, created_at
        ON agent_session_actor_v2_cutover_journal
        WHEN typeof(OLD.profile_id) != typeof(NEW.profile_id)
          OR CAST(OLD.profile_id AS BLOB) != CAST(NEW.profile_id AS BLOB)
          OR typeof(OLD.session_id) != typeof(NEW.session_id)
          OR CAST(OLD.session_id AS BLOB) != CAST(NEW.session_id AS BLOB)
          OR typeof(OLD.cutover_epoch) != typeof(NEW.cutover_epoch)
          OR OLD.cutover_epoch != NEW.cutover_epoch
          OR typeof(OLD.cutover_id) != typeof(NEW.cutover_id)
          OR CAST(OLD.cutover_id AS BLOB) != CAST(NEW.cutover_id AS BLOB)
          OR typeof(OLD.legacy_session_id) != typeof(NEW.legacy_session_id)
          OR CAST(OLD.legacy_session_id AS BLOB) !=
                CAST(NEW.legacy_session_id AS BLOB)
          OR typeof(OLD.adapter_instance_ids_json) !=
                typeof(NEW.adapter_instance_ids_json)
          OR CAST(OLD.adapter_instance_ids_json AS BLOB) !=
                CAST(NEW.adapter_instance_ids_json AS BLOB)
          OR typeof(OLD.initiated_by) != typeof(NEW.initiated_by)
          OR CAST(OLD.initiated_by AS BLOB) != CAST(NEW.initiated_by AS BLOB)
          OR typeof(OLD.created_at) != typeof(NEW.created_at)
          OR OLD.created_at != NEW.created_at
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 cutover journal identity is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_journal_phase_lifecycle
        BEFORE UPDATE ON agent_session_actor_v2_cutover_journal
        WHEN NOT (
            NEW.updated_at >= OLD.updated_at
            AND (
                (OLD.phase = 'preflighted' AND NEW.phase IN ('admission_reserved', 'blocked'))
                OR (OLD.phase = 'admission_reserved' AND NEW.phase IN ('legacy_quiesced', 'blocked'))
                OR (OLD.phase = 'legacy_quiesced' AND NEW.phase IN ('actor_owner_committed', 'blocked'))
                OR (OLD.phase = 'actor_owner_committed' AND NEW.phase IN ('target_published', 'blocked'))
                OR (OLD.phase = 'target_published' AND NEW.phase IN ('ingress_resumed', 'blocked'))
            )
            AND EXISTS (
                SELECT 1
                FROM agent_session_actor_v2_cutover_events AS event
                WHERE event.cutover_id = NEW.cutover_id
                  AND event.phase = NEW.phase
            )
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid Actor v2 cutover journal phase transition'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_journal_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_cutover_journal
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 cutover journal history cannot be deleted');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_event_insert_order
        BEFORE INSERT ON agent_session_actor_v2_cutover_events
        WHEN NOT (
            (
                NEW.phase = 'preflighted'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase = 'preflighted'
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                )
            )
            OR (
                NEW.phase = 'admission_reserved'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase = 'preflighted'
                )
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                      AND event.phase = 'preflighted'
                )
            )
            OR (
                NEW.phase = 'legacy_quiesced'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase = 'admission_reserved'
                )
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                      AND event.phase = 'admission_reserved'
                )
            )
            OR (
                NEW.phase = 'actor_owner_committed'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase = 'legacy_quiesced'
                )
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                      AND event.phase = 'legacy_quiesced'
                )
            )
            OR (
                NEW.phase = 'target_published'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase = 'actor_owner_committed'
                )
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                      AND event.phase = 'actor_owner_committed'
                )
            )
            OR (
                NEW.phase = 'ingress_resumed'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase = 'target_published'
                )
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                      AND event.phase = 'target_published'
                )
            )
            OR (
                NEW.phase = 'blocked'
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_journal AS journal
                    WHERE journal.cutover_id = NEW.cutover_id
                      AND journal.phase NOT IN ('ingress_resumed', 'blocked')
                )
                AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_cutover_events AS event
                    WHERE event.cutover_id = NEW.cutover_id
                      AND event.phase = 'preflighted'
                )
            )
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid Actor v2 cutover event insertion order'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_event_immutable
        BEFORE UPDATE ON agent_session_actor_v2_cutover_events
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 cutover events are immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_cutover_event_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_cutover_events
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 cutover event history cannot be deleted');
        END
        """
    )


_MIGRATION_BARRIER_REQUIRED_COLUMNS = frozenset(
    {
        "profile_id",
        "session_id",
        "barrier_id",
        "legacy_session_id",
        "adapter_instance_ids_json",
        "source_generation",
        "migration_generation",
        "holder_id",
        "holder_token_digest",
        "status",
        "created_at",
        "updated_at",
        "aborted_at",
        "abort_reason",
    }
)

_MIGRATION_BARRIER_TRIGGER_NAMES = (
    "trg_actor_v2_migration_barrier_initial_state",
    "trg_actor_v2_migration_barrier_identity_immutable",
    "trg_actor_v2_migration_barrier_lifecycle",
    "trg_actor_v2_migration_barrier_delete_forbidden",
)


def _migrate_actor_v2_migration_barrier_schema(conn: sqlite3.Connection) -> None:
    """Validate the immutable holder-fenced legacy-to-Actor barrier table."""

    table_name = "agent_session_actor_v2_migration_barriers"
    columns = _table_columns(conn, table_name)
    if not columns:
        raise sqlite3.IntegrityError("Actor v2 migration barrier table is missing")
    missing_columns = _MIGRATION_BARRIER_REQUIRED_COLUMNS.difference(columns)
    if missing_columns:
        raise sqlite3.IntegrityError(
            "Actor v2 migration barrier table lacks columns: "
            + ", ".join(sorted(missing_columns))
        )
    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_session_actor_v2_migration_barriers"
        in statement
    )
    _require_exact_table_schema(
        conn,
        table_name=table_name,
        create_statement=table_statement,
        label="Actor v2 migration barrier",
    )
    index_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE INDEX IF NOT EXISTS idx_actor_v2_migration_barriers_status"
        in statement
    )
    _ensure_exact_index(
        conn,
        index_name="idx_actor_v2_migration_barriers_status",
        expected_table=table_name,
        create_statement=index_statement,
        label="Actor v2 migration barrier",
    )
    rows = conn.execute(
        "SELECT * FROM agent_session_actor_v2_migration_barriers ORDER BY barrier_id"
    ).fetchall()
    for row in rows:
        _validate_actor_v2_migration_barrier_row(row)
    _replace_actor_v2_migration_barrier_triggers(conn)


def _validate_actor_v2_migration_barrier_row(row: sqlite3.Row) -> None:
    """Reject malformed immutable barrier state before runtime startup."""

    try:
        source_generation = row["source_generation"]
        migration_generation = row["migration_generation"]
        status = str(row["status"])
        created_at = float(row["created_at"])
        updated_at = float(row["updated_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError("Actor v2 migration barrier has malformed state") from exc
    if (
        not str(row["profile_id"])
        or not str(row["session_id"])
        or not str(row["barrier_id"])
        or not str(row["legacy_session_id"])
        or not str(row["holder_id"])
        or not str(row["holder_token_digest"])
        or isinstance(source_generation, bool)
        or not isinstance(source_generation, int)
        or source_generation < 1
        or isinstance(migration_generation, bool)
        or not isinstance(migration_generation, int)
        or migration_generation != source_generation + 1
        or status not in {"migrating", "aborted"}
        or not math.isfinite(created_at)
        or not math.isfinite(updated_at)
        or updated_at < created_at
    ):
        raise sqlite3.IntegrityError("Actor v2 migration barrier has invalid state")
    try:
        adapter_instance_ids = json.loads(str(row["adapter_instance_ids_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise sqlite3.IntegrityError(
            "Actor v2 migration barrier adapter identities are invalid"
        ) from exc
    if (
        not isinstance(adapter_instance_ids, list)
        or not adapter_instance_ids
        or any(
            not isinstance(adapter_instance_id, str) or not adapter_instance_id
            for adapter_instance_id in adapter_instance_ids
        )
        or adapter_instance_ids != sorted(set(adapter_instance_ids))
    ):
        raise sqlite3.IntegrityError(
            "Actor v2 migration barrier adapter identities are non-canonical"
        )
    aborted_at = row["aborted_at"]
    abort_reason = str(row["abort_reason"])
    if status == "migrating":
        valid_terminal_state = aborted_at is None and abort_reason == ""
    else:
        valid_terminal_state = (
            aborted_at is not None
            and float(aborted_at) == updated_at
            and bool(abort_reason)
        )
    if not valid_terminal_state:
        raise sqlite3.IntegrityError("Actor v2 migration barrier has invalid terminal state")


def _replace_actor_v2_migration_barrier_triggers(conn: sqlite3.Connection) -> None:
    """Install SQL guards against barrier replacement or generic reopening."""

    for trigger_name in _MIGRATION_BARRIER_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_migration_barrier_initial_state
        BEFORE INSERT ON agent_session_actor_v2_migration_barriers
        WHEN NEW.status != 'migrating'
          OR NEW.aborted_at IS NOT NULL
          OR NEW.abort_reason != ''
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 migration barrier must begin migrating');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_migration_barrier_identity_immutable
        BEFORE UPDATE OF profile_id, session_id, barrier_id, legacy_session_id,
                         adapter_instance_ids_json, source_generation,
                         migration_generation, holder_id, holder_token_digest, created_at
        ON agent_session_actor_v2_migration_barriers
        WHEN NEW.profile_id != OLD.profile_id
          OR NEW.session_id != OLD.session_id
          OR NEW.barrier_id != OLD.barrier_id
          OR NEW.legacy_session_id != OLD.legacy_session_id
          OR NEW.adapter_instance_ids_json != OLD.adapter_instance_ids_json
          OR NEW.source_generation != OLD.source_generation
          OR NEW.migration_generation != OLD.migration_generation
          OR NEW.holder_id != OLD.holder_id
          OR NEW.holder_token_digest != OLD.holder_token_digest
          OR NEW.created_at != OLD.created_at
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 migration barrier identity is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_migration_barrier_lifecycle
        BEFORE UPDATE ON agent_session_actor_v2_migration_barriers
        WHEN NOT (
            OLD.status = 'migrating'
            AND NEW.status = 'aborted'
            AND NEW.updated_at >= OLD.updated_at
            AND NEW.aborted_at = NEW.updated_at
            AND NEW.abort_reason != ''
            AND NOT EXISTS (
                SELECT 1
                FROM agent_session_actor_v2_legacy_state_handoff_finalizations
                WHERE barrier_id = OLD.barrier_id
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid Actor v2 migration barrier transition');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_migration_barrier_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_migration_barriers
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 migration barrier history cannot be deleted');
        END
        """
    )


_INGRESS_DRAIN_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "agent_runtime_actor_v2_ingress_participants": frozenset(
        {
            "member_id",
            "adapter_instance_id",
            "participant_id",
            "participant_epoch",
            "holder_token_digest",
            "status",
            "registered_at",
            "last_heartbeat_at",
            "updated_at",
            "retired_at",
            "revoked_at",
            "stop_proof_issuer_id",
            "stop_proof_epoch",
            "stop_proof_digest",
            "stop_proof_summary_code",
        }
    ),
    "agent_session_actor_v2_ingress_drain_requests": frozenset(
        {
            "request_id",
            "cutover_id",
            "cutover_epoch",
            "profile_id",
            "session_id",
            "legacy_session_id",
            "adapter_instance_ids_json",
            "admission_fence_id",
            "admission_fence_generation",
            "status",
            "created_at",
            "updated_at",
            "drained_at",
        }
    ),
    "agent_session_actor_v2_ingress_drain_members": frozenset(
        {
            "request_id",
            "member_id",
            "adapter_instance_id",
            "participant_id",
            "participant_epoch",
        }
    ),
    "agent_session_actor_v2_ingress_drain_acknowledgements": frozenset(
        {
            "request_id",
            "member_id",
            "adapter_pause_digest",
            "legacy_quiescence_digest",
            "proof_epoch",
            "summary_code",
            "acknowledged_at",
        }
    ),
}

_INGRESS_DRAIN_INDEXES = (
    (
        "idx_actor_v2_ingress_participants_adapter_status",
        "agent_runtime_actor_v2_ingress_participants",
    ),
    (
        "idx_actor_v2_ingress_drain_requests_status",
        "agent_session_actor_v2_ingress_drain_requests",
    ),
    (
        "idx_actor_v2_ingress_drain_members_adapter",
        "agent_session_actor_v2_ingress_drain_members",
    ),
    (
        "idx_actor_v2_ingress_drain_acknowledgements_request",
        "agent_session_actor_v2_ingress_drain_acknowledgements",
    ),
)

_INGRESS_DRAIN_TRIGGER_NAMES = (
    "trg_actor_v2_ingress_participant_initial_state",
    "trg_actor_v2_ingress_participant_identity_immutable",
    "trg_actor_v2_ingress_participant_lifecycle",
    "trg_actor_v2_ingress_participant_delete_forbidden",
    "trg_actor_v2_ingress_drain_request_initial_state",
    "trg_actor_v2_ingress_drain_request_identity_immutable",
    "trg_actor_v2_ingress_drain_request_lifecycle",
    "trg_actor_v2_ingress_drain_request_delete_forbidden",
    "trg_actor_v2_ingress_drain_member_insert_guard",
    "trg_actor_v2_ingress_drain_member_immutable",
    "trg_actor_v2_ingress_drain_member_delete_forbidden",
    "trg_actor_v2_ingress_drain_ack_insert_guard",
    "trg_actor_v2_ingress_drain_ack_immutable",
    "trg_actor_v2_ingress_drain_ack_delete_forbidden",
)

_INGRESS_DRAIN_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_INGRESS_DRAIN_SUMMARY_CODE_PATTERN = re.compile(r"[a-z][a-z0-9_.:-]{0,127}")


def _migrate_actor_v2_ingress_drain_schema(conn: sqlite3.Connection) -> None:
    """Validate the immutable, fail-closed cross-process drain data model.

    These rows are intentionally dormant until adapter lifecycles register and
    service requests.  Still, malformed historic membership must prevent
    startup rather than silently shrinking a future request's coverage set.
    """

    for table_name, required_columns in _INGRESS_DRAIN_TABLE_COLUMNS.items():
        columns = _table_columns(conn, table_name)
        if not columns:
            raise sqlite3.IntegrityError(f"Actor v2 ingress drain table is missing: {table_name}")
        missing_columns = required_columns.difference(columns)
        if missing_columns:
            raise sqlite3.IntegrityError(
                f"Actor v2 ingress drain table lacks columns: {table_name}: "
                + ", ".join(sorted(missing_columns))
            )
        create_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if f"CREATE TABLE IF NOT EXISTS {table_name}" in statement
        )
        _require_exact_table_schema(
            conn,
            table_name=table_name,
            create_statement=create_statement,
            label="Actor v2 ingress drain",
        )
    for index_name, table_name in _INGRESS_DRAIN_INDEXES:
        create_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if f"CREATE INDEX IF NOT EXISTS {index_name}" in statement
        )
        _ensure_exact_index(
            conn,
            index_name=index_name,
            expected_table=table_name,
            create_statement=create_statement,
            label="Actor v2 ingress drain",
        )
    _validate_actor_v2_ingress_drain_rows(conn)
    _replace_actor_v2_ingress_drain_triggers(conn)


def _validate_actor_v2_ingress_drain_rows(conn: sqlite3.Connection) -> None:
    """Reject impossible participant snapshots and incomplete sealed requests."""

    participant_rows = conn.execute(
        "SELECT * FROM agent_runtime_actor_v2_ingress_participants ORDER BY member_id"
    ).fetchall()
    for row in participant_rows:
        _validate_actor_v2_ingress_participant_row(row)
    request_rows = conn.execute(
        "SELECT * FROM agent_session_actor_v2_ingress_drain_requests ORDER BY request_id"
    ).fetchall()
    for row in request_rows:
        _validate_actor_v2_ingress_drain_request_row(conn, row)


def _validate_actor_v2_ingress_participant_row(row: sqlite3.Row) -> None:
    """Validate one token-free participant row without inferring a lease expiry."""

    try:
        status = str(row["status"])
        participant_epoch = row["participant_epoch"]
        stop_proof_epoch = row["stop_proof_epoch"]
        registered_at = float(row["registered_at"])
        heartbeat_at = float(row["last_heartbeat_at"])
        updated_at = float(row["updated_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError("Actor v2 ingress participant has malformed state") from exc
    if (
        not str(row["member_id"])
        or not str(row["adapter_instance_id"])
        or not str(row["participant_id"])
        or not str(row["holder_token_digest"])
        or isinstance(participant_epoch, bool)
        or not isinstance(participant_epoch, int)
        or participant_epoch < 1
        or isinstance(stop_proof_epoch, bool)
        or not isinstance(stop_proof_epoch, int)
        or status not in {"active", "retired", "revoked"}
        or not math.isfinite(registered_at)
        or not math.isfinite(heartbeat_at)
        or not math.isfinite(updated_at)
        or heartbeat_at < registered_at
        or updated_at < heartbeat_at
    ):
        raise sqlite3.IntegrityError("Actor v2 ingress participant has invalid state")
    retired_at = row["retired_at"]
    revoked_at = row["revoked_at"]
    proof_fields = (
        str(row["stop_proof_issuer_id"]),
        stop_proof_epoch,
        str(row["stop_proof_digest"]),
        str(row["stop_proof_summary_code"]),
    )
    if status == "active":
        valid_terminal_state = (
            retired_at is None
            and revoked_at is None
            and proof_fields == ("", 0, "", "")
        )
    elif status == "retired":
        valid_terminal_state = (
            retired_at is not None
            and revoked_at is None
            and float(retired_at) == updated_at
            and proof_fields == ("", 0, "", "")
        )
    else:
        valid_terminal_state = (
            retired_at is None
            and revoked_at is not None
            and float(revoked_at) == updated_at
            and bool(proof_fields[0])
            and proof_fields[1] >= 1
            and _INGRESS_DRAIN_DIGEST_PATTERN.fullmatch(proof_fields[2]) is not None
            and _INGRESS_DRAIN_SUMMARY_CODE_PATTERN.fullmatch(proof_fields[3]) is not None
        )
    if not valid_terminal_state:
        raise sqlite3.IntegrityError("Actor v2 ingress participant has invalid terminal state")


def _validate_actor_v2_ingress_drain_request_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> None:
    """Validate one sealed request graph and its immutable acknowledgement set."""

    try:
        request_id = str(row["request_id"])
        status = str(row["status"])
        created_at = float(row["created_at"])
        updated_at = float(row["updated_at"])
        cutover_epoch = row["cutover_epoch"]
        generation = row["admission_fence_generation"]
        adapter_values = json.loads(str(row["adapter_instance_ids_json"]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise sqlite3.IntegrityError("Actor v2 ingress drain request has malformed state") from exc
    if (
        not request_id
        or not str(row["cutover_id"])
        or not str(row["profile_id"])
        or not str(row["session_id"])
        or not str(row["legacy_session_id"])
        or not str(row["admission_fence_id"])
        or isinstance(cutover_epoch, bool)
        or not isinstance(cutover_epoch, int)
        or cutover_epoch < 1
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
        or status not in {"open", "drained"}
        or not math.isfinite(created_at)
        or not math.isfinite(updated_at)
        or updated_at < created_at
        or not isinstance(adapter_values, list)
        or not adapter_values
        or any(not isinstance(value, str) or not value for value in adapter_values)
        or adapter_values != sorted(set(adapter_values))
    ):
        raise sqlite3.IntegrityError("Actor v2 ingress drain request has invalid state")
    drained_at = row["drained_at"]
    if status == "open":
        if drained_at is not None:
            raise sqlite3.IntegrityError("open Actor v2 ingress drain retains drained_at")
    elif drained_at is None or float(drained_at) != updated_at:
        raise sqlite3.IntegrityError("drained Actor v2 ingress drain lacks terminal time")
    journal = conn.execute(
        """
        SELECT profile_id, session_id, cutover_epoch, legacy_session_id,
               adapter_instance_ids_json, admission_fence_id,
               admission_fence_generation
        FROM agent_session_actor_v2_cutover_journal
        WHERE cutover_id = ?
        """,
        (str(row["cutover_id"]),),
    ).fetchone()
    if (
        journal is None
        or str(journal["profile_id"]) != str(row["profile_id"])
        or str(journal["session_id"]) != str(row["session_id"])
        or int(journal["cutover_epoch"]) != cutover_epoch
        or str(journal["legacy_session_id"]) != str(row["legacy_session_id"])
        or str(journal["adapter_instance_ids_json"])
        != str(row["adapter_instance_ids_json"])
        or str(journal["admission_fence_id"]) != str(row["admission_fence_id"])
        or int(journal["admission_fence_generation"]) != generation
    ):
        raise sqlite3.IntegrityError("ingress drain request no longer matches cutover identity")
    members = conn.execute(
        """
        SELECT member.member_id, member.adapter_instance_id, member.participant_id,
               member.participant_epoch, participant.adapter_instance_id AS current_adapter,
               participant.participant_id AS current_participant,
               participant.participant_epoch AS current_epoch
        FROM agent_session_actor_v2_ingress_drain_members AS member
        LEFT JOIN agent_runtime_actor_v2_ingress_participants AS participant
          ON participant.member_id = member.member_id
        WHERE member.request_id = ?
        ORDER BY member.member_id
        """,
        (request_id,),
    ).fetchall()
    if not members:
        raise sqlite3.IntegrityError("ingress drain request has no member snapshot")
    covered_adapters: set[str] = set()
    member_ids: set[str] = set()
    for member in members:
        member_id = str(member["member_id"])
        if (
            not member_id
            or member_id in member_ids
            or member["current_adapter"] is None
            or str(member["adapter_instance_id"]) != str(member["current_adapter"])
            or str(member["participant_id"]) != str(member["current_participant"])
            or int(member["participant_epoch"]) != int(member["current_epoch"])
        ):
            raise sqlite3.IntegrityError("ingress drain member snapshot is invalid")
        member_ids.add(member_id)
        covered_adapters.add(str(member["adapter_instance_id"]))
    if not set(adapter_values).issubset(covered_adapters):
        raise sqlite3.IntegrityError("ingress drain request lacks adapter coverage")
    acknowledgements = conn.execute(
        """
        SELECT member_id, adapter_pause_digest, legacy_quiescence_digest,
               proof_epoch, summary_code, acknowledged_at
        FROM agent_session_actor_v2_ingress_drain_acknowledgements
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchall()
    acknowledged_ids: set[str] = set()
    for acknowledgement in acknowledgements:
        member_id = str(acknowledgement["member_id"])
        proof_epoch = acknowledgement["proof_epoch"]
        acknowledged_at = float(acknowledgement["acknowledged_at"])
        if (
            member_id not in member_ids
            or member_id in acknowledged_ids
            or isinstance(proof_epoch, bool)
            or not isinstance(proof_epoch, int)
            or proof_epoch < 1
            or not math.isfinite(acknowledged_at)
            or acknowledged_at < created_at
            or acknowledged_at > updated_at
            or _INGRESS_DRAIN_DIGEST_PATTERN.fullmatch(
                str(acknowledgement["adapter_pause_digest"])
            )
            is None
            or _INGRESS_DRAIN_DIGEST_PATTERN.fullmatch(
                str(acknowledgement["legacy_quiescence_digest"])
            )
            is None
            or _INGRESS_DRAIN_SUMMARY_CODE_PATTERN.fullmatch(
                str(acknowledgement["summary_code"])
            )
            is None
        ):
            raise sqlite3.IntegrityError("ingress drain acknowledgement is invalid")
        acknowledged_ids.add(member_id)
    if status == "drained" and acknowledged_ids != member_ids:
        raise sqlite3.IntegrityError("drained ingress request lacks member acknowledgement")


def _replace_actor_v2_ingress_drain_triggers(conn: sqlite3.Connection) -> None:
    """Install SQL guards against membership shrinkage and receipt mutation."""

    for trigger_name in _INGRESS_DRAIN_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_participant_initial_state
        BEFORE INSERT ON agent_runtime_actor_v2_ingress_participants
        WHEN NEW.status != 'active'
          OR NEW.registered_at != NEW.last_heartbeat_at
          OR NEW.last_heartbeat_at != NEW.updated_at
          OR NEW.retired_at IS NOT NULL
          OR NEW.revoked_at IS NOT NULL
          OR NEW.stop_proof_issuer_id != ''
          OR NEW.stop_proof_epoch != 0
          OR NEW.stop_proof_digest != ''
          OR NEW.stop_proof_summary_code != ''
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress participant must begin active');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_participant_identity_immutable
        BEFORE UPDATE OF member_id, adapter_instance_id, participant_id,
                         participant_epoch, holder_token_digest, registered_at
        ON agent_runtime_actor_v2_ingress_participants
        WHEN NEW.member_id != OLD.member_id
          OR NEW.adapter_instance_id != OLD.adapter_instance_id
          OR NEW.participant_id != OLD.participant_id
          OR NEW.participant_epoch != OLD.participant_epoch
          OR NEW.holder_token_digest != OLD.holder_token_digest
          OR NEW.registered_at != OLD.registered_at
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress participant identity is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_participant_lifecycle
        BEFORE UPDATE ON agent_runtime_actor_v2_ingress_participants
        WHEN NOT (
            NEW.updated_at >= OLD.updated_at
            AND (
                (OLD.status = 'active'
                 AND NEW.status = 'active'
                 AND NEW.last_heartbeat_at >= OLD.last_heartbeat_at
                 AND NEW.retired_at IS NULL
                 AND NEW.revoked_at IS NULL
                 AND NEW.stop_proof_issuer_id = ''
                 AND NEW.stop_proof_epoch = 0
                 AND NEW.stop_proof_digest = ''
                 AND NEW.stop_proof_summary_code = '')
                OR
                (OLD.status = 'active'
                 AND NEW.status = 'retired'
                 AND NEW.last_heartbeat_at = OLD.last_heartbeat_at
                 AND NEW.retired_at = NEW.updated_at
                 AND NEW.revoked_at IS NULL
                 AND NEW.stop_proof_issuer_id = ''
                 AND NEW.stop_proof_epoch = 0
                 AND NEW.stop_proof_digest = ''
                 AND NEW.stop_proof_summary_code = '')
                OR
                (OLD.status = 'active'
                 AND NEW.status = 'revoked'
                 AND NEW.last_heartbeat_at = OLD.last_heartbeat_at
                 AND NEW.retired_at IS NULL
                 AND NEW.revoked_at = NEW.updated_at
                 AND NEW.stop_proof_issuer_id != ''
                 AND NEW.stop_proof_epoch >= 1
                 AND NEW.stop_proof_digest != ''
                 AND NEW.stop_proof_summary_code != '')
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid Actor v2 ingress participant transition');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_participant_delete_forbidden
        BEFORE DELETE ON agent_runtime_actor_v2_ingress_participants
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress participant history cannot be deleted');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_request_initial_state
        BEFORE INSERT ON agent_session_actor_v2_ingress_drain_requests
        WHEN NEW.status != 'assembling' OR NEW.drained_at IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain request must begin assembling');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_request_identity_immutable
        BEFORE UPDATE OF request_id, cutover_id, cutover_epoch, profile_id, session_id,
                         legacy_session_id, adapter_instance_ids_json,
                         admission_fence_id, admission_fence_generation, created_at
        ON agent_session_actor_v2_ingress_drain_requests
        WHEN NEW.request_id != OLD.request_id
          OR NEW.cutover_id != OLD.cutover_id
          OR NEW.cutover_epoch != OLD.cutover_epoch
          OR NEW.profile_id != OLD.profile_id
          OR NEW.session_id != OLD.session_id
          OR NEW.legacy_session_id != OLD.legacy_session_id
          OR NEW.adapter_instance_ids_json != OLD.adapter_instance_ids_json
          OR NEW.admission_fence_id != OLD.admission_fence_id
          OR NEW.admission_fence_generation != OLD.admission_fence_generation
          OR NEW.created_at != OLD.created_at
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain request identity is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_request_lifecycle
        BEFORE UPDATE ON agent_session_actor_v2_ingress_drain_requests
        WHEN NOT (
            NEW.updated_at >= OLD.updated_at
            AND (
                (OLD.status = 'assembling'
                 AND NEW.status = 'open'
                 AND NEW.drained_at IS NULL
                 AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_ingress_drain_members AS member
                    WHERE member.request_id = OLD.request_id
                 ))
                OR
                (OLD.status = 'open'
                 AND NEW.status = 'open'
                 AND NEW.drained_at IS NULL)
                OR
                (OLD.status = 'open'
                 AND NEW.status = 'drained'
                 AND NEW.drained_at = NEW.updated_at
                 AND NOT EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_ingress_drain_members AS member
                    LEFT JOIN agent_session_actor_v2_ingress_drain_acknowledgements
                      AS acknowledgement
                      ON acknowledgement.request_id = member.request_id
                     AND acknowledgement.member_id = member.member_id
                    WHERE member.request_id = OLD.request_id
                      AND acknowledgement.member_id IS NULL
                 ))
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid Actor v2 ingress drain request transition');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_request_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_ingress_drain_requests
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain request history cannot be deleted');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_member_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_ingress_drain_members
        WHEN NOT (
            EXISTS (
                SELECT 1
                FROM agent_session_actor_v2_ingress_drain_requests AS request
                WHERE request.request_id = NEW.request_id
                  AND request.status = 'assembling'
            )
            AND EXISTS (
                SELECT 1
                FROM agent_runtime_actor_v2_ingress_participants AS participant
                WHERE participant.member_id = NEW.member_id
                  AND participant.adapter_instance_id = NEW.adapter_instance_id
                  AND participant.participant_id = NEW.participant_id
                  AND participant.participant_epoch = NEW.participant_epoch
                  AND participant.status = 'active'
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid Actor v2 ingress drain member snapshot');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_member_immutable
        BEFORE UPDATE ON agent_session_actor_v2_ingress_drain_members
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain member is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_member_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_ingress_drain_members
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain member history cannot be deleted');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_ack_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_ingress_drain_acknowledgements
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_session_actor_v2_ingress_drain_requests AS request
            WHERE request.request_id = NEW.request_id
              AND request.status = 'open'
        )
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain request is not open for acknowledgement');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_ack_immutable
        BEFORE UPDATE ON agent_session_actor_v2_ingress_drain_acknowledgements
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain acknowledgement is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_ingress_drain_ack_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_ingress_drain_acknowledgements
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 ingress drain acknowledgement history cannot be deleted');
        END
        """
    )


_CORE_INGRESS_DRAIN_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "agent_session_actor_v2_core_ingress_drain_requests": frozenset(
        {
            "request_id",
            "barrier_id",
            "profile_id",
            "session_id",
            "legacy_session_id",
            "adapter_instance_ids_json",
            "source_generation",
            "migration_generation",
            "status",
            "created_at",
            "updated_at",
            "drained_at",
        }
    ),
    "agent_session_actor_v2_core_ingress_drain_members": frozenset(
        {
            "request_id",
            "member_id",
            "adapter_instance_id",
            "participant_id",
            "participant_epoch",
        }
    ),
    "agent_session_actor_v2_core_ingress_drain_acknowledgements": frozenset(
        {
            "request_id",
            "member_id",
            "core_ingress_digest",
            "legacy_quiescence_digest",
            "proof_epoch",
            "summary_code",
            "acknowledged_at",
        }
    ),
}

_CORE_INGRESS_DRAIN_INDEXES = (
    (
        "idx_actor_v2_core_ingress_drain_requests_status",
        "agent_session_actor_v2_core_ingress_drain_requests",
    ),
    (
        "idx_actor_v2_core_ingress_drain_members_adapter",
        "agent_session_actor_v2_core_ingress_drain_members",
    ),
    (
        "idx_actor_v2_core_ingress_drain_members_participant",
        "agent_session_actor_v2_core_ingress_drain_members",
    ),
    (
        "idx_actor_v2_core_ingress_drain_acknowledgements_request",
        "agent_session_actor_v2_core_ingress_drain_acknowledgements",
    ),
)

_CORE_INGRESS_DRAIN_TRIGGER_NAMES = (
    "trg_actor_v2_core_ingress_drain_request_initial_state",
    "trg_actor_v2_core_ingress_drain_request_identity_immutable",
    "trg_actor_v2_core_ingress_drain_request_lifecycle",
    "trg_actor_v2_core_ingress_drain_request_delete_forbidden",
    "trg_actor_v2_core_ingress_drain_member_insert_guard",
    "trg_actor_v2_core_ingress_drain_member_immutable",
    "trg_actor_v2_core_ingress_drain_member_delete_forbidden",
    "trg_actor_v2_core_ingress_drain_ack_insert_guard",
    "trg_actor_v2_core_ingress_drain_ack_immutable",
    "trg_actor_v2_core_ingress_drain_ack_delete_forbidden",
)


def _migrate_actor_v2_core_ingress_drain_schema(conn: sqlite3.Connection) -> None:
    """Validate immutable barrier-bound core-ingress drain history.

    The request is a durable proof boundary, not a timeout-based lease.  A
    malformed member or acknowledgement can therefore not be repaired by
    shrinking the snapshot during startup.
    """

    for table_name, required_columns in _CORE_INGRESS_DRAIN_TABLE_COLUMNS.items():
        columns = _table_columns(conn, table_name)
        if not columns:
            raise sqlite3.IntegrityError(
                f"Actor v2 core ingress drain table is missing: {table_name}"
            )
        missing_columns = required_columns.difference(columns)
        if missing_columns:
            raise sqlite3.IntegrityError(
                f"Actor v2 core ingress drain table lacks columns: {table_name}: "
                + ", ".join(sorted(missing_columns))
            )
        create_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if f"CREATE TABLE IF NOT EXISTS {table_name}" in statement
        )
        _require_exact_table_schema(
            conn,
            table_name=table_name,
            create_statement=create_statement,
            label="Actor v2 core ingress drain",
        )
    for index_name, table_name in _CORE_INGRESS_DRAIN_INDEXES:
        create_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if f"CREATE INDEX IF NOT EXISTS {index_name}" in statement
        )
        _ensure_exact_index(
            conn,
            index_name=index_name,
            expected_table=table_name,
            create_statement=create_statement,
            label="Actor v2 core ingress drain",
        )
    _validate_actor_v2_core_ingress_drain_rows(conn)
    _replace_actor_v2_core_ingress_drain_triggers(conn)


def _validate_actor_v2_core_ingress_drain_rows(conn: sqlite3.Connection) -> None:
    """Reject core requests that lost their barrier, coverage, or receipts."""

    rows = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_core_ingress_drain_requests
        ORDER BY request_id
        """
    ).fetchall()
    for row in rows:
        _validate_actor_v2_core_ingress_drain_request_row(conn, row)


def _validate_actor_v2_core_ingress_drain_request_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> None:
    """Validate one complete core drain graph against its live barrier identity."""

    try:
        request_id = str(row["request_id"])
        status = str(row["status"])
        created_at = float(row["created_at"])
        updated_at = float(row["updated_at"])
        source_generation = row["source_generation"]
        migration_generation = row["migration_generation"]
        adapter_values = json.loads(str(row["adapter_instance_ids_json"]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise sqlite3.IntegrityError(
            "Actor v2 core ingress drain request has malformed state"
        ) from exc
    if (
        not request_id
        or not str(row["barrier_id"])
        or not str(row["profile_id"])
        or not str(row["session_id"])
        or not str(row["legacy_session_id"])
        or isinstance(source_generation, bool)
        or not isinstance(source_generation, int)
        or source_generation < 1
        or isinstance(migration_generation, bool)
        or not isinstance(migration_generation, int)
        or migration_generation != source_generation + 1
        or status not in {"open", "drained"}
        or not math.isfinite(created_at)
        or not math.isfinite(updated_at)
        or updated_at < created_at
        or not isinstance(adapter_values, list)
        or not adapter_values
        or any(not isinstance(value, str) or not value for value in adapter_values)
        or adapter_values != sorted(set(adapter_values))
    ):
        raise sqlite3.IntegrityError("Actor v2 core ingress drain request has invalid state")
    drained_at = row["drained_at"]
    if status == "open":
        if drained_at is not None:
            raise sqlite3.IntegrityError("open core ingress drain retains drained_at")
    elif drained_at is None or float(drained_at) != updated_at:
        raise sqlite3.IntegrityError("drained core ingress drain lacks terminal time")
    barrier = conn.execute(
        """
        SELECT profile_id, session_id, legacy_session_id, adapter_instance_ids_json,
               source_generation, migration_generation, status
        FROM agent_session_actor_v2_migration_barriers
        WHERE barrier_id = ?
        """,
        (str(row["barrier_id"]),),
    ).fetchone()
    if (
        barrier is None
        or str(barrier["status"]) != "migrating"
        or str(barrier["profile_id"]) != str(row["profile_id"])
        or str(barrier["session_id"]) != str(row["session_id"])
        or str(barrier["legacy_session_id"]) != str(row["legacy_session_id"])
        or str(barrier["adapter_instance_ids_json"])
        != str(row["adapter_instance_ids_json"])
        or int(barrier["source_generation"]) != source_generation
        or int(barrier["migration_generation"]) != migration_generation
    ):
        raise sqlite3.IntegrityError(
            "core ingress drain request no longer matches an active migration barrier"
        )
    members = conn.execute(
        """
        SELECT member.member_id, member.adapter_instance_id, member.participant_id,
               member.participant_epoch, participant.adapter_instance_id AS current_adapter,
               participant.participant_id AS current_participant,
               participant.participant_epoch AS current_epoch
        FROM agent_session_actor_v2_core_ingress_drain_members AS member
        LEFT JOIN agent_runtime_actor_v2_ingress_participants AS participant
          ON participant.member_id = member.member_id
        WHERE member.request_id = ?
        ORDER BY member.member_id
        """,
        (request_id,),
    ).fetchall()
    if not members:
        raise sqlite3.IntegrityError("core ingress drain request has no member snapshot")
    covered_adapters: set[str] = set()
    member_ids: set[str] = set()
    for member in members:
        member_id = str(member["member_id"])
        if (
            not member_id
            or member_id in member_ids
            or member["current_adapter"] is None
            or str(member["adapter_instance_id"]) != str(member["current_adapter"])
            or str(member["participant_id"]) != str(member["current_participant"])
            or int(member["participant_epoch"]) != int(member["current_epoch"])
        ):
            raise sqlite3.IntegrityError("core ingress drain member snapshot is invalid")
        member_ids.add(member_id)
        covered_adapters.add(str(member["adapter_instance_id"]))
    if not set(adapter_values).issubset(covered_adapters):
        raise sqlite3.IntegrityError("core ingress drain request lacks adapter coverage")
    acknowledgements = conn.execute(
        """
        SELECT member_id, core_ingress_digest, legacy_quiescence_digest,
               proof_epoch, summary_code, acknowledged_at
        FROM agent_session_actor_v2_core_ingress_drain_acknowledgements
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchall()
    acknowledged_ids: set[str] = set()
    for acknowledgement in acknowledgements:
        member_id = str(acknowledgement["member_id"])
        proof_epoch = acknowledgement["proof_epoch"]
        acknowledged_at = float(acknowledgement["acknowledged_at"])
        if (
            member_id not in member_ids
            or member_id in acknowledged_ids
            or isinstance(proof_epoch, bool)
            or not isinstance(proof_epoch, int)
            or proof_epoch < 1
            or not math.isfinite(acknowledged_at)
            or acknowledged_at < created_at
            or acknowledged_at > updated_at
            or _INGRESS_DRAIN_DIGEST_PATTERN.fullmatch(
                str(acknowledgement["core_ingress_digest"])
            )
            is None
            or _INGRESS_DRAIN_DIGEST_PATTERN.fullmatch(
                str(acknowledgement["legacy_quiescence_digest"])
            )
            is None
            or _INGRESS_DRAIN_SUMMARY_CODE_PATTERN.fullmatch(
                str(acknowledgement["summary_code"])
            )
            is None
        ):
            raise sqlite3.IntegrityError("core ingress drain acknowledgement is invalid")
        acknowledged_ids.add(member_id)
    if status == "drained" and acknowledged_ids != member_ids:
        raise sqlite3.IntegrityError(
            "drained core ingress request lacks member acknowledgement"
        )


def _replace_actor_v2_core_ingress_drain_triggers(conn: sqlite3.Connection) -> None:
    """Install SQL guards against core-drain mutation or coverage shrinkage."""

    for trigger_name in _CORE_INGRESS_DRAIN_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_request_initial_state
        BEFORE INSERT ON agent_session_actor_v2_core_ingress_drain_requests
        WHEN NEW.status != 'assembling' OR NEW.drained_at IS NOT NULL
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain request must begin assembling');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_request_identity_immutable
        BEFORE UPDATE OF request_id, barrier_id, profile_id, session_id,
                         legacy_session_id, adapter_instance_ids_json,
                         source_generation, migration_generation, created_at
        ON agent_session_actor_v2_core_ingress_drain_requests
        WHEN NEW.request_id != OLD.request_id
          OR NEW.barrier_id != OLD.barrier_id
          OR NEW.profile_id != OLD.profile_id
          OR NEW.session_id != OLD.session_id
          OR NEW.legacy_session_id != OLD.legacy_session_id
          OR NEW.adapter_instance_ids_json != OLD.adapter_instance_ids_json
          OR NEW.source_generation != OLD.source_generation
          OR NEW.migration_generation != OLD.migration_generation
          OR NEW.created_at != OLD.created_at
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain request identity is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_request_lifecycle
        BEFORE UPDATE ON agent_session_actor_v2_core_ingress_drain_requests
        WHEN NOT (
            NEW.updated_at >= OLD.updated_at
            AND (
                (OLD.status = 'assembling'
                 AND NEW.status = 'open'
                 AND NEW.drained_at IS NULL
                 AND EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_core_ingress_drain_members AS member
                    WHERE member.request_id = OLD.request_id
                 ))
                OR
                (OLD.status = 'open'
                 AND NEW.status = 'open'
                 AND NEW.drained_at IS NULL)
                OR
                (OLD.status = 'open'
                 AND NEW.status = 'drained'
                 AND NEW.drained_at = NEW.updated_at
                 AND NOT EXISTS (
                    SELECT 1
                    FROM agent_session_actor_v2_core_ingress_drain_members AS member
                    LEFT JOIN agent_session_actor_v2_core_ingress_drain_acknowledgements
                      AS acknowledgement
                      ON acknowledgement.request_id = member.request_id
                     AND acknowledgement.member_id = member.member_id
                    WHERE member.request_id = OLD.request_id
                      AND acknowledgement.member_id IS NULL
                 ))
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid Actor v2 core ingress drain request transition');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_request_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_core_ingress_drain_requests
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain request history cannot be deleted');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_member_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_core_ingress_drain_members
        WHEN NOT (
            EXISTS (
                SELECT 1
                FROM agent_session_actor_v2_core_ingress_drain_requests AS request
                WHERE request.request_id = NEW.request_id
                  AND request.status = 'assembling'
            )
            AND EXISTS (
                SELECT 1
                FROM agent_runtime_actor_v2_ingress_participants AS participant
                WHERE participant.member_id = NEW.member_id
                  AND participant.adapter_instance_id = NEW.adapter_instance_id
                  AND participant.participant_id = NEW.participant_id
                  AND participant.participant_epoch = NEW.participant_epoch
                  AND participant.status = 'active'
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid Actor v2 core ingress drain member snapshot');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_member_immutable
        BEFORE UPDATE ON agent_session_actor_v2_core_ingress_drain_members
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain member is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_member_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_core_ingress_drain_members
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain member history cannot be deleted');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_ack_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_core_ingress_drain_acknowledgements
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_session_actor_v2_core_ingress_drain_requests AS request
            WHERE request.request_id = NEW.request_id
              AND request.status = 'open'
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 core ingress drain request is not open for acknowledgement'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_ack_immutable
        BEFORE UPDATE ON agent_session_actor_v2_core_ingress_drain_acknowledgements
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain acknowledgement is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_core_ingress_drain_ack_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_core_ingress_drain_acknowledgements
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 core ingress drain acknowledgement history cannot be deleted');
        END
        """
    )


_LEGACY_STATE_HANDOFF_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "agent_session_actor_v2_legacy_state_handoff_manifests": frozenset(
        {
            "manifest_id",
            "barrier_id",
            "core_ingress_drain_request_id",
            "profile_id",
            "session_id",
            "legacy_session_id",
            "source_generation",
            "migration_generation",
            "manifest_version",
            "scope_json",
            "source_payload_json",
            "core_ingress_digest",
            "legacy_quiescence_digest",
            "source_digest",
            "captured_at",
        }
    ),
    "agent_session_actor_v2_legacy_state_handoff_materializations": frozenset(
        {
            "manifest_id",
            "materializer_id",
            "materializer_version",
            "target_schema_version",
            "source_digest",
            "target_payload_json",
            "target_digest",
            "materialized_at",
        }
    ),
    "agent_session_actor_v2_legacy_state_handoff_finalizations": frozenset(
        {
            "barrier_id",
            "manifest_id",
            "materializer_id",
            "materializer_version",
            "target_schema_version",
            "source_digest",
            "target_digest",
            "ownership_generation",
            "completion_reason",
            "requested_by",
            "completed_at",
        }
    ),
}

_LEGACY_STATE_HANDOFF_INDEXES = (
    (
        "idx_actor_v2_legacy_state_handoff_manifest_source",
        "agent_session_actor_v2_legacy_state_handoff_manifests",
    ),
    (
        "idx_actor_v2_legacy_state_handoff_materializations_manifest",
        "agent_session_actor_v2_legacy_state_handoff_materializations",
    ),
    (
        "idx_actor_v2_legacy_state_handoff_finalizations_owner",
        "agent_session_actor_v2_legacy_state_handoff_finalizations",
    ),
)

_LEGACY_STATE_HANDOFF_TRIGGER_NAMES = (
    "trg_actor_v2_legacy_state_handoff_manifest_insert_guard",
    "trg_actor_v2_legacy_state_handoff_manifest_immutable",
    "trg_actor_v2_legacy_state_handoff_manifest_delete_forbidden",
    "trg_actor_v2_legacy_state_handoff_materialization_insert_guard",
    "trg_actor_v2_legacy_state_handoff_materialization_immutable",
    "trg_actor_v2_legacy_state_handoff_materialization_delete_forbidden",
    "trg_actor_v2_legacy_state_handoff_finalization_insert_guard",
    "trg_actor_v2_legacy_state_handoff_finalization_immutable",
    "trg_actor_v2_legacy_state_handoff_finalization_delete_forbidden",
)


def _migrate_actor_v2_legacy_state_handoff_schema(conn: sqlite3.Connection) -> None:
    """Validate immutable source manifests and staged target materializations."""

    for table_name, required_columns in _LEGACY_STATE_HANDOFF_TABLE_COLUMNS.items():
        columns = _table_columns(conn, table_name)
        if not columns:
            raise sqlite3.IntegrityError(
                f"Actor v2 legacy state handoff table is missing: {table_name}"
            )
        missing_columns = required_columns.difference(columns)
        if missing_columns:
            raise sqlite3.IntegrityError(
                f"Actor v2 legacy state handoff table lacks columns: {table_name}: "
                + ", ".join(sorted(missing_columns))
            )
        create_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if f"CREATE TABLE IF NOT EXISTS {table_name}" in statement
        )
        _require_exact_table_schema(
            conn,
            table_name=table_name,
            create_statement=create_statement,
            label="Actor v2 legacy state handoff",
        )
    for index_name, table_name in _LEGACY_STATE_HANDOFF_INDEXES:
        create_statement = next(
            statement
            for statement in SCHEMA_STATEMENTS
            if f"CREATE INDEX IF NOT EXISTS {index_name}" in statement
        )
        _ensure_exact_index(
            conn,
            index_name=index_name,
            expected_table=table_name,
            create_statement=create_statement,
            label="Actor v2 legacy state handoff",
        )
    manifest_rows = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_manifests
        ORDER BY manifest_id
        """
    ).fetchall()
    for row in manifest_rows:
        _validate_actor_v2_legacy_state_handoff_manifest_row(conn, row)
    materialization_rows = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_materializations
        ORDER BY manifest_id, materializer_id, materializer_version
        """
    ).fetchall()
    for row in materialization_rows:
        _validate_actor_v2_legacy_state_handoff_materialization_row(conn, row)
    finalization_rows = conn.execute(
        """
        SELECT *
        FROM agent_session_actor_v2_legacy_state_handoff_finalizations
        ORDER BY barrier_id
        """
    ).fetchall()
    for row in finalization_rows:
        _validate_actor_v2_legacy_state_handoff_finalization_row(conn, row)
    _replace_actor_v2_legacy_state_handoff_triggers(conn)


def _validate_actor_v2_legacy_state_handoff_manifest_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> None:
    """Reject a manifest detached from its drained barrier or canonical payload."""

    try:
        manifest_id = str(row["manifest_id"])
        barrier_id = str(row["barrier_id"])
        request_id = str(row["core_ingress_drain_request_id"])
        legacy_session_id = str(row["legacy_session_id"])
        source_generation = row["source_generation"]
        migration_generation = row["migration_generation"]
        manifest_version = row["manifest_version"]
        captured_at = float(row["captured_at"])
        scope_validation = validate_canonical_json_object(str(row["scope_json"]))
        payload_validation = validate_canonical_json_object(
            str(row["source_payload_json"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff manifest has malformed state"
        ) from exc
    if (
        not manifest_id
        or not barrier_id
        or not request_id
        or not str(row["profile_id"])
        or not str(row["session_id"])
        or not legacy_session_id
        or isinstance(source_generation, bool)
        or not isinstance(source_generation, int)
        or source_generation < 1
        or isinstance(migration_generation, bool)
        or not isinstance(migration_generation, int)
        or migration_generation != source_generation + 1
        or isinstance(manifest_version, bool)
        or not isinstance(manifest_version, int)
        or manifest_version < 1
        or not math.isfinite(captured_at)
        or scope_validation.payload is None
        or scope_validation.violations
        or payload_validation.payload is None
        or payload_validation.violations
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(row[field_name])) is None
            for field_name in (
                "core_ingress_digest",
                "legacy_quiescence_digest",
                "source_digest",
            )
        )
    ):
        raise sqlite3.IntegrityError("Actor v2 legacy state handoff manifest has invalid state")
    scope = scope_validation.payload
    source_payload = payload_validation.payload
    assert scope is not None
    assert source_payload is not None
    if manifest_version == 1:
        members = scope.get("members")
        if (
            set(scope) != {"legacy_session_id", "members"}
            or scope.get("legacy_session_id") != legacy_session_id
            or not isinstance(members, list)
            or len(members) != 1
            or members[0]
            != {
                "profile_id": str(row["profile_id"]),
                "session_id": str(row["session_id"]),
            }
            or set(source_payload)
            != {
                "schema_version",
                "scheduler_state",
                "unread_messages",
                "route_deliveries",
                "unread_ranges",
                "high_priority_events",
                "recent_mentions",
                "review_summaries",
                "summaries",
            }
            or source_payload.get("schema_version") != 1
            or (
                source_payload.get("scheduler_state") is not None
                and not isinstance(source_payload.get("scheduler_state"), dict)
            )
            or any(
                not isinstance(source_payload.get(section), list)
                or any(not isinstance(item, dict) for item in source_payload[section])
                for section in (
                    "unread_messages",
                    "route_deliveries",
                    "unread_ranges",
                    "high_priority_events",
                    "recent_mentions",
                    "review_summaries",
                    "summaries",
                )
            )
        ):
            raise sqlite3.IntegrityError(
                "Actor v2 v1 legacy state handoff manifest has invalid payload shape"
            )
    drain = conn.execute(
        """
        SELECT barrier_id, profile_id, session_id, legacy_session_id,
               source_generation, migration_generation, status
        FROM agent_session_actor_v2_core_ingress_drain_requests
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchone()
    if (
        drain is None
        or str(drain["barrier_id"]) != barrier_id
        or str(drain["profile_id"]) != str(row["profile_id"])
        or str(drain["session_id"]) != str(row["session_id"])
        or str(drain["legacy_session_id"]) != legacy_session_id
        or int(drain["source_generation"]) != source_generation
        or int(drain["migration_generation"]) != migration_generation
        or str(drain["status"]) != "drained"
    ):
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff manifest no longer matches drained core ingress"
        )
    expected_digest_payload = {
        "barrier_id": barrier_id,
        "core_ingress_digest": str(row["core_ingress_digest"]),
        "core_ingress_drain_request_id": request_id,
        "legacy_quiescence_digest": str(row["legacy_quiescence_digest"]),
        "legacy_session_id": legacy_session_id,
        "manifest_version": manifest_version,
        "migration_generation": migration_generation,
        "profile_id": str(row["profile_id"]),
        "scope": scope,
        "session_id": str(row["session_id"]),
        "source_generation": source_generation,
        "source_payload": source_payload,
    }
    expected_digest = hashlib.sha256(
        json.dumps(
            expected_digest_payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if str(row["source_digest"]) != expected_digest:
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff manifest source digest is invalid"
        )


def _validate_actor_v2_legacy_state_handoff_materialization_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> None:
    """Reject a staged target that is detached from immutable source content."""

    try:
        target_validation = validate_canonical_json_object(str(row["target_payload_json"]))
        materializer_version = row["materializer_version"]
        target_schema_version = row["target_schema_version"]
        materialized_at = float(row["materialized_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff materialization has malformed state"
        ) from exc
    if (
        not str(row["manifest_id"])
        or not str(row["materializer_id"])
        or isinstance(materializer_version, bool)
        or not isinstance(materializer_version, int)
        or materializer_version < 1
        or isinstance(target_schema_version, bool)
        or not isinstance(target_schema_version, int)
        or target_schema_version < 1
        or not math.isfinite(materialized_at)
        or target_validation.payload is None
        or target_validation.violations
        or re.fullmatch(r"[0-9a-f]{64}", str(row["source_digest"])) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(row["target_digest"])) is None
    ):
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff materialization has invalid state"
        )
    source = conn.execute(
        """
        SELECT source_digest
        FROM agent_session_actor_v2_legacy_state_handoff_manifests
        WHERE manifest_id = ?
        """,
        (str(row["manifest_id"]),),
    ).fetchone()
    if source is None or str(source["source_digest"]) != str(row["source_digest"]):
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff materialization source digest is detached"
        )
    target_payload = target_validation.payload
    assert target_payload is not None
    expected_digest = hashlib.sha256(
        json.dumps(
            {
                "manifest_id": str(row["manifest_id"]),
                "materializer_id": str(row["materializer_id"]),
                "materializer_version": materializer_version,
                "source_digest": str(row["source_digest"]),
                "target_payload": target_payload,
                "target_schema_version": target_schema_version,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    if str(row["target_digest"]) != expected_digest:
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff materialization target digest is invalid"
        )


def _validate_actor_v2_legacy_state_handoff_finalization_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
) -> None:
    """Validate one immutable activation record against its frozen source graph."""

    try:
        barrier_id = str(row["barrier_id"])
        manifest_id = str(row["manifest_id"])
        materializer_id = str(row["materializer_id"])
        materializer_version = row["materializer_version"]
        target_schema_version = row["target_schema_version"]
        ownership_generation = row["ownership_generation"]
        completion_reason = str(row["completion_reason"])
        completed_at = float(row["completed_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff finalization has malformed state"
        ) from exc
    if (
        not barrier_id
        or not manifest_id
        or not materializer_id
        or isinstance(materializer_version, bool)
        or not isinstance(materializer_version, int)
        or materializer_version < 1
        or isinstance(target_schema_version, bool)
        or not isinstance(target_schema_version, int)
        or target_schema_version < 1
        or isinstance(ownership_generation, bool)
        or not isinstance(ownership_generation, int)
        or ownership_generation < 1
        or not completion_reason
        or not math.isfinite(completed_at)
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(row[field_name])) is None
            for field_name in ("source_digest", "target_digest")
        )
    ):
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff finalization has invalid state"
        )
    joined = conn.execute(
        """
        SELECT barrier.profile_id AS barrier_profile_id,
               barrier.session_id AS barrier_session_id,
               barrier.legacy_session_id AS barrier_legacy_session_id,
               barrier.source_generation AS barrier_source_generation,
               barrier.migration_generation AS barrier_migration_generation,
               barrier.status AS barrier_status,
               manifest.barrier_id AS manifest_barrier_id,
               manifest.source_digest AS manifest_source_digest,
               materialization.source_digest AS materialization_source_digest,
               materialization.target_digest AS materialization_target_digest,
               materialization.target_schema_version AS materialization_target_schema_version,
               ownership.legacy_session_id AS ownership_legacy_session_id,
               ownership.mode AS ownership_mode,
               ownership.status AS ownership_status,
               ownership.generation AS ownership_generation
        FROM agent_session_actor_v2_migration_barriers AS barrier
        JOIN agent_session_actor_v2_legacy_state_handoff_manifests AS manifest
          ON manifest.manifest_id = ?
        JOIN agent_session_actor_v2_legacy_state_handoff_materializations AS materialization
          ON materialization.manifest_id = manifest.manifest_id
         AND materialization.materializer_id = ?
         AND materialization.materializer_version = ?
        JOIN agent_session_runtime_ownership AS ownership
          ON ownership.profile_id = barrier.profile_id
         AND ownership.session_id = barrier.session_id
        WHERE barrier.barrier_id = ?
        """,
        (manifest_id, materializer_id, materializer_version, barrier_id),
    ).fetchone()
    if (
        joined is None
        or str(joined["barrier_status"]) != "migrating"
        or str(joined["manifest_barrier_id"]) != barrier_id
        or str(joined["manifest_source_digest"]) != str(row["source_digest"])
        or str(joined["materialization_source_digest"])
        != str(row["source_digest"])
        or str(joined["materialization_target_digest"])
        != str(row["target_digest"])
        or int(joined["materialization_target_schema_version"])
        != target_schema_version
        or ownership_generation != int(joined["barrier_migration_generation"]) + 1
        or str(joined["ownership_legacy_session_id"])
        != str(joined["barrier_legacy_session_id"])
        or str(joined["ownership_mode"]) != "actor_v2"
        or str(joined["ownership_status"]) != "active"
        or int(joined["ownership_generation"]) != ownership_generation
    ):
        raise sqlite3.IntegrityError(
            "Actor v2 legacy state handoff finalization is detached from its completed Actor owner"
        )


def _replace_actor_v2_legacy_state_handoff_triggers(conn: sqlite3.Connection) -> None:
    """Install SQL guards that prevent source or target staging history rewrites."""

    for trigger_name in _LEGACY_STATE_HANDOFF_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_manifest_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_legacy_state_handoff_manifests
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_session_actor_v2_migration_barriers AS barrier
            JOIN agent_session_actor_v2_core_ingress_drain_requests AS drain
              ON drain.barrier_id = barrier.barrier_id
            WHERE barrier.barrier_id = NEW.barrier_id
              AND barrier.status = 'migrating'
              AND drain.request_id = NEW.core_ingress_drain_request_id
              AND drain.status = 'drained'
              AND barrier.profile_id = NEW.profile_id
              AND barrier.session_id = NEW.session_id
              AND barrier.legacy_session_id = NEW.legacy_session_id
              AND barrier.source_generation = NEW.source_generation
              AND barrier.migration_generation = NEW.migration_generation
              AND drain.profile_id = NEW.profile_id
              AND drain.session_id = NEW.session_id
              AND drain.legacy_session_id = NEW.legacy_session_id
              AND drain.source_generation = NEW.source_generation
              AND drain.migration_generation = NEW.migration_generation
        )
        OR NOT json_valid(NEW.scope_json)
        OR NOT json_valid(NEW.source_payload_json)
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid Actor v2 legacy state handoff manifest source boundary'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_manifest_immutable
        BEFORE UPDATE ON agent_session_actor_v2_legacy_state_handoff_manifests
        BEGIN
            SELECT RAISE(ABORT, 'Actor v2 legacy state handoff manifest is immutable');
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_manifest_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_legacy_state_handoff_manifests
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 legacy state handoff manifest history cannot be deleted'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_materialization_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_legacy_state_handoff_materializations
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_session_actor_v2_legacy_state_handoff_manifests AS manifest
            WHERE manifest.manifest_id = NEW.manifest_id
              AND manifest.source_digest = NEW.source_digest
        )
        OR NOT json_valid(NEW.target_payload_json)
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid Actor v2 legacy state handoff materialization source'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_materialization_immutable
        BEFORE UPDATE ON agent_session_actor_v2_legacy_state_handoff_materializations
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 legacy state handoff materialization is immutable'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_materialization_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_legacy_state_handoff_materializations
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 legacy state handoff materialization history cannot be deleted'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_finalization_insert_guard
        BEFORE INSERT ON agent_session_actor_v2_legacy_state_handoff_finalizations
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_session_actor_v2_migration_barriers AS barrier
            JOIN agent_session_actor_v2_legacy_state_handoff_manifests AS manifest
              ON manifest.barrier_id = barrier.barrier_id
            JOIN agent_session_actor_v2_legacy_state_handoff_materializations AS materialization
              ON materialization.manifest_id = manifest.manifest_id
            JOIN agent_session_runtime_ownership AS ownership
              ON ownership.profile_id = manifest.profile_id
             AND ownership.session_id = manifest.session_id
            WHERE barrier.barrier_id = NEW.barrier_id
              AND barrier.status = 'migrating'
              AND manifest.manifest_id = NEW.manifest_id
              AND materialization.materializer_id = NEW.materializer_id
              AND materialization.materializer_version = NEW.materializer_version
              AND materialization.target_schema_version = NEW.target_schema_version
              AND materialization.source_digest = NEW.source_digest
              AND materialization.target_digest = NEW.target_digest
              AND ownership.mode = 'actor_v2'
              AND ownership.status = 'active'
              AND ownership.generation = NEW.ownership_generation
              AND ownership.legacy_session_id = barrier.legacy_session_id
              AND NEW.ownership_generation = barrier.migration_generation + 1
        )
        OR length(NEW.completion_reason) = 0
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid Actor v2 legacy state handoff finalization boundary'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_finalization_immutable
        BEFORE UPDATE ON agent_session_actor_v2_legacy_state_handoff_finalizations
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 legacy state handoff finalization is immutable'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_legacy_state_handoff_finalization_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_legacy_state_handoff_finalizations
        BEGIN
            SELECT RAISE(
                ABORT,
                'Actor v2 legacy state handoff finalization history cannot be deleted'
            );
        END
        """
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
        "ALTER TABLE agent_external_action_attempts RENAME TO agent_external_action_attempts_legacy"
    )
    conn.execute(
        "ALTER TABLE agent_external_action_receipts RENAME TO agent_external_action_receipts_legacy"
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

    for index_name, expected_table, create_statement in _ACTOR_RAW_LOGICAL_KEY_INDEXES:
        _ensure_exact_index(
            conn,
            index_name=index_name,
            expected_table=expected_table,
            create_statement=create_statement,
            label="actor raw logical-key",
        )


def _ensure_manual_review_request_unique_index(conn: sqlite3.Connection) -> None:
    """Ensure manual request idempotency cannot be bypassed by index drift."""

    _assert_manual_review_request_identity_is_unique(conn)
    index_name, expected_table, create_statement = _MANUAL_REVIEW_REQUEST_UNIQUE_INDEX
    _ensure_exact_index(
        conn,
        index_name=index_name,
        expected_table=expected_table,
        create_statement=create_statement,
        label="manual review request",
    )


def _ensure_exact_index(
    conn: sqlite3.Connection,
    *,
    index_name: str,
    expected_table: str,
    create_statement: str,
    label: str,
) -> None:
    """Repair one named index or reject an occupied incompatible name."""

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
        return
    if str(row["type"]) != "index":
        raise sqlite3.IntegrityError(
            f"{label} index name is occupied by another schema object: {index_name}"
        )
    if str(row["tbl_name"]) != expected_table:
        raise sqlite3.IntegrityError(f"{label} index belongs to an unexpected table: {index_name}")
    if row["sql"] is None:
        raise sqlite3.IntegrityError(f"{label} index has no mutable SQL definition: {index_name}")
    if _normalized_create_index_sql(row["sql"]) == _normalized_create_index_sql(create_statement):
        return
    escaped_index_name = index_name.replace('"', '""')
    conn.execute(f'DROP INDEX "{escaped_index_name}"')
    conn.execute(create_statement)


def _assert_manual_review_request_identity_is_unique(
    conn: sqlite3.Connection,
) -> None:
    """Fail with a diagnostic before a legacy duplicate blocks index creation."""

    duplicate = conn.execute(
        """
        SELECT hex(CAST(profile_id AS BLOB)) AS profile_id_hex,
               hex(CAST(session_id AS BLOB)) AS session_id_hex,
               hex(CAST(causation_id AS BLOB)) AS request_id_hex
        FROM agent_session_mailbox
        WHERE CAST(kind AS BLOB) = X'4D616E75616C526576696577526571756573746564'
          AND CAST(source AS BLOB) = X'6D616E75616C5F7265766965775F61646D697373696F6E'
        GROUP BY CAST(profile_id AS BLOB),
                 CAST(session_id AS BLOB),
                 CAST(causation_id AS BLOB)
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if duplicate is not None:
        raise sqlite3.IntegrityError(
            "manual review request unique index cannot be installed: "
            "duplicate raw request identity "
            f"profile_id={duplicate['profile_id_hex']}, "
            f"session_id={duplicate['session_id_hex']}, "
            f"request_id={duplicate['request_id_hex']}"
        )


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
            f"invalid operation input fence: {invalid_input_fence['operation_id']}"
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
    _rebuild_agent_review_execution_runs_constraints(conn)


_LEGACY_RECOVERY_GATE_REQUIRED_COLUMNS = frozenset(
    {
        "gate_id",
        "mode",
        "epoch",
        "holder_id",
        "holder_token_digest",
        "activated_at",
        "updated_at",
    }
)

_LEGACY_RECOVERY_GATE_TRIGGER_NAMES = (
    "trg_agent_runtime_legacy_recovery_gate_delete_forbidden",
    "trg_agent_runtime_legacy_recovery_gate_fenced_only_irreversible",
)


def _migrate_legacy_recovery_gate_schema(conn: sqlite3.Connection) -> None:
    """Seed or conservatively fence the global legacy-recovery interlock.

    This runs after mailbox handoff backfill.  A database with any prior Actor
    admission, mailbox sidecar, or actor ownership cannot prove that a legacy
    key-only recovery is still safe, so it starts in the irreversible
    ``fenced_only`` mode.  Only a provably unused database receives
    ``legacy_open``.
    """

    columns = _table_columns(conn, "agent_runtime_legacy_recovery_gate")
    if not columns:
        raise sqlite3.IntegrityError("legacy recovery gate table is missing")
    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_runtime_legacy_recovery_gate" in statement
    )
    table_schema = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'agent_runtime_legacy_recovery_gate'
        """
    ).fetchone()
    if table_schema is None or _normalized_create_table_sql(
        table_schema["sql"]
    ) != _normalized_create_table_sql(table_statement):
        raise sqlite3.IntegrityError(
            "legacy recovery gate table does not match its immutable contract"
        )
    missing_columns = _LEGACY_RECOVERY_GATE_REQUIRED_COLUMNS.difference(columns)
    if missing_columns:
        raise sqlite3.IntegrityError(
            "legacy recovery gate lacks required columns: "
            + ", ".join(sorted(missing_columns))
        )
    _replace_legacy_recovery_gate_triggers(conn)

    rows = conn.execute(
        "SELECT * FROM agent_runtime_legacy_recovery_gate ORDER BY gate_id"
    ).fetchall()
    if len(rows) > 1 or (rows and int(rows[0]["gate_id"]) != 1):
        raise sqlite3.IntegrityError("legacy recovery gate singleton is invalid")
    history_exists = _legacy_recovery_gate_history_exists(conn)
    now = time.time()
    if not rows:
        mode = "fenced_only" if history_exists else "legacy_open"
        conn.execute(
            """
            INSERT INTO agent_runtime_legacy_recovery_gate (
                gate_id, mode, epoch, holder_id, holder_token_digest,
                activated_at, updated_at
            ) VALUES (1, ?, 0, '', '', NULL, ?)
            """,
            (mode, now),
        )
        rows = conn.execute(
            "SELECT * FROM agent_runtime_legacy_recovery_gate WHERE gate_id = 1"
        ).fetchall()
    if len(rows) != 1:
        raise sqlite3.IntegrityError("legacy recovery gate singleton is missing")
    row = rows[0]
    _validate_legacy_recovery_gate_row(row)
    if history_exists and str(row["mode"]) == "legacy_open":
        updated = conn.execute(
            """
            UPDATE agent_runtime_legacy_recovery_gate
            SET mode = 'fenced_only',
                epoch = epoch + 1,
                holder_id = '',
                holder_token_digest = '',
                activated_at = NULL,
                updated_at = ?
            WHERE gate_id = 1
              AND mode = 'legacy_open'
            """,
            (now,),
        )
        if updated.rowcount != 1:
            raise sqlite3.IntegrityError(
                "legacy recovery gate changed while historical evidence was fenced"
            )


def _legacy_recovery_gate_history_exists(conn: sqlite3.Connection) -> bool:
    """Return whether this database has non-fresh Actor runtime evidence."""

    for statement in (
        "SELECT 1 FROM agent_session_actor_v2_admission_fences LIMIT 1",
        "SELECT 1 FROM agent_session_actor_v2_cutover_journal LIMIT 1",
        "SELECT 1 FROM agent_session_mailbox_handoffs LIMIT 1",
        "SELECT 1 FROM agent_session_runtime_ownership LIMIT 1",
    ):
        if conn.execute(statement).fetchone() is not None:
            return True
    return False


def _validate_legacy_recovery_gate_row(row: sqlite3.Row) -> None:
    """Reject malformed singleton data rather than silently reopening the gate."""

    try:
        gate_id = row["gate_id"]
        epoch = row["epoch"]
        mode = str(row["mode"])
        holder_id = str(row["holder_id"])
        holder_token_digest = str(row["holder_token_digest"])
        updated_at = float(row["updated_at"])
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError("legacy recovery gate has malformed state") from exc
    if (
        isinstance(gate_id, bool)
        or not isinstance(gate_id, int)
        or gate_id != 1
        or isinstance(epoch, bool)
        or not isinstance(epoch, int)
        or epoch < 0
        or mode not in {"legacy_open", "legacy_recovery_active", "fenced_only"}
        or not math.isfinite(updated_at)
    ):
        raise sqlite3.IntegrityError("legacy recovery gate has invalid state")
    active = mode == "legacy_recovery_active"
    activated_at = row["activated_at"]
    if active:
        if not holder_id or not holder_token_digest or activated_at is None:
            raise sqlite3.IntegrityError("active legacy recovery gate lacks holder state")
        try:
            if not math.isfinite(float(activated_at)):
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise sqlite3.IntegrityError(
                "active legacy recovery gate has invalid activation timestamp"
            ) from exc
    elif holder_id or holder_token_digest or activated_at is not None:
        raise sqlite3.IntegrityError("inactive legacy recovery gate retains holder state")


def _replace_legacy_recovery_gate_triggers(conn: sqlite3.Connection) -> None:
    """Install irreversibility guards for the global recovery interlock."""

    for trigger_name in _LEGACY_RECOVERY_GATE_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_agent_runtime_legacy_recovery_gate_delete_forbidden
        BEFORE DELETE ON agent_runtime_legacy_recovery_gate
        BEGIN
            SELECT RAISE(
                ABORT,
                'legacy recovery gate singleton cannot be deleted'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER
        trg_agent_runtime_legacy_recovery_gate_fenced_only_irreversible
        BEFORE UPDATE OF mode ON agent_runtime_legacy_recovery_gate
        WHEN OLD.mode = 'fenced_only' AND NEW.mode != 'fenced_only'
        BEGIN
            SELECT RAISE(
                ABORT,
                'legacy recovery gate fenced_only mode is irreversible'
            );
        END
        """
    )


_CANARY_ISOLATION_LEASE_REQUIRED_COLUMNS = frozenset(
    {
        "lease_id",
        "lease_epoch",
        "holder_id",
        "holder_token_digest",
        "status",
        "created_at",
        "updated_at",
        "released_at",
        "revoked_at",
        "revocation_reason",
    }
)

_CANARY_ISOLATION_LEASE_TRIGGER_NAMES = (
    "trg_actor_v2_canary_isolation_lease_initial_insert",
    "trg_actor_v2_canary_isolation_lease_lifecycle",
    "trg_actor_v2_canary_isolation_lease_delete_forbidden",
)


def _migrate_actor_v2_canary_isolation_lease_schema(conn: sqlite3.Connection) -> None:
    """Verify the immutable singleton contract for clean-canary isolation.

    Unlike a timed work lease, this row is intentionally absent before first
    use. Its absence is a valid clean-domain state; once created, history is
    retained so a stale holder cannot release or resume a replacement epoch.
    """

    columns = _table_columns(conn, "agent_runtime_actor_v2_canary_isolation_leases")
    if not columns:
        raise sqlite3.IntegrityError("canary isolation lease table is missing")
    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_runtime_actor_v2_canary_isolation_leases"
        in statement
    )
    table_schema = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'agent_runtime_actor_v2_canary_isolation_leases'
        """
    ).fetchone()
    if table_schema is None or _normalized_create_table_sql(
        table_schema["sql"]
    ) != _normalized_create_table_sql(table_statement):
        raise sqlite3.IntegrityError(
            "canary isolation lease table does not match its immutable contract"
        )
    missing_columns = _CANARY_ISOLATION_LEASE_REQUIRED_COLUMNS.difference(columns)
    if missing_columns:
        raise sqlite3.IntegrityError(
            "canary isolation lease table lacks required columns: "
            + ", ".join(sorted(missing_columns))
        )
    rows = conn.execute(
        "SELECT * FROM agent_runtime_actor_v2_canary_isolation_leases ORDER BY lease_id"
    ).fetchall()
    if len(rows) > 1 or (rows and int(rows[0]["lease_id"]) != 1):
        raise sqlite3.IntegrityError("canary isolation lease singleton is invalid")
    if rows:
        _validate_canary_isolation_lease_row(rows[0])
    _replace_canary_isolation_lease_triggers(conn)


def _validate_canary_isolation_lease_row(row: sqlite3.Row) -> None:
    """Reject malformed singleton data instead of reopening isolation implicitly."""

    try:
        lease_id = row["lease_id"]
        lease_epoch = row["lease_epoch"]
        holder_id = str(row["holder_id"])
        holder_token_digest = str(row["holder_token_digest"])
        status = str(row["status"])
        created_at = float(row["created_at"])
        updated_at = float(row["updated_at"])
        released_at = row["released_at"]
        revoked_at = row["revoked_at"]
        reason = str(row["revocation_reason"])
    except (KeyError, TypeError, ValueError) as exc:
        raise sqlite3.IntegrityError("canary isolation lease has malformed state") from exc
    if (
        isinstance(lease_id, bool)
        or not isinstance(lease_id, int)
        or lease_id != 1
        or isinstance(lease_epoch, bool)
        or not isinstance(lease_epoch, int)
        or lease_epoch < 1
        or not holder_id
        or not holder_token_digest
        or status not in {"active", "released", "revoked"}
        or not math.isfinite(created_at)
        or not math.isfinite(updated_at)
        or updated_at < created_at
    ):
        raise sqlite3.IntegrityError("canary isolation lease has invalid state")
    if status == "active":
        valid_terminal_state = released_at is None and revoked_at is None and not reason
    elif status == "released":
        try:
            valid_terminal_state = (
                released_at is not None
                and float(released_at) == updated_at
                and revoked_at is None
                and not reason
            )
        except (TypeError, ValueError):
            valid_terminal_state = False
    else:
        try:
            valid_terminal_state = (
                released_at is None
                and revoked_at is not None
                and float(revoked_at) == updated_at
                and bool(reason)
            )
        except (TypeError, ValueError):
            valid_terminal_state = False
    if not valid_terminal_state:
        raise sqlite3.IntegrityError("canary isolation lease has invalid terminal state")


def _replace_canary_isolation_lease_triggers(conn: sqlite3.Connection) -> None:
    """Prevent stale holders or SQL maintenance from reopening the lease slot."""

    for trigger_name in _CANARY_ISOLATION_LEASE_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_canary_isolation_lease_initial_insert
        BEFORE INSERT ON agent_runtime_actor_v2_canary_isolation_leases
        WHEN NEW.lease_id != 1
          OR NEW.lease_epoch != 1
          OR NEW.status != 'active'
        BEGIN
            SELECT RAISE(
                ABORT,
                'canary isolation lease must begin at active epoch one'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_canary_isolation_lease_lifecycle
        BEFORE UPDATE ON agent_runtime_actor_v2_canary_isolation_leases
        WHEN NOT (
            (
                OLD.status = 'active'
                AND NEW.status = 'released'
                AND NEW.lease_epoch = OLD.lease_epoch
                AND NEW.holder_id = OLD.holder_id
                AND NEW.holder_token_digest = OLD.holder_token_digest
                AND NEW.created_at = OLD.created_at
                AND NEW.updated_at >= OLD.updated_at
                AND NEW.released_at = NEW.updated_at
                AND NEW.revoked_at IS NULL
                AND NEW.revocation_reason = ''
            )
            OR (
                OLD.status = 'active'
                AND NEW.status = 'revoked'
                AND NEW.lease_epoch = OLD.lease_epoch
                AND NEW.holder_id = OLD.holder_id
                AND NEW.holder_token_digest = OLD.holder_token_digest
                AND NEW.created_at = OLD.created_at
                AND NEW.updated_at >= OLD.updated_at
                AND NEW.released_at IS NULL
                AND NEW.revoked_at = NEW.updated_at
                AND NEW.revocation_reason != ''
            )
            OR (
                OLD.status IN ('released', 'revoked')
                AND NEW.status = 'active'
                AND NEW.lease_epoch = OLD.lease_epoch + 1
                AND NEW.created_at >= OLD.updated_at
                AND NEW.updated_at = NEW.created_at
                AND NEW.released_at IS NULL
                AND NEW.revoked_at IS NULL
                AND NEW.revocation_reason = ''
            )
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid canary isolation lease lifecycle transition'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_canary_isolation_lease_delete_forbidden
        BEFORE DELETE ON agent_runtime_actor_v2_canary_isolation_leases
        BEGIN
            SELECT RAISE(
                ABORT,
                'canary isolation lease history cannot be deleted'
            );
        END
        """
    )


_MAILBOX_HANDOFF_REQUIRED_COLUMNS = frozenset(
    {
        "mailbox_id",
        "handoff_id",
        "profile_id",
        "session_id",
        "event_id",
        "ownership_generation",
        "evidence_state",
        "admission_fence_id",
        "admission_fence_generation",
        "state",
        "attempt_count",
        "available_at",
        "claim_id",
        "lease_owner",
        "lease_until",
        "target_id",
        "target_incarnation_id",
        "target_disposition",
        "created_at",
        "updated_at",
        "claimed_at",
        "settled_at",
        "last_error",
    }
)

_MAILBOX_HANDOFF_TRIGGER_NAMES = (
    "trg_agent_session_mailbox_handoff_source_insert",
    "trg_agent_session_mailbox_handoff_source_identity_immutable",
    "trg_agent_session_mailbox_handoff_identity_immutable",
    "trg_agent_session_mailbox_handoff_delete_forbidden",
    "trg_agent_session_mailbox_handoff_initial_state",
    "trg_agent_session_mailbox_handoff_state_transition",
)

_FENCED_WAKE_TARGET_LEASE_REQUIRED_COLUMNS = frozenset(
    {
        "profile_id",
        "session_id",
        "ownership_generation",
        "admission_fence_id",
        "admission_fence_generation",
        "lease_epoch",
        "target_id",
        "target_incarnation_id",
        "holder_token_digest",
        "status",
        "expires_at",
        "created_at",
        "updated_at",
        "released_at",
    }
)

_FENCED_WAKE_TARGET_LEASE_TRIGGER_NAMES = (
    "trg_actor_v2_fenced_wake_target_lease_owner_immutable",
    "trg_actor_v2_fenced_wake_target_lease_lifecycle",
    "trg_actor_v2_fenced_wake_target_lease_delete_forbidden",
)


def _migrate_mailbox_handoff_schema(conn: sqlite3.Connection) -> None:
    """Backfill untrusted mailbox history without inferring current fence evidence.

    This table is deliberately a sidecar rather than new columns on the shared
    mailbox table.  Existing producers remain unblocked until they are moved to
    a typed handoff writer, while a missing sidecar remains fail-closed at the
    repository boundary.
    """

    columns = _table_columns(conn, "agent_session_mailbox_handoffs")
    if not columns:
        raise sqlite3.IntegrityError("mailbox handoff sidecar table is missing")
    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_session_mailbox_handoffs" in statement
    )
    table_schema = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'agent_session_mailbox_handoffs'
        """
    ).fetchone()
    if table_schema is None or _normalized_create_table_sql(
        table_schema["sql"]
    ) != _normalized_create_table_sql(table_statement):
        raise sqlite3.IntegrityError(
            "mailbox handoff sidecar table does not match its immutable contract"
        )
    missing_columns = _MAILBOX_HANDOFF_REQUIRED_COLUMNS.difference(columns)
    if missing_columns:
        raise sqlite3.IntegrityError(
            "mailbox handoff sidecar lacks required columns: "
            + ", ".join(sorted(missing_columns))
        )
    pending_index_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE INDEX IF NOT EXISTS idx_agent_session_mailbox_handoffs_pending"
        in statement
    )
    _ensure_exact_index(
        conn,
        index_name="idx_agent_session_mailbox_handoffs_pending",
        expected_table="agent_session_mailbox_handoffs",
        create_statement=pending_index_statement,
        label="mailbox handoff pending",
    )
    _replace_mailbox_handoff_triggers(conn)
    invalid = conn.execute(
        """
        SELECT handoff.mailbox_id
        FROM agent_session_mailbox_handoffs AS handoff
        LEFT JOIN agent_session_mailbox AS mailbox
          ON mailbox.mailbox_id = handoff.mailbox_id
        WHERE mailbox.mailbox_id IS NULL
           OR typeof(mailbox.profile_id) != typeof(handoff.profile_id)
           OR CAST(mailbox.profile_id AS BLOB) != CAST(handoff.profile_id AS BLOB)
           OR typeof(mailbox.session_id) != typeof(handoff.session_id)
           OR CAST(mailbox.session_id AS BLOB) != CAST(handoff.session_id AS BLOB)
           OR typeof(mailbox.event_id) != typeof(handoff.event_id)
           OR CAST(mailbox.event_id AS BLOB) != CAST(handoff.event_id AS BLOB)
           OR typeof(mailbox.ownership_generation) !=
                typeof(handoff.ownership_generation)
           OR mailbox.ownership_generation != handoff.ownership_generation
        LIMIT 1
        """
    ).fetchone()
    if invalid is not None:
        raise sqlite3.IntegrityError(
            "mailbox handoff immutable identity differs from source mailbox: "
            f"{invalid['mailbox_id']}"
        )
    conn.execute(
        """
        INSERT INTO agent_session_mailbox_handoffs (
            mailbox_id, handoff_id,
            profile_id, session_id, event_id, ownership_generation,
            evidence_state, admission_fence_id, admission_fence_generation,
            state, attempt_count, available_at,
            claim_id, lease_owner, lease_until,
            target_id, target_incarnation_id, target_disposition,
            created_at, updated_at, claimed_at, settled_at, last_error
        )
        SELECT mailbox.mailbox_id,
               'unknown-mailbox-' || CAST(mailbox.mailbox_id AS TEXT),
               mailbox.profile_id,
               mailbox.session_id,
               mailbox.event_id,
               mailbox.ownership_generation,
               'unknown', '', 0,
               'blocked', 0, mailbox.available_at,
               '', '', NULL,
               '', '', '',
               mailbox.created_at, mailbox.created_at, NULL, NULL, ''
        FROM agent_session_mailbox AS mailbox
        WHERE NOT EXISTS (
            SELECT 1
            FROM agent_session_mailbox_handoffs AS handoff
            WHERE handoff.mailbox_id = mailbox.mailbox_id
        )
        """
    )


def _migrate_fenced_wake_target_lease_schema(conn: sqlite3.Connection) -> None:
    """Verify the immutable durable contract for future target publication."""

    columns = _table_columns(conn, "agent_session_actor_v2_fenced_wake_target_leases")
    if not columns:
        raise sqlite3.IntegrityError("fenced wake target lease table is missing")
    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_session_actor_v2_fenced_wake_target_leases"
        in statement
    )
    table_schema = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'agent_session_actor_v2_fenced_wake_target_leases'
        """
    ).fetchone()
    if table_schema is None or _normalized_create_table_sql(
        table_schema["sql"]
    ) != _normalized_create_table_sql(table_statement):
        raise sqlite3.IntegrityError(
            "fenced wake target lease table does not match its immutable contract"
        )
    missing_columns = _FENCED_WAKE_TARGET_LEASE_REQUIRED_COLUMNS.difference(columns)
    if missing_columns:
        raise sqlite3.IntegrityError(
            "fenced wake target lease table lacks required columns: "
            + ", ".join(sorted(missing_columns))
        )
    expiry_index_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE INDEX IF NOT EXISTS idx_actor_v2_fenced_wake_target_leases_expiry"
        in statement
    )
    _ensure_exact_index(
        conn,
        index_name="idx_actor_v2_fenced_wake_target_leases_expiry",
        expected_table="agent_session_actor_v2_fenced_wake_target_leases",
        create_statement=expiry_index_statement,
        label="fenced wake target lease expiry",
    )
    _replace_fenced_wake_target_lease_triggers(conn)


def _replace_fenced_wake_target_lease_triggers(conn: sqlite3.Connection) -> None:
    """Keep durable target publication tied to one immutable owner request.

    Repository methods already validate the same rules, but this table is the
    cross-process source of truth.  The trigger layer prevents a future SQL
    maintenance path from silently retargeting an active epoch, changing its
    owner identity, or deleting the history that prevents incarnation reuse.
    """

    for trigger_name in _FENCED_WAKE_TARGET_LEASE_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_fenced_wake_target_lease_owner_immutable
        BEFORE UPDATE OF profile_id, session_id, ownership_generation,
                         admission_fence_id, admission_fence_generation
        ON agent_session_actor_v2_fenced_wake_target_leases
        WHEN typeof(OLD.profile_id) != typeof(NEW.profile_id)
          OR CAST(OLD.profile_id AS BLOB) != CAST(NEW.profile_id AS BLOB)
          OR typeof(OLD.session_id) != typeof(NEW.session_id)
          OR CAST(OLD.session_id AS BLOB) != CAST(NEW.session_id AS BLOB)
          OR typeof(OLD.ownership_generation) != typeof(NEW.ownership_generation)
          OR OLD.ownership_generation != NEW.ownership_generation
          OR typeof(OLD.admission_fence_id) != typeof(NEW.admission_fence_id)
          OR CAST(OLD.admission_fence_id AS BLOB) !=
                CAST(NEW.admission_fence_id AS BLOB)
          OR typeof(OLD.admission_fence_generation) !=
                typeof(NEW.admission_fence_generation)
          OR OLD.admission_fence_generation != NEW.admission_fence_generation
        BEGIN
            SELECT RAISE(
                ABORT,
                'fenced wake target lease owner identity cannot change'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_fenced_wake_target_lease_lifecycle
        BEFORE UPDATE ON agent_session_actor_v2_fenced_wake_target_leases
        WHEN NOT (
            (
                OLD.status = 'active'
                AND NEW.status = 'active'
                AND NEW.lease_epoch = OLD.lease_epoch
                AND NEW.target_id = OLD.target_id
                AND NEW.target_incarnation_id = OLD.target_incarnation_id
                AND NEW.holder_token_digest = OLD.holder_token_digest
                AND NEW.created_at = OLD.created_at
                AND NEW.updated_at >= OLD.updated_at
                AND NEW.released_at IS NULL
            )
            OR (
                OLD.status = 'active'
                AND NEW.status = 'released'
                AND NEW.lease_epoch = OLD.lease_epoch
                AND NEW.target_id = OLD.target_id
                AND NEW.target_incarnation_id = OLD.target_incarnation_id
                AND NEW.holder_token_digest = OLD.holder_token_digest
                AND NEW.expires_at = OLD.expires_at
                AND NEW.created_at = OLD.created_at
                AND NEW.updated_at >= OLD.updated_at
                AND NEW.released_at = NEW.updated_at
            )
            OR (
                OLD.status = 'released'
                AND NEW.status = 'released'
                AND NEW.lease_epoch = OLD.lease_epoch
                AND NEW.target_id = OLD.target_id
                AND NEW.target_incarnation_id = OLD.target_incarnation_id
                AND NEW.holder_token_digest = OLD.holder_token_digest
                AND NEW.expires_at = OLD.expires_at
                AND NEW.created_at = OLD.created_at
                AND NEW.updated_at = OLD.updated_at
                AND NEW.released_at = OLD.released_at
            )
            OR (
                OLD.status = 'active'
                AND NEW.status = 'active'
                AND NEW.lease_epoch = OLD.lease_epoch + 1
                AND OLD.expires_at <= NEW.created_at
                AND (
                    NEW.target_id != OLD.target_id
                    OR NEW.target_incarnation_id != OLD.target_incarnation_id
                )
                AND NEW.updated_at = NEW.created_at
                AND NEW.released_at IS NULL
            )
            OR (
                OLD.status = 'released'
                AND NEW.status = 'active'
                AND NEW.lease_epoch = OLD.lease_epoch + 1
                AND (
                    NEW.target_id != OLD.target_id
                    OR NEW.target_incarnation_id != OLD.target_incarnation_id
                )
                AND NEW.created_at >= OLD.updated_at
                AND NEW.updated_at = NEW.created_at
                AND NEW.released_at IS NULL
            )
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid fenced wake target lease lifecycle transition'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_actor_v2_fenced_wake_target_lease_delete_forbidden
        BEFORE DELETE ON agent_session_actor_v2_fenced_wake_target_leases
        BEGIN
            SELECT RAISE(
                ABORT,
                'fenced wake target lease history cannot be deleted'
            );
        END
        """
    )


def _replace_mailbox_handoff_triggers(conn: sqlite3.Connection) -> None:
    """Install the canonical source-copy and immutable-evidence trigger guards."""

    for trigger_name in _MAILBOX_HANDOFF_TRIGGER_NAMES:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(
        """
        CREATE TRIGGER trg_agent_session_mailbox_handoff_source_insert
        BEFORE INSERT ON agent_session_mailbox_handoffs
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_session_mailbox AS mailbox
            WHERE mailbox.mailbox_id = NEW.mailbox_id
              AND typeof(mailbox.profile_id) = typeof(NEW.profile_id)
              AND CAST(mailbox.profile_id AS BLOB) = CAST(NEW.profile_id AS BLOB)
              AND typeof(mailbox.session_id) = typeof(NEW.session_id)
              AND CAST(mailbox.session_id AS BLOB) = CAST(NEW.session_id AS BLOB)
              AND typeof(mailbox.event_id) = typeof(NEW.event_id)
              AND CAST(mailbox.event_id AS BLOB) = CAST(NEW.event_id AS BLOB)
              AND typeof(mailbox.ownership_generation) =
                    typeof(NEW.ownership_generation)
              AND mailbox.ownership_generation = NEW.ownership_generation
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'mailbox handoff identity does not match source mailbox'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_agent_session_mailbox_handoff_initial_state
        BEFORE INSERT ON agent_session_mailbox_handoffs
        WHEN (NEW.evidence_state = 'fenced' AND NEW.state != 'pending')
          OR (
              NEW.evidence_state IN ('unfenced_legacy', 'unknown')
              AND NEW.state != 'blocked'
          )
        BEGIN
            SELECT RAISE(
                ABORT,
                'mailbox handoff must begin pending when fenced or blocked otherwise'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_agent_session_mailbox_handoff_source_identity_immutable
        BEFORE UPDATE OF profile_id, session_id, event_id, ownership_generation
        ON agent_session_mailbox
        WHEN EXISTS (
            SELECT 1
            FROM agent_session_mailbox_handoffs AS handoff
            WHERE handoff.mailbox_id = OLD.mailbox_id
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'mailbox source identity cannot change while handoff exists'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_agent_session_mailbox_handoff_state_transition
        BEFORE UPDATE ON agent_session_mailbox_handoffs
        WHEN NOT (
            (
                OLD.state = 'pending'
                AND NEW.state = 'pending'
            )
            OR (
                OLD.state = 'pending'
                AND NEW.state = 'claimed'
                AND NEW.attempt_count = OLD.attempt_count + 1
            )
            OR (
                OLD.state = 'claimed'
                AND NEW.state = 'claimed'
                AND (
                    (
                        NEW.attempt_count = OLD.attempt_count
                        AND NEW.claim_id = OLD.claim_id
                        AND NEW.lease_owner = OLD.lease_owner
                        AND NEW.target_id = OLD.target_id
                        AND NEW.target_incarnation_id = OLD.target_incarnation_id
                    )
                    OR (
                        NEW.attempt_count = OLD.attempt_count + 1
                        AND OLD.lease_until <= NEW.claimed_at
                    )
                )
            )
            OR (
                OLD.state = 'claimed'
                AND NEW.state = 'pending'
                AND NEW.attempt_count = OLD.attempt_count
            )
            OR (
                OLD.state = 'claimed'
                AND NEW.state = 'settled'
                AND NEW.attempt_count = OLD.attempt_count
                AND NEW.target_id = OLD.target_id
                AND NEW.target_incarnation_id = OLD.target_incarnation_id
            )
            OR (
                OLD.state = 'settled'
                AND NEW.state = 'settled'
                AND NEW.attempt_count = OLD.attempt_count
                AND NEW.target_id = OLD.target_id
                AND NEW.target_incarnation_id = OLD.target_incarnation_id
                AND NEW.target_disposition = OLD.target_disposition
                AND NEW.settled_at = OLD.settled_at
            )
            OR (
                OLD.state = 'blocked'
                AND NEW.state = 'blocked'
            )
        )
        BEGIN
            SELECT RAISE(
                ABORT,
                'invalid mailbox handoff state transition'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_agent_session_mailbox_handoff_identity_immutable
        BEFORE UPDATE OF mailbox_id, handoff_id, profile_id, session_id,
                         event_id, ownership_generation, evidence_state,
                         admission_fence_id, admission_fence_generation
        ON agent_session_mailbox_handoffs
        WHEN OLD.mailbox_id != NEW.mailbox_id
          OR typeof(OLD.handoff_id) != typeof(NEW.handoff_id)
          OR CAST(OLD.handoff_id AS BLOB) != CAST(NEW.handoff_id AS BLOB)
          OR typeof(OLD.profile_id) != typeof(NEW.profile_id)
          OR CAST(OLD.profile_id AS BLOB) != CAST(NEW.profile_id AS BLOB)
          OR typeof(OLD.session_id) != typeof(NEW.session_id)
          OR CAST(OLD.session_id AS BLOB) != CAST(NEW.session_id AS BLOB)
          OR typeof(OLD.event_id) != typeof(NEW.event_id)
          OR CAST(OLD.event_id AS BLOB) != CAST(NEW.event_id AS BLOB)
          OR typeof(OLD.ownership_generation) != typeof(NEW.ownership_generation)
          OR OLD.ownership_generation != NEW.ownership_generation
          OR typeof(OLD.evidence_state) != typeof(NEW.evidence_state)
          OR CAST(OLD.evidence_state AS BLOB) != CAST(NEW.evidence_state AS BLOB)
          OR typeof(OLD.admission_fence_id) != typeof(NEW.admission_fence_id)
          OR CAST(OLD.admission_fence_id AS BLOB) !=
                CAST(NEW.admission_fence_id AS BLOB)
          OR typeof(OLD.admission_fence_generation) !=
                typeof(NEW.admission_fence_generation)
          OR OLD.admission_fence_generation != NEW.admission_fence_generation
        BEGIN
            SELECT RAISE(
                ABORT,
                'mailbox handoff immutable evidence cannot change'
            );
        END
        """
    )
    conn.execute(
        """
        CREATE TRIGGER trg_agent_session_mailbox_handoff_delete_forbidden
        BEFORE DELETE ON agent_session_mailbox_handoffs
        BEGIN
            SELECT RAISE(
                ABORT,
                'mailbox handoff immutable evidence cannot be deleted'
            );
        END
        """
    )


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
    if _normalized_create_table_sql(row["sql"]) == _normalized_create_table_sql(table_statement):
        return

    conn.execute("SAVEPOINT rebuild_agent_effect_outbox")
    try:
        conn.execute("ALTER TABLE agent_effect_outbox RENAME TO agent_effect_outbox_legacy")
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


_REVIEW_EXECUTION_RUN_MIGRATION_COLUMNS = (
    "run_seq",
    "profile_id",
    "session_id",
    "ownership_generation",
    "review_effect_id",
    "review_operation_id",
    "review_effect_kind",
    "review_contract_version",
    "review_contract_signature",
    "claim_id",
    "worker_id",
    "execution_status",
    "started_at",
    "finished_at",
    "unknown_at",
    "unknown_reason",
)


def _rebuild_agent_review_execution_runs_constraints(
    conn: sqlite3.Connection,
) -> None:
    """Upgrade review witnesses without treating old running work as finished.

    Earlier versions had no durable expiry state for a model execution witness.
    A historical ``running`` row therefore cannot establish that its process is
    still alive, but it also cannot safely be replayed. Rebuild it as
    ``unknown`` instead of guessing a terminal model result.
    """

    table_statement = next(
        statement
        for statement in SCHEMA_STATEMENTS
        if "CREATE TABLE IF NOT EXISTS agent_review_execution_runs" in statement
    )
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'agent_review_execution_runs'
        """
    ).fetchone()
    if row is None:
        return
    if _normalized_create_table_sql(row["sql"]) == _normalized_create_table_sql(table_statement):
        return

    conn.execute("SAVEPOINT rebuild_agent_review_execution_runs")
    try:
        legacy_columns = _table_columns(conn, "agent_review_execution_runs")
        conn.execute(
            "ALTER TABLE agent_review_execution_runs RENAME TO agent_review_execution_runs_legacy"
        )
        conn.execute(table_statement)
        select_columns = tuple(
            column for column in _REVIEW_EXECUTION_RUN_MIGRATION_COLUMNS if column in legacy_columns
        )
        if not select_columns:
            raise sqlite3.IntegrityError(
                "review execution witness migration found no source columns"
            )
        rows = conn.execute(
            "SELECT "
            + ", ".join(select_columns)
            + " FROM agent_review_execution_runs_legacy ORDER BY run_seq"
        ).fetchall()
        for legacy in rows:
            values = dict(legacy)
            status = values.get("execution_status")
            if status == "running":
                migrated_status = "unknown"
                finished_at = None
                unknown_at = values.get("started_at")
                unknown_reason = "legacy_execution_witness_without_expiry"
            elif status in {"finished", "cancelled"}:
                migrated_status = str(status)
                finished_at = values.get("finished_at")
                unknown_at = None
                unknown_reason = ""
            elif status == "unknown":
                migrated_status = "unknown"
                finished_at = None
                unknown_at = values.get("unknown_at")
                unknown_reason = values.get("unknown_reason")
            else:
                raise sqlite3.IntegrityError(
                    "review execution witness migration found invalid status: " + repr(status)
                )
            required_text = (
                "profile_id",
                "session_id",
                "review_effect_id",
                "review_operation_id",
                "review_effect_kind",
                "review_contract_signature",
                "claim_id",
                "worker_id",
            )
            if any(not _is_canonical_nonempty_text(values.get(column)) for column in required_text):
                raise sqlite3.IntegrityError(
                    "review execution witness migration found invalid identity"
                )
            if values.get("review_effect_kind") != "run_review_workflow":
                raise sqlite3.IntegrityError(
                    "review execution witness migration found invalid review kind"
                )
            for column in (
                "run_seq",
                "ownership_generation",
                "review_contract_version",
            ):
                if not _is_integer_at_least(values.get(column), 1):
                    raise sqlite3.IntegrityError(
                        "review execution witness migration found invalid " + column
                    )
            if not _is_nonnegative_finite_number(values.get("started_at")):
                raise sqlite3.IntegrityError(
                    "review execution witness migration found invalid started_at"
                )
            if migrated_status in {"finished", "cancelled"}:
                if not _is_nonnegative_finite_number(finished_at):
                    raise sqlite3.IntegrityError(
                        "review execution witness migration found invalid finished_at"
                    )
            else:
                if not _is_nonnegative_finite_number(unknown_at) or not _is_canonical_nonempty_text(
                    unknown_reason
                ):
                    raise sqlite3.IntegrityError(
                        "review execution witness migration found invalid unknown evidence"
                    )
            conn.execute(
                """
                INSERT INTO agent_review_execution_runs (
                    run_seq, profile_id, session_id, ownership_generation,
                    review_effect_id, review_operation_id, review_effect_kind,
                    review_contract_version, review_contract_signature, claim_id,
                    worker_id, execution_status, started_at, finished_at,
                    unknown_at, unknown_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    values["run_seq"],
                    values["profile_id"],
                    values["session_id"],
                    values["ownership_generation"],
                    values["review_effect_id"],
                    values["review_operation_id"],
                    values["review_effect_kind"],
                    values["review_contract_version"],
                    values["review_contract_signature"],
                    values["claim_id"],
                    values["worker_id"],
                    migrated_status,
                    values["started_at"],
                    finished_at,
                    unknown_at,
                    unknown_reason,
                ),
            )
        conn.execute("DROP TABLE agent_review_execution_runs_legacy")
        conn.execute(
            """
            CREATE INDEX idx_agent_review_execution_runs_live
            ON agent_review_execution_runs(
                profile_id, session_id, ownership_generation,
                review_effect_id, execution_status
            )
            """
        )
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT rebuild_agent_review_execution_runs")
        conn.execute("RELEASE SAVEPOINT rebuild_agent_review_execution_runs")
        raise
    conn.execute("RELEASE SAVEPOINT rebuild_agent_review_execution_runs")


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
            "source.trace_id" if field_name == "source_trace_id" else f"effect.{field_name}"
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
        "cancelled",
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
    if row["status"] in {"pending", "processing"} and _is_integer_at_least(
        row["attempt_count"], _SQLITE_INT64_MAX
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
    prefixes = (
        ("CREATE UNIQUE INDEX IF NOT EXISTS ", "CREATE UNIQUE INDEX "),
        ("CREATE INDEX IF NOT EXISTS ", "CREATE INDEX "),
    )
    for optional_prefix, required_prefix in prefixes:
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
    if not _is_canonical_nonempty_text(profile_id) or not (_is_canonical_nonempty_text(session_id)):
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
        raise sqlite3.IntegrityError("malformed effect row has no active actor ownership")

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
        str(row["operation_id"]) if _is_canonical_text(row["operation_id"]) else ""
    )
    identity = "\x1f".join((profile_id, session_id, normalized_effect_id, "quarantined"))
    quarantine_event_id = (
        "effect-quarantined:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
    )
    failure_message = "durable effect row failed validation: " + ", ".join(violations)
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
        int(row["attempt_count"]) if _is_integer_at_least(row["attempt_count"], 0) else 0
    )
    normalized_created_at = (
        float(row["created_at"]) if _is_nonnegative_finite_number(row["created_at"]) else now
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
    trace_id = str(row["source_trace_id"]) if isinstance(row["source_trace_id"], str) else ""
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
    "trg_agent_recovery_case_terminal_delivery_state",
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
        for row in conn.execute("PRAGMA table_info(agent_session_recovery_cases)").fetchall()
    )
    column_mismatch = set(columns) != set(_RECOVERY_CASE_COLUMNS)
    row_count = int(conn.execute("SELECT COUNT(*) FROM agent_session_recovery_cases").fetchone()[0])
    if column_mismatch and row_count > 0:
        raise sqlite3.IntegrityError(
            "invalid recovery case authority: legacy columns do not match "
            "the canonical recovery ledger"
        )

    conn.execute("SAVEPOINT rebuild_agent_session_recovery_cases")
    try:
        conn.execute(
            "ALTER TABLE agent_session_recovery_cases RENAME TO agent_session_recovery_cases_legacy"
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
            if "CREATE INDEX IF NOT EXISTS idx_agent_session_recovery_cases_" in statement:
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
    _migrate_actor_v2_admission_schema(conn)
    _migrate_actor_v2_cutover_journal_schema(conn)
    _migrate_actor_v2_migration_barrier_schema(conn)
    _migrate_actor_v2_ingress_drain_schema(conn)
    _migrate_actor_v2_core_ingress_drain_schema(conn)
    _migrate_actor_v2_legacy_state_handoff_schema(conn)
    _migrate_external_action_receipts_schema(conn)
    _migrate_session_actor_schema(conn)
    _migrate_mailbox_handoff_schema(conn)
    _migrate_fenced_wake_target_lease_schema(conn)
    _migrate_legacy_recovery_gate_schema(conn)
    _migrate_actor_v2_canary_isolation_lease_schema(conn)
    _ensure_actor_raw_logical_key_indexes(conn)
    _ensure_manual_review_request_unique_index(conn)
