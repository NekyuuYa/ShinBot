"""Integration coverage for canonical Agent runtime diagnostics."""

from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.admin.agent_runtime_diagnostics import (
    AgentRuntimeDiagnosticsInvalidKey,
    AgentRuntimeDiagnosticsNotFound,
    get_agent_runtime_session_diagnostics,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.durable_routing import MessageRoutingJobEnvelope
from shinbot.persistence import DatabaseManager, MessageLogRecord


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _insert_aggregate(
    database: DatabaseManager,
    key: SessionKey,
    *,
    current_plan_id: str = "",
    data_json: str = "{}",
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, state, state_revision, event_sequence,
                activity_generation, active_epoch, review_plan_json,
                current_plan_id, review_plan_revision, data_json,
                created_at, updated_at
            ) VALUES (?, ?, 'idle', 7, 9, 3, 2, '{}', ?, 2, ?, 1.0, 9.0)
            """,
            (key.profile_id, key.session_id, current_plan_id, data_json),
        )


def _insert_actor_diagnostic_evidence(
    database: DatabaseManager,
    key: SessionKey,
    *,
    ownership_generation: int,
) -> None:
    _insert_aggregate(
        database,
        key,
        current_plan_id="plan-current",
        data_json='{"owner":"profile-a"}',
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, kind, source, occurred_at,
                payload_json, status, attempt_count, available_at, created_at,
                handled_at, last_error
            ) VALUES (
                'mailbox-failed', ?, ?, 'EffectFailed', 'effect_executor', 2.0,
                '{"effect_id":"effect-failed","tool_output":"mailbox-secret"}',
                'failed', 3, 2.0, 2.0,
                3.0, 'mailbox terminal failure'
            )
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
                INSERT INTO agent_session_operations (
                    operation_id, profile_id, session_id,
                    ownership_generation, kind, status, launched_by_event_id,
                    state_revision, active_epoch, activity_generation,
                    input_watermark, input_ledger_sequence, started_at,
                    finished_at, failure_code, failure_message, metadata_json
                ) VALUES (
                    'operation-failed', ?, ?, ?,
                    'idle_review_planning', 'failed', 'mailbox-start',
                    6, 2, 3, 41, 4, 2.0, 3.0,
                    'planner_failed', 'Bearer operation-secret',
                    '{"plan_id":"plan-current","prompt":"operation-prompt-secret"}'
                )
                """,
                (key.profile_id, key.session_id, ownership_generation),
            )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id, event_id,
                operation_id, kind, contract_version, contract_signature,
                payload_json, status, attempt_count,
                available_at, created_at, updated_at, completed_at, last_error
            ) VALUES (
                'effect-failed', 'effect-key-failed', ?, ?, 'mailbox-start',
                'operation-failed', 'run_idle_review_planning',
                1, 'contract-signature-1',
                '{"plan_id":"plan-current","expected_activity_generation":3,"api_key":"effect-secret","prompt":"effect-prompt-secret"}',
                'failed', 4, 2.0, 2.0, 4.0, 4.0, 'Bearer effect-error-secret'
            )
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id,
                session_id, ownership_generation, action_ordinal, action_kind,
                contract_version, request_digest, request_json, status,
                attempt_count, claim_id, lease_owner, lease_until,
                platform_result_json, rejection_json, unknown_json,
                assistant_message_log_id, prepared_at,
                execution_started_at, settled_at, updated_at
            ) VALUES (
                'external-action-key', 'external-action-effect',
                'operation-failed', ?, ?, ?, 0, 'send_poke', 1, ?,
                '{"operation_id":"operation-failed","text":"reply-secret","api_token":"token-secret"}',
                'unknown', 1, 'external-claim', 'external-worker', NULL,
                '{"adapter_result":"platform-result-secret"}', '{}',
                '{"reason_code":"lost_ack","error":"unknown-error-secret"}',
                NULL, 2.0, 2.5, 3.0, 3.0
            )
            """,
            (
                key.profile_id,
                key.session_id,
                ownership_generation,
                "a" * 64,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_attempts (
                idempotency_key, attempt_count, claim_id, lease_owner,
                claimed_at, lease_until, status, platform_result_json,
                rejection_json, unknown_json, assistant_message_log_id,
                settled_at
            ) VALUES (
                'external-action-key', 1, 'external-claim', 'external-worker',
                2.5, 30.0, 'unknown',
                '{"adapter_result":"platform-result-secret"}', '{}',
                '{"reason_code":"lost_ack","error":"unknown-error-secret"}',
                NULL, 3.0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO agent_review_schedules (
                plan_id, profile_id, session_id, plan_revision, status,
                trigger, outcome, source, requested_delay_seconds,
                applied_delay_seconds, scheduled_from, next_review_at,
                reason, model_execution_id, prompt_signature,
                expected_active_epoch, expected_activity_generation,
                committed_state_revision, available_at, created_at, updated_at
            ) VALUES (
                'plan-current', ?, ?, 2, 'scheduled', 'active_chat_exit',
                'planned', 'llm', 120.0, 120.0, 5.0, 125.0,
                'conversation settled', 'execution-current', 'prompt-current',
                2, 3, 7, 125.0, 5.0, 5.0
            )
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
            INSERT INTO agent_review_schedules (
                plan_id, profile_id, session_id, plan_revision, status,
                trigger, outcome, source, requested_delay_seconds,
                applied_delay_seconds, scheduled_from, next_review_at,
                fallback_reason, model_execution_id, prompt_signature,
                expected_active_epoch, expected_activity_generation,
                committed_state_revision, available_at, created_at, updated_at
            ) VALUES (
                'plan-stale', ?, ?, 3, 'superseded', 'active_chat_exit',
                'superseded', 'llm', 600.0, 30.0, 6.0, 36.0,
                'stale activity generation', 'execution-stale', 'prompt-stale',
                1, 2, 6, 36.0, 6.0, 6.0
            )
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
            INSERT INTO agent_state_transitions (
                transition_id, profile_id, session_id, event_id,
                from_state, to_state, trigger, disposition, state_revision,
                event_sequence, operation_id, plan_id, trace_id,
                metadata_json, created_at
            ) VALUES (
                'transition-1', ?, ?, 'mailbox-failed', 'active_chat_settling',
                'idle', 'idle_planning_failed', 'fallback_applied', 7, 9,
                'operation-failed', 'plan-current', 'trace-actor',
                '{"ownership_generation":1}', 7.0
            )
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
            INSERT INTO agent_review_schedule_events (
                schedule_event_id, profile_id, session_id, event_id,
                plan_id, previous_plan_id, event_type, trigger, outcome,
                source, requested_delay_seconds, applied_delay_seconds,
                scheduled_from, next_review_at, fallback_reason,
                model_execution_id, prompt_signature, expected_active_epoch,
                expected_activity_generation, committed_state_revision,
                operation_id, trace_id, metadata_json, created_at
            ) VALUES (
                'schedule-event-stale', ?, ?, 'mailbox-failed', 'plan-stale',
                'plan-current', 'superseded', 'active_chat_exit', 'superseded',
                'llm', 600.0, 30.0, 6.0, 36.0,
                'stale activity generation', 'execution-stale', 'prompt-stale',
                1, 2, 6, 'operation-failed', 'trace-actor',
                '{"fence":"activity_generation"}', 7.0
            )
            """,
            (key.profile_id, key.session_id),
        )
        message_log_id = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES ('legacy:group:room', 'user', 1.0)
            """
        ).lastrowid
        assert message_log_id is not None
        conn.execute(
            """
            INSERT INTO message_routing_jobs (
                routing_job_id, idempotency_key, message_log_id, version,
                message_fingerprint, payload_json, payload_digest, trace_id,
                correlation_id, causation_id, occurred_at, status,
                attempt_count, available_at, decision_version, decision_kind,
                decision_id, decision_payload_json, decision_payload_digest,
                created_at, updated_at, completed_at
            ) VALUES (
                'routing-job-1', 'routing-key-1', ?, 1, 'fingerprint-1',
                '{"event_type":"message-created"}', 'payload-digest',
                'trace-route', 'correlation-route', 'platform-message-1', 1.0,
                'completed', 1, 1.0, 1, 'agent_delivery', 'decision-1',
                '{"delivery_count":1}', 'decision-digest', 1.0, 2.0, 2.0
            )
            """,
            (message_log_id,),
        )
        conn.execute(
            """
            INSERT INTO agent_route_outbox (
                delivery_id, idempotency_key, routing_job_id, profile_id,
                session_id, message_log_id, route_rule_id, version,
                ownership_generation, event_id, payload_json, payload_digest,
                trace_id, correlation_id, causation_id, status, attempt_count,
                available_at, created_at, updated_at, completed_at
            ) VALUES (
                'delivery-1', 'delivery-key-1', 'routing-job-1', ?, ?, ?,
                'builtin.agent_entry_fallback', 1, ?, 'route-event-1',
                '{"message_log_id":1}', 'delivery-digest', 'trace-route',
                'correlation-route', 'routing-job-1', 'completed', 1,
                1.0, 1.0, 2.0, 2.0
            )
            """,
            (key.profile_id, key.session_id, message_log_id, ownership_generation),
        )


def _insert_legacy_evidence(
    database: DatabaseManager,
    legacy_session_id: str,
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (
                session_id, state, next_review_at, review_reason,
                active_reply_threshold_json, active_chat_state_json, updated_at
            ) VALUES (
                ?, 'review', 42.0, 'legacy review due',
                '{"at_count":2,"window_seconds":60}',
                '{"interest_value":4.0}', 10.0
            )
            """,
            (legacy_session_id,),
        )
        first = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 1.0)
            """,
            (legacy_session_id,),
        ).lastrowid
        second = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 2.0)
            """,
            (legacy_session_id,),
        ).lastrowid
        assert first is not None and second is not None
        conn.execute(
            """
            INSERT INTO agent_unread_messages (
                session_id, message_log_id, created_at, is_poke_to_bot
            ) VALUES (?, ?, 1.0, 1)
            """,
            (legacy_session_id, first),
        )
        conn.execute(
            """
            INSERT INTO agent_unread_ranges (
                session_id, start_msg_log_id, end_msg_log_id,
                start_at, end_at, message_count
            ) VALUES (?, ?, ?, 1.0, 2.0, 2)
            """,
            (legacy_session_id, first, second),
        )


def test_actor_diagnostics_isolate_profiles_and_link_failure_provenance(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    session_id = "shared:group:room"
    key_a = SessionKey("profile-a", session_id)
    key_b = SessionKey("profile-b", session_id)
    owner_a = database.agent_runtime_ownership.claim(
        key_a,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor diagnostics fixture",
        legacy_session_id="legacy-a:group:room",
    ).ownership
    database.agent_runtime_ownership.claim(
        key_b,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor isolation fixture",
        legacy_session_id="legacy-b:group:room",
    )
    _insert_actor_diagnostic_evidence(
        database,
        key_a,
        ownership_generation=owner_a.generation,
    )
    _insert_aggregate(database, key_b, data_json='{"owner":"profile-b"}')

    diagnostics = get_agent_runtime_session_diagnostics(
        database,
        profile_id=key_a.profile_id,
        session_id=key_a.session_id,
    )
    payload = diagnostics.to_payload()

    assert payload["runtimeKind"] == "actor_v2"
    assert payload["sensitiveDataPolicy"] == "redacted"
    assert payload["actorCanonical"] is True
    assert payload["actorDataStatus"] == "available"
    assert payload["aggregate"]["profileId"] == "profile-a"
    assert payload["aggregate"]["data"]["redacted"] is True
    assert payload["mailbox"]["byStatus"] == {"failed": 1}
    assert payload["operations"]["recent"][0]["failureCode"] == "planner_failed"
    assert payload["effects"]["recent"][0]["status"] == "failed"
    assert payload["effects"]["recent"][0]["lastError"]["redacted"] is True
    assert payload["effects"]["recent"][0]["payload"]["references"] == {
        "expectedActivityGeneration": 3,
        "planId": "plan-current",
    }
    assert payload["externalActions"]["status"] == "attention_required"
    assert payload["externalActions"]["attentionRequired"] is True
    assert payload["externalActions"]["unknownCount"] == 1
    assert payload["externalActions"]["liveCount"] == 0
    assert payload["externalActions"]["receipts"]["byStatus"] == {"unknown": 1}
    assert payload["externalActions"]["attempts"]["byStatus"] == {"unknown": 1}
    receipt = payload["externalActions"]["receipts"]["recent"][0]
    assert receipt["requestDigest"] == "a" * 64
    assert receipt["request"]["redacted"] is True
    assert receipt["request"]["references"] == {
        "operationId": "operation-failed"
    }
    assert receipt["platformResult"]["redacted"] is True
    assert receipt["unknown"]["redacted"] is True
    assert payload["reviewSchedule"]["currentPlanId"] == "plan-current"
    assert payload["reviewSchedule"]["resolution"] == "resolved"
    assert payload["reviewSchedule"]["current"]["modelExecutionId"] == "execution-current"
    assert payload["reviewSchedule"]["recent"][0]["planId"] == "plan-stale"
    assert payload["recentScheduleEvents"][0]["eventType"] == "superseded"
    assert payload["recentScheduleEvents"][0]["promptSignature"] == "prompt-stale"
    assert payload["recentTransitions"][0]["planId"] == "plan-current"
    assert payload["routeDeliveries"]["recent"][0]["ownershipGeneration"] == (
        owner_a.generation
    )
    assert payload["routingJobs"][0]["routingJobId"] == "routing-job-1"
    assert "profile-b" not in repr(payload)
    assert "mailbox-secret" not in repr(payload)
    assert "operation-secret" not in repr(payload)
    assert "effect-secret" not in repr(payload)
    assert "prompt-secret" not in repr(payload)
    assert "reply-secret" not in repr(payload)
    assert "token-secret" not in repr(payload)
    assert "platform-result-secret" not in repr(payload)
    assert "unknown-error-secret" not in repr(payload)


def test_actor_diagnostics_includes_pending_job_before_outbox_decision(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="pending routing diagnostics fixture",
        legacy_session_id="instance-a:group:room",
    ).ownership
    database.durable_routing.persist_message_and_job(
        MessageLogRecord(
            session_id="instance-a:group:room",
            platform_msg_id="platform-message-pending",
            role="user",
            created_at=1_000.0,
        ),
        MessageRoutingJobEnvelope(
            job_id="routing-job-pending",
            idempotency_key="routing-key-pending",
            trace_id="trace-pending",
            profile_id=key.profile_id,
            session_id=key.session_id,
            ownership_generation=ownership.generation,
            payload={"event_type": "message-created"},
        ),
    )

    payload = get_agent_runtime_session_diagnostics(
        database,
        profile_id=key.profile_id,
        session_id=key.session_id,
    ).to_payload()

    assert payload["routeDeliveries"]["total"] == 0
    assert payload["routingJobs"][0]["routingJobId"] == "routing-job-pending"
    assert payload["routingJobs"][0]["ownershipGeneration"] == ownership.generation
    assert payload["routingJobs"][0]["status"] == "pending"


def test_actor_ownership_without_aggregate_is_typed_not_initialized(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor selected before first delivery",
    )

    payload = get_agent_runtime_session_diagnostics(
        database,
        profile_id=key.profile_id,
        session_id=key.session_id,
    ).to_payload()

    assert payload["runtimeKind"] == "actor_v2"
    assert payload["actorCanonical"] is True
    assert payload["actorDataStatus"] == "not_initialized"
    assert payload["aggregate"] is None
    assert payload["mailbox"] == {"total": 0, "byStatus": {}, "recent": []}


def test_migrating_legacy_ownership_reports_both_durable_evidence_sides(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    legacy_session_id = "legacy-instance:group:room"
    claimed = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy baseline",
        legacy_session_id=legacy_session_id,
    ).ownership
    _insert_legacy_evidence(database, legacy_session_id)
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=claimed.generation,
        reason="actor migration under observation",
        requested_by="operator",
    )
    _insert_aggregate(database, key, data_json='{"migration":"target"}')

    payload = get_agent_runtime_session_diagnostics(
        database,
        profile_id=key.profile_id,
        session_id=key.session_id,
    ).to_payload()

    assert payload["runtimeKind"] == "legacy"
    assert payload["ownership"]["status"] == "migrating"
    assert payload["ownership"]["pendingMode"] == "actor_v2"
    assert payload["ownership"]["generation"] == migrating.generation
    assert payload["actorCanonical"] is False
    assert payload["actorDataStatus"] == "available"
    assert payload["aggregate"]["data"]["redacted"] is True
    assert payload["legacy"]["canonical"] is False
    assert payload["legacy"]["dataStatus"] == "available"
    assert payload["legacy"]["schedulerState"]["state"] == "review"
    assert payload["legacy"]["unreadMessages"]["highPriorityPending"] == 1
    assert payload["legacy"]["unreadMessages"]["pending"] == 1
    assert payload["legacy"]["unreadRanges"]["pendingMessageCount"] == 2
    assert [event["eventType"] for event in payload["ownershipEvents"]] == [
        "migration_started",
        "claimed",
    ]


def test_diagnostics_missing_and_invalid_keys_are_explicit(tmp_path: Path) -> None:
    database = _database(tmp_path)

    with pytest.raises(AgentRuntimeDiagnosticsNotFound):
        get_agent_runtime_session_diagnostics(
            database,
            profile_id="missing-profile",
            session_id="missing:group:room",
        )
    for profile_id, session_id in (
        (".", "valid:group:room"),
        ("..", "valid:group:room"),
        ("profile/a", "valid:group:room"),
        ("profile-a", "room with spaces"),
        ("profile-a", "control\x00character"),
    ):
        with pytest.raises(AgentRuntimeDiagnosticsInvalidKey):
            get_agent_runtime_session_diagnostics(
                database,
                profile_id=profile_id,
                session_id=session_id,
            )
