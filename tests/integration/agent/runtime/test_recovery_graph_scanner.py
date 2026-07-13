"""Integration tests for inactive typed recovery-graph discovery."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryDeliveryEnvelopeIdentity,
    RecoveryDeliveryPayload,
    RecoveryV1Policy,
    canonical_recovery_json,
    decode_recovery_delivery_payload,
)
from shinbot.agent.runtime.session_actor.recovery_graph_reader import (
    RecoveryDeliveryClaimLost,
    RecoveryGraphNotEligible,
    RecoveryGraphReadError,
    SQLiteRecoveryGraphReader,
)
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    RecoveryScanDisposition,
    SQLiteRecoveryGraphScanner,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


def _make_database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _review_operation_fence_data(generation: int) -> str:
    """Return the minimal durable fence for one orphaned review operation."""

    return json.dumps(
        {
            "operation_fences": {
                "review-operation": {
                    "operation_id": "review-operation",
                    "ownership_generation": generation,
                }
            }
        },
        separators=(",", ":"),
        sort_keys=True,
    )


async def _seed_orphaned_review(
    database: DatabaseManager,
    *,
    key: SessionKey,
) -> int:
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="typed recovery graph scanner test",
    ).ownership.generation
    store = SQLiteSessionActorStore(database, clock=lambda: 10.0)
    await store.ensure(key, ownership_generation=generation)
    with database.connect() as conn:
        updated = conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = 'review', state_revision = 1,
                review_operation_id = 'review-operation',
                data_json = ?, updated_at = 10
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (
                _review_operation_fence_data(generation),
                key.profile_id,
                key.session_id,
                generation,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, started_at, metadata_json
            ) VALUES ('review-operation', ?, ?, ?, 'review', 'pending',
                      'review-launch', 1, 0, 0, 10, '{}')
            """,
            (key.profile_id, key.session_id, generation),
        )
    assert updated.rowcount == 1
    return generation


def _durable_authority_snapshot(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
) -> tuple[object, ...]:
    """Capture the rows a read-only graph rebuild must not change."""

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT state, state_revision, event_sequence, review_operation_id, data_json
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, ownership_generation),
        ).fetchone()
        operation = conn.execute(
            """
            SELECT status, lease_owner, lease_until, input_watermark,
                   input_ledger_sequence
            FROM agent_session_operations
            WHERE operation_id = 'review-operation'
            """,
        ).fetchone()
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_mailbox),
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings),
                (SELECT COUNT(*) FROM agent_state_transitions),
                (SELECT COUNT(*) FROM agent_effect_outbox)
            """
        ).fetchone()
    assert aggregate is not None
    assert operation is not None
    assert counts is not None
    return (tuple(aggregate), tuple(operation), tuple(counts))


async def test_scanner_emits_one_typed_delivery_for_orphaned_work(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)

    first = scanner.scan()
    second = scanner.scan()

    assert first.delivered_count == 1
    assert first.results[0].disposition is RecoveryScanDisposition.DELIVERED
    assert second.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT case_id, status, delivery_count, next_delivery_cycle, last_event_id
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT event_id, profile_id, session_id, ownership_generation,
                   kind, source, payload_json, causation_id, correlation_id,
                   trace_id, status
            FROM agent_session_mailbox
            """
        ).fetchone()
    assert case is not None
    assert tuple(
        case[name] for name in ("status", "delivery_count", "next_delivery_cycle")
    ) == ("open", 1, 1)
    assert mailbox is not None
    assert tuple(mailbox[name] for name in ("kind", "source", "status")) == (
        RECOVERY_DELIVERY_EVENT_KIND,
        RECOVERY_DELIVERY_EVENT_SOURCE,
        "pending",
    )
    envelope = RecoveryDeliveryEnvelopeIdentity(
        event_id=str(mailbox["event_id"]),
        profile_id=str(mailbox["profile_id"]),
        session_id=str(mailbox["session_id"]),
        ownership_generation=int(mailbox["ownership_generation"]),
        kind=str(mailbox["kind"]),
        source=str(mailbox["source"]),
    )
    payload = decode_recovery_delivery_payload(
        json.loads(str(mailbox["payload_json"])),
        envelope=envelope,
    )
    assert payload.case_id == str(case["case_id"])
    assert payload.delivery_cycle == 0
    assert str(mailbox["causation_id"]) == payload.case_id
    assert str(mailbox["correlation_id"]) == payload.case_id
    assert str(mailbox["trace_id"]) == payload.event_id
    assert payload.certificate.subject.ownership_generation == generation


async def test_scanner_does_not_exhaust_an_in_flight_final_delivery_cycle(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(
        database,
        clock=lambda: 100.0,
        max_delivery_cycles=1,
    )
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: 200.0,
        retry_delay_seconds=0.0,
    )

    first = scanner.scan()
    claim = await store.claim_next(key, worker_id="recovery-claim-worker")
    assert claim is not None
    second = scanner.scan()
    await store.release(claim, error="retry recovery delivery")
    third = scanner.scan()
    retried_claim = await store.claim_next(key, worker_id="recovery-claim-worker")
    assert retried_claim is not None
    await store.fail(retried_claim, error="recovery delivery exhausted")
    final = scanner.scan()

    assert first.delivered_count == 1
    assert second.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    assert third.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    assert final.results[0].disposition is RecoveryScanDisposition.DELIVERY_EXHAUSTED
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT status, delivery_count
            FROM agent_session_recovery_cases
            """
        ).fetchone()
    assert case is not None
    assert tuple(case) == ("delivery_exhausted", 1)


async def test_reader_is_read_only_and_scanner_delegates_to_its_authority(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    policy = RecoveryV1Policy()
    reader = SQLiteRecoveryGraphReader(database, policy=policy)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0, policy=policy)
    before = _durable_authority_snapshot(
        database,
        key=key,
        ownership_generation=generation,
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        direct = reader.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        delegated = scanner.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        port = scanner.graph_reader.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")

    assert reader.persistence_domain is database
    assert scanner.graph_reader.persistence_domain is database
    assert reader.policy is policy
    assert scanner.policy is policy
    assert scanner.graph_reader.policy is policy
    assert direct.certificate_digest == delegated.certificate_digest
    assert direct.certificate_digest == port.certificate_digest
    assert all("row_id" not in node.facts for node in direct.nodes)
    assert _durable_authority_snapshot(
        database,
        key=key,
        ownership_generation=generation,
    ) == before


async def test_reader_reports_corruption_without_recording_a_finding(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    reader = SQLiteRecoveryGraphReader(database)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET data_json = '{"duplicate":1,"duplicate":2}'
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphReadError) as raised:
            reader.rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_authority_json_invalid"
    with database.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_mailbox),
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings)
            """
        ).fetchone()
    assert counts is not None
    assert tuple(counts) == (0, 0, 0)


async def test_reader_requires_a_caller_owned_transaction(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)

    with database.connect() as conn:
        with pytest.raises(ValueError, match="caller-owned transaction"):
            SQLiteRecoveryGraphReader(database).rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )


async def test_reader_marks_an_idle_aggregate_as_not_eligible(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="recovery graph reader ineligibility test",
    ).ownership.generation
    await SQLiteSessionActorStore(database, clock=lambda: 10.0).ensure(
        key,
        ownership_generation=generation,
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphNotEligible) as raised:
            SQLiteRecoveryGraphReader(database).rebuild_certificate(
                conn,
                key=key,
                ownership_generation=generation,
            )
        conn.execute("ROLLBACK")

    assert raised.value.reason_code == "aggregate_idle"


async def test_scanner_rolls_back_partial_delivery_before_recording_finding(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER mutate_recovery_delivery_payload
            AFTER INSERT ON agent_session_mailbox
            WHEN NEW.source = 'durable_session_recovery_scanner'
            BEGIN
                UPDATE agent_session_mailbox
                SET payload_json = '{}'
                WHERE mailbox_id = NEW.mailbox_id;
            END
            """
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.finding_count == 1
    assert result.results[0].reason_codes == (
        "recovery_delivery_immutable_value_conflict",
    )
    with database.connect() as conn:
        counts = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM agent_session_mailbox),
                (SELECT COUNT(*) FROM agent_session_recovery_cases),
                (SELECT COUNT(*) FROM agent_session_recovery_findings)
            """
        ).fetchone()
    assert counts is not None
    assert tuple(counts) == (0, 0, 1)


async def test_reader_validates_a_claimed_recovery_delivery_from_raw_authority(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    assert scan.delivered_count == 1
    store = SQLiteSessionActorStore(database, clock=lambda: 200.0)
    claim = await store.claim_next(key, worker_id="recovery-commit-worker")
    assert claim is not None
    reader = SQLiteRecoveryGraphReader(database)

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        validated = reader.validate_claimed_delivery(
            conn,
            claim=claim,
            commit_now=200.0,
        )
        conn.execute("ROLLBACK")

    assert validated.mailbox_id > 0
    assert validated.delivery.event_id == claim.envelope.event_id
    assert validated.delivery.case_id == claim.envelope.causation_id


async def test_reader_loads_a_raw_case_snapshot_for_claimed_delivery(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    assert scan.delivered_count == 1
    case_id = scan.results[0].case_id
    assert case_id

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        snapshot = SQLiteRecoveryGraphReader(database).load_case_snapshot(
            conn,
            case_id=case_id,
        )
        conn.execute("ROLLBACK")

    assert snapshot is not None
    assert snapshot.case_id == case_id
    assert snapshot.status == "open"
    assert snapshot.delivery_count == 1
    assert snapshot.next_delivery_cycle == 1
    assert snapshot.last_event_id.startswith("recovery-requested:v1:")


async def test_reader_rejects_a_recovery_case_storage_alias(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    case_id = scan.results[0].case_id
    assert case_id
    with database.connect() as conn:
        conn.execute("DROP TRIGGER trg_agent_recovery_case_identity_immutable")
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET work_graph_digest = CAST(work_graph_digest AS BLOB)
            WHERE case_id = ?
            """,
            (case_id,),
        )
        conn.execute("PRAGMA ignore_check_constraints = OFF")

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryGraphReadError) as raised:
            SQLiteRecoveryGraphReader(database).load_case_snapshot(
                conn,
                case_id=case_id,
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_authority_text_storage_class_invalid"


async def test_reader_rejects_a_claim_that_changed_after_actor_load(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    claim = await SQLiteSessionActorStore(database, clock=lambda: 200.0).claim_next(
        key,
        worker_id="recovery-commit-worker",
    )
    assert claim is not None
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET attempt_count = attempt_count + 1
            WHERE event_id = ?
            """,
            (claim.envelope.event_id,),
        )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RecoveryDeliveryClaimLost) as raised:
            SQLiteRecoveryGraphReader(database).validate_claimed_delivery(
                conn,
                claim=claim,
                commit_now=200.0,
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_delivery_claim_attempt_count_changed"


async def test_scanner_records_raw_json_finding_and_resolves_after_repair(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET data_json = '{"duplicate":1,"duplicate":2}'
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    broken = scanner.scan()

    assert broken.finding_count == 1
    assert broken.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    with database.connect() as conn:
        finding = conn.execute(
            """
            SELECT code, status, occurrence_count
            FROM agent_session_recovery_findings
            """
        ).fetchone()
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()
    assert finding is not None
    assert str(finding["code"]) == "recovery_authority_json_invalid"
    assert tuple(finding[name] for name in ("status", "occurrence_count")) == (
        "open",
        1,
    )
    assert mailbox_count is not None
    assert int(mailbox_count[0]) == 0

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET data_json = ?
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (
                _review_operation_fence_data(generation),
                key.profile_id,
                key.session_id,
                generation,
            ),
        )

    repaired = scanner.scan()

    assert repaired.delivered_count == 1
    with database.connect() as conn:
        finding = conn.execute(
            """
            SELECT status, resolved_at
            FROM agent_session_recovery_findings
            """
        ).fetchone()
    assert finding is not None
    assert finding["status"] == "resolved"
    assert finding["resolved_at"] is not None


async def test_scanner_blocks_review_without_its_required_operation(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET review_operation_id = '', data_json = '{}'
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_state_requires_operation" in result.results[0].reason_codes
    with database.connect() as conn:
        case = conn.execute(
            "SELECT status, delivery_count FROM agent_session_recovery_cases"
        ).fetchone()
    assert case is not None
    assert tuple(case) == ("scanner_blocked", 0)


async def test_scanner_blocks_terminal_operation_referenced_by_review(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET status = 'completed', finished_at = 20
            WHERE operation_id = 'review-operation'
            """
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_references_terminal_operation" in result.results[0].reason_codes


async def test_scanner_leaves_quiescent_active_chat_without_recovery(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="quiescent active chat recovery scanner test",
    ).ownership.generation
    await SQLiteSessionActorStore(database, clock=lambda: 10.0).ensure(
        key,
        ownership_generation=generation,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = 'active_chat', state_revision = 1,
                active_chat_state_json = '{"bootstrap_status":"completed"}',
                updated_at = 10
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.NO_RECOVERY
    with database.connect() as conn:
        case_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_recovery_cases"
        ).fetchone()
    assert case_count is not None
    assert int(case_count[0]) == 0


async def test_scanner_blocks_unknown_external_action_receipt(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id, session_id,
                ownership_generation, action_ordinal, action_kind, contract_version,
                request_digest, request_json, status, attempt_count, claim_id,
                lease_owner, lease_until, platform_result_json, rejection_json,
                unknown_json, prepared_at, execution_started_at, settled_at, updated_at
            ) VALUES (
                'external-action-idempotency', 'external-action-effect',
                'review-operation', ?, ?, ?, 0, 'send_poke', 1, ?, '{}',
                'unknown', 1, 'external-action-claim', 'external-action-worker',
                NULL, '{}', '{}', '{"reason":"ambiguous"}', 10, 11, 12, 12
            )
            """,
            (key.profile_id, key.session_id, generation, "a" * 64),
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_attempts (
                idempotency_key, attempt_count, claim_id, lease_owner, claimed_at,
                lease_until, status, platform_result_json, rejection_json,
                unknown_json, settled_at
            ) VALUES (
                'external-action-idempotency', 1, 'external-action-claim',
                'external-action-worker', 11, 20, 'unknown', '{}', '{}',
                '{"reason":"ambiguous"}', 12
            )
            """
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "external_action_unknown" in result.results[0].reason_codes


async def test_scanner_waits_for_a_running_operation_lease(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET status = 'running', lease_owner = 'workflow-worker', lease_until = 200
            WHERE operation_id = 'review-operation'
            """
        )

    result = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.WAITING
    assert result.results[0].reason_codes == ("running_operation_lease",)


async def test_scanner_records_delivery_logical_key_storage_alias(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        certificate = scanner.rebuild_certificate(
            conn,
            key=key,
            ownership_generation=generation,
        )
        conn.execute("ROLLBACK")
    payload = RecoveryDeliveryPayload(certificate=certificate, delivery_cycle=0)
    payload_json = canonical_recovery_json(payload.to_record())
    with database.connect() as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, source, occurred_at, payload_json, causation_id,
                correlation_id, trace_id, status, attempt_count, available_at,
                claim_id, lease_owner, lease_until, created_at, handled_at, last_error
            ) VALUES (
                CAST(? AS BLOB), CAST(? AS BLOB), CAST(? AS BLOB), ?, ?, ?,
                100.0, ?, ?, ?, ?, 'pending', 0, 100.0, '', '', NULL, 100.0,
                NULL, ''
            )
            """,
            (
                payload.event_id,
                key.profile_id,
                key.session_id,
                generation,
                RECOVERY_DELIVERY_EVENT_KIND,
                RECOVERY_DELIVERY_EVENT_SOURCE,
                payload_json,
                payload.case_id,
                payload.case_id,
                payload.event_id,
            ),
        )
        conn.execute("PRAGMA foreign_keys = ON")

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.finding_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    assert result.results[0].reason_codes == (
        "recovery_delivery_storage_class_conflict",
    )
    with database.connect() as conn:
        case_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_recovery_cases"
        ).fetchone()
    assert case_count is not None
    assert int(case_count[0]) == 0


async def test_scanner_records_bounded_mailbox_row_overflow(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        for index in range(9):
            conn.execute(
                """
                INSERT INTO agent_session_mailbox (
                    event_id, profile_id, session_id, ownership_generation,
                    kind, source, occurred_at, payload_json, causation_id,
                    correlation_id, trace_id, status, attempt_count, available_at,
                    claim_id, lease_owner, lease_until, created_at, handled_at,
                    last_error
                ) VALUES (?, ?, ?, ?, 'MessageReceived', 'test', 10, '{}', '', '',
                          '', 'pending', 0, 10, '', '', NULL, 10, NULL, '')
                """,
                (f"ordinary-mailbox-{index}", key.profile_id, key.session_id, generation),
            )

    result = scanner.scan()

    assert result.finding_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    assert result.results[0].reason_codes == (
        "recovery_authority_row_limit_exceeded",
    )


async def test_scanner_records_raw_operation_identity_alias(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, started_at, metadata_json
            ) VALUES (CAST(? AS BLOB), ?, ?, ?, 'review', 'pending',
                      'aliased-review-launch', 1, 0, 0, 10, '{}')
            """,
            ("review-operation", key.profile_id, key.session_id, generation),
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.finding_count == 1
    assert result.results[0].disposition is RecoveryScanDisposition.FINDING_RECORDED
    assert result.results[0].reason_codes == (
        "recovery_authority_text_storage_class_invalid",
    )


async def test_scanner_blocks_missing_transition_journal_tail(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET event_sequence = 1
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, generation),
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_transition_tail_missing" in result.results[0].reason_codes


async def test_scanner_blocks_operation_kind_incompatible_with_state(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET kind = 'active_chat_round'
            WHERE operation_id = 'review-operation'
            """
        )

    result = scanner.scan()

    assert result.delivered_count == 0
    assert result.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert "aggregate_operation_kind_conflict" in result.results[0].reason_codes
