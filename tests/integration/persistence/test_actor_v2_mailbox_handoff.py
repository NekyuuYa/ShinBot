"""Integration coverage for dormant immutable Actor v2 mailbox handoffs."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceNotFound
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffReceipt,
    MailboxHandoffEvidenceState,
    MailboxHandoffState,
    MailboxHandoffTarget,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_mailbox_handoff import (
    ActorV2MailboxHandoffRepository,
    MailboxHandoffDiscoveryCursor,
    MailboxHandoffEvidenceConflict,
    MailboxHandoffEvidenceUnavailable,
    MailboxHandoffLeaseLost,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for mailbox-handoff tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _insert_mailbox(
    database: DatabaseManager,
    key: SessionKey,
    *,
    event_id: str,
    ownership_generation: int,
) -> int:
    """Insert one minimal mailbox event without creating a handoff sidecar."""

    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1.0, 1.0)
            ON CONFLICT(profile_id, session_id) DO NOTHING
            """,
            (key.profile_id, key.session_id, ownership_generation),
        )
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, occurred_at, available_at, created_at
            ) VALUES (?, ?, ?, ?, 'MessageReceived', 1.0, 1.0, 1.0)
            """,
            (event_id, key.profile_id, key.session_id, ownership_generation),
        )
    return int(inserted.lastrowid)


def _fenced_mailbox(
    database: DatabaseManager,
    *,
    event_id: str = "mailbox-event-a",
    key: SessionKey | None = None,
) -> tuple[int, FencedMailboxWakeRequest]:
    """Create one current fenced owner and a mailbox event for that incarnation."""

    key = key or SessionKey("profile-a", "profile-a:group:room")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="handoff-test-holder",
        ttl_seconds=300.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="mailbox handoff persistence test",
        admission_grant=grant,
    ).ownership
    mailbox_id = _insert_mailbox(
        database,
        key,
        event_id=event_id,
        ownership_generation=ownership.generation,
    )
    return (
        mailbox_id,
        FencedMailboxWakeRequest(
            key=key,
            ownership_generation=ownership.generation,
            admission_fence_id=ownership.admission_fence_id,
            admission_fence_generation=ownership.admission_fence_generation,
        ),
    )


def test_existing_mailboxes_backfill_to_blocked_unknown_evidence(tmp_path: Path) -> None:
    """Schema startup does not infer a historical mailbox fence from current state."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    mailbox_id = _insert_mailbox(
        database,
        key,
        event_id="historical-mailbox",
        ownership_generation=0,
    )
    with database.connect() as conn:
        conn.execute("DROP TABLE agent_session_mailbox_handoffs")

    database.initialize()

    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.evidence.state is MailboxHandoffEvidenceState.UNKNOWN
    assert record.state is MailboxHandoffState.BLOCKED
    assert record.evidence.admission_fence_id == ""
    assert record.evidence.admission_fence_generation == 0
    with pytest.raises(MailboxHandoffEvidenceUnavailable, match="unknown"):
        database.actor_v2_mailbox_handoffs.require_fenced_evidence(mailbox_id)


def test_missing_sidecar_fails_closed_even_for_current_fenced_owner(tmp_path: Path) -> None:
    """A repository read never reconstructs omitted evidence from current ownership."""

    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)

    assert database.actor_v2_mailbox_handoffs.read(mailbox_id) is None
    assert (
        database.actor_v2_mailbox_handoffs.read_evidence(mailbox_id).state
        is MailboxHandoffEvidenceState.UNKNOWN
    )
    with pytest.raises(MailboxHandoffEvidenceUnavailable, match="no handoff sidecar"):
        database.actor_v2_mailbox_handoffs.require_fenced_evidence(
            mailbox_id,
            expected_request=request,
        )


def test_backfilled_unknown_stays_blocked_for_a_current_fenced_owner(tmp_path: Path) -> None:
    """Restart backfill never upgrades historical evidence from the live owner row."""

    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database, event_id="historical-fenced-owner")
    with database.connect() as conn:
        conn.execute("DROP TABLE agent_session_mailbox_handoffs")
    database.initialize()

    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.evidence.state is MailboxHandoffEvidenceState.UNKNOWN
    with pytest.raises(MailboxHandoffEvidenceUnavailable, match="unknown"):
        database.actor_v2_mailbox_handoffs.require_fenced_evidence(
            mailbox_id,
            expected_request=request,
        )
    with pytest.raises(MailboxHandoffEvidenceUnavailable, match="cannot be upgraded"):
        database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)


def test_sidecar_rejects_mismatched_copied_identity_and_bad_fenced_request(
    tmp_path: Path,
) -> None:
    """Fenced evidence must match the source mailbox key, generation, and row copy."""

    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)
    repository = database.actor_v2_mailbox_handoffs
    mismatched_request = FencedMailboxWakeRequest(
        key=SessionKey("profile-a", "profile-a:group:other"),
        ownership_generation=request.ownership_generation,
        admission_fence_id=request.admission_fence_id,
        admission_fence_generation=request.admission_fence_generation,
    )

    with pytest.raises(MailboxHandoffEvidenceConflict, match="does not match mailbox"):
        repository.record_fenced_handoff(mailbox_id, mismatched_request)
    with pytest.raises(ValueError, match="requires an admission fence"):
        repository.record_fenced_handoff(
            mailbox_id,
            FencedMailboxWakeRequest(
                key=request.key,
                ownership_generation=request.ownership_generation,
            ),
        )

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="does not match source mailbox"):
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
                ) VALUES (?, 'wrong-source-copy', 'profile-a', 'profile-a:group:room',
                          'different-event', ?, 'unknown', '', 0,
                          'blocked', 0, 1.0, '', '', NULL, '', '', '',
                          1.0, 1.0, NULL, NULL, '')
                """,
                (mailbox_id, request.ownership_generation),
            )
        with pytest.raises(sqlite3.IntegrityError):
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
                ) VALUES (?, 'fenced-without-proof', ?, ?, 'mailbox-event-a', ?,
                          'fenced', '', 0,
                          'pending', 0, 1.0, '', '', NULL, '', '', '',
                          1.0, 1.0, NULL, NULL, '')
                """,
                (
                    mailbox_id,
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                ),
            )


def test_explicit_legacy_handoff_is_blocked_with_blank_fence_fields(tmp_path: Path) -> None:
    """Legacy evidence has an explicit durable encoding but no typed wake path."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    mailbox_id = _insert_mailbox(
        database,
        key,
        event_id="legacy-mailbox",
        ownership_generation=0,
    )

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        record = database.actor_v2_mailbox_handoffs.record_unfenced_legacy_handoff_in_transaction(
            conn,
            mailbox_id,
        )
        assert (
            database.actor_v2_mailbox_handoffs.record_unfenced_legacy_handoff_in_transaction(
                conn,
                mailbox_id,
            )
            == record
        )

    assert record.evidence.state is MailboxHandoffEvidenceState.UNFENCED_LEGACY
    assert record.state is MailboxHandoffState.BLOCKED
    assert record.evidence.admission_fence_id == ""
    assert record.evidence.admission_fence_generation == 0
    with pytest.raises(MailboxHandoffEvidenceUnavailable, match="unfenced_legacy"):
        database.actor_v2_mailbox_handoffs.require_fenced_evidence(mailbox_id)


def test_exact_fenced_request_is_immutable_after_recording(tmp_path: Path) -> None:
    """A valid fenced request is replayable exactly, but no evidence field can drift."""

    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)
    repository = database.actor_v2_mailbox_handoffs

    recorded = repository.record_fenced_handoff(mailbox_id, request)
    assert recorded.evidence.as_fenced_wake_request() == request
    assert repository.require_fenced_evidence(
        mailbox_id,
        expected_request=request,
    ) == recorded.evidence
    assert repository.record_fenced_handoff(mailbox_id, request) == recorded

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="immutable evidence cannot change"):
            conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET event_id = 'changed-event'
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable evidence cannot change"):
            conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET admission_fence_generation = admission_fence_generation + 1
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="cannot be deleted"):
            conn.execute(
                "DELETE FROM agent_session_mailbox_handoffs WHERE mailbox_id = ?",
                (mailbox_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="invalid mailbox handoff state"):
            conn.execute(
                """
                UPDATE agent_session_mailbox_handoffs
                SET state = 'settled',
                    target_id = 'forged-target',
                    target_incarnation_id = 'forged-incarnation',
                    target_disposition = 'accepted',
                    settled_at = 2.0
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="mailbox source identity cannot change",
        ):
            conn.execute(
                """
                UPDATE agent_session_mailbox
                SET ownership_generation = ownership_generation + 1
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            )


def test_fenced_handoff_cannot_be_inserted_as_a_forged_terminal_state(tmp_path: Path) -> None:
    """A fenced sidecar must start pending before any target can settle it."""

    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database, event_id="forged-terminal-event")
    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="must begin pending"):
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
                ) VALUES (?, 'forged-terminal', ?, ?, 'forged-terminal-event', ?,
                          'fenced', ?, ?,
                          'settled', 1, 1.0, '', '', NULL,
                          'forged-target', 'forged-incarnation', 'accepted',
                          1.0, 1.0, NULL, 1.0, '')
                """,
                (
                    mailbox_id,
                    request.key.profile_id,
                    request.key.session_id,
                    request.ownership_generation,
                    request.admission_fence_id,
                    request.admission_fence_generation,
                ),
            )


def test_fenced_handoff_rolls_back_when_final_admission_gate_changes(tmp_path: Path) -> None:
    """A fence revoked during candidate writes leaves no durable handoff evidence."""

    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)
    with database.connect() as conn:
        conn.execute(
            """
            CREATE TRIGGER test_mailbox_handoff_remove_fence
            AFTER INSERT ON agent_session_mailbox_handoffs
            BEGIN
                DELETE FROM agent_session_actor_v2_admission_fences
                WHERE profile_id = NEW.profile_id AND session_id = NEW.session_id;
            END
            """
        )

    with pytest.raises(ActorV2AdmissionFenceNotFound):
        database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)

    with database.connect() as conn:
        sidecar_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox_handoffs WHERE mailbox_id = ?",
            (mailbox_id,),
        ).fetchone()
        fence_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_actor_v2_admission_fences"
        ).fetchone()
        conn.execute("DROP TRIGGER test_mailbox_handoff_remove_fence")
    assert sidecar_count is not None
    assert fence_count is not None
    assert int(sidecar_count[0]) == 0
    assert int(fence_count[0]) == 1


def test_fenced_handoff_joins_the_mailbox_producer_transaction(tmp_path: Path) -> None:
    """A future producer can persist mailbox and exact handoff evidence atomically."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:transaction")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="transaction-holder",
        ttl_seconds=300.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="transactional handoff test",
        admission_grant=grant,
    ).ownership
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id, ownership.generation),
        )
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, occurred_at, available_at, created_at
            ) VALUES ('transactional-event', ?, ?, ?, 'MessageReceived', 1.0, 1.0, 1.0)
            """,
            (key.profile_id, key.session_id, ownership.generation),
        )
        record = database.actor_v2_mailbox_handoffs.record_fenced_handoff_in_transaction(
            conn,
            int(inserted.lastrowid),
            request,
        )
        assert record.evidence.as_fenced_wake_request() == request

    assert database.actor_v2_mailbox_handoffs.read(record.evidence.identity.mailbox_id) == record


def test_fenced_handoff_lease_release_and_settlement_are_target_bound(tmp_path: Path) -> None:
    """Only the live target incarnation can release or settle its bounded lease."""

    now = [10.0]
    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)
    repository = ActorV2MailboxHandoffRepository(
        database,
        clock=lambda: now[0],
        handoff_id_factory=lambda: "handoff-a",
        claim_id_factory=lambda: "claim-a" if now[0] == 10.0 else "claim-b",
        lease_seconds=5.0,
    )
    repository.record_fenced_handoff(mailbox_id, request)
    target = MailboxHandoffTarget("wake-target-a", "incarnation-a")
    first_claim = repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="worker-a",
        target=target,
    )
    assert first_claim is not None
    assert first_claim.lease_expires_at == 15.0

    now[0] = 11.0
    released = repository.release_fenced_claim(
        first_claim,
        retry_at=11.0,
        error_message="target restart",
    )
    assert released.state is MailboxHandoffState.PENDING
    assert released.target is None
    assert released.last_error == "target restart"

    now[0] = 12.0
    second_claim = repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="worker-a",
        target=target,
    )
    assert second_claim is not None
    receipt = FencedMailboxHandoffReceipt(
        claim=second_claim,
        wake_receipt=FencedMailboxWakeReceipt(
            request=second_claim.request,
            disposition=FencedMailboxWakeDisposition.ACCEPTED,
        ),
    )
    settled = repository.settle_fenced_claim(receipt)

    assert settled.state is MailboxHandoffState.SETTLED
    assert settled.target == target
    assert settled.target_disposition == FencedMailboxWakeDisposition.ACCEPTED.value
    assert repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="worker-b",
        target=MailboxHandoffTarget("wake-target-b", "incarnation-b"),
    ) is None


def test_deferred_receipt_releases_exact_claim_and_cannot_settle_it(tmp_path: Path) -> None:
    """Temporary target unavailability preserves handoff debt for redelivery."""

    now = [10.0]
    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)
    repository = ActorV2MailboxHandoffRepository(
        database,
        clock=lambda: now[0],
        lease_seconds=5.0,
    )
    repository.record_fenced_handoff(mailbox_id, request)
    claim = repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="worker-a",
        target=MailboxHandoffTarget("wake-target-a", "incarnation-a"),
    )
    assert claim is not None
    deferred = FencedMailboxHandoffReceipt(
        claim=claim,
        wake_receipt=FencedMailboxWakeReceipt(
            request=claim.request,
            disposition=FencedMailboxWakeDisposition.DEFERRED,
        ),
    )

    with pytest.raises(ValueError, match="terminal"):
        repository.settle_fenced_claim(deferred)
    released = repository.defer_fenced_claim(deferred)

    assert released.state is MailboxHandoffState.PENDING
    assert released.target is None
    assert released.target_disposition == ""
    assert released.last_error == "target deferred fenced mailbox handoff"
    redelivered = repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="worker-b",
        target=MailboxHandoffTarget("wake-target-b", "incarnation-b"),
    )
    assert redelivered is not None
    assert redelivered.attempt_count == 2


def test_live_claim_validation_is_transactional_and_exact(tmp_path: Path) -> None:
    """A future target can prove its complete handoff claim without key collapse."""

    now = [10.0]
    database = _database(tmp_path)
    mailbox_id, request = _fenced_mailbox(database)
    repository = ActorV2MailboxHandoffRepository(
        database,
        clock=lambda: now[0],
        lease_seconds=5.0,
    )
    repository.record_fenced_handoff(mailbox_id, request)
    claim = repository.claim_fenced_handoff(
        mailbox_id,
        worker_id="target-worker",
        target=MailboxHandoffTarget("target-a", "incarnation-a"),
    )
    assert claim is not None

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        assert repository.require_live_fenced_claim_in_transaction(conn, claim) == claim

    now[0] = 15.0
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(MailboxHandoffLeaseLost, match="expired"):
            repository.require_live_fenced_claim_in_transaction(conn, claim)


def test_weak_pre_release_sidecar_schema_fails_closed_on_startup(tmp_path: Path) -> None:
    """A same-column table without the contract constraints cannot silently survive."""

    database = _database(tmp_path)
    with database.connect() as conn:
        conn.execute("DROP TABLE agent_session_mailbox_handoffs")
        conn.execute(
            """
            CREATE TABLE agent_session_mailbox_handoffs (
                mailbox_id INTEGER,
                handoff_id TEXT,
                profile_id TEXT,
                session_id TEXT,
                event_id TEXT,
                ownership_generation INTEGER,
                evidence_state TEXT,
                admission_fence_id TEXT,
                admission_fence_generation INTEGER,
                state TEXT,
                attempt_count INTEGER,
                available_at REAL,
                claim_id TEXT,
                lease_owner TEXT,
                lease_until REAL,
                target_id TEXT,
                target_incarnation_id TEXT,
                target_disposition TEXT,
                created_at REAL,
                updated_at REAL,
                claimed_at REAL,
                settled_at REAL,
                last_error TEXT
            )
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="does not match its immutable contract"):
        database.initialize()


def test_discover_fenced_pending_uses_stable_mailbox_keyset_and_preserves_events(
    tmp_path: Path,
) -> None:
    """Pagination keeps every mailbox event, even when SessionKey is shared."""

    database = _database(tmp_path)
    repository = database.actor_v2_mailbox_handoffs
    first_id, request = _fenced_mailbox(database, event_id="discovery-event-a")
    repository.record_fenced_handoff(first_id, request)
    second_id = _insert_mailbox(
        database,
        request.key,
        event_id="discovery-event-b",
        ownership_generation=request.ownership_generation,
    )
    repository.record_fenced_handoff(second_id, request)
    third_id = _insert_mailbox(
        database,
        request.key,
        event_id="discovery-event-c",
        ownership_generation=request.ownership_generation,
    )
    repository.record_fenced_handoff(third_id, request)

    first_page = repository.discover_fenced_pending(
        limit=2,
        profile_id=request.key.profile_id,
        session_id=request.key.session_id,
    )
    assert [record.evidence.identity.mailbox_id for record in first_page.records] == [
        first_id,
        second_id,
    ]
    assert [record.evidence.identity.event_id for record in first_page.records] == [
        "discovery-event-a",
        "discovery-event-b",
    ]
    assert first_page.has_more is True
    assert first_page.next_cursor is not None
    assert first_page.next_cursor.mailbox_id == second_id

    second_page = repository.discover_fenced_pending(
        limit=2,
        after=first_page.next_cursor,
        profile_id=request.key.profile_id,
        session_id=request.key.session_id,
    )
    assert [record.evidence.identity.mailbox_id for record in second_page.records] == [
        third_id,
    ]
    assert [record.evidence.identity.event_id for record in second_page.records] == [
        "discovery-event-c",
    ]
    assert second_page.has_more is False


def test_discover_fenced_pending_can_scope_to_one_complete_fence_request(
    tmp_path: Path,
) -> None:
    """A target scan cannot widen from a session key to another fence identity."""

    database = _database(tmp_path)
    repository = database.actor_v2_mailbox_handoffs
    mailbox_id, request = _fenced_mailbox(database, event_id="exact-fence-scope")
    repository.record_fenced_handoff(mailbox_id, request)
    different_fence = FencedMailboxWakeRequest(
        key=request.key,
        ownership_generation=request.ownership_generation,
        admission_fence_id="another-admission-fence",
        admission_fence_generation=request.admission_fence_generation,
    )

    excluded = repository.discover_fenced_pending(
        limit=10,
        expected_request=different_fence,
    )
    scoped = repository.discover_fenced_pending(
        limit=10,
        expected_request=request,
    )

    assert excluded.records == ()
    assert [record.evidence.identity.mailbox_id for record in scoped.records] == [
        mailbox_id
    ]
    assert scoped.next_cursor is not None
    with pytest.raises(ValueError, match="expected_request filter"):
        repository.discover_fenced_pending(
            limit=10,
            after=scoped.next_cursor,
            expected_request=different_fence,
        )


def test_discover_fenced_pending_excludes_non_pending_and_non_fenced_rows(
    tmp_path: Path,
) -> None:
    """Discovery never broadens into claims, settled debt, or legacy evidence."""

    database = _database(tmp_path)
    repository = database.actor_v2_mailbox_handoffs

    pending_id, pending_request = _fenced_mailbox(
        database,
        event_id="discover-pending",
        key=SessionKey("profile-pending", "profile-pending:group:room"),
    )
    repository.record_fenced_handoff(pending_id, pending_request)

    claimed_id, claimed_request = _fenced_mailbox(
        database,
        event_id="discover-claimed",
        key=SessionKey("profile-claimed", "profile-claimed:group:room"),
    )
    repository.record_fenced_handoff(claimed_id, claimed_request)
    claimed = repository.claim_fenced_handoff(
        claimed_id,
        worker_id="discovery-worker",
        target=MailboxHandoffTarget("discovery-target", "incarnation-claimed"),
    )
    assert claimed is not None

    settled_id, settled_request = _fenced_mailbox(
        database,
        event_id="discover-settled",
        key=SessionKey("profile-settled", "profile-settled:group:room"),
    )
    repository.record_fenced_handoff(settled_id, settled_request)
    settled_claim = repository.claim_fenced_handoff(
        settled_id,
        worker_id="discovery-worker",
        target=MailboxHandoffTarget("discovery-target", "incarnation-settled"),
    )
    assert settled_claim is not None
    repository.settle_fenced_claim(
        FencedMailboxHandoffReceipt(
            claim=settled_claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=settled_claim.request,
                disposition=FencedMailboxWakeDisposition.ACCEPTED,
            ),
        )
    )

    legacy_id = _insert_mailbox(
        database,
        SessionKey("profile-legacy", "profile-legacy:group:room"),
        event_id="discover-legacy",
        ownership_generation=0,
    )
    repository.record_unfenced_legacy_handoff(legacy_id)
    unknown_id = _insert_mailbox(
        database,
        SessionKey("profile-unknown", "profile-unknown:group:room"),
        event_id="discover-unknown",
        ownership_generation=0,
    )
    database.initialize()

    page = repository.discover_fenced_pending(limit=100)
    assert [record.evidence.identity.mailbox_id for record in page.records] == [pending_id]
    assert legacy_id != pending_id
    unknown = repository.read(unknown_id)
    assert unknown is not None
    assert unknown.state is MailboxHandoffState.BLOCKED
    assert unknown.evidence.state is MailboxHandoffEvidenceState.UNKNOWN


def test_discovery_fails_closed_on_copied_identity_conflict(tmp_path: Path) -> None:
    """A corrupt copied identity is rejected instead of exposing a SQL row."""

    database = _database(tmp_path)
    repository = database.actor_v2_mailbox_handoffs
    mailbox_id, request = _fenced_mailbox(database, event_id="discover-identity")
    repository.record_fenced_handoff(mailbox_id, request)

    with database.connect() as conn:
        conn.execute("DROP TRIGGER trg_agent_session_mailbox_handoff_identity_immutable")
        conn.execute(
            """
            UPDATE agent_session_mailbox_handoffs
            SET event_id = 'forged-discovery-event'
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        )

    with pytest.raises(MailboxHandoffEvidenceConflict, match="immutable identity"):
        repository.discover_fenced_pending(limit=1)


def test_discovery_cursor_and_filters_validate_strictly(tmp_path: Path) -> None:
    """Reject invalid page bounds, cursor types, and mismatched filters."""

    database = _database(tmp_path)
    repository = database.actor_v2_mailbox_handoffs
    mailbox_id, request = _fenced_mailbox(database, event_id="discover-inputs")
    repository.record_fenced_handoff(mailbox_id, request)
    page = repository.discover_fenced_pending(limit=1)
    assert page.next_cursor is not None

    for invalid_limit in (0, -1, True, 1001):
        with pytest.raises(ValueError, match="limit"):
            repository.discover_fenced_pending(limit=invalid_limit)
    with pytest.raises(TypeError, match="after"):
        repository.discover_fenced_pending(after=object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="profile_id"):
        repository.discover_fenced_pending(profile_id=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="profile_id"):
        repository.discover_fenced_pending(profile_id=" ")
    with pytest.raises(ValueError, match="session_id"):
        repository.discover_fenced_pending(session_id=" ")
    with pytest.raises(ValueError, match="profile_id filter"):
        repository.discover_fenced_pending(
            after=page.next_cursor,
            profile_id="different-profile",
        )
    with pytest.raises(ValueError, match="mailbox_id must be a positive integer"):
        MailboxHandoffDiscoveryCursor(
            mailbox_id=0,
            handoff_id="handoff",
            profile_id=request.key.profile_id,
            session_id=request.key.session_id,
            event_id="event",
            ownership_generation=request.ownership_generation,
        )
