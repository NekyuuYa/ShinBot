"""Integration coverage for typed recovery's commit-time authority boundary."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.actor import AgentSessionActor
from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RecoveryCertificate,
    RecoveryDeliveryEnvelopeIdentity,
    canonical_recovery_json,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryCommitIntent,
    RecoveryDeliveryClaimLost,
    RecoveryMaterializationBlocked,
)
from shinbot.agent.runtime.session_actor.recovery_commit_coordinator import (
    PreparedRecoveryCommit,
    RecoveryCommitAuthorityError,
    RecoveryCommitResolution,
    SQLiteRecoveryCommitCoordinator,
)
from shinbot.agent.runtime.session_actor.recovery_graph_reader import (
    SQLiteRecoveryGraphReader,
    ValidatedClaimedRecoveryDelivery,
)
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    RecoveryScanDisposition,
    SQLiteRecoveryGraphScanner,
)
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.store import (
    DurableRecordConflict,
    SQLiteSessionActorStore,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager


def _make_database(tmp_path: Path) -> DatabaseManager:
    """Create one initialized SQLite domain for a coordinator test."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _review_operation_fence_data(generation: int) -> str:
    """Return the minimum aggregate fence used by an orphaned review operation."""

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
    """Create the smallest Actor-v2 review state eligible for typed recovery."""

    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="recovery commit coordinator integration test",
    ).ownership.generation
    await SQLiteSessionActorStore(database, clock=lambda: 10.0).ensure(
        key,
        ownership_generation=generation,
    )
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


class _ApplyingMaterializer:
    """Test materializer which repairs the review aggregate into idle."""

    def __init__(self) -> None:
        self.calls = 0
        self.certificates: list[RecoveryCertificate] = []

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition:
        """Return one ordinary durable repair transition."""

        del intent
        self.calls += 1
        self.certificates.append(certificate)
        return SessionTransition(
            aggregate=aggregate.advance(
                state_changed=True,
                state="idle",
                review_operation_id="",
                data={"recovered_operation": "review-operation"},
            ),
            disposition="recovery_repaired",
            reason="recovery_requested",
        )


class _BlockingMaterializer:
    """Test materializer which returns an explicit fail-closed result."""

    def __init__(self, code: str = "recovery_test_blocked") -> None:
        self.calls = 0
        self.code = code

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> RecoveryMaterializationBlocked:
        """Block without deriving a business state transition."""

        del aggregate, intent, certificate
        self.calls += 1
        return RecoveryMaterializationBlocked(code=self.code)


class _RaisingMaterializer:
    """Test materializer whose implementation error must become a blocker."""

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition:
        """Fail before producing a transition."""

        del aggregate, intent, certificate
        raise RuntimeError("recovery materializer crashed")


class _InvalidTransitionMaterializer:
    """Test materializer that violates the normal actor transition contract."""

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition:
        """Return a transition that omits the required event-sequence advance."""

        del intent, certificate
        return SessionTransition(
            aggregate=aggregate,
            disposition="recovery_invalid_transition",
            reason="recovery_requested",
        )


class _OversizedResultMaterializer:
    """Test materializer which exceeds the raw-reader journal metadata bound."""

    def __init__(self) -> None:
        self.calls = 0

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition:
        """Return a normal transition with intentionally oversized metadata."""

        del intent, certificate
        self.calls += 1
        return SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="recovery_oversized_result",
            result={"payload": "x" * 4_096},
            reason="recovery_requested",
        )


class _AuthorityLeakingMaterializer:
    """Test materializer which tries to persist its complete certificate."""

    def __init__(self) -> None:
        self.calls = 0

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition:
        """Return one otherwise-valid transition containing forbidden authority."""

        del intent
        self.calls += 1
        return SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="recovery_authority_leak",
            result={"certificate": certificate.to_record()},
            reason="recovery_requested",
        )


class _AggregateAuthorityLeakingMaterializer:
    """Test materializer that hides authority in aggregate persistence state."""

    def __init__(self, *, serialized: bool = False, padding: int = 0) -> None:
        self.calls = 0
        self._serialized = serialized
        self._padding = padding

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition:
        """Return a valid-looking state update containing forbidden authority."""

        del intent
        self.calls += 1
        if self._serialized:
            authority: object = (
                " " * self._padding + canonical_recovery_json(certificate.to_record())
            )
        else:
            authority = certificate.to_record()
        return SessionTransition(
            aggregate=aggregate.advance(
                state_changed=True,
                data={"certificate": authority},
            ),
            disposition="recovery_aggregate_authority_leak",
            reason="recovery_requested",
        )


class _MismatchedMailboxCoordinator(SQLiteRecoveryCommitCoordinator):
    """Inject an impossible physical mailbox id to protect ordering at the seam."""

    def prepare(
        self,
        conn: sqlite3.Connection,
        *,
        claim: ClaimedSessionEvent,
        intent: RecoveryCommitIntent,
        provisional_transition: SessionTransition,
        commit_now: float,
    ) -> PreparedRecoveryCommit:
        """Return valid raw proof with a deliberately mismatched physical row id."""

        prepared = super().prepare(
            conn,
            claim=claim,
            intent=intent,
            provisional_transition=provisional_transition,
            commit_now=commit_now,
        )
        return replace(
            prepared,
            delivery=ValidatedClaimedRecoveryDelivery(
                mailbox_id=prepared.delivery.mailbox_id + 1,
                delivery=prepared.delivery.delivery,
            ),
        )


class _FailingFinalizeCoordinator(SQLiteRecoveryCommitCoordinator):
    """Inject a post-mailbox failure to verify the enclosing transaction rolls back."""

    def __init__(self, reader: SQLiteRecoveryGraphReader, **kwargs: object) -> None:
        super().__init__(reader, **kwargs)
        self.saw_completed_mailbox = False

    def finalize_case(
        self,
        conn: sqlite3.Connection,
        resolution: RecoveryCommitResolution,
        *,
        commit_now: float,
    ) -> None:
        """Observe completion inside the transaction, then abort it."""

        del commit_now
        row = conn.execute(
            "SELECT status FROM agent_session_mailbox WHERE mailbox_id = ?",
            (resolution.mailbox_id,),
        ).fetchone()
        self.saw_completed_mailbox = row is not None and str(row["status"]) == "completed"
        raise RuntimeError("injected recovery case finalization failure")


def _coordinated_store(
    database: DatabaseManager,
    *,
    materializers: dict[str, object] | None = None,
    coordinator_type: type[SQLiteRecoveryCommitCoordinator] = SQLiteRecoveryCommitCoordinator,
) -> SQLiteSessionActorStore:
    """Create a store whose coordinator shares the exact database domain."""

    coordinator = coordinator_type(
        SQLiteRecoveryGraphReader(database),
        materializers=materializers,
    )
    return SQLiteSessionActorStore(
        database,
        clock=lambda: 200.0,
        retry_delay_seconds=0.0,
        recovery_commit_coordinator=coordinator,
    )


async def _claim_and_reduce(
    store: SQLiteSessionActorStore,
    *,
    key: SessionKey,
) -> tuple[ClaimedSessionEvent, AgentSessionAggregate, SessionTransition]:
    """Claim the scanner delivery and produce its provisional pure carrier."""

    claim = await store.claim_next(key, worker_id="recovery-commit-worker")
    assert claim is not None
    aggregate = await store.load(key)
    transition = AgentSessionReducer().reduce(aggregate, claim.envelope)
    assert transition.recovery_commit_intent is not None
    return claim, aggregate, transition


def _recovery_state(
    database: DatabaseManager,
    *,
    key: SessionKey,
    event_id: str,
) -> tuple[tuple[object, ...], tuple[object, ...], tuple[object, ...]]:
    """Return aggregate, mailbox, and case evidence for one typed delivery."""

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT state, state_revision, event_sequence
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT status, attempt_count, claim_id, lease_owner
            FROM agent_session_mailbox
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        case = conn.execute(
            """
            SELECT status, delivery_count, next_delivery_cycle, last_error
            FROM agent_session_recovery_cases
            """,
        ).fetchone()
    assert aggregate is not None
    assert mailbox is not None
    assert case is not None
    return tuple(aggregate), tuple(mailbox), tuple(case)


def _append_intervening_no_op_transition(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
    event_id: str,
) -> None:
    """Advance only the aggregate/journal clock to model external drift.

    The recovery mailbox remains the actor's processing head. This
    fault-injection fixture preserves the exact tail evidence the raw reader
    requires before it will rebuild a new certificate, while simulating a
    certificate fence drift between scan and commit.
    """

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT state, state_revision, event_sequence
            FROM agent_session_aggregates
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, ownership_generation),
        ).fetchone()
        assert aggregate is not None
        next_sequence = int(aggregate["event_sequence"]) + 1
        updated = conn.execute(
            """
            UPDATE agent_session_aggregates
            SET event_sequence = ?, updated_at = 150
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND event_sequence = ?
            """,
            (
                next_sequence,
                key.profile_id,
                key.session_id,
                ownership_generation,
                int(aggregate["event_sequence"]),
            ),
        )
        assert updated.rowcount == 1
        conn.execute(
            """
            INSERT INTO agent_state_transitions (
                transition_id, profile_id, session_id, ownership_generation,
                event_id, from_state, to_state, trigger, disposition,
                state_revision, event_sequence, operation_id, plan_id,
                trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'test_reconciliation',
                      'intervening_noop', ?, ?, '', '', '', '{"result":{}}', 150)
            """,
            (
                f"intervening-recovery-transition:{event_id}",
                key.profile_id,
                key.session_id,
                ownership_generation,
                event_id,
                str(aggregate["state"]),
                str(aggregate["state"]),
                int(aggregate["state_revision"]),
                next_sequence,
            ),
        )


def _advance_to_idle_for_recovery_fence_drift(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
) -> None:
    """Create a valid newer aggregate revision before a stale recovery commit."""

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT state, state_revision, event_sequence
            FROM agent_session_aggregates
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
            """,
            (key.profile_id, key.session_id, ownership_generation),
        ).fetchone()
        assert aggregate is not None
        next_revision = int(aggregate["state_revision"]) + 1
        next_sequence = int(aggregate["event_sequence"]) + 1
        updated = conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = 'idle', state_revision = ?, event_sequence = ?,
                review_operation_id = '', updated_at = 150
            WHERE profile_id = ?
              AND session_id = ?
              AND ownership_generation = ?
              AND state_revision = ?
              AND event_sequence = ?
            """,
            (
                next_revision,
                next_sequence,
                key.profile_id,
                key.session_id,
                ownership_generation,
                int(aggregate["state_revision"]),
                int(aggregate["event_sequence"]),
            ),
        )
        assert updated.rowcount == 1
        conn.execute(
            """
            INSERT INTO agent_state_transitions (
                transition_id, profile_id, session_id, ownership_generation,
                event_id, from_state, to_state, trigger, disposition,
                state_revision, event_sequence, operation_id, plan_id,
                trace_id, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, 'recovery-fence-drift', ?, 'idle',
                      'test_reconciliation', 'intervening_state_change', ?, ?,
                      '', '', '', '{"result":{}}', 150)
            """,
            (
                "intervening-recovery-state-change",
                key.profile_id,
                key.session_id,
                ownership_generation,
                str(aggregate["state"]),
                next_revision,
                next_sequence,
            ),
        )


async def test_coordinator_applies_materialized_recovery_atomically(
    tmp_path: Path,
) -> None:
    """A proven delivery applies its normal transition and settles the case once."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scan = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan()
    assert scan.delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    committed = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 1
    assert committed.state == "idle"
    assert committed.state_revision == 2
    assert committed.event_sequence == 1
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("idle", 2, 1)
    assert mailbox_row[0] == "completed"
    assert case_row == ("applied", 1, 1, "")
    with database.connect() as conn:
        journal = conn.execute(
            """
            SELECT disposition, event_sequence
            FROM agent_state_transitions
            WHERE event_id = ?
            """,
            (claim.envelope.event_id,),
        ).fetchone()
    assert journal is not None
    assert tuple(journal) == ("recovery_repaired", 1)


async def test_coordinator_supersedes_changed_graph_without_materializing(
    tmp_path: Path,
) -> None:
    """A graph whose semantic case changed produces only a fenced no-op journal."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_operations
            SET status = 'completed', finished_at = 150
            WHERE operation_id = 'review-operation'
            """,
        )

    committed = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 0
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        1,
    )
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 1)
    assert mailbox_row[0] == "completed"
    assert case_row[0] == "superseded"
    assert case_row[3] == "recovery_semantic_graph_changed"
    with database.connect() as conn:
        disposition = conn.execute(
            "SELECT disposition FROM agent_state_transitions WHERE event_id = ?",
            (claim.envelope.event_id,),
        ).fetchone()
    assert disposition is not None
    assert str(disposition[0]) == "recovery_superseded"


async def test_coordinator_blocks_missing_or_explicit_materialization(
    tmp_path: Path,
) -> None:
    """Missing and explicit blockers settle mailbox/case without business mutation."""

    for materializers, expected_error in (
        ({}, "recovery_materializer_missing"),
        ({"review": _BlockingMaterializer()}, "recovery_test_blocked"),
        ({"review": _RaisingMaterializer()}, "recovery_materializer_failed"),
        (
            {"review": _InvalidTransitionMaterializer()},
            "recovery_materialized_transition_invalid",
        ),
    ):
        database = _make_database(tmp_path / expected_error)
        key = SessionKey("profile-a", "bot:group:room")
        await _seed_orphaned_review(database, key=key)
        assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
        store = _coordinated_store(database, materializers=materializers)
        claim, aggregate, transition = await _claim_and_reduce(store, key=key)

        committed = await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

        assert (committed.state, committed.state_revision, committed.event_sequence) == (
            "review",
            1,
            1,
        )
        aggregate_row, mailbox_row, case_row = _recovery_state(
            database,
            key=key,
            event_id=claim.envelope.event_id,
        )
        assert aggregate_row == ("review", 1, 1)
        assert mailbox_row[0] == "completed"
        assert case_row == ("scanner_blocked", 1, 1, expected_error)


async def test_coordinator_rejects_raw_claim_drift_before_materializing(
    tmp_path: Path,
) -> None:
    """A changed claim fence leaves all recovery-owned durable state untouched."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET attempt_count = attempt_count + 1
            WHERE event_id = ?
            """,
            (claim.envelope.event_id,),
        )

    with pytest.raises(RecoveryDeliveryClaimLost) as raised:
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    assert raised.value.code == "recovery_delivery_claim_attempt_count_changed"
    assert materializer.calls == 0
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 0)
    assert mailbox_row[0:2] == ("processing", 2)
    assert case_row == ("open", 1, 1, "")
    with database.connect() as conn:
        journal_count = conn.execute(
            "SELECT COUNT(*) FROM agent_state_transitions"
        ).fetchone()
    assert journal_count is not None
    assert int(journal_count[0]) == 0


async def test_actor_does_not_dead_letter_an_undecodable_typed_recovery_delivery(
    tmp_path: Path,
) -> None:
    """Raw typed payload corruption must not advance the normal mailbox sequence."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    store = _coordinated_store(database, materializers={"review": _ApplyingMaterializer()})
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET payload_json = '{}'
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        )
    actor = AgentSessionActor(
        key=key,
        store=store,
        handler=AgentSessionReducer().reduce,
        worker_id="undecodable-typed-recovery-worker",
        max_attempts=1,
    )

    assert await actor._drain_mailbox() is False

    with database.connect() as conn:
        aggregate = conn.execute(
            """
            SELECT state, state_revision, event_sequence
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT status, attempt_count
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        case = conn.execute(
            "SELECT status FROM agent_session_recovery_cases"
        ).fetchone()
        transition_count = conn.execute(
            "SELECT COUNT(*) FROM agent_state_transitions"
        ).fetchone()
    assert aggregate is not None
    assert mailbox is not None
    assert case is not None
    assert transition_count is not None
    assert tuple(aggregate) == ("review", 1, 0)
    assert tuple(mailbox) == ("processing", 1)
    assert str(case[0]) == "open"
    assert int(transition_count[0]) == 0


async def test_typed_delivery_cannot_bypass_commit_time_recovery_proof(
    tmp_path: Path,
) -> None:
    """A custom handler cannot treat scanner-owned recovery as an ordinary event."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim = await store.claim_next(key, worker_id="recovery-commit-worker")
    assert claim is not None
    aggregate = await store.load(key)
    bypass = SessionTransition(
        aggregate=aggregate.advance(state_changed=False),
        disposition="custom_handler_bypass",
        reason="test",
    )

    with pytest.raises(DurableRecordConflict, match="requires a recovery commit intent"):
        await store.commit(
            claim,
            bypass,
            expected_revision=aggregate.state_revision,
        )

    assert materializer.calls == 0
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 0)
    assert mailbox_row[0] == "processing"
    assert case_row == ("open", 1, 1, "")


async def test_coordinator_settles_superseded_case_after_actor_revision_drift(
    tmp_path: Path,
) -> None:
    """Commit-time proof supersedes using its current revision, not actor stale state."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, stale_aggregate, transition = await _claim_and_reduce(store, key=key)
    _advance_to_idle_for_recovery_fence_drift(
        database,
        key=key,
        ownership_generation=generation,
    )

    committed = await store.commit(
        claim,
        transition,
        expected_revision=stale_aggregate.state_revision,
    )

    assert materializer.calls == 0
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "idle",
        2,
        2,
    )
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("idle", 2, 2)
    assert mailbox_row[0] == "completed"
    assert case_row[0] == "superseded"


async def test_scanner_reopens_a_resolved_materializer_blocker(
    tmp_path: Path,
) -> None:
    """A repaired materializer capability can issue a fresh typed delivery cycle."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    assert scanner.scan().delivered_count == 1
    store = _coordinated_store(database, materializers={})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)
    await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    retried = scanner.scan()

    assert retried.delivered_count == 1
    assert retried.results[0].disposition is RecoveryScanDisposition.DELIVERED
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT status, delivery_count, next_delivery_cycle, last_error
            FROM agent_session_recovery_cases
            """
        ).fetchone()
    assert case is not None
    assert tuple(case) == ("open", 2, 2, "")


async def test_coordinator_rolls_back_every_write_after_mailbox_completion_failure(
    tmp_path: Path,
) -> None:
    """A failure after mailbox completion still restores aggregate, mailbox, and case."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    coordinator = _FailingFinalizeCoordinator(
        SQLiteRecoveryGraphReader(database),
        materializers={"review": materializer},
    )
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: 200.0,
        retry_delay_seconds=0.0,
        recovery_commit_coordinator=coordinator,
    )
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    with pytest.raises(RuntimeError, match="finalization failure"):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    assert coordinator.saw_completed_mailbox is True
    assert materializer.calls == 1
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 0)
    assert mailbox_row[0] == "processing"
    assert case_row == ("open", 1, 1, "")
    with database.connect() as conn:
        journal_count = conn.execute(
            "SELECT COUNT(*) FROM agent_state_transitions"
        ).fetchone()
    assert journal_count is not None
    assert int(journal_count[0]) == 0


async def test_refreshed_certificate_emits_the_next_delivery_cycle(
    tmp_path: Path,
) -> None:
    """A completed stale delivery is retried only after a new certificate exists."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    first_scan = scanner.scan()
    assert first_scan.delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    recovery_claim, recovery_aggregate, recovery_transition = await _claim_and_reduce(
        store,
        key=key,
    )
    _append_intervening_no_op_transition(
        database,
        key=key,
        ownership_generation=generation,
        event_id="recovery-refresh-intervening",
    )

    committed = await store.commit(
        recovery_claim,
        recovery_transition,
        expected_revision=recovery_aggregate.state_revision,
    )

    assert materializer.calls == 0
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        2,
    )
    follow_up = scanner.scan()
    assert follow_up.delivered_count == 1
    assert follow_up.results[0].disposition is RecoveryScanDisposition.DELIVERED
    assert follow_up.results[0].case_id == first_scan.results[0].case_id
    assert follow_up.results[0].event_id.endswith(":1")
    with database.connect() as conn:
        case = conn.execute(
            """
            SELECT status, delivery_count, next_delivery_cycle, last_event_id
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        mailbox = conn.execute(
            "SELECT status FROM agent_session_mailbox WHERE event_id = ?",
            (follow_up.results[0].event_id,),
        ).fetchone()
    assert case is not None
    assert mailbox is not None
    assert tuple(case) == ("open", 2, 2, follow_up.results[0].event_id)
    assert str(mailbox[0]) == "pending"


async def test_refresh_cycle_limit_becomes_a_completed_delivery_blocker(
    tmp_path: Path,
) -> None:
    """A completed refresh at the limit cannot be misclassified as dead-lettered."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    generation = await _seed_orphaned_review(database, key=key)
    scanner = SQLiteRecoveryGraphScanner(
        database,
        clock=lambda: 100.0,
        max_delivery_cycles=1,
    )
    assert scanner.scan().delivered_count == 1
    store = _coordinated_store(database, materializers={"review": _ApplyingMaterializer()})
    recovery_claim, recovery_aggregate, recovery_transition = await _claim_and_reduce(
        store,
        key=key,
    )
    _append_intervening_no_op_transition(
        database,
        key=key,
        ownership_generation=generation,
        event_id="recovery-refresh-limit-intervening",
    )
    await store.commit(
        recovery_claim,
        recovery_transition,
        expected_revision=recovery_aggregate.state_revision,
    )

    limited = scanner.scan()

    assert limited.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert limited.results[0].reason_codes == ("recovery_refresh_cycle_limit_reached",)
    with database.connect() as conn:
        case = conn.execute(
            "SELECT status, last_error, updated_at FROM agent_session_recovery_cases"
        ).fetchone()
    assert case is not None
    assert tuple(case[0:2]) == (
        "scanner_blocked",
        "recovery_refresh_cycle_limit_reached",
    )

    repeated = scanner.scan()

    assert repeated.results[0].disposition is RecoveryScanDisposition.BLOCKED
    assert repeated.results[0].reason_codes == ("recovery_refresh_cycle_limit_reached",)
    with database.connect() as conn:
        repeated_case = conn.execute(
            "SELECT status, last_error, updated_at FROM agent_session_recovery_cases"
        ).fetchone()
    assert repeated_case is not None
    assert tuple(repeated_case) == tuple(case)


async def test_materializer_metadata_cannot_exceed_the_recovery_reader_bound(
    tmp_path: Path,
) -> None:
    """Settle unreadable materializer metadata as a proven durable blocker."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _OversizedResultMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    committed = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 1
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        1,
    )
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 1)
    assert mailbox_row[0] == "completed"
    assert case_row == (
        "scanner_blocked",
        1,
        1,
        "recovery_materializer_result_too_large",
    )


async def test_materializer_cannot_leak_its_complete_recovery_authority(
    tmp_path: Path,
) -> None:
    """Settle forbidden certificate metadata without persisting the authority."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _AuthorityLeakingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    committed = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 1
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        1,
    )
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 1)
    assert mailbox_row[0] == "completed"
    assert case_row == (
        "scanner_blocked",
        1,
        1,
        "recovery_materializer_persistence_leaks_authority",
    )


@pytest.mark.parametrize(
    ("serialized", "padding"),
    [(False, 0), (True, 0), (True, 4_097)],
)
async def test_materializer_cannot_leak_recovery_authority_through_aggregate_state(
    tmp_path: Path,
    serialized: bool,
    padding: int,
) -> None:
    """Aggregate persistence fields are not a second recovery authority channel."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _AggregateAuthorityLeakingMaterializer(
        serialized=serialized,
        padding=padding,
    )
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    committed = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 1
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        1,
    )
    assert committed.data == {
        "operation_fences": {
            "review-operation": {
                "operation_id": "review-operation",
                "ownership_generation": 1,
            }
        }
    }
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 1)
    assert mailbox_row[0] == "completed"
    assert case_row == (
        "scanner_blocked",
        1,
        1,
        "recovery_materializer_persistence_leaks_authority",
    )


async def test_provisional_recovery_carrier_cannot_add_journal_metadata(
    tmp_path: Path,
) -> None:
    """A valid delivery turns an unsafe provisional carrier into a blocker."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)
    injected = replace(transition, result={"untrusted": "carrier"})

    committed = await store.commit(
        claim,
        injected,
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 0
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        1,
    )
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 1)
    assert mailbox_row[0] == "completed"
    assert case_row == (
        "scanner_blocked",
        1,
        1,
        "recovery_provisional_transition_not_empty",
    )


async def test_mismatched_recovery_intent_settles_the_raw_proven_case(
    tmp_path: Path,
) -> None:
    """The coordinator must not leave a valid case open for a carrier mismatch."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    store = _coordinated_store(database, materializers={"review": materializer})
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)
    mismatched_digest = "f" * 64
    mismatched_case_id = f"recovery-case:v1:{mismatched_digest}"
    mismatched_intent = RecoveryCommitIntent(
        envelope=RecoveryDeliveryEnvelopeIdentity(
            event_id=f"recovery-requested:v1:{mismatched_digest}:0",
            profile_id=key.profile_id,
            session_id=key.session_id,
            ownership_generation=claim.envelope.ownership_generation,
            kind=claim.envelope.kind,
            source=claim.envelope.source,
        ),
        case_id=mismatched_case_id,
        delivery_cycle=0,
        certificate_digest="0" * 64,
    )

    committed = await store.commit(
        claim,
        replace(transition, recovery_commit_intent=mismatched_intent),
        expected_revision=aggregate.state_revision,
    )

    assert materializer.calls == 0
    assert (committed.state, committed.state_revision, committed.event_sequence) == (
        "review",
        1,
        1,
    )
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 1)
    assert mailbox_row[0] == "completed"
    assert case_row == (
        "scanner_blocked",
        1,
        1,
        "recovery_intent_claim_identity_changed",
    )


async def test_physical_mailbox_fence_rejects_before_materialization(
    tmp_path: Path,
) -> None:
    """The store must not materialize after raw proof identifies another row."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    coordinator = _MismatchedMailboxCoordinator(
        SQLiteRecoveryGraphReader(database),
        materializers={"review": materializer},
    )
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: 200.0,
        recovery_commit_coordinator=coordinator,
    )
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    with pytest.raises(DurableRecordConflict, match="physical mailbox identity"):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    assert materializer.calls == 0
    aggregate_row, mailbox_row, case_row = _recovery_state(
        database,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 0)
    assert mailbox_row[0] == "processing"
    assert case_row == ("open", 1, 1, "")


async def test_store_requires_same_domain_coordinator_and_never_falls_back(
    tmp_path: Path,
) -> None:
    """A typed carrier cannot silently use another database or normal commit path."""

    first = _make_database(tmp_path / "first")
    second = _make_database(tmp_path / "second")
    mismatched = SQLiteRecoveryCommitCoordinator(SQLiteRecoveryGraphReader(second))
    with pytest.raises(ValueError, match="share the store persistence domain"):
        SQLiteSessionActorStore(first, recovery_commit_coordinator=mismatched)

    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(first, key=key)
    assert SQLiteRecoveryGraphScanner(first, clock=lambda: 100.0).scan().delivered_count == 1
    store = SQLiteSessionActorStore(first, clock=lambda: 200.0)
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    with pytest.raises(DurableRecordConflict, match="requires a recovery commit coordinator"):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )

    aggregate_row, mailbox_row, case_row = _recovery_state(
        first,
        key=key,
        event_id=claim.envelope.event_id,
    )
    assert aggregate_row == ("review", 1, 0)
    assert mailbox_row[0] == "processing"
    assert case_row == ("open", 1, 1, "")


async def test_coordinator_rejects_a_changed_aggregate_fence_before_materializing(
    tmp_path: Path,
) -> None:
    """The materializer never sees an aggregate that differs from raw proof."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", "bot:group:room")
    await _seed_orphaned_review(database, key=key)
    assert SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0).scan().delivered_count == 1
    materializer = _ApplyingMaterializer()
    coordinator = SQLiteRecoveryCommitCoordinator(
        SQLiteRecoveryGraphReader(database),
        materializers={"review": materializer},
    )
    store = SQLiteSessionActorStore(database, clock=lambda: 200.0)
    claim, aggregate, transition = await _claim_and_reduce(store, key=key)

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        prepared = coordinator.prepare(
            conn,
            claim=claim,
            intent=transition.recovery_commit_intent,
            provisional_transition=transition,
            commit_now=200.0,
        )
        with pytest.raises(RecoveryCommitAuthorityError) as raised:
            coordinator.resolve(
                prepared,
                aggregate=replace(
                    aggregate,
                    event_sequence=aggregate.event_sequence + 1,
                ),
            )
        conn.execute("ROLLBACK")

    assert raised.value.code == "recovery_certificate_aggregate_fence_changed"
    assert materializer.calls == 0
