"""Cross-store ownership-generation fencing for the session actor runtime."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectEnvelope,
    EffectClaimLost,
    EffectExecutionContract,
    EffectLane,
    completion_event_id,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.reducer import (
    AgentSessionEventKind,
    AgentSessionReducer,
    AgentSessionState,
    IdleExitReducerConfig,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipGenerationConflict,
    AgentRuntimeOwnershipMigrationConflict,
    AgentRuntimeOwnershipMode,
)
from shinbot.persistence import DatabaseManager


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _contract() -> EffectExecutionContract:
    return EffectExecutionContract(
        effect_kind="test_control",
        version=1,
        lane=EffectLane.CONTROL,
        completion_event_kind="TestControlCompleted",
        max_attempts=1,
    )


def _event(key: SessionKey, generation: int, event_id: str = "event-1") -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=key,
        kind="TestEvent",
        ownership_generation=generation,
        occurred_at=100.0,
    )


def _seed_effect(
    database: DatabaseManager,
    key: SessionKey,
    generation: int,
    *,
    status: str = "pending",
    claim_id: str = "",
    lease_owner: str = "",
    lease_until: float | None = None,
) -> DurableEffectEnvelope:
    contract = _contract()
    effect = DurableEffectEnvelope(
        effect_id="effect-1",
        key=key,
        kind=contract.effect_kind,
        idempotency_key="effect-1",
        ownership_generation=generation,
        contract_version=contract.version,
        contract_signature=contract.signature,
        source_event_id="source-1",
        available_at=100.0,
        created_at=100.0,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json, status,
                attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, '{}', ?, 0, 100.0,
                      ?, ?, ?, 100.0, 100.0, NULL, '')
            """,
            (
                effect.effect_id,
                effect.idempotency_key,
                key.profile_id,
                key.session_id,
                generation,
                effect.source_event_id,
                effect.kind,
                effect.contract_version,
                effect.contract_signature,
                status,
                claim_id,
                lease_owner,
                lease_until,
            ),
        )
    return effect


def _seed_message_ledger_state(
    database: DatabaseManager,
    key: SessionKey,
    generation: int,
    *,
    suffix: str = "1",
) -> tuple[str, str]:
    operation_id = f"ledger-operation-{suffix}"
    consumption_id = f"ledger-consumption-{suffix}"
    ledger_canonical = '{"message":"stable"}'
    consumption_canonical = '{"selection":"stable"}'
    with database.connect() as conn:
        message_log_id = conn.execute(
            """
            INSERT INTO message_logs (session_id, role, created_at)
            VALUES (?, 'user', 100.0)
            """,
            (key.session_id,),
        ).lastrowid
        assert message_log_id is not None
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, started_at
            ) VALUES (?, ?, ?, ?, 'review', 'completed', 100.0)
            """,
            (operation_id, key.profile_id, key.session_id, generation),
        )
        conn.execute(
            """
            INSERT INTO agent_message_ledger_consumptions (
                consumption_id, profile_id, session_id, ownership_generation,
                kind, selection, idempotency_key, operation_id,
                source_event_id, input_watermark,
                explicit_message_log_ids_json, canonical_json,
                occurred_at, committed_at
            ) VALUES (?, ?, ?, ?, 'review', 'all_through_watermark', ?, ?,
                      ?, 1, '[]', ?, 100.0, 100.0)
            """,
            (
                consumption_id,
                key.profile_id,
                key.session_id,
                generation,
                f"ledger-key-{suffix}",
                operation_id,
                f"ledger-consume-event-{suffix}",
                consumption_canonical,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_message_ledger (
                profile_id, session_id, ledger_sequence, message_log_id,
                ownership_generation, source_event_id, actor_event_id,
                delivery_version, event_source, instance_id, event_type,
                is_private, is_mentioned, is_mention_to_other,
                is_reply_to_bot, is_poke_to_bot, is_poke_to_other,
                already_handled, is_stopped, is_self_message,
                eligible_for_work, priority_mention, priority_reply_to_bot,
                priority_repeated_mention, priority_poke_to_bot,
                priority_should_wake, priority_reasons_json,
                observed_at, occurred_at, event_created_at, canonical_json,
                review_consumption_id, recorded_at, updated_at
            ) VALUES (?, ?, 1, ?, ?, ?, ?, 1, 'test', 'adapter-a',
                      'message-created', 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      1, 0, 0, 0, 0, 0, '{}', 100.0, 100.0, 100.0, ?, ?,
                      100.0, 100.0)
            """,
            (
                key.profile_id,
                key.session_id,
                message_log_id,
                generation,
                f"ledger-source-{suffix}",
                f"ledger-actor-{suffix}",
                ledger_canonical,
                consumption_id,
            ),
        )
    return ledger_canonical, consumption_canonical


def _completion(effect: DurableEffectEnvelope) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=completion_event_id(effect),
        key=effect.key,
        kind="TestControlCompleted",
        ownership_generation=effect.ownership_generation,
        payload={
            "effect_id": effect.effect_id,
            "effect_kind": effect.kind,
            "idempotency_key": effect.idempotency_key,
            "operation_id": effect.operation_id,
            "attempt_count": 1,
            "contract_version": effect.contract_version,
            "contract_signature": effect.contract_signature,
        },
        source="effect_executor",
        causation_id=effect.source_event_id,
        correlation_id=effect.operation_id or effect.effect_id,
        trace_id=effect.trace_id,
    )


@pytest.mark.asyncio
async def test_actor_claim_and_commit_fail_after_migration_begins(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor active",
    ).ownership
    store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    await store.enqueue(_event(key, owner.generation))
    claim = await store.claim_next(key, worker_id="actor-worker")
    assert claim is not None
    aggregate = await store.load(key)
    transition = SessionTransition(
        aggregate=aggregate.advance(data={"handled": True}),
        disposition="handled",
    )

    database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="switch runtime",
    )

    with pytest.raises(AgentRuntimeOwnershipGenerationConflict):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
        )
    assert await store.claim_next(key, worker_id="other-worker") is None
    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, ownership_generation FROM agent_session_mailbox"
        ).fetchone()
    assert tuple(row) == ("processing", owner.generation)


@pytest.mark.asyncio
async def test_effect_settlement_fails_after_generation_changes(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor active",
    ).ownership
    actor_store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    await actor_store.ensure(key, ownership_generation=owner.generation)
    effect = _seed_effect(database, key, owner.generation)
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: 100.0,
        contract_authority=EffectContractAuthority((_contract(),)),
    )
    claim = await effect_store.claim_next(
        worker_id="effect-worker",
        effect_contracts=(_contract().ref,),
    )
    assert claim is not None

    database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="switch runtime",
    )

    with pytest.raises(EffectClaimLost):
        await effect_store.complete_with_event(claim, _completion(effect))
    with database.connect() as conn:
        effect_row = conn.execute(
            "SELECT status, ownership_generation FROM agent_effect_outbox"
        ).fetchone()
        completion_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert tuple(effect_row) == ("processing", owner.generation)
    assert completion_count == 0


@pytest.mark.asyncio
async def test_actor_abort_refences_and_releases_all_live_work(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor active",
    ).ownership
    actor_store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    await actor_store.enqueue(_event(key, owner.generation))
    mailbox_claim = await actor_store.claim_next(key, worker_id="actor-worker")
    assert mailbox_claim is not None
    _seed_effect(database, key, owner.generation)
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        clock=lambda: 100.0,
    )
    effect_claim = await effect_store.claim_next(
        worker_id="effect-worker",
        effect_contracts=(_contract().ref,),
    )
    assert effect_claim is not None
    ledger_canonical, consumption_canonical = _seed_message_ledger_state(
        database,
        key,
        owner.generation,
    )
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        expected_generation=owner.generation,
        reason="trial migration",
    )

    restored = database.agent_runtime_ownership.abort_migration(
        key,
        expected_generation=migrating.generation,
        reason="return to actor",
    )

    assert restored.generation == owner.generation + 2
    with database.connect() as conn:
        aggregate = conn.execute(
            "SELECT ownership_generation FROM agent_session_aggregates"
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_session_mailbox
            """
        ).fetchone()
        effect = conn.execute(
            """
            SELECT ownership_generation, status, claim_id, lease_owner, lease_until
            FROM agent_effect_outbox
            """
        ).fetchone()
        ledger = conn.execute(
            """
            SELECT ownership_generation, canonical_json
            FROM agent_message_ledger
            """
        ).fetchone()
        consumption = conn.execute(
            """
            SELECT ownership_generation, canonical_json
            FROM agent_message_ledger_consumptions
            """
        ).fetchone()
    assert tuple(aggregate) == (restored.generation,)
    assert tuple(mailbox) == (restored.generation, "pending", "", "", None)
    assert tuple(effect) == (restored.generation, "pending", "", "", None)
    assert tuple(ledger) == (restored.generation, ledger_canonical)
    assert tuple(consumption) == (restored.generation, consumption_canonical)
    recovered_mailbox = await actor_store.claim_next(key, worker_id="actor-recovered")
    recovered_effect = await effect_store.claim_next(
        worker_id="effect-recovered",
        effect_contracts=(_contract().ref,),
    )
    assert recovered_mailbox is not None
    assert recovered_mailbox.envelope.ownership_generation == restored.generation
    assert recovered_effect is not None
    assert recovered_effect.effect.ownership_generation == restored.generation


def test_legacy_to_actor_completion_refences_seeded_target_state(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    legacy = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy active",
    ).ownership
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=legacy.generation,
        reason="seed actor target",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, migrating.generation),
        )
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation, kind,
                occurred_at, available_at, created_at
            ) VALUES ('seeded', ?, ?, ?, 'Seeded', 100.0, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, migrating.generation),
        )
    ledger_canonical, consumption_canonical = _seed_message_ledger_state(
        database,
        key,
        migrating.generation,
        suffix="activation",
    )

    activated = database.agent_runtime_ownership.complete_migration(
        key,
        expected_generation=migrating.generation,
        reason="activate actor target",
    )

    assert activated.generation == migrating.generation + 1
    with database.connect() as conn:
        generations = {
            int(row["ownership_generation"])
            for row in conn.execute(
                """
                SELECT ownership_generation FROM agent_session_aggregates
                UNION ALL
                SELECT ownership_generation FROM agent_session_mailbox
                UNION ALL
                SELECT ownership_generation FROM agent_message_ledger
                UNION ALL
                SELECT ownership_generation
                FROM agent_message_ledger_consumptions
                """
            ).fetchall()
        }
        canonicals = tuple(
            row["canonical_json"]
            for row in conn.execute(
                """
                SELECT canonical_json FROM agent_message_ledger
                UNION ALL
                SELECT canonical_json FROM agent_message_ledger_consumptions
                """
            ).fetchall()
        )
    assert generations == {activated.generation}
    assert set(canonicals) == {ledger_canonical, consumption_canonical}


def test_legacy_to_actor_completion_rejects_live_target_lease(tmp_path: Path) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    legacy = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy active",
    ).ownership
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=legacy.generation,
        reason="seed actor target",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, migrating.generation),
        )
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation, kind,
                occurred_at, status, claim_id, lease_owner, lease_until,
                available_at, created_at
            ) VALUES ('leased', ?, ?, ?, 'Seeded', 100.0, 'processing',
                      'claim', 'worker', 200.0, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, migrating.generation),
        )

    with pytest.raises(AgentRuntimeOwnershipMigrationConflict, match="live leases"):
        database.agent_runtime_ownership.complete_migration(
            key,
            expected_generation=migrating.generation,
            reason="must reject leased target",
        )

    current = database.agent_runtime_ownership.get(key)
    assert current is not None
    assert current.generation == migrating.generation
    assert current.status.value == "migrating"


def test_legacy_to_actor_completion_rejects_stale_ledger_generations(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    legacy = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy active",
    ).ownership
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=legacy.generation,
        reason="seed actor target",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, created_at, updated_at
            ) VALUES (?, ?, ?, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id, migrating.generation),
        )
    _seed_message_ledger_state(
        database,
        key,
        migrating.generation,
        suffix="stale",
    )
    with database.connect() as conn:
        conn.execute(
            "UPDATE agent_message_ledger SET ownership_generation = ?",
            (legacy.generation,),
        )
        conn.execute(
            """
            UPDATE agent_message_ledger_consumptions
            SET ownership_generation = ?
            """,
            (legacy.generation,),
        )

    with pytest.raises(
        AgentRuntimeOwnershipGenerationConflict,
        match="agent_message_ledger",
    ):
        database.agent_runtime_ownership.complete_migration(
            key,
            expected_generation=migrating.generation,
            reason="must reject stale ledger generations",
        )

    current = database.agent_runtime_ownership.get(key)
    assert current is not None
    assert current.generation == migrating.generation
    assert current.status.value == "migrating"


@pytest.mark.asyncio
async def test_generation_zero_orphans_are_never_claimed_or_recovered(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor active",
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, state,
                created_at, updated_at
            ) VALUES (?, ?, 0, 'active_chat_settling', 100.0, 100.0)
            """,
            (key.profile_id, key.session_id),
        )
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation, kind,
                occurred_at, available_at, created_at
            ) VALUES ('orphan', ?, ?, 0, 'Orphan', 100.0, 100.0, 100.0)
            """,
            (key.profile_id, key.session_id),
        )
    _seed_effect(database, key, 0)
    actor_store = SQLiteSessionActorStore(database, clock=lambda: 100.0)
    effect_store = SQLiteDurableEffectStore(database, clock=lambda: 100.0)

    assert await actor_store.claim_next(key, worker_id="actor") is None
    assert await effect_store.claim_next(
        worker_id="effect",
        effect_contracts=(_contract().ref,),
    ) is None
    assert await actor_store.enqueue_recovery_requests() == 0
    with database.connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_idle_deadline_clock_is_sampled_after_write_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="actor active",
    ).ownership
    now = [100.0]
    store = SQLiteSessionActorStore(database, clock=lambda: now[0])
    await store.ensure(key, ownership_generation=owner.generation)
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = ?, state_revision = 1, active_epoch = 2,
                activity_generation = 3, updated_at = 100.0
            WHERE profile_id = ? AND session_id = ?
            """,
            (AgentSessionState.ACTIVE_CHAT.value, key.profile_id, key.session_id),
        )
    event = SessionEventEnvelope(
        event_id="exit-after-lock-wait",
        key=key,
        kind=AgentSessionEventKind.EXIT_REQUESTED,
        ownership_generation=owner.generation,
        occurred_at=10.0,
    )
    await store.enqueue(event)
    claim = await store.claim_next(key, worker_id="actor")
    assert claim is not None
    aggregate = await store.load(key)
    transition = AgentSessionReducer(
        config=IdleExitReducerConfig(planning_deadline_seconds=25.0)
    ).reduce(aggregate, event)
    original_connect = database.connect

    class _ConnectionProxy:
        def __init__(self, connection) -> None:
            self._connection = connection

        def execute(self, statement, parameters=()):
            result = self._connection.execute(statement, parameters)
            if statement.strip().upper() == "BEGIN IMMEDIATE":
                now[0] = 110.0
            return result

        def __getattr__(self, name):
            return getattr(self._connection, name)

    @contextmanager
    def delayed_lock_connection():
        with original_connect() as connection:
            yield _ConnectionProxy(connection)

    monkeypatch.setattr(database, "connect", delayed_lock_connection)

    committed = await store.commit(
        claim,
        transition,
        expected_revision=aggregate.state_revision,
    )

    idle_exit = committed.data["idle_exit"]
    assert committed.updated_at == 110.0
    assert idle_exit["deadline_scheduled_from"] == 110.0
    assert idle_exit["deadline_at"] == 135.0
    with original_connect() as conn:
        deadline = conn.execute(
            """
            SELECT available_at, payload_json FROM agent_effect_outbox
            WHERE kind = 'enqueue_idle_review_planning_deadline'
            """
        ).fetchone()
        operation = conn.execute(
            """
            SELECT metadata_json FROM agent_session_operations
            WHERE operation_id = ?
            """,
            (committed.idle_planning_operation_id,),
        ).fetchone()
    assert float(deadline["available_at"]) == 135.0
    assert '"deadline_at":135.0' in str(deadline["payload_json"])
    assert '"deadline_at":135.0' in str(operation["metadata_json"])
