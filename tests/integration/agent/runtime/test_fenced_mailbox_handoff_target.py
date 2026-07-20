"""Integration coverage for the dormant single-request fenced handoff target."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate, SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectExecutionContext,
    EffectExecutionContract,
    EffectHandlerRegistry,
    EffectHandlerResult,
    EffectLane,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_supervisor import (
    FencedMailboxHandoffSupervisor,
    FencedMailboxHandoffSupervisorState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTarget,
    FencedMailboxHandoffTargetState,
)
from shinbot.agent.runtime.session_actor.fenced_registry import FencedSessionActorRegistry
from shinbot.agent.runtime.session_actor.mailbox_handoff_dispatcher import (
    DurableMailboxHandoffDispatcher,
    MailboxHandoffDispatchDisposition,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.mailbox_handoff import (
    MailboxHandoffState,
    MailboxHandoffTarget,
)
from shinbot.persistence import DatabaseManager


def _record_handler(
    aggregate: AgentSessionAggregate,
    envelope: SessionEventEnvelope,
) -> SessionTransition:
    """Record a minimal actor transition without producing effects."""

    return SessionTransition(
        aggregate=aggregate.advance(data={"last_event_id": envelope.event_id}),
        disposition="fenced_handoff_target_recorded",
    )


async def _components(
    tmp_path: Path,
    *,
    effect_contracts: tuple[EffectExecutionContract, ...] = (),
    register_handlers: Callable[[EffectHandlerRegistry], None] | None = None,
) -> tuple[
    DatabaseManager,
    ActorV2AdmissionGrant,
    SessionKey,
    FencedMailboxWakeRequest,
    FencedActorExecutionBinding,
    SQLiteSessionActorStore,
    SQLiteDurableEffectStore,
    FencedSessionActorRegistry,
    DurableEffectExecutor,
    FencedMailboxHandoffTarget,
]:
    """Compose one unmounted target with a single fenced owner request."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-target", "profile-target:group:room")
    admission_grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="fenced-handoff-target-test",
        ttl_seconds=300.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="fenced handoff target integration test",
        admission_grant=admission_grant,
    ).ownership
    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )
    target_identity = MailboxHandoffTarget(
        "fenced-handoff-target-test",
        "fenced-handoff-target-incarnation-a",
    )
    target_lease = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=target_identity,
        ttl_seconds=60.0,
    )
    binding = FencedActorExecutionBinding(request=request, target_lease=target_lease)
    authority = EffectContractAuthority(
        (*builtin_effect_contract_authority().contracts(), *effect_contracts)
    )
    actor_store = SQLiteSessionActorStore(
        database,
        effect_contract_authority=authority,
    )
    await actor_store.ensure(
        key,
        ownership_generation=ownership.generation,
        ownership_binding=request,
        execution_binding=binding,
    )
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        contract_authority=authority,
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    if register_handlers is not None:
        register_handlers(handlers)
    handlers.seal()
    effect_executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        execution_binding=binding,
        poll_interval_seconds=0.01,
        renew_interval_seconds=0.01,
    )
    actor_registry = FencedSessionActorRegistry(
        store=actor_store,
        handler=_record_handler,
    )
    target = FencedMailboxHandoffTarget(
        database=database,
        execution_binding=binding,
        actor_registry=actor_registry,
        effect_executor=effect_executor,
    )
    return (
        database,
        admission_grant,
        key,
        request,
        binding,
        actor_store,
        effect_store,
        actor_registry,
        effect_executor,
        target,
    )


async def _replacement_target(
    database: DatabaseManager,
    request: FencedMailboxWakeRequest,
    *,
    target_identity: MailboxHandoffTarget,
) -> tuple[
    FencedActorExecutionBinding,
    SQLiteSessionActorStore,
    FencedSessionActorRegistry,
    DurableEffectExecutor,
    FencedMailboxHandoffTarget,
]:
    """Compose a distinct target incarnation after an old lease has retired."""

    target_lease = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=target_identity,
        ttl_seconds=60.0,
    )
    binding = FencedActorExecutionBinding(request=request, target_lease=target_lease)
    authority = EffectContractAuthority(builtin_effect_contract_authority().contracts())
    actor_store = SQLiteSessionActorStore(
        database,
        effect_contract_authority=authority,
    )
    await actor_store.ensure(
        request.key,
        ownership_generation=request.ownership_generation,
        ownership_binding=request,
        execution_binding=binding,
    )
    effect_store = SQLiteDurableEffectStore(
        database,
        lease_seconds=5.0,
        contract_authority=authority,
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    handlers.seal()
    effect_executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        execution_binding=binding,
        poll_interval_seconds=0.01,
        renew_interval_seconds=0.01,
    )
    actor_registry = FencedSessionActorRegistry(
        store=actor_store,
        handler=_record_handler,
    )
    target = FencedMailboxHandoffTarget(
        database=database,
        execution_binding=binding,
        actor_registry=actor_registry,
        effect_executor=effect_executor,
    )
    return binding, actor_store, actor_registry, effect_executor, target


def _insert_mailbox(
    database: DatabaseManager,
    request: FencedMailboxWakeRequest,
    *,
    event_id: str,
) -> int:
    """Insert one minimal pending mailbox row for the already-active owner."""

    with database.connect() as conn:
        inserted = conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, payload_json, occurred_at, available_at, created_at
            ) VALUES (?, ?, ?, ?, 'TargetHandoffEvent', '{}', 1.0, 1.0, 1.0)
            """,
            (
                event_id,
                request.key.profile_id,
                request.key.session_id,
                request.ownership_generation,
            ),
        )
    return int(inserted.lastrowid)


def _seed_blocking_effect(
    database: DatabaseManager,
    request: FencedMailboxWakeRequest,
    contract: EffectExecutionContract,
) -> None:
    """Seed one pending scoped effect without involving an actor transition."""

    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id, event_id,
                ownership_generation, operation_id, kind, contract_version,
                contract_signature, payload_json, status, attempt_count,
                available_at, claim_id, lease_owner, lease_until, created_at,
                updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, '{}', 'pending', 0,
                      0.0, '', '', NULL, 0.0, 0.0, NULL, '')
            """,
            (
                "blocking-effect",
                "blocking-effect-idempotency",
                request.key.profile_id,
                request.key.session_id,
                "blocking-effect-source",
                request.ownership_generation,
                contract.effect_kind,
                contract.version,
                contract.signature,
            ),
        )


@pytest.mark.asyncio
async def test_target_accepts_only_live_exact_claim_and_retires_after_unpublish(
    tmp_path: Path,
) -> None:
    """A live target validates a full claim, wakes the actor, then releases cleanly."""

    (
        database,
        _admission_grant,
        key,
        request,
        binding,
        actor_store,
        _effect_store,
        actor_registry,
        _effect_executor,
        target,
    ) = await _components(tmp_path)
    await target.activate()
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="fenced-target-dispatcher",
    )
    dispatcher.bind_target(target, target_identity=target.target_identity)
    foreign_mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="fenced-target-foreign-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(foreign_mailbox_id, request)
    foreign_claim = database.actor_v2_mailbox_handoffs.claim_fenced_handoff(
        foreign_mailbox_id,
        worker_id="foreign-fenced-target-dispatcher",
        target=MailboxHandoffTarget("other-target", "other-incarnation"),
    )
    assert foreign_claim is not None
    foreign_receipt = await target.wake_handoff(foreign_claim)
    assert foreign_receipt.disposition is FencedMailboxWakeDisposition.DEFERRED

    mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="fenced-target-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)
    dispatch_result = await dispatcher.dispatch(mailbox_id)

    assert dispatch_result.disposition is MailboxHandoffDispatchDisposition.ACCEPTED
    actor = actor_registry.actor_for(request)
    assert actor is not None
    await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
    aggregate = await actor_store.load(
        key,
        ownership_binding=request,
        execution_binding=binding,
    )
    assert aggregate.data == {"last_event_id": "fenced-target-mailbox-event"}
    settled = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert settled is not None
    assert settled.state.value == "settled"
    assert _effect_executor.session_registry is None

    renewed_binding = await target.renew_target_lease(ttl_seconds=60.0)
    assert renewed_binding.has_same_authority(binding)
    renewed_mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="fenced-target-renewed-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(renewed_mailbox_id, request)
    renewed_dispatch = await dispatcher.dispatch(renewed_mailbox_id)
    assert renewed_dispatch.disposition is MailboxHandoffDispatchDisposition.ACCEPTED
    await asyncio.wait_for(actor.wait_idle(), timeout=1.0)

    dispatcher.unbind_target()
    await target.unpublish()
    retirement = await target.retire(quiescence_timeout_seconds=1.0)
    await dispatcher.close()

    assert retirement.state is FencedMailboxHandoffTargetState.STOPPED
    assert retirement.target_lease_released is True
    with pytest.raises(FencedWakeTargetLeaseError):
        database.actor_v2_fenced_wake_target_leases.validate(binding.target_lease)


@pytest.mark.asyncio
async def test_target_recovers_expired_mailbox_before_dispatcher_binding(
    tmp_path: Path,
) -> None:
    """A replacement target restarts only its own expired mailbox history."""

    (
        database,
        _admission_grant,
        key,
        request,
        binding,
        _actor_store,
        _effect_store,
        _actor_registry,
        _effect_executor,
        _target,
    ) = await _components(tmp_path)
    mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="fenced-native-history-expired-mailbox-event",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_mailbox
            SET status = 'processing',
                claim_id = 'crashed-history-mailbox-claim',
                lease_owner = 'crashed-history-mailbox-worker',
                lease_until = 0.0
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        )
    database.actor_v2_fenced_wake_target_leases.release(binding.target_lease)
    (
        replacement_binding,
        replacement_store,
        replacement_registry,
        _replacement_executor,
        replacement_target,
    ) = await _replacement_target(
        database,
        request,
        target_identity=MailboxHandoffTarget(
            "fenced-native-history-test",
            "fenced-native-history-incarnation-b",
        ),
    )

    recovered = await replacement_target.recover_native_history()

    assert recovered.actor_wake.request == request
    assert recovered.actor_wake.disposition is FencedMailboxWakeDisposition.ACCEPTED
    assert recovered.effect_recovery.recovered_count == 0
    assert recovered.effect_recovery.notifications == ()
    assert replacement_target.state is FencedMailboxHandoffTargetState.NEW
    actor = replacement_registry.actor_for(request)
    assert actor is not None
    await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
    aggregate = await replacement_store.load(
        key,
        ownership_binding=request,
        execution_binding=replacement_binding,
    )
    assert aggregate.data == {"last_event_id": "fenced-native-history-expired-mailbox-event"}

    await replacement_target.unpublish()
    retirement = await replacement_target.retire(quiescence_timeout_seconds=1.0)
    assert retirement.state is FencedMailboxHandoffTargetState.STOPPED


@pytest.mark.asyncio
async def test_unpublished_target_preserves_handoff_for_a_replacement_incarnation(
    tmp_path: Path,
) -> None:
    """Unpublish cannot terminally consume work that the next target must deliver."""

    (
        database,
        _admission_grant,
        key,
        request,
        _binding,
        _actor_store,
        _effect_store,
        _actor_registry,
        _effect_executor,
        target,
    ) = await _components(tmp_path)
    await target.activate()
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="unpublish-handoff-dispatcher",
    )
    dispatcher.bind_target(target, target_identity=target.target_identity)
    mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="unpublished-target-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)

    # Deliberately leave the dispatcher bound to exercise the safe fallback
    # when a controller's unbind and target unpublish overlap.
    await target.unpublish()
    deferred = await dispatcher.dispatch(mailbox_id)

    assert deferred.disposition is MailboxHandoffDispatchDisposition.DEFERRED
    pending = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert pending is not None
    assert pending.state is MailboxHandoffState.PENDING
    assert pending.target is None
    assert pending.target_disposition == ""

    dispatcher.unbind_target()
    retired = await target.retire(quiescence_timeout_seconds=1.0)
    assert retired.state is FencedMailboxHandoffTargetState.STOPPED
    assert retired.target_lease_released is True

    (
        _replacement_binding,
        replacement_store,
        replacement_registry,
        _replacement_executor,
        replacement,
    ) = await _replacement_target(
        database,
        request,
        target_identity=MailboxHandoffTarget(
            "fenced-handoff-target-test",
            "fenced-handoff-target-incarnation-b",
        ),
    )
    await replacement.activate()
    dispatcher.bind_target(
        replacement,
        target_identity=replacement.target_identity,
    )

    accepted = await dispatcher.dispatch(mailbox_id)

    assert accepted.disposition is MailboxHandoffDispatchDisposition.ACCEPTED
    actor = replacement_registry.actor_for(request)
    assert actor is not None
    await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
    aggregate = await replacement_store.load(
        key,
        ownership_binding=request,
        execution_binding=replacement.execution_binding,
    )
    assert aggregate.data == {"last_event_id": "unpublished-target-mailbox-event"}
    settled = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert settled is not None
    assert settled.state is MailboxHandoffState.SETTLED
    assert settled.target == replacement.target_identity

    dispatcher.unbind_target()
    await replacement.unpublish()
    replacement_retirement = await replacement.retire(quiescence_timeout_seconds=1.0)
    await dispatcher.close()
    assert replacement_retirement.state is FencedMailboxHandoffTargetState.STOPPED


@pytest.mark.asyncio
async def test_lost_target_lease_defers_instead_of_settling_handoff(tmp_path: Path) -> None:
    """A local publication loss is retryable work debt, not stale ownership proof."""

    (
        database,
        _admission_grant,
        _key,
        request,
        binding,
        _actor_store,
        _effect_store,
        _actor_registry,
        _effect_executor,
        target,
    ) = await _components(tmp_path)
    await target.activate()
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="lost-target-lease-dispatcher",
    )
    dispatcher.bind_target(target, target_identity=target.target_identity)
    mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="lost-target-lease-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)
    database.actor_v2_fenced_wake_target_leases.release(binding.target_lease)

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.DEFERRED
    assert target.state is FencedMailboxHandoffTargetState.BLOCKED
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.PENDING
    assert record.target_disposition == ""

    dispatcher.unbind_target()
    retirement = await target.retire(quiescence_timeout_seconds=1.0)
    await dispatcher.close()
    assert retirement.state is FencedMailboxHandoffTargetState.STOPPED


@pytest.mark.asyncio
async def test_closed_local_registry_defers_without_declaring_owner_stale(
    tmp_path: Path,
) -> None:
    """A lower-level stale wake cannot terminally settle a still-live owner."""

    (
        database,
        _admission_grant,
        _key,
        request,
        _binding,
        _actor_store,
        _effect_store,
        actor_registry,
        _effect_executor,
        target,
    ) = await _components(tmp_path)
    await target.activate()
    await actor_registry.shutdown(drain=False)
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="closed-registry-dispatcher",
    )
    dispatcher.bind_target(target, target_identity=target.target_identity)
    mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="closed-registry-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)

    result = await dispatcher.dispatch(mailbox_id)

    assert result.disposition is MailboxHandoffDispatchDisposition.DEFERRED
    assert target.state is FencedMailboxHandoffTargetState.BLOCKED
    record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert record is not None
    assert record.state is MailboxHandoffState.PENDING
    assert record.target_disposition == ""

    dispatcher.unbind_target()
    retirement = await target.retire(quiescence_timeout_seconds=1.0)
    await dispatcher.close()
    assert retirement.state is FencedMailboxHandoffTargetState.STOPPED


@pytest.mark.asyncio
async def test_stale_target_only_terminally_rejects_its_matching_owner_claim(
    tmp_path: Path,
) -> None:
    """A stale target cannot settle a foreign request through a bad binding."""

    (
        database,
        admission_grant,
        _key,
        request,
        _binding,
        _actor_store,
        _effect_store,
        _actor_registry,
        _effect_executor,
        target,
    ) = await _components(tmp_path)
    await target.activate()
    terminal_mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="stale-target-terminal-mailbox-event",
    )
    foreign_mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="stale-target-foreign-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(terminal_mailbox_id, request)
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(foreign_mailbox_id, request)
    database.actor_v2_admission_fences.revoke(
        admission_grant,
        reason="stale target classification test",
    )
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="stale-target-dispatcher",
    )
    dispatcher.bind_target(target, target_identity=target.target_identity)

    terminal = await dispatcher.dispatch(terminal_mailbox_id)

    assert terminal.disposition is MailboxHandoffDispatchDisposition.STALE
    assert target.state is FencedMailboxHandoffTargetState.STALE
    settled = database.actor_v2_mailbox_handoffs.read(terminal_mailbox_id)
    assert settled is not None
    assert settled.state is MailboxHandoffState.SETTLED
    foreign_claim = database.actor_v2_mailbox_handoffs.claim_fenced_handoff(
        foreign_mailbox_id,
        worker_id="foreign-stale-target-dispatcher",
        target=MailboxHandoffTarget("other-target", "other-incarnation"),
    )
    assert foreign_claim is not None

    foreign = await target.wake_handoff(foreign_claim)

    assert foreign.disposition is FencedMailboxWakeDisposition.DEFERRED
    database.actor_v2_mailbox_handoffs.release_fenced_claim(
        foreign_claim,
        error_message="stale target test cleanup",
    )
    dispatcher.unbind_target()
    retirement = await target.retire(quiescence_timeout_seconds=1.0)
    await dispatcher.close()
    assert retirement.state is FencedMailboxHandoffTargetState.STOPPED


@pytest.mark.asyncio
async def test_target_keeps_lease_when_local_effect_handler_has_not_quiesced(
    tmp_path: Path,
) -> None:
    """Retirement remains blocked until an interrupted local handler actually exits."""

    handler_started = asyncio.Event()
    allow_handler_exit = asyncio.Event()

    async def blocking_handler(_context: EffectExecutionContext) -> EffectHandlerResult:
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            while not allow_handler_exit.is_set():
                try:
                    await allow_handler_exit.wait()
                except asyncio.CancelledError:
                    continue
            raise

    contract = EffectExecutionContract(
        effect_kind="target-blocking-effect",
        version=1,
        lane=EffectLane.DEFAULT,
        completion_event_kind="TargetBlockingCompleted",
        max_attempts=1,
    )

    def register_handlers(registry: EffectHandlerRegistry) -> None:
        registry.register(contract.effect_kind, blocking_handler, contract=contract)

    (
        database,
        _admission_grant,
        _key,
        request,
        binding,
        _actor_store,
        _effect_store,
        _actor_registry,
        _effect_executor,
        target,
    ) = await _components(
        tmp_path,
        effect_contracts=(contract,),
        register_handlers=register_handlers,
    )
    await target.activate()
    _seed_blocking_effect(database, request, contract)
    await asyncio.wait_for(handler_started.wait(), timeout=1.0)

    await target.unpublish()
    blocked = await target.retire(quiescence_timeout_seconds=0.01)

    assert blocked.state is FencedMailboxHandoffTargetState.BLOCKED
    assert blocked.target_lease_released is False
    assert blocked.quiescence is not None
    assert blocked.quiescence.remaining_handler_keys
    assert database.actor_v2_fenced_wake_target_leases.validate(binding.target_lease)

    allow_handler_exit.set()
    retired = await target.retire(quiescence_timeout_seconds=1.0)

    assert retired.state is FencedMailboxHandoffTargetState.STOPPED
    assert retired.target_lease_released is True


@pytest.mark.asyncio
async def test_supervisor_renews_dispatches_and_retires_one_real_fenced_target(
    tmp_path: Path,
) -> None:
    """The unmounted supervisor operates one exact target without cutover authority."""

    (
        database,
        _admission_grant,
        _key,
        request,
        _binding,
        _actor_store,
        _effect_store,
        _actor_registry,
        _effect_executor,
        target,
    ) = await _components(tmp_path)
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="fenced-supervisor-dispatcher",
        target_timeout_seconds=0.05,
    )
    mailbox_id = _insert_mailbox(
        database,
        request,
        event_id="fenced-supervisor-mailbox-event",
    )
    database.actor_v2_mailbox_handoffs.record_fenced_handoff(mailbox_id, request)
    supervisor = FencedMailboxHandoffSupervisor(
        target=target,
        dispatcher=dispatcher,
        tick_interval_seconds=0.01,
        target_lease_ttl_seconds=0.5,
        dispatch_limit=1,
        quiescence_timeout_seconds=1.0,
    )

    await supervisor.start()
    for _attempt in range(100):
        record = database.actor_v2_mailbox_handoffs.read(mailbox_id)
        if record is not None and record.state is MailboxHandoffState.SETTLED:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("supervisor did not settle the scoped fenced handoff")
    shutdown = await supervisor.shutdown()

    assert shutdown.state is FencedMailboxHandoffSupervisorState.STOPPED
    assert target.state is FencedMailboxHandoffTargetState.STOPPED
    assert dispatcher.target_bound is False
    assert database.actor_v2_mailbox_handoffs.read(mailbox_id).state is MailboxHandoffState.SETTLED
    await dispatcher.close()
