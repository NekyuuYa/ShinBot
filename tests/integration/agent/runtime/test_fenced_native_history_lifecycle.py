"""Integration coverage for restartable fenced native-history lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate, SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract_authority,
)
from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectHandlerRegistry,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.fenced_ingress_lifecycle import (
    FencedIngressLifecycleController,
    FencedIngressLifecycleState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_supervisor import (
    FencedMailboxHandoffSupervisor,
    FencedMailboxHandoffSupervisorState,
)
from shinbot.agent.runtime.session_actor.fenced_mailbox_handoff_target import (
    FencedMailboxHandoffTarget,
    FencedMailboxHandoffTargetState,
)
from shinbot.agent.runtime.session_actor.fenced_native_history_lifecycle import (
    FencedNativeHistoryLifecycleController,
)
from shinbot.agent.runtime.session_actor.fenced_registry import FencedSessionActorRegistry
from shinbot.agent.runtime.session_actor.mailbox_handoff_dispatcher import (
    DurableMailboxHandoffDispatcher,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.durable_routing_service import FencedDurableRoutingService
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import FencedActorExecutionBinding
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence import DatabaseManager


def _database(tmp_path: Path) -> DatabaseManager:
    """Build an initialized persistence domain for one restart simulation."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _record_handler(
    aggregate: AgentSessionAggregate,
    envelope: SessionEventEnvelope,
) -> SessionTransition:
    """Record a recovered mailbox event without introducing workflow effects."""

    return SessionTransition(
        aggregate=aggregate.advance(data={"last_event_id": envelope.event_id}),
        disposition="native_history_recovered_mailbox",
    )


def _request(
    database: DatabaseManager,
    key: SessionKey,
) -> tuple[ActorV2AdmissionGrant, FencedMailboxWakeRequest]:
    """Create one fenced Actor owner whose previous target will be replaced."""

    admission_grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="native-history-restart-test",
        ttl_seconds=300.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="native history restart integration test",
        admission_grant=admission_grant,
    ).ownership
    return (
        admission_grant,
        FencedMailboxWakeRequest(
            key=key,
            ownership_generation=ownership.generation,
            admission_fence_id=ownership.admission_fence_id,
            admission_fence_generation=ownership.admission_fence_generation,
        ),
    )


def _acquire_binding(
    database: DatabaseManager,
    request: FencedMailboxWakeRequest,
    *,
    incarnation_id: str,
) -> FencedActorExecutionBinding:
    """Acquire one durable target incarnation for the exact owner request."""

    lease = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=MailboxHandoffTarget("native-history-restart-test", incarnation_id),
        ttl_seconds=60.0,
    )
    return FencedActorExecutionBinding(request=request, target_lease=lease)


@pytest.mark.asyncio
async def test_native_history_lifecycle_recovers_expired_mailbox_after_target_replacement(
    tmp_path: Path,
) -> None:
    """A replacement target resumes its exact durable history before publication."""

    database = _database(tmp_path)
    key = SessionKey("profile-native-history", "bot:group:native-history")
    _admission_grant, request = _request(database, key)
    old_binding = _acquire_binding(
        database,
        request,
        incarnation_id="previous-process-incarnation",
    )
    database.actor_v2_fenced_wake_target_leases.release(old_binding.target_lease)
    binding = _acquire_binding(
        database,
        request,
        incarnation_id="replacement-process-incarnation",
    )
    authority = builtin_effect_contract_authority()
    actor_store = SQLiteSessionActorStore(
        database,
        effect_contract_authority=authority,
    )
    await actor_store.ensure(
        key,
        ownership_generation=request.ownership_generation,
        ownership_binding=request,
        execution_binding=binding,
    )
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_mailbox (
                event_id, profile_id, session_id, ownership_generation,
                kind, payload_json, status, attempt_count, claim_id,
                lease_owner, lease_until, occurred_at, available_at, created_at
            ) VALUES (?, ?, ?, ?, 'RecoveredMailboxEvent', '{}', 'processing',
                      1, 'previous-mailbox-claim', 'previous-mailbox-worker',
                      0.0, 1.0, 1.0, 1.0)
            """,
            (
                "native-history-recovery-mailbox-event",
                key.profile_id,
                key.session_id,
                request.ownership_generation,
            ),
        )
    effect_store = SQLiteDurableEffectStore(
        database,
        contract_authority=authority,
    )
    handlers = EffectHandlerRegistry(contract_authority=authority)
    handlers.seal()
    executor = DurableEffectExecutor(
        store=effect_store,
        handlers=handlers,
        execution_binding=binding,
        poll_interval_seconds=0.01,
        renew_interval_seconds=0.01,
    )
    registry = FencedSessionActorRegistry(store=actor_store, handler=_record_handler)
    target = FencedMailboxHandoffTarget(
        database=database,
        execution_binding=binding,
        actor_registry=registry,
        effect_executor=executor,
    )
    dispatcher = DurableMailboxHandoffDispatcher(
        database.actor_v2_mailbox_handoffs,
        worker_id="native-history-restart-dispatcher",
        target_timeout_seconds=0.05,
    )
    supervisor = FencedMailboxHandoffSupervisor(
        target=target,
        dispatcher=dispatcher,
        tick_interval_seconds=0.01,
        target_lease_ttl_seconds=0.5,
        dispatch_limit=1,
        quiescence_timeout_seconds=1.0,
    )
    history_lifecycle = FencedNativeHistoryLifecycleController(
        target=target,
        supervisor=supervisor,
    )

    async def replay_unexpected_route(*_args: object) -> None:
        """Reject unrelated route replay while this test restores native history."""

        raise AssertionError("native-history fixture has no pending routing job")

    relay = FencedDurableRoutingService(
        repository=database.durable_routing,
        replay=replay_unexpected_route,
        adapter_resolver=lambda _instance_id: None,
        request=request,
        poll_interval_seconds=0.01,
    )
    lifecycle = FencedIngressLifecycleController(
        history=history_lifecycle,
        relay=relay,
    )
    try:
        active = await lifecycle.activate()
        actor = registry.actor_for(request)
        assert actor is not None
        await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
        aggregate = await actor_store.load(
            key,
            ownership_binding=request,
            execution_binding=binding,
        )

        assert active.state is FencedIngressLifecycleState.ACTIVE
        assert active.history.recovery is not None
        assert active.history.recovery.actor_wake.request == request
        assert aggregate.data == {"last_event_id": "native-history-recovery-mailbox-event"}
        assert target.state is FencedMailboxHandoffTargetState.ACTIVE
        assert supervisor.snapshot.state is FencedMailboxHandoffSupervisorState.ACTIVE
        assert (await lifecycle.verify_active()).state is FencedIngressLifecycleState.ACTIVE
    finally:
        closed = await lifecycle.shutdown()
        await dispatcher.close()

    assert closed.state is FencedIngressLifecycleState.CLOSED
    assert target.state is FencedMailboxHandoffTargetState.STOPPED
    assert supervisor.snapshot.state is FencedMailboxHandoffSupervisorState.STOPPED
