"""Integration coverage for ownership-incarnation-bound session actors."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.actor import AgentSessionActor
from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.fenced_registry import FencedSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceError,
    ActorV2AdmissionGrant,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import (
    AgentRuntimeOwnershipGenerationConflict,
    AgentRuntimeOwnershipMode,
)
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence import DatabaseManager


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized SQLite domain for a bound-actor test."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _fenced_components(
    tmp_path: Path,
) -> tuple[
    DatabaseManager,
    ActorV2AdmissionGrant,
    SQLiteSessionActorStore,
    FencedMailboxWakeRequest,
]:
    """Create one active fenced owner and its matching actor-store binding."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="bound-actor-test",
        ttl_seconds=60.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="bound actor test owner",
        admission_grant=grant,
    ).ownership
    binding = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership.generation,
        admission_fence_id=ownership.admission_fence_id,
        admission_fence_generation=ownership.admission_fence_generation,
    )
    return database, grant, SQLiteSessionActorStore(database), binding


def _execution_binding(
    database: DatabaseManager,
    request: FencedMailboxWakeRequest,
    *,
    incarnation_id: str = "incarnation-a",
) -> FencedActorExecutionBinding:
    """Acquire one durable target lease for the exact bound actor owner."""

    grant = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=MailboxHandoffTarget("fenced-actor-target", incarnation_id),
        ttl_seconds=60.0,
    )
    return FencedActorExecutionBinding(request=request, target_lease=grant)


async def _wait_for_stop(actor: AgentSessionActor) -> None:
    """Wait for an actor that stopped itself after losing its owner binding."""

    for _ in range(100):
        if not actor.started:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("bound actor did not stop after ownership binding loss")


def _record_handler(
    aggregate: AgentSessionAggregate,
    envelope: SessionEventEnvelope,
) -> SessionTransition:
    """Record one test event without emitting effects or external actions."""

    return SessionTransition(
        aggregate=aggregate.advance(data={"event_id": envelope.event_id}),
        disposition="bound_actor_event_recorded",
    )


@pytest.mark.asyncio
async def test_bound_actor_stops_when_its_admission_fence_is_revoked(
    tmp_path: Path,
) -> None:
    """A stale actor cannot remain a key-only consumer after fence revocation."""

    database, grant, store, binding = _fenced_components(tmp_path)
    key = binding.key
    event = SessionEventEnvelope(
        event_id="bound-actor-event",
        key=key,
        kind="BoundActorEvent",
        ownership_generation=binding.ownership_generation,
    )
    await store.enqueue(event)

    actor = AgentSessionActor(
        key=key,
        store=store,
        handler=_record_handler,
        ownership_binding=binding,
    )
    try:
        await actor.start()
        await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
        assert (await store.load(key)).data == {"event_id": event.event_id}
        assert actor.started is True

        database.actor_v2_admission_fences.revoke(
            grant,
            reason="bound actor ownership revoked",
        )
        with pytest.raises(ActorV2AdmissionFenceError):
            await store.has_pending_for_key(key, ownership_binding=binding)

        actor.wake()
        await _wait_for_stop(actor)

        assert actor.closed is True
        assert actor.last_error is not None
        assert "ActorV2AdmissionFence" in actor.last_error
    finally:
        await actor.shutdown(drain=False)


@pytest.mark.asyncio
async def test_bound_store_rejects_claim_lifecycle_after_fence_revocation(
    tmp_path: Path,
) -> None:
    """No bound actor-store operation can complete under a revoked fence."""

    database, grant, store, binding = _fenced_components(tmp_path)
    event = SessionEventEnvelope(
        event_id="bound-store-event",
        key=binding.key,
        kind="BoundStoreEvent",
        ownership_generation=binding.ownership_generation,
    )
    await store.enqueue(event)
    claim = await store.claim_next(
        binding.key,
        worker_id="bound-store-worker",
        ownership_binding=binding,
    )
    assert claim is not None
    aggregate = await store.load(binding.key, ownership_binding=binding)
    transition = SessionTransition(
        aggregate=aggregate.advance(data={"event_id": event.event_id}),
        disposition="bound_store_event_recorded",
    )

    database.actor_v2_admission_fences.revoke(
        grant,
        reason="bound store ownership revoked",
    )

    with pytest.raises(ActorV2AdmissionFenceError):
        await store.load(binding.key, ownership_binding=binding)
    with pytest.raises(ActorV2AdmissionFenceError):
        await store.recover(
            binding.key,
            worker_id="bound-store-worker",
            ownership_binding=binding,
        )
    with pytest.raises(ActorV2AdmissionFenceError):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
            ownership_binding=binding,
        )
    with pytest.raises(ActorV2AdmissionFenceError):
        await store.release(
            claim,
            error="must not release under a revoked binding",
            ownership_binding=binding,
        )
    with pytest.raises(ActorV2AdmissionFenceError):
        await store.fail(
            claim,
            error="must not fail under a revoked binding",
            ownership_binding=binding,
        )


@pytest.mark.asyncio
async def test_execution_bound_store_rejects_all_lifecycle_writes_after_target_loss(
    tmp_path: Path,
) -> None:
    """An expired target incarnation cannot keep mutating the same fenced actor."""

    database, _grant, store, binding = _fenced_components(tmp_path)
    execution_binding = _execution_binding(database, binding)
    event = SessionEventEnvelope(
        event_id="execution-bound-store-event",
        key=binding.key,
        kind="ExecutionBoundStoreEvent",
        ownership_generation=binding.ownership_generation,
    )
    await store.enqueue(event)
    claim = await store.claim_next(
        binding.key,
        worker_id="execution-bound-store-worker",
        ownership_binding=binding,
        execution_binding=execution_binding,
    )
    assert claim is not None
    aggregate = await store.load(
        binding.key,
        ownership_binding=binding,
        execution_binding=execution_binding,
    )
    transition = SessionTransition(
        aggregate=aggregate.advance(data={"event_id": event.event_id}),
        disposition="execution_bound_actor_event_recorded",
    )

    database.actor_v2_fenced_wake_target_leases.release(
        execution_binding.target_lease,
    )

    with pytest.raises(FencedWakeTargetLeaseError):
        await store.ensure(
            binding.key,
            ownership_generation=binding.ownership_generation,
            ownership_binding=binding,
            execution_binding=execution_binding,
        )
    with pytest.raises(FencedWakeTargetLeaseError):
        await store.load(
            binding.key,
            ownership_binding=binding,
            execution_binding=execution_binding,
        )
    with pytest.raises(FencedWakeTargetLeaseError):
        await store.recover(
            binding.key,
            worker_id="execution-bound-store-worker",
            ownership_binding=binding,
            execution_binding=execution_binding,
        )
    with pytest.raises(FencedWakeTargetLeaseError):
        await store.has_pending_for_key(
            binding.key,
            ownership_binding=binding,
            execution_binding=execution_binding,
        )
    with pytest.raises(FencedWakeTargetLeaseError):
        await store.commit(
            claim,
            transition,
            expected_revision=aggregate.state_revision,
            ownership_binding=binding,
            execution_binding=execution_binding,
        )
    with pytest.raises(FencedWakeTargetLeaseError):
        await store.release(
            claim,
            error="target incarnation is no longer live",
            ownership_binding=binding,
            execution_binding=execution_binding,
        )
    with pytest.raises(FencedWakeTargetLeaseError):
        await store.fail(
            claim,
            error="target incarnation is no longer live",
            ownership_binding=binding,
            execution_binding=execution_binding,
        )


@pytest.mark.asyncio
async def test_execution_bound_actor_stops_after_target_lease_loss(tmp_path: Path) -> None:
    """A target-lost actor stops instead of retrying under only its owner fence."""

    database, _grant, store, binding = _fenced_components(tmp_path)
    execution_binding = _execution_binding(database, binding)
    event = SessionEventEnvelope(
        event_id="execution-bound-actor-event",
        key=binding.key,
        kind="ExecutionBoundActorEvent",
        ownership_generation=binding.ownership_generation,
    )
    await store.enqueue(event)
    actor = AgentSessionActor(
        key=binding.key,
        store=store,
        handler=_record_handler,
        execution_binding=execution_binding,
    )
    try:
        await actor.start()
        await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
        database.actor_v2_fenced_wake_target_leases.release(
            execution_binding.target_lease,
        )

        actor.wake()
        await _wait_for_stop(actor)

        assert actor.closed is True
        assert actor.last_error is not None
        assert "FencedWakeTargetLease" in actor.last_error
    finally:
        await actor.shutdown(drain=False)


@pytest.mark.asyncio
async def test_bound_store_rejects_an_unfenced_binding_for_a_fenced_owner(
    tmp_path: Path,
) -> None:
    """The absence of a fence is itself part of an immutable binding identity."""

    _database, _grant, store, binding = _fenced_components(tmp_path)
    unfenced_binding = FencedMailboxWakeRequest(
        key=binding.key,
        ownership_generation=binding.ownership_generation,
    )

    with pytest.raises(AgentRuntimeOwnershipGenerationConflict, match="fence differs"):
        await store.ensure(
            binding.key,
            ownership_generation=binding.ownership_generation,
            ownership_binding=unfenced_binding,
        )
    with pytest.raises(AgentRuntimeOwnershipGenerationConflict, match="generation"):
        await store.ensure(
            binding.key,
            ownership_generation=binding.ownership_generation + 1,
            ownership_binding=binding,
        )

    assert (
        await store.ensure(
            binding.key,
            ownership_generation=binding.ownership_generation,
            ownership_binding=binding,
        )
    ).ownership_generation == binding.ownership_generation


@pytest.mark.asyncio
async def test_fenced_registry_owns_an_actor_by_the_full_request(
    tmp_path: Path,
) -> None:
    """A dormant registry cannot collapse a fenced wake to a SessionKey."""

    _database, _grant, store, binding = _fenced_components(tmp_path)
    event = SessionEventEnvelope(
        event_id="fenced-registry-event",
        key=binding.key,
        kind="FencedRegistryEvent",
        ownership_generation=binding.ownership_generation,
    )
    await store.enqueue(event)
    registry = FencedSessionActorRegistry(store=store, handler=_record_handler)
    try:
        receipt = await registry.wake_fenced(binding)

        assert receipt.disposition is FencedMailboxWakeDisposition.ACCEPTED
        actor = registry.actor_for(binding)
        assert actor is not None
        assert actor.ownership_binding == binding
        await asyncio.wait_for(actor.wait_idle(), timeout=1.0)
        assert (await store.load(binding.key)).data == {"event_id": event.event_id}
        assert not hasattr(registry, "wake")
        assert not hasattr(registry, "wake_handoff")
    finally:
        await registry.shutdown()

    assert registry.closed is True
    assert registry.shutdown_complete is True
    assert actor.started is False
    assert (
        await registry.wake_fenced(binding)
    ).disposition is FencedMailboxWakeDisposition.STALE


@pytest.mark.asyncio
async def test_fenced_registry_refuses_unfenced_or_revoked_requests(
    tmp_path: Path,
) -> None:
    """The lower-level registry has no unfenced or stale-owner fallback path."""

    database, grant, store, binding = _fenced_components(tmp_path)
    registry = FencedSessionActorRegistry(store=store, handler=_record_handler)
    unfenced = FencedMailboxWakeRequest(
        key=binding.key,
        ownership_generation=binding.ownership_generation,
    )
    try:
        with pytest.raises(ValueError, match="admission-fenced"):
            await registry.wake_fenced(unfenced)

        database.actor_v2_admission_fences.revoke(
            grant,
            reason="fenced registry owner revoked",
        )
        assert (
            await registry.wake_fenced(binding)
        ).disposition is FencedMailboxWakeDisposition.STALE
        assert registry.actor_for(binding) is None
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_fenced_registry_replaces_a_stopped_target_bound_actor(
    tmp_path: Path,
) -> None:
    """A new lease epoch cannot reuse the previous target-bound actor instance."""

    database, _grant, store, binding = _fenced_components(tmp_path)
    first_binding = _execution_binding(database, binding)
    registry = FencedSessionActorRegistry(store=store, handler=_record_handler)
    try:
        first = await registry.wake_leased(first_binding)
        assert first.disposition is FencedMailboxWakeDisposition.ACCEPTED
        first_actor = registry.actor_for(binding)
        assert first_actor is not None
        assert first_actor.execution_binding == first_binding

        database.actor_v2_fenced_wake_target_leases.release(first_binding.target_lease)
        stale = await registry.wake_leased(first_binding)
        assert stale.disposition is FencedMailboxWakeDisposition.STALE
        assert registry.actor_for(binding) is None

        next_binding = _execution_binding(
            database,
            binding,
            incarnation_id="incarnation-b",
        )
        accepted = await registry.wake_leased(next_binding)
        assert accepted.disposition is FencedMailboxWakeDisposition.ACCEPTED
        next_actor = registry.actor_for(binding)
        assert next_actor is not None
        assert next_actor is not first_actor
        assert next_actor.execution_binding == next_binding
    finally:
        await registry.shutdown()
