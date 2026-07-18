"""Integration coverage for durable per-process core-drain request discovery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from shinbot.agent.runtime.actor_v2_core_ingress_drain_service import (
    ActorV2CoreIngressDrainServiceDisposition,
    DurableActorV2CoreIngressDrainService,
)
from shinbot.agent.runtime.actor_v2_core_ingress_drain_worker import (
    ActorV2CoreIngressDrainProcessWorker,
)
from shinbot.agent.runtime.legacy_session_local_drain import (
    LegacySessionLocalDrainReceipt,
    LegacySessionLocalDrainRequest,
    LegacySessionLocalDrainTicket,
)
from shinbot.agent.runtime.legacy_session_quiescence import (
    LegacySessionAllProfilesTaskQuiescence,
)
from shinbot.agent.runtime.legacy_signal_admission import (
    LegacyAgentSignalFreezeTicket,
    LegacyAgentSignalQuiescenceReceipt,
    LegacyAgentSignalQuiescenceStatus,
)
from shinbot.agent.runtime.service_health import RuntimeServiceStatus
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainNotReady,
    ActorV2CoreIngressDrainStatus,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressFreezeTicket,
    LegacyIngressQuiescenceReceipt,
    LegacyIngressQuiescenceStatus,
)
from shinbot.core.dispatch.message_context import (
    WaitingInputFreezeTicket,
    WaitingInputQuiescenceReceipt,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.repositories.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainRepository,
)
from shinbot.persistence.repositories.actor_v2_ingress_drain import (
    ActorV2IngressDrainRepository,
)
from shinbot.persistence.repositories.actor_v2_migration_barrier import (
    ActorV2MigrationBarrierRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)


@dataclass(slots=True)
class _LegacyDrain:
    """Local drain fake that preserves tickets across retry attempts."""

    outcomes: list[bool]
    fail: bool = False
    freeze_calls: int = 0
    drain_calls: int = 0

    def freeze(self, request: LegacySessionLocalDrainRequest) -> LegacySessionLocalDrainTicket:
        """Build one opaque ticket matching the supplied local drain request."""

        self.freeze_calls += 1
        return LegacySessionLocalDrainTicket(
            request=request,
            ingress_ticket=LegacyIngressFreezeTicket(
                session_id=request.legacy_session_id,
                cutover_id=request.cutover_id,
                freeze_epoch=1,
                token="legacy-ingress-token",
            ),
            waiting_input_ticket=WaitingInputFreezeTicket(
                scope=request.waiting_input_scope,
                cutover_id=request.cutover_id,
                token="waiting-input-token",
            ),
            signal_ticket=LegacyAgentSignalFreezeTicket(
                session_id=request.legacy_session_id,
                cutover_id=request.cutover_id,
                freeze_epoch=1,
                token="legacy-signal-token",
            ),
        )

    async def drain(
        self,
        ticket: LegacySessionLocalDrainTicket,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalDrainReceipt:
        """Return one complete, retryable, or failed local drain observation."""

        del timeout_seconds
        self.drain_calls += 1
        if self.fail:
            raise RuntimeError("local legacy drain worker failed")
        quiescent = self.outcomes.pop(0)
        return LegacySessionLocalDrainReceipt(
            ticket=ticket,
            ingress=LegacyIngressQuiescenceReceipt(
                ticket=ticket.ingress_ticket,
                status=(
                    LegacyIngressQuiescenceStatus.QUIESCENT
                    if quiescent
                    else LegacyIngressQuiescenceStatus.TIMED_OUT
                ),
            ),
            waiting_input=WaitingInputQuiescenceReceipt(
                ticket=ticket.waiting_input_ticket,
                quiescent=quiescent,
            ),
            agent_signals=LegacyAgentSignalQuiescenceReceipt(
                ticket=ticket.signal_ticket,
                status=(
                    LegacyAgentSignalQuiescenceStatus.QUIESCENT
                    if quiescent
                    else LegacyAgentSignalQuiescenceStatus.TIMED_OUT
                ),
            ),
            agent_tasks=LegacySessionAllProfilesTaskQuiescence(
                session_id=ticket.request.legacy_session_id,
                observations=(),
            ),
        )


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain shared by local process views."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _repositories(
    database: DatabaseManager,
    now: list[float],
) -> tuple[
    AgentRuntimeOwnershipRepository,
    ActorV2MigrationBarrierRepository,
    ActorV2IngressDrainRepository,
    ActorV2CoreIngressDrainRepository,
]:
    """Install deterministic migration repositories over one SQLite domain."""

    member_ids = iter(("member-a", "member-b"))
    participant_tokens = iter(("participant-token-a", "participant-token-b"))
    ownership = AgentRuntimeOwnershipRepository(database, clock=lambda: now[0])
    barrier = ActorV2MigrationBarrierRepository(
        database,
        clock=lambda: now[0],
        barrier_id_factory=lambda: "migration-barrier-a",
        holder_token_factory=lambda: "migration-holder-token",
    )
    ingress = ActorV2IngressDrainRepository(
        database,
        clock=lambda: now[0],
        member_id_factory=lambda: next(member_ids),
        request_id_factory=lambda: "adapter-drain-request-unused",
        holder_token_factory=lambda: next(participant_tokens),
    )
    core = ActorV2CoreIngressDrainRepository(
        database,
        clock=lambda: now[0],
        request_id_factory=lambda: "core-drain-request-a",
    )
    database.agent_runtime_ownership = ownership
    database.actor_v2_migration_barriers = barrier
    database.actor_v2_ingress_drains = ingress
    database.actor_v2_core_ingress_drains = core
    return ownership, barrier, ingress, core


def _open_request(
    database: DatabaseManager,
    now: list[float],
    *,
    adapter_instance_ids: tuple[str, ...],
) -> tuple[
    ActorV2CoreIngressDrainRepository,
    object,
    dict[str, object],
]:
    """Create one migrating owner, frozen members, and a shared open request."""

    ownership, barrier, ingress, core = _repositories(database, now)
    grants: dict[str, object] = {}
    for adapter_instance_id in adapter_instance_ids:
        suffix = adapter_instance_id.rsplit("-", 1)[-1]
        grants[adapter_instance_id] = ingress.register_participant(
            adapter_instance_id=adapter_instance_id,
            participant_id=f"process-{suffix}:incarnation-{suffix}",
            participant_epoch=1,
        )
    key = SessionKey("profile-a", "profile-a:group:room")
    source = ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="core ingress service integration source",
        legacy_session_id="legacy-session-a",
        requested_by="test",
    ).ownership
    barrier_grant = barrier.start_legacy_to_actor_v2(
        key,
        expected_generation=source.generation,
        adapter_instance_ids=adapter_instance_ids,
        holder_id="cutover-controller-a",
        reason="begin durable local core ingress service test",
    )
    return core, barrier_grant, grants | {"request": core.begin_drain(barrier_grant)}


def _worker(
    core: ActorV2CoreIngressDrainRepository,
    *,
    adapter_instance_id: str,
    grant: object,
    legacy_drain: _LegacyDrain,
) -> ActorV2CoreIngressDrainProcessWorker:
    """Build one process worker with exactly one local adapter grant."""

    return ActorV2CoreIngressDrainProcessWorker(
        repository=core,
        participant_grants={adapter_instance_id: grant},
        legacy_drain=legacy_drain,
    )


@pytest.mark.asyncio
async def test_services_deliver_one_durable_request_to_each_covered_process(
    tmp_path: Path,
) -> None:
    """Two local services acknowledge only their own members in a shared request."""

    now = [100.0]
    database = _database(tmp_path)
    core, barrier_grant, context = _open_request(
        database,
        now,
        adapter_instance_ids=("adapter-a", "adapter-b"),
    )
    request = context["request"]
    drain_a = _LegacyDrain([True])
    drain_b = _LegacyDrain([True])
    service_a = DurableActorV2CoreIngressDrainService(
        repository=core,
        worker=_worker(
            core,
            adapter_instance_id="adapter-a",
            grant=context["adapter-a"],
            legacy_drain=drain_a,
        ),
        runtime_id="process-a",
    )
    service_b = DurableActorV2CoreIngressDrainService(
        repository=core,
        worker=_worker(
            core,
            adapter_instance_id="adapter-b",
            grant=context["adapter-b"],
            legacy_drain=drain_b,
        ),
        runtime_id="process-b",
    )

    summary_a = await service_a.run_once()

    assert summary_a.results[0].request_id == request.request_id
    assert (
        summary_a.results[0].disposition
        is ActorV2CoreIngressDrainServiceDisposition.ACKNOWLEDGED
    )
    still_open = core.get(request.request_id)
    assert still_open is not None
    assert still_open.status is ActorV2CoreIngressDrainStatus.OPEN
    assert tuple(item.member_id for item in still_open.acknowledgements) == ("member-a",)
    assert (await service_a.run_once()).results == ()

    summary_b = await service_b.run_once()

    assert summary_b.results[0].request_id == request.request_id
    assert (
        summary_b.results[0].disposition
        is ActorV2CoreIngressDrainServiceDisposition.ACKNOWLEDGED
    )
    assert drain_a.freeze_calls == drain_a.drain_calls == 1
    assert drain_b.freeze_calls == drain_b.drain_calls == 1
    drained = core.confirm_drained(
        request_id=request.request_id,
        barrier_grant=barrier_grant,
    )
    assert drained.status is ActorV2CoreIngressDrainStatus.DRAINED


@pytest.mark.asyncio
async def test_service_retries_one_nonquiescent_local_drain_without_refreezing(
    tmp_path: Path,
) -> None:
    """A retry preserves the opaque local ticket until the process becomes quiescent."""

    now = [100.0]
    database = _database(tmp_path)
    core, barrier_grant, context = _open_request(
        database,
        now,
        adapter_instance_ids=("adapter-a",),
    )
    request = context["request"]
    local_drain = _LegacyDrain([False, True])
    service = DurableActorV2CoreIngressDrainService(
        repository=core,
        worker=_worker(
            core,
            adapter_instance_id="adapter-a",
            grant=context["adapter-a"],
            legacy_drain=local_drain,
        ),
    )

    first = await service.run_once()

    assert (
        first.results[0].disposition
        is ActorV2CoreIngressDrainServiceDisposition.AWAITING_LOCAL_DRAIN
    )
    assert core.get(request.request_id).status is ActorV2CoreIngressDrainStatus.OPEN
    with pytest.raises(ActorV2CoreIngressDrainNotReady):
        core.confirm_drained(request_id=request.request_id, barrier_grant=barrier_grant)

    second = await service.run_once()

    assert (
        second.results[0].disposition
        is ActorV2CoreIngressDrainServiceDisposition.ACKNOWLEDGED
    )
    assert local_drain.freeze_calls == 1
    assert local_drain.drain_calls == 2
    assert core.confirm_drained(
        request_id=request.request_id,
        barrier_grant=barrier_grant,
    ).durably_drained


@pytest.mark.asyncio
async def test_service_failure_leaves_the_request_open_and_operator_visible(
    tmp_path: Path,
) -> None:
    """A local exception cannot become a durable acknowledgement or confirmation."""

    now = [100.0]
    database = _database(tmp_path)
    core, barrier_grant, context = _open_request(
        database,
        now,
        adapter_instance_ids=("adapter-a",),
    )
    request = context["request"]
    service = DurableActorV2CoreIngressDrainService(
        repository=core,
        worker=_worker(
            core,
            adapter_instance_id="adapter-a",
            grant=context["adapter-a"],
            legacy_drain=_LegacyDrain([], fail=True),
        ),
    )

    summary = await service.run_once()

    assert (
        summary.results[0].disposition
        is ActorV2CoreIngressDrainServiceDisposition.FAILED
    )
    assert summary.results[0].error_code == "RuntimeError"
    assert service.health_snapshot().status is RuntimeServiceStatus.DEGRADED
    current = core.get(request.request_id)
    assert current is not None
    assert current.status is ActorV2CoreIngressDrainStatus.OPEN
    assert not current.acknowledgements
    with pytest.raises(ActorV2CoreIngressDrainNotReady):
        core.confirm_drained(request_id=request.request_id, barrier_grant=barrier_grant)
