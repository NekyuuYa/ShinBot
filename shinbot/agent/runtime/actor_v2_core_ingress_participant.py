"""Unmounted lifecycle for one process's Actor v2 core-ingress membership.

The lifecycle deliberately owns only a process-local boundary:

* durable membership is registered before an adapter callback can begin;
* the registered grants compose the existing local core-drain worker and
  durable request-discovery service;
* advisory heartbeats remain visible while the process can serve a frozen
  request; and
* callback ingress stops before the complete member set can retire.

It does not acquire a migration barrier, create or confirm a drain request,
publish an Actor target, or mount itself into the normal adapter/runtime boot
path. A future production cutover controller must retain those authorities.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.actor_v2_core_ingress_drain_service import (
    MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH,
    DurableActorV2CoreIngressDrainService,
)
from shinbot.agent.runtime.actor_v2_core_ingress_drain_worker import (
    ActorV2CoreIngressDrainProcessWorker,
    LocalLegacySessionDrainPort,
)
from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    RuntimeServiceStatus,
    supervised_backoff_seconds,
)
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainAcknowledgement,
    ActorV2CoreIngressDrainDiscoveryCursor,
    ActorV2CoreIngressDrainDiscoveryPage,
    ActorV2CoreIngressDrainReceipt,
    ActorV2CoreIngressDrainRequest,
)
from shinbot.core.dispatch.actor_v2_ingress_drain import (
    ActorV2IngressDrainConflict,
    ActorV2IngressParticipant,
    ActorV2IngressParticipantGrant,
    ActorV2IngressParticipantStatus,
)
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:core-ingress-participant", color="yellow")


class ActorV2CoreIngressAdapterLifecyclePort(Protocol):
    """One adapter callback boundary supervised by a future cutover owner.

    ``start_receiving_callbacks`` must not allow any platform callback into
    ShinBot before it returns. ``stop_receiving_callbacks`` must not return
    until that callback admission has stopped. The port is intentionally more
    specific than ``BaseAdapter``: built-in adapters do not currently prove
    this lifecycle and therefore are not mounted through this class.
    """

    @property
    def adapter_instance_id(self) -> str:
        """Return the immutable adapter instance covered by this port."""

    @property
    def receiving_callbacks(self) -> bool:
        """Return whether this process can currently admit adapter callbacks."""

    async def start_receiving_callbacks(self) -> None:
        """Begin callback admission only after membership registration."""

    async def stop_receiving_callbacks(self) -> None:
        """Stop callback admission before membership retirement."""


class ActorV2IngressMembershipLifecyclePort(Protocol):
    """Durable membership operations owned by the local process lifecycle."""

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain protecting membership state."""

    def register_participants(
        self,
        *,
        adapter_instance_ids: tuple[str, ...],
        participant_id: str,
        participant_epoch: int,
    ) -> tuple[ActorV2IngressParticipantGrant, ...]:
        """Register every local adapter before callback admission."""

    def heartbeat_participants(
        self,
        grants: tuple[ActorV2IngressParticipantGrant, ...],
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """Record advisory liveness for the complete local member set."""

    def retire_participants(
        self,
        grants: tuple[ActorV2IngressParticipantGrant, ...],
    ) -> tuple[ActorV2IngressParticipant, ...]:
        """Retire every local member only after durable drain acknowledgement."""


class ActorV2CoreIngressDrainLifecycleRepositoryPort(Protocol):
    """Combined worker and discovery repository used by one local service."""

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared with membership state."""

    def get(self, request_id: str) -> ActorV2CoreIngressDrainRequest | None:
        """Return one exact core-ingress drain request."""

    def acknowledge_quiescent(
        self,
        *,
        request_id: str,
        participant_grant: ActorV2IngressParticipantGrant,
        receipt: ActorV2CoreIngressDrainReceipt,
    ) -> ActorV2CoreIngressDrainAcknowledgement:
        """Persist one token-free local core-drain acknowledgement."""

    def discover_open_for_participant(
        self,
        participant_id: str,
        *,
        limit: int,
        after: ActorV2CoreIngressDrainDiscoveryCursor | None = None,
    ) -> ActorV2CoreIngressDrainDiscoveryPage:
        """Discover one bounded page of unacknowledged local work."""


class ActorV2CoreIngressParticipantLifecycleState(StrEnum):
    """Terminal-safe local state for one non-reusable process participant."""

    READY = "ready"
    ACTIVE = "active"
    RETIRE_BLOCKED = "retire_blocked"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressParticipantLifecycleSnapshot:
    """Token-free diagnostics for a process-local membership lifetime."""

    state: ActorV2CoreIngressParticipantLifecycleState
    participant_id: str
    participant_epoch: int
    adapter_instance_ids: tuple[str, ...]
    participants: tuple[ActorV2IngressParticipant, ...]
    receiving_callback_adapter_ids: tuple[str, ...]
    drain_service_health: RuntimeServiceHealthSnapshot
    heartbeat_health: RuntimeServiceHealthSnapshot
    persistence_domain_matches: bool
    members_retired: bool
    cleanup_failed: bool
    error: str = ""

    def __post_init__(self) -> None:
        """Validate bounded, token-free lifecycle diagnostics."""

        if not isinstance(self.state, ActorV2CoreIngressParticipantLifecycleState):
            raise TypeError("state must be an ActorV2CoreIngressParticipantLifecycleState")
        participant_id = _identifier(self.participant_id, "participant_id")
        participant_epoch = _positive_int(self.participant_epoch, "participant_epoch")
        adapter_instance_ids = _adapter_instance_ids(self.adapter_instance_ids)
        participants = tuple(self.participants)
        if any(not isinstance(item, ActorV2IngressParticipant) for item in participants):
            raise TypeError("participants must contain ActorV2IngressParticipant values")
        member_ids = tuple(item.member_id for item in participants)
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("participants cannot repeat a member")
        if any(item.participant_id != participant_id for item in participants):
            raise ValueError("participants must retain the lifecycle participant_id")
        if any(item.participant_epoch != participant_epoch for item in participants):
            raise ValueError("participants must retain the lifecycle participant_epoch")
        if any(item.adapter_instance_id not in adapter_instance_ids for item in participants):
            raise ValueError("participants must belong to a configured adapter")
        receiving_callback_adapter_ids = _optional_adapter_instance_ids(
            self.receiving_callback_adapter_ids,
            field_name="receiving_callback_adapter_ids",
        )
        if not set(receiving_callback_adapter_ids).issubset(adapter_instance_ids):
            raise ValueError("callback diagnostics include an unknown adapter")
        if not isinstance(self.drain_service_health, RuntimeServiceHealthSnapshot):
            raise TypeError("drain_service_health must be a RuntimeServiceHealthSnapshot")
        if not isinstance(self.heartbeat_health, RuntimeServiceHealthSnapshot):
            raise TypeError("heartbeat_health must be a RuntimeServiceHealthSnapshot")
        if not isinstance(self.persistence_domain_matches, bool):
            raise TypeError("persistence_domain_matches must be a bool")
        if not isinstance(self.members_retired, bool):
            raise TypeError("members_retired must be a bool")
        if not isinstance(self.cleanup_failed, bool):
            raise TypeError("cleanup_failed must be a bool")
        object.__setattr__(self, "participant_id", participant_id)
        object.__setattr__(self, "participant_epoch", participant_epoch)
        object.__setattr__(self, "adapter_instance_ids", adapter_instance_ids)
        object.__setattr__(
            self,
            "participants",
            tuple(sorted(participants, key=lambda item: item.adapter_instance_id)),
        )
        object.__setattr__(
            self,
            "receiving_callback_adapter_ids",
            receiving_callback_adapter_ids,
        )
        object.__setattr__(self, "error", str(self.error or "").strip()[:500])


class ActorV2CoreIngressParticipantLifecycleError(RuntimeError):
    """Raised when the process-local participant cannot prove a safe boundary."""


class ActorV2CoreIngressParticipantRetirementBlocked(
    ActorV2CoreIngressParticipantLifecycleError
):
    """Raised when a stopped callback source still has unacknowledged drain work."""


class ActorV2CoreIngressParticipantLifecycle:
    """Register, supervise, and retire one process's complete adapter scope.

    A caller supplies only adapter wrappers that can prove callback admission
    starts after durable registration and stops before retirement. The class
    builds the existing ``ActorV2CoreIngressDrainProcessWorker`` and
    ``DurableActorV2CoreIngressDrainService`` internally, so a frozen request
    remains deliverable to the same local grants that registered it.

    This class is deliberately unmounted. Constructing or activating it does
    not change the standard ``AdapterManager`` or ``AgentRuntime`` boot path.
    A future production cutover controller must explicitly own its instance.
    """

    def __init__(
        self,
        *,
        membership_repository: ActorV2IngressMembershipLifecyclePort,
        core_drain_repository: ActorV2CoreIngressDrainLifecycleRepositoryPort,
        callback_ingresses: Mapping[str, ActorV2CoreIngressAdapterLifecyclePort],
        participant_id: str,
        participant_epoch: int,
        legacy_drain: LocalLegacySessionDrainPort,
        heartbeat_interval_seconds: float = 30.0,
        core_drain_tick_interval_seconds: float = 5.0,
        core_drain_batch_limit: int = 25,
        core_drain_local_timeout_seconds: float | None = None,
    ) -> None:
        """Bind one inactive process scope to same-domain durable primitives.

        ``legacy_drain`` is normally built with
        ``AgentRuntime.build_legacy_session_local_drain_participant(ingress)``.
        Passing it here does not register it on ingress, timers, or a runtime
        lifecycle; only an explicit caller can activate this participant.
        """

        _require_membership_repository(membership_repository)
        _require_core_drain_repository(core_drain_repository)
        _require_legacy_drain(legacy_drain)
        ingresses = _callback_ingresses(callback_ingresses)
        if any(ingress.receiving_callbacks for ingress in ingresses.values()):
            raise ValueError(
                "core ingress participant requires callback ingress to be inactive"
            )
        if membership_repository.persistence_domain is not core_drain_repository.persistence_domain:
            raise ValueError(
                "membership and core drain repositories must share one persistence domain"
            )
        participant_id = _identifier(participant_id, "participant_id")
        participant_epoch = _positive_int(participant_epoch, "participant_epoch")
        heartbeat_interval_seconds = _positive_finite(
            heartbeat_interval_seconds,
            "heartbeat_interval_seconds",
        )
        core_drain_tick_interval_seconds = _positive_finite(
            core_drain_tick_interval_seconds,
            "core_drain_tick_interval_seconds",
        )
        if (
            isinstance(core_drain_batch_limit, bool)
            or not isinstance(core_drain_batch_limit, int)
            or not 1 <= core_drain_batch_limit <= MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH
        ):
            raise ValueError(
                "core_drain_batch_limit must be between 1 and "
                f"{MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH}"
            )
        core_drain_local_timeout_seconds = _optional_timeout(
            core_drain_local_timeout_seconds,
            "core_drain_local_timeout_seconds",
        )

        self._membership_repository = membership_repository
        self._core_drain_repository = core_drain_repository
        self._callback_ingresses = ingresses
        self._participant_id = participant_id
        self._participant_epoch = participant_epoch
        self._legacy_drain = legacy_drain
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._core_drain_tick_interval_seconds = core_drain_tick_interval_seconds
        self._core_drain_batch_limit = core_drain_batch_limit
        self._core_drain_local_timeout_seconds = core_drain_local_timeout_seconds
        self._persistence_domain = membership_repository.persistence_domain
        self._lifecycle_lock = asyncio.Lock()
        self._heartbeat_health = RuntimeServiceHealth("actor_v2_ingress_participant_heartbeat")
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._worker: ActorV2CoreIngressDrainProcessWorker | None = None
        self._service: DurableActorV2CoreIngressDrainService | None = None
        self._grants: tuple[ActorV2IngressParticipantGrant, ...] = ()
        self._participants: tuple[ActorV2IngressParticipant, ...] = ()
        self._active = False
        self._closed = False
        self._members_retired = False
        self._retirement_blocked = False
        self._cleanup_failed = False
        self._error = ""

    @property
    def participant_id(self) -> str:
        """Return the immutable process-incarnation identity."""

        return self._participant_id

    @property
    def participant_epoch(self) -> int:
        """Return the immutable process-incarnation epoch."""

        return self._participant_epoch

    @property
    def adapter_instance_ids(self) -> tuple[str, ...]:
        """Return the complete local adapter scope in canonical order."""

        return tuple(self._callback_ingresses)

    @property
    def persistence_domain(self) -> object:
        """Return the same durable domain shared by both repositories."""

        return self._persistence_domain

    @property
    def snapshot(self) -> ActorV2CoreIngressParticipantLifecycleSnapshot:
        """Return a token-free lifecycle and service health snapshot."""

        drain_service_health = self._drain_service_health()
        receiving_callback_adapter_ids = self._receiving_callback_adapter_ids()
        if self._closed:
            state = ActorV2CoreIngressParticipantLifecycleState.CLOSED
        elif self._retirement_blocked:
            state = ActorV2CoreIngressParticipantLifecycleState.RETIRE_BLOCKED
        elif self._cleanup_failed:
            state = ActorV2CoreIngressParticipantLifecycleState.FAILED
        elif self._active and self._components_active(
            drain_service_health,
            receiving_callback_adapter_ids,
        ):
            state = ActorV2CoreIngressParticipantLifecycleState.ACTIVE
        elif not self._grants and not receiving_callback_adapter_ids:
            state = ActorV2CoreIngressParticipantLifecycleState.READY
        else:
            state = ActorV2CoreIngressParticipantLifecycleState.FAILED
        return ActorV2CoreIngressParticipantLifecycleSnapshot(
            state=state,
            participant_id=self._participant_id,
            participant_epoch=self._participant_epoch,
            adapter_instance_ids=self.adapter_instance_ids,
            participants=self._participants,
            receiving_callback_adapter_ids=receiving_callback_adapter_ids,
            drain_service_health=drain_service_health,
            heartbeat_health=self._heartbeat_health.snapshot(),
            persistence_domain_matches=self._domains_match(),
            members_retired=self._members_retired,
            cleanup_failed=self._cleanup_failed,
            error=self._error,
        )

    async def activate(self) -> ActorV2CoreIngressParticipantLifecycleSnapshot:
        """Register all members before starting any adapter callback ingress."""

        async with self._lifecycle_lock:
            if self._closed:
                raise ActorV2CoreIngressParticipantLifecycleError(
                    "a closed core ingress participant cannot activate"
                )
            if self._retirement_blocked or self._cleanup_failed or self._members_retired:
                raise ActorV2CoreIngressParticipantLifecycleError(
                    "a failed core ingress participant may only retry shutdown"
                )
            if self._active:
                return await self._verify_active_locked()
            try:
                self._require_ready_for_activation()
                grants = self._membership_repository.register_participants(
                    adapter_instance_ids=self.adapter_instance_ids,
                    participant_id=self._participant_id,
                    participant_epoch=self._participant_epoch,
                )
                self._install_grants(grants)
                worker = ActorV2CoreIngressDrainProcessWorker(
                    repository=self._core_drain_repository,
                    participant_grants={
                        grant.participant.adapter_instance_id: grant for grant in self._grants
                    },
                    legacy_drain=self._legacy_drain,
                )
                service = DurableActorV2CoreIngressDrainService(
                    repository=self._core_drain_repository,
                    worker=worker,
                    tick_interval_seconds=self._core_drain_tick_interval_seconds,
                    batch_limit=self._core_drain_batch_limit,
                    local_drain_timeout_seconds=self._core_drain_local_timeout_seconds,
                    runtime_id=self._participant_id,
                )
                self._worker = worker
                self._service = service
                service.start()
                if not self._drain_service_running():
                    raise ActorV2CoreIngressParticipantLifecycleError(
                        "core ingress drain service did not start"
                    )
                self._start_heartbeat()
                for adapter_instance_id, ingress in self._callback_ingresses.items():
                    await ingress.start_receiving_callbacks()
                    if not ingress.receiving_callbacks:
                        raise ActorV2CoreIngressParticipantLifecycleError(
                            "adapter callback ingress did not start for "
                            + adapter_instance_id
                        )
            except BaseException as exc:
                self._error = _error_text(exc)
                if self._grants or self._service is not None:
                    await self._terminate()
                raise
            self._active = True
            self._error = ""
            return self.snapshot

    async def verify_active(self) -> ActorV2CoreIngressParticipantLifecycleSnapshot:
        """Revalidate membership, local service, and callback boundaries."""

        async with self._lifecycle_lock:
            if self._closed:
                raise ActorV2CoreIngressParticipantLifecycleError(
                    "a closed core ingress participant cannot verify activity"
                )
            if self._retirement_blocked or self._cleanup_failed:
                raise ActorV2CoreIngressParticipantLifecycleError(
                    "core ingress participant is blocked; only shutdown may retry it"
                )
            return await self._verify_active_locked()

    async def shutdown(self) -> ActorV2CoreIngressParticipantLifecycleSnapshot:
        """Stop callback ingress, drain local work, then retire every member."""

        async with self._lifecycle_lock:
            if self._closed:
                return self.snapshot
            await self._terminate()
            return self.snapshot

    async def _verify_active_locked(self) -> ActorV2CoreIngressParticipantLifecycleSnapshot:
        """Verify active components under the serialized lifecycle lock."""

        if not self._active:
            raise ActorV2CoreIngressParticipantLifecycleError(
                "core ingress participant has not activated"
            )
        try:
            if not self._domains_match() or not self._drain_service_running():
                raise ActorV2CoreIngressParticipantLifecycleError(
                    "core ingress participant lost durable service alignment"
                )
            if self._receiving_callback_adapter_ids() != self.adapter_instance_ids:
                raise ActorV2CoreIngressParticipantLifecycleError(
                    "core ingress participant lost adapter callback admission"
                )
            self._record_heartbeat()
        except BaseException as exc:
            self._error = _error_text(exc)
            await self._terminate()
            raise
        return self.snapshot

    def _require_ready_for_activation(self) -> None:
        """Reject callback or repository drift before membership registration."""

        if not self._domains_match():
            raise ActorV2CoreIngressParticipantLifecycleError(
                "core ingress participant repositories no longer share a durable domain"
            )
        if self._receiving_callback_adapter_ids():
            raise ActorV2CoreIngressParticipantLifecycleError(
                "adapter callback ingress is already active before membership registration"
            )

    def _install_grants(
        self,
        grants: tuple[ActorV2IngressParticipantGrant, ...],
    ) -> None:
        """Validate the complete local capability set without exposing tokens."""

        if any(not isinstance(grant, ActorV2IngressParticipantGrant) for grant in grants):
            raise TypeError("membership registration returned an invalid participant grant")
        # Retain every typed capability before validating scope. A malformed
        # external repository must not turn an already-registered member into
        # an orphan merely because its result fails local composition checks.
        self._grants = tuple(grants)
        self._participants = tuple(grant.participant for grant in self._grants)
        if len(grants) != len(self._callback_ingresses):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership registration did not return every local adapter grant"
            )
        by_adapter = {grant.participant.adapter_instance_id: grant for grant in grants}
        if len(by_adapter) != len(grants) or tuple(sorted(by_adapter)) != self.adapter_instance_ids:
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership registration returned a mismatched adapter scope"
            )
        if any(
            grant.participant.participant_id != self._participant_id
            or grant.participant.participant_epoch != self._participant_epoch
            or not grant.participant.active
            for grant in grants
        ):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership registration returned a mismatched active participant"
            )
        self._grants = tuple(by_adapter[adapter_id] for adapter_id in self.adapter_instance_ids)
        self._participants = tuple(grant.participant for grant in self._grants)

    def _start_heartbeat(self) -> None:
        """Validate and begin advisory heartbeat supervision for active grants."""

        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_health.start()
        try:
            self._record_heartbeat()
        except BaseException as exc:
            self._heartbeat_health.failed(exc)
            raise
        self._heartbeat_task = asyncio.get_running_loop().create_task(
            self._run_heartbeat_loop(),
            name=f"agent-core-ingress-participant-heartbeat:{self._participant_id}",
        )

    def _record_heartbeat(self) -> None:
        """Refresh and validate the complete local membership set once."""

        if not self._grants or self._members_retired:
            raise ActorV2CoreIngressParticipantLifecycleError(
                "core ingress participant has no active grants to heartbeat"
            )
        participants = self._membership_repository.heartbeat_participants(self._grants)
        self._install_heartbeated_participants(participants)
        self._heartbeat_health.succeeded()

    def _install_heartbeated_participants(
        self,
        participants: tuple[ActorV2IngressParticipant, ...],
    ) -> None:
        """Require a heartbeat response for the exact complete local member set."""

        if len(participants) != len(self._grants):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership heartbeat returned an incomplete local member set"
            )
        if any(not isinstance(participant, ActorV2IngressParticipant) for participant in participants):
            raise TypeError("membership heartbeat returned an invalid participant")
        by_member_id = {participant.member_id: participant for participant in participants}
        expected_member_ids = tuple(grant.participant.member_id for grant in self._grants)
        if tuple(sorted(by_member_id)) != tuple(sorted(expected_member_ids)):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership heartbeat returned a mismatched member set"
            )
        ordered = tuple(by_member_id[member_id] for member_id in expected_member_ids)
        if any(
            not participant.active
            or participant.adapter_instance_id != grant.participant.adapter_instance_id
            or participant.participant_id != self._participant_id
            or participant.participant_epoch != self._participant_epoch
            for participant, grant in zip(ordered, self._grants, strict=True)
        ):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership heartbeat no longer proves the active local participant"
            )
        self._participants = ordered

    def _install_retired_participants(
        self,
        participants: tuple[ActorV2IngressParticipant, ...],
    ) -> None:
        """Require terminal retirement for the exact complete local member set."""

        if len(participants) != len(self._grants):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership retirement returned an incomplete local member set"
            )
        if any(not isinstance(participant, ActorV2IngressParticipant) for participant in participants):
            raise TypeError("membership retirement returned an invalid participant")
        by_member_id = {participant.member_id: participant for participant in participants}
        expected_member_ids = tuple(grant.participant.member_id for grant in self._grants)
        if tuple(sorted(by_member_id)) != tuple(sorted(expected_member_ids)):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership retirement returned a mismatched member set"
            )
        ordered = tuple(by_member_id[member_id] for member_id in expected_member_ids)
        if any(
            participant.status is not ActorV2IngressParticipantStatus.RETIRED
            or participant.adapter_instance_id != grant.participant.adapter_instance_id
            or participant.participant_id != self._participant_id
            or participant.participant_epoch != self._participant_epoch
            for participant, grant in zip(ordered, self._grants, strict=True)
        ):
            raise ActorV2CoreIngressParticipantLifecycleError(
                "membership retirement did not prove the exact local member set stopped"
            )
        self._participants = ordered

    async def _run_heartbeat_loop(self) -> None:
        """Maintain advisory observations without converting them into leases."""

        delay = self._heartbeat_interval_seconds
        try:
            while True:
                await asyncio.sleep(delay)
                try:
                    self._record_heartbeat()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._heartbeat_health.failed(exc)
                    logger.exception(
                        format_log_event(
                            "agent.core_ingress_participant.heartbeat_failed",
                            participant_id=self._participant_id,
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._heartbeat_health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._heartbeat_interval_seconds,
                        consecutive_failures=(
                            self._heartbeat_health.snapshot().consecutive_failures
                        ),
                    )
                else:
                    delay = self._heartbeat_interval_seconds
        finally:
            self._heartbeat_health.stop()

    async def _terminate(self) -> None:
        """Complete stop-before-retire cleanup even when the caller cancels."""

        task = asyncio.create_task(
            self._terminate_once(),
            name=f"agent-core-ingress-participant-shutdown:{self._participant_id}",
        )
        cancelled_while_waiting = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled_while_waiting = True
        task.result()
        if cancelled_while_waiting:
            raise asyncio.CancelledError

    async def _terminate_once(self) -> None:
        """Stop callbacks, retain blocked members, or prove full retirement."""

        if not self._grants and self._service is None:
            await self._stop_heartbeat()
            self._active = False
            self._cleanup_failed = False
            self._retirement_blocked = False
            self._closed = True
            return

        try:
            await self._stop_callback_ingress()
        except BaseException as exc:
            self._active = False
            self._cleanup_failed = True
            self._error = _error_text(exc)
            raise
        self._active = False
        if self._receiving_callback_adapter_ids():
            error = ActorV2CoreIngressParticipantLifecycleError(
                "adapter callback ingress remained active during participant retirement"
            )
            self._cleanup_failed = True
            self._error = _error_text(error)
            raise error

        if self._members_retired:
            await self._stop_heartbeat()
            await self._shutdown_drain_service()
            self._cleanup_failed = False
            self._retirement_blocked = False
            self._closed = True
            return

        service = self._service
        if service is not None:
            try:
                await service.run_once()
            except BaseException as exc:
                self._cleanup_failed = True
                self._error = _error_text(exc)
                raise
        await self._stop_heartbeat()
        try:
            retired = self._membership_repository.retire_participants(self._grants)
        except ActorV2IngressDrainConflict as exc:
            self._retirement_blocked = True
            self._cleanup_failed = False
            self._error = _error_text(exc)
            try:
                self._start_heartbeat()
            except BaseException as heartbeat_error:
                self._cleanup_failed = True
                self._error = _error_text(heartbeat_error)
            raise ActorV2CoreIngressParticipantRetirementBlocked(
                "core ingress participant cannot retire until every drain request "
                "is acknowledged"
            ) from exc
        except BaseException as exc:
            self._cleanup_failed = True
            self._error = _error_text(exc)
            raise
        self._install_retired_participants(retired)
        self._members_retired = True
        self._retirement_blocked = False
        try:
            await self._shutdown_drain_service()
        except BaseException as exc:
            self._cleanup_failed = True
            self._error = _error_text(exc)
            raise
        self._cleanup_failed = False
        self._error = ""
        self._closed = True

    async def _stop_callback_ingress(self) -> None:
        """Stop every currently receiving callback source before retirement."""

        failures: list[BaseException] = []
        for adapter_instance_id in reversed(self.adapter_instance_ids):
            ingress = self._callback_ingresses[adapter_instance_id]
            if not ingress.receiving_callbacks:
                continue
            try:
                await ingress.stop_receiving_callbacks()
            except BaseException as exc:
                failures.append(exc)
                continue
            if ingress.receiving_callbacks:
                failures.append(
                    ActorV2CoreIngressParticipantLifecycleError(
                        "adapter callback ingress did not stop for " + adapter_instance_id
                    )
                )
        if failures:
            raise failures[0]

    async def _stop_heartbeat(self) -> None:
        """Stop advisory heartbeat writes before committing terminal retirement."""

        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._heartbeat_health.stop()

    async def _shutdown_drain_service(self) -> None:
        """Stop local request discovery only after no active grant remains."""

        service = self._service
        if service is None:
            return
        await service.shutdown()
        if service.health_snapshot().status is not RuntimeServiceStatus.STOPPED:
            raise ActorV2CoreIngressParticipantLifecycleError(
                "core ingress drain service did not prove shutdown completion"
            )

    def _receiving_callback_adapter_ids(self) -> tuple[str, ...]:
        """Return callback sources that currently claim admission authority."""

        return tuple(
            adapter_instance_id
            for adapter_instance_id, ingress in self._callback_ingresses.items()
            if ingress.receiving_callbacks
        )

    def _drain_service_health(self) -> RuntimeServiceHealthSnapshot:
        """Return stopped diagnostics until the local service is constructed."""

        service = self._service
        if service is None:
            return RuntimeServiceHealthSnapshot(
                service_name="durable_core_ingress_drain",
                status=RuntimeServiceStatus.STOPPED,
            )
        return service.health_snapshot()

    def _drain_service_running(self) -> bool:
        """Return whether the local request-delivery loop has not stopped."""

        return self._service is not None and (
            self._service.health_snapshot().status is not RuntimeServiceStatus.STOPPED
        )

    def _components_active(
        self,
        drain_service_health: RuntimeServiceHealthSnapshot,
        receiving_callback_adapter_ids: tuple[str, ...],
    ) -> bool:
        """Return whether all mutable local authorities remain aligned."""

        return (
            not self._members_retired
            and self._domains_match()
            and receiving_callback_adapter_ids == self.adapter_instance_ids
            and drain_service_health.status is not RuntimeServiceStatus.STOPPED
        )

    def _domains_match(self) -> bool:
        """Return whether both repositories retain the bound durable domain."""

        return (
            self._membership_repository.persistence_domain is self._persistence_domain
            and self._core_drain_repository.persistence_domain is self._persistence_domain
        )


def _require_membership_repository(repository: object) -> None:
    """Validate the narrow membership lifecycle port before composition."""

    required_methods = (
        "register_participants",
        "heartbeat_participants",
        "retire_participants",
    )
    if not hasattr(repository, "persistence_domain") or any(
        not callable(getattr(repository, method_name, None)) for method_name in required_methods
    ):
        raise TypeError("membership_repository must implement ingress membership lifecycle calls")


def _require_core_drain_repository(repository: object) -> None:
    """Validate the worker-plus-discovery port before composition."""

    required_methods = ("get", "acknowledge_quiescent", "discover_open_for_participant")
    if not hasattr(repository, "persistence_domain") or any(
        not callable(getattr(repository, method_name, None)) for method_name in required_methods
    ):
        raise TypeError("core_drain_repository must implement core drain worker and discovery")


def _require_legacy_drain(legacy_drain: object) -> None:
    """Validate the process-local legacy drain port before it can be frozen."""

    if not callable(getattr(legacy_drain, "freeze", None)) or not callable(
        getattr(legacy_drain, "drain", None)
    ):
        raise TypeError("legacy_drain must implement freeze and drain")


def _callback_ingresses(
    values: Mapping[str, ActorV2CoreIngressAdapterLifecyclePort],
) -> dict[str, ActorV2CoreIngressAdapterLifecyclePort]:
    """Normalize one complete inactive callback-ingress scope."""

    if not isinstance(values, Mapping) or not values:
        raise ValueError("callback_ingresses must be a non-empty mapping")
    normalized: dict[str, ActorV2CoreIngressAdapterLifecyclePort] = {}
    for raw_adapter_instance_id, ingress in values.items():
        adapter_instance_id = _identifier(raw_adapter_instance_id, "adapter_instance_id")
        if adapter_instance_id in normalized:
            raise ValueError("callback_ingresses cannot repeat an adapter")
        if (
            not hasattr(ingress, "adapter_instance_id")
            or not hasattr(ingress, "receiving_callbacks")
            or not callable(getattr(ingress, "start_receiving_callbacks", None))
            or not callable(getattr(ingress, "stop_receiving_callbacks", None))
        ):
            raise TypeError("callback ingress must implement the adapter lifecycle port")
        if _identifier(ingress.adapter_instance_id, "adapter_instance_id") != adapter_instance_id:
            raise ValueError("callback ingress mapping key differs from adapter identity")
        if not isinstance(ingress.receiving_callbacks, bool):
            raise TypeError("callback ingress receiving_callbacks must be a bool")
        normalized[adapter_instance_id] = ingress
    return dict(sorted(normalized.items()))


def _identifier(value: object, field_name: str) -> str:
    """Normalize one opaque non-empty lifecycle identity."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_int(value: object, field_name: str) -> int:
    """Require one positive non-boolean process-incarnation epoch."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _positive_finite(value: object, field_name: str) -> float:
    """Require one finite positive service interval."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and positive")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized


def _optional_timeout(value: object, field_name: str) -> float | None:
    """Normalize one optional finite non-negative local drain budget."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _adapter_instance_ids(values: object) -> tuple[str, ...]:
    """Return a canonical non-empty unique adapter identity set."""

    if isinstance(values, str):
        raise TypeError("adapter_instance_ids must be iterable, not a string")
    try:
        normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    except TypeError as exc:
        raise TypeError("adapter_instance_ids must be iterable") from exc
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("adapter_instance_ids must be a non-empty unique set")
    return tuple(sorted(normalized))


def _optional_adapter_instance_ids(
    values: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    """Return a canonical possibly-empty adapter identity subset."""

    if isinstance(values, str):
        raise TypeError(f"{field_name} must be iterable, not a string")
    try:
        normalized = tuple(_identifier(value, "adapter_instance_id") for value in values)
    except TypeError as exc:
        raise TypeError(f"{field_name} must be iterable") from exc
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{field_name} cannot repeat an adapter")
    return tuple(sorted(normalized))


def _error_text(error: BaseException) -> str:
    """Return bounded operator-safe lifecycle error text."""

    return (str(error).strip() or type(error).__name__)[:500]


__all__ = [
    "ActorV2CoreIngressAdapterLifecyclePort",
    "ActorV2CoreIngressDrainLifecycleRepositoryPort",
    "ActorV2CoreIngressParticipantLifecycle",
    "ActorV2CoreIngressParticipantLifecycleError",
    "ActorV2CoreIngressParticipantLifecycleSnapshot",
    "ActorV2CoreIngressParticipantLifecycleState",
    "ActorV2CoreIngressParticipantRetirementBlocked",
    "ActorV2IngressMembershipLifecyclePort",
]
