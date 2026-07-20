"""Dormant single-request target for exact fenced mailbox handoffs.

This module deliberately has no runtime registration, dispatcher binding,
timer, ingress hook, or ownership cutover. A caller must explicitly acquire a
target lease, compose the matching actor/effect components, optionally recover
native history, activate this target, and only then bind it to a handoff
dispatcher.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from enum import StrEnum

from shinbot.agent.runtime.session_actor.effect_executor import (
    DurableEffectExecutor,
    EffectExpiryRecoveryResult,
    FencedEffectExecutionLeaseLost,
    LocalEffectExecutorQuiescence,
    LocalOperationQuiescenceStatus,
)
from shinbot.agent.runtime.session_actor.execution_binding import (
    require_live_execution_binding_in_transaction,
)
from shinbot.agent.runtime.session_actor.fenced_registry import (
    FencedSessionActorRegistry,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceError
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
)
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffReceipt,
    MailboxHandoffTarget,
)
from shinbot.persistence.engine import DatabaseManager
from shinbot.persistence.repositories.actor_v2_mailbox_handoff import (
    MailboxHandoffError,
    MailboxHandoffLeaseLost,
)


class FencedMailboxHandoffTargetState(StrEnum):
    """Lifecycle state for one explicit target incarnation and owner request."""

    NEW = "new"
    ACTIVE = "active"
    UNPUBLISHED = "unpublished"
    BLOCKED = "blocked"
    STALE = "stale"
    STOPPED = "stopped"


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffTargetRetirement:
    """Result of one target retirement attempt without hiding a blocked stop."""

    state: FencedMailboxHandoffTargetState
    target_lease_released: bool
    quiescence: LocalEffectExecutorQuiescence | None = None
    error: str = ""

    def __post_init__(self) -> None:
        """Normalize terminal diagnostics while retaining typed state."""

        state = FencedMailboxHandoffTargetState(self.state)
        if self.target_lease_released and state is not FencedMailboxHandoffTargetState.STOPPED:
            raise ValueError("a released target lease requires stopped target state")
        if self.quiescence is not None and not isinstance(
            self.quiescence,
            LocalEffectExecutorQuiescence,
        ):
            raise TypeError("quiescence must be LocalEffectExecutorQuiescence or None")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "error", str(self.error or "").strip())


@dataclass(slots=True, frozen=True)
class FencedMailboxHandoffTargetHistoryRecovery:
    """One target-local recovery pass before its handoff dispatcher is bound."""

    actor_wake: FencedMailboxWakeReceipt
    effect_recovery: EffectExpiryRecoveryResult

    def __post_init__(self) -> None:
        """Require typed recovery results without exposing target capabilities."""

        if not isinstance(self.actor_wake, FencedMailboxWakeReceipt):
            raise TypeError("actor_wake must be a FencedMailboxWakeReceipt")
        if not isinstance(self.effect_recovery, EffectExpiryRecoveryResult):
            raise TypeError("effect_recovery must be an EffectExpiryRecoveryResult")


class FencedMailboxHandoffTarget:
    """Consume one exact fenced handoff only under one target-lease binding.

    The target intentionally owns one :class:`FencedMailboxWakeRequest`, not
    a session-key map. This keeps a future canary or cutover controller from
    widening an incarnation-bound handoff back to a reusable local registry.
    """

    def __init__(
        self,
        *,
        database: DatabaseManager,
        execution_binding: FencedActorExecutionBinding,
        actor_registry: FencedSessionActorRegistry,
        effect_executor: DurableEffectExecutor,
    ) -> None:
        """Compose an inactive target around already-acquired lease authority.

        Args:
            database: Durable domain shared by handoffs, target leases, actors,
                effect work, and execution witnesses.
            execution_binding: Exact owner request plus live target capability.
            actor_registry: Request-bound actor supervisor for this domain.
            effect_executor: Scoped effect executor carrying the same target
                authority and no broad recovery capability.
        """

        if not isinstance(database, DatabaseManager):
            raise TypeError("database must be a DatabaseManager")
        if not isinstance(execution_binding, FencedActorExecutionBinding):
            raise TypeError("execution_binding must be a FencedActorExecutionBinding")
        if not isinstance(actor_registry, FencedSessionActorRegistry):
            raise TypeError("actor_registry must be a FencedSessionActorRegistry")
        if not isinstance(effect_executor, DurableEffectExecutor):
            raise TypeError("effect_executor must be a DurableEffectExecutor")
        if actor_registry.persistence_domain is not database:
            raise ValueError("actor_registry must share the target persistence domain")
        if effect_executor.persistence_domain is not database:
            raise ValueError("effect_executor must share the target persistence domain")
        executor_binding = effect_executor.execution_binding
        if executor_binding is None or not executor_binding.has_same_authority(
            execution_binding
        ):
            raise ValueError("effect_executor must carry the same target lease authority")
        if actor_registry.effect_contract_authority is not effect_executor.effect_contract_authority:
            raise ValueError("actor_registry and effect_executor must share contract authority")

        self._database = database
        self._execution_binding = execution_binding
        self._actor_registry = actor_registry
        self._effect_executor = effect_executor
        self._state = FencedMailboxHandoffTargetState.NEW
        self._lifecycle_lock = asyncio.Lock()
        self._history_recovery: FencedMailboxHandoffTargetHistoryRecovery | None = None

    @property
    def state(self) -> FencedMailboxHandoffTargetState:
        """Return the current local target lifecycle state."""

        return self._state

    @property
    def execution_binding(self) -> FencedActorExecutionBinding:
        """Return the current exact owner and target-lease capability."""

        return self._execution_binding

    @property
    def target_identity(self) -> MailboxHandoffTarget:
        """Return the immutable dispatcher-facing target incarnation identity."""

        return self._execution_binding.target_lease.lease.target

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared by this target's actor components."""

        return self._database

    async def activate(self) -> None:
        """Start scoped effect workers before this target can accept a handoff."""

        async with self._lifecycle_lock:
            if self._state is FencedMailboxHandoffTargetState.ACTIVE:
                return
            if self._state is not FencedMailboxHandoffTargetState.NEW:
                raise RuntimeError("a retired or blocked handoff target cannot be activated")
            try:
                await self._effect_executor.start_fenced()
            except Exception:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                raise
            if not self._effect_executor.healthy:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                raise RuntimeError("fenced effect executor is unhealthy after target activation")
            self._state = FencedMailboxHandoffTargetState.ACTIVE

    async def recover_native_history(self) -> FencedMailboxHandoffTargetHistoryRecovery:
        """Recover this exact target's mailbox and effect history before start.

        The actor wake handles a previously accepted handoff whose actor died
        before completing the mailbox claim. Expiry maintenance then emits only
        exact fenced sidecars. It does not invoke a notifier or settle those
        sidecars; the caller must bind a handoff supervisor afterwards.
        """

        async with self._lifecycle_lock:
            if self._history_recovery is not None:
                return self._history_recovery
            if self._state is not FencedMailboxHandoffTargetState.NEW:
                raise RuntimeError("native history recovery requires a new handoff target")
            binding = self._execution_binding
            try:
                actor_wake = await self._actor_registry.wake_leased(binding)
                if (
                    actor_wake.request != binding.request
                    or actor_wake.disposition is not FencedMailboxWakeDisposition.ACCEPTED
                ):
                    self._require_live_execution_binding(binding)
                    self._state = FencedMailboxHandoffTargetState.BLOCKED
                    raise RuntimeError("fenced actor registry rejected native history recovery")
                effect_recovery = await self._effect_executor.recover_fenced_history()
            except (ActorV2AdmissionFenceError, AgentRuntimeOwnershipError):
                self._state = FencedMailboxHandoffTargetState.STALE
                raise
            except (FencedWakeTargetLeaseError, FencedEffectExecutionLeaseLost):
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                raise
            except Exception:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                raise
            result = FencedMailboxHandoffTargetHistoryRecovery(
                actor_wake=actor_wake,
                effect_recovery=effect_recovery,
            )
            self._history_recovery = result
            return result

    async def renew_target_lease(self, *, ttl_seconds: float) -> FencedActorExecutionBinding:
        """Renew this target's durable publication without changing its epoch.

        Renewal is caller-driven on purpose. This dormant target does not hide
        a timer or perform background publication work; a future controller
        must supervise expiry and retire on renewal failure.
        """

        normalized_ttl = _positive_finite(ttl_seconds, field_name="ttl_seconds")
        async with self._lifecycle_lock:
            if self._state not in {
                FencedMailboxHandoffTargetState.ACTIVE,
                FencedMailboxHandoffTargetState.UNPUBLISHED,
                FencedMailboxHandoffTargetState.BLOCKED,
            }:
                raise RuntimeError("only a live target may renew its lease")
            try:
                grant = self._database.actor_v2_fenced_wake_target_leases.renew(
                    self._execution_binding.target_lease,
                    ttl_seconds=normalized_ttl,
                )
            except (ActorV2AdmissionFenceError, AgentRuntimeOwnershipError):
                self._state = FencedMailboxHandoffTargetState.STALE
                raise
            except FencedWakeTargetLeaseError:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                raise
            self._execution_binding = FencedActorExecutionBinding(
                request=self._execution_binding.request,
                target_lease=grant,
            )
            return self._execution_binding

    async def wake_handoff(
        self,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        """Validate and wake one complete durable handoff claim.

        The acceptance transaction verifies both the exact sidecar claim and
        the exact target lease. The actor registry repeats the lease check at
        its own persistence boundary before it can create or wake an actor.
        """

        if not isinstance(claim, FencedMailboxHandoffClaim):
            raise TypeError("claim must be a FencedMailboxHandoffClaim")
        async with self._lifecycle_lock:
            binding = self._execution_binding
            if claim.target != self.target_identity or claim.request != binding.request:
                return self._deferred_receipt(claim)
            if self._state is FencedMailboxHandoffTargetState.STALE:
                return self._stale_receipt(claim)
            if self._state is not FencedMailboxHandoffTargetState.ACTIVE:
                return self._deferred_receipt(claim)
            if not self._effect_executor.healthy:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                return self._deferred_receipt(claim)
            try:
                with self._database.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    self._database.actor_v2_mailbox_handoffs.require_live_fenced_claim_in_transaction(
                        conn,
                        claim,
                    )
                    require_live_execution_binding_in_transaction(
                        self._database,
                        conn,
                        binding,
                        key=claim.request.key,
                        ownership_generation=claim.request.ownership_generation,
                    )
            except MailboxHandoffLeaseLost:
                return self._deferred_receipt(claim)
            except (
                ActorV2AdmissionFenceError,
                AgentRuntimeOwnershipError,
            ):
                self._state = FencedMailboxHandoffTargetState.STALE
                return self._stale_receipt(claim)
            except FencedWakeTargetLeaseError:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                return self._deferred_receipt(claim)
            except MailboxHandoffError:
                raise

            wake_receipt = await self._actor_registry.wake_leased(binding)
            if wake_receipt.disposition is FencedMailboxWakeDisposition.ACCEPTED:
                # Workers poll their scoped outbox, but this removes an avoidable
                # wait when the actor's just-accepted mailbox event emits work.
                self._effect_executor.wake()
            else:
                # The lower-level registry intentionally folds a local close,
                # owner loss, and target-lease loss into STALE. Re-prove the
                # binding here because only owner/admission-fence loss makes
                # this exact handoff terminal; all local target failures must
                # preserve work for an explicit replacement incarnation.
                try:
                    with self._database.connect() as conn:
                        conn.execute("BEGIN IMMEDIATE")
                        require_live_execution_binding_in_transaction(
                            self._database,
                            conn,
                            binding,
                            key=claim.request.key,
                            ownership_generation=claim.request.ownership_generation,
                        )
                except (ActorV2AdmissionFenceError, AgentRuntimeOwnershipError):
                    self._state = FencedMailboxHandoffTargetState.STALE
                    return self._stale_receipt(claim)
                except FencedWakeTargetLeaseError:
                    self._state = FencedMailboxHandoffTargetState.BLOCKED
                    return self._deferred_receipt(claim)
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                return self._deferred_receipt(claim)
            return FencedMailboxHandoffReceipt(claim=claim, wake_receipt=wake_receipt)

    async def unpublish(self) -> None:
        """Stop accepting dispatcher claims before retiring local components.

        The caller must first remove this same target identity from the
        dispatcher. Calling this method before dispatcher unbind is still
        fail-closed: any later in-flight dispatcher call receives a deferred
        receipt, so the dispatcher retains or releases its exact durable claim
        for an explicit later redrive instead of settling it.
        """

        async with self._lifecycle_lock:
            if self._state is FencedMailboxHandoffTargetState.NEW:
                # A composed but never-activated target still owns a durable
                # publication lease. It must cross the same unpublish boundary
                # before retirement can release that lease safely.
                self._state = FencedMailboxHandoffTargetState.UNPUBLISHED
                return
            if self._state is FencedMailboxHandoffTargetState.ACTIVE:
                self._state = FencedMailboxHandoffTargetState.UNPUBLISHED
                return
            if self._state is FencedMailboxHandoffTargetState.UNPUBLISHED:
                return
            if self._state in {
                FencedMailboxHandoffTargetState.BLOCKED,
                FencedMailboxHandoffTargetState.STALE,
            }:
                # These states already reject every wake. Treat this as the
                # same publication boundary so a supervisor can always retain
                # the strict unbind -> unpublish -> retire shutdown order.
                return
            raise RuntimeError("only a live handoff target may be unpublished")

    async def retire(
        self,
        *,
        quiescence_timeout_seconds: float | None = None,
    ) -> FencedMailboxHandoffTargetRetirement:
        """Stop local work and release the target lease only after quiescence.

        A timeout or release failure leaves the target in ``BLOCKED`` and
        retains durable lease state. It never fabricates a successful stop that
        could let another incarnation assume no old local task remains.
        """

        timeout = _optional_nonnegative_finite(
            quiescence_timeout_seconds,
            field_name="quiescence_timeout_seconds",
        )
        async with self._lifecycle_lock:
            if self._state is FencedMailboxHandoffTargetState.STOPPED:
                return FencedMailboxHandoffTargetRetirement(
                    state=self._state,
                    target_lease_released=True,
                )
            if self._state not in {
                FencedMailboxHandoffTargetState.UNPUBLISHED,
                FencedMailboxHandoffTargetState.BLOCKED,
                FencedMailboxHandoffTargetState.STALE,
            }:
                raise RuntimeError("a handoff target must be unpublished before retirement")

            try:
                await self._actor_registry.shutdown(drain=False)
                await self._effect_executor.shutdown(drain=False)
                quiescence = await self._effect_executor.ensure_local_executor_quiescent(
                    cancel=True,
                    timeout_seconds=timeout,
                )
            except asyncio.CancelledError:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                raise
            except Exception as exc:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                return FencedMailboxHandoffTargetRetirement(
                    state=self._state,
                    target_lease_released=False,
                    error=_error_text(exc),
                )
            if quiescence.status is LocalOperationQuiescenceStatus.TIMED_OUT:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                return FencedMailboxHandoffTargetRetirement(
                    state=self._state,
                    target_lease_released=False,
                    quiescence=quiescence,
                    error="local effect handler quiescence timed out",
                )
            try:
                self._database.actor_v2_fenced_wake_target_leases.release(
                    self._execution_binding.target_lease,
                )
            except FencedWakeTargetLeaseError as exc:
                self._state = FencedMailboxHandoffTargetState.BLOCKED
                return FencedMailboxHandoffTargetRetirement(
                    state=self._state,
                    target_lease_released=False,
                    quiescence=quiescence,
                    error=_error_text(exc),
                )
            self._state = FencedMailboxHandoffTargetState.STOPPED
            return FencedMailboxHandoffTargetRetirement(
                state=self._state,
                target_lease_released=True,
                quiescence=quiescence,
            )

    def _require_live_execution_binding(
        self,
        binding: FencedActorExecutionBinding,
    ) -> None:
        """Re-prove target authority after a lower-level stale result."""

        with self._database.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            require_live_execution_binding_in_transaction(
                self._database,
                conn,
                binding,
                key=binding.request.key,
                ownership_generation=binding.request.ownership_generation,
            )

    @staticmethod
    def _stale_receipt(
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        """Return a typed stale receipt without weakening claim identity."""

        return FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=claim.request,
                disposition=FencedMailboxWakeDisposition.STALE,
            ),
        )

    @staticmethod
    def _deferred_receipt(
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        """Return a retryable receipt without weakening claim identity."""

        return FencedMailboxHandoffReceipt(
            claim=claim,
            wake_receipt=FencedMailboxWakeReceipt(
                request=claim.request,
                disposition=FencedMailboxWakeDisposition.DEFERRED,
            ),
        )


def _positive_finite(value: object, *, field_name: str) -> float:
    """Normalize one finite positive lifecycle duration."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and positive")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized


def _optional_nonnegative_finite(value: object, *, field_name: str) -> float | None:
    """Normalize an optional finite local wait bound."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and non-negative")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and non-negative")
    return normalized


def _error_text(exc: BaseException) -> str:
    """Return a bounded lifecycle diagnostic without retaining exception state."""

    return (str(exc).strip() or type(exc).__name__)[:500]


__all__ = [
    "FencedMailboxHandoffTarget",
    "FencedMailboxHandoffTargetHistoryRecovery",
    "FencedMailboxHandoffTargetRetirement",
    "FencedMailboxHandoffTargetState",
]
