"""Single-writer actor for one profile-scoped Agent session mailbox."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from inspect import isawaitable
from typing import Protocol, runtime_checkable

from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    EventEnqueueResult,
    SessionEventEnvelope,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryDeliveryClaimLost,
)
from shinbot.agent.runtime.session_actor.transition_validation import (
    validate_session_transition,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceError
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipError
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.legacy_recovery_gate import LegacyRecoveryPermit

logger = logging.getLogger(__name__)

SessionEventHandler = Callable[
    [AgentSessionAggregate, SessionEventEnvelope],
    SessionTransition,
]


class SessionActorStore(Protocol):
    """Durable mailbox and aggregate operations required by a session actor."""

    @property
    def persistence_domain(self) -> object:
        """Return the stable identity of the backing transaction domain."""

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        """Return the sealed effect authority shared with durable execution."""

    async def enqueue(self, envelope: SessionEventEnvelope) -> EventEnqueueResult:
        """Idempotently persist an event before its actor is awakened."""

    async def ensure(
        self,
        key: SessionKey,
        *,
        ownership_generation: int | None = None,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> AgentSessionAggregate:
        """Ensure and return the durable aggregate for a session."""

    async def load(
        self,
        key: SessionKey,
        *,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> AgentSessionAggregate:
        """Load the latest durable aggregate for a session."""

    async def claim_next(
        self,
        key: SessionKey,
        *,
        worker_id: str,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> ClaimedSessionEvent | None:
        """Atomically lease the next available event for a session."""

    async def commit(
        self,
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
        *,
        expected_revision: int,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> AgentSessionAggregate:
        """Atomically commit a transition and complete its claimed event."""

    async def release(
        self,
        claim: ClaimedSessionEvent,
        *,
        error: str,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> None:
        """Release a failed claim back to the durable pending queue."""

    async def fail(
        self,
        claim: ClaimedSessionEvent,
        *,
        error: str,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> None:
        """Move a claimed poison event into a terminal failed state."""

    async def recover(
        self,
        key: SessionKey,
        *,
        worker_id: str,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> int:
        """Release expired claims that may be retried by this actor."""

    async def pending_keys(self) -> list[SessionKey]:
        """Return session keys with recoverable pending mailbox events."""

    async def has_pending_for_key(
        self,
        key: SessionKey,
        *,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> bool:
        """Return whether one exact actor key still has mailbox work."""


@runtime_checkable
class _LegacyRecoveryActorStore(Protocol):
    """Store boundaries required before a lifecycle-owned recovery actor starts."""

    async def ensure_for_legacy_recovery(
        self,
        key: SessionKey,
        *,
        permit: LegacyRecoveryPermit,
    ) -> AgentSessionAggregate:
        """Ensure one aggregate while the owning lifecycle retains ``permit``."""

    async def recover_for_legacy_recovery(
        self,
        key: SessionKey,
        *,
        worker_id: str,
        permit: LegacyRecoveryPermit,
    ) -> int:
        """Release stale actor leases while the owning lifecycle retains ``permit``."""


class AgentSessionActor:
    """Drain one durable session mailbox through a single event handler."""

    def __init__(
        self,
        *,
        key: SessionKey,
        store: SessionActorStore,
        handler: SessionEventHandler,
        worker_id: str | None = None,
        retry_delay_seconds: float = 1.0,
        max_attempts: int = 5,
        ownership_binding: FencedMailboxWakeRequest | None = None,
        execution_binding: FencedActorExecutionBinding | None = None,
    ) -> None:
        """Initialize an actor without starting its background task.

        Args:
            key: Profile-scoped session identity owned by this actor.
            store: Durable mailbox and aggregate store.
            handler: Pure orchestration handler returning a declarative transition.
            worker_id: Optional durable lease owner identifier.
            retry_delay_seconds: Delay before retrying a released failed event.
            max_attempts: Infrastructure attempts before an event is failed.
            ownership_binding: Optional immutable owner incarnation. A bound
                actor proves this exact generation and admission fence at each
                persistence boundary instead of widening to a SessionKey wake.
            execution_binding: Optional target-lease capability paired with
                ``ownership_binding``. When present, every actor persistence
                operation additionally proves that this target incarnation is
                still the live consumer for the same owner.
        """

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if ownership_binding is not None:
            if not isinstance(ownership_binding, FencedMailboxWakeRequest):
                raise TypeError("ownership_binding must be a FencedMailboxWakeRequest")
            if ownership_binding.key != key:
                raise ValueError("ownership_binding key does not match actor key")
        if execution_binding is not None:
            if not isinstance(execution_binding, FencedActorExecutionBinding):
                raise TypeError("execution_binding must be a FencedActorExecutionBinding")
            if execution_binding.request.key != key:
                raise ValueError("execution_binding key does not match actor key")
            if ownership_binding is None:
                ownership_binding = execution_binding.request
            elif ownership_binding != execution_binding.request:
                raise ValueError("execution_binding request does not match ownership_binding")
        self.key = key
        self._store = store
        self._handler = handler
        self._effect_contract_authority = store.effect_contract_authority
        if not isinstance(self._effect_contract_authority, EffectContractAuthority):
            raise TypeError(
                "session actor store must expose an EffectContractAuthority"
            )
        if not self._effect_contract_authority.sealed:
            raise TypeError("session actor effect contract authority must be sealed")
        self.worker_id = str(worker_id or f"session-actor:{uuid.uuid4().hex}")
        self._retry_delay_seconds = max(0.0, float(retry_delay_seconds))
        self._max_attempts = max_attempts
        self._ownership_binding = ownership_binding
        self._execution_binding = execution_binding
        self._wake_event = asyncio.Event()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._stopped_event = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._retry_handle: asyncio.TimerHandle | None = None
        self._current_claim: ClaimedSessionEvent | None = None
        self._closing = False
        self._drain_on_shutdown = True
        self._started = False
        self._last_error: str | None = None

    @property
    def started(self) -> bool:
        """Return whether the actor background task has been started."""

        return self._started

    @property
    def closed(self) -> bool:
        """Return whether actor shutdown has been requested."""

        return self._closing

    @property
    def last_error(self) -> str | None:
        """Return the latest mailbox processing error, if any."""

        return self._last_error

    @property
    def ownership_binding(self) -> FencedMailboxWakeRequest | None:
        """Return the immutable owner incarnation, if this actor is bound."""

        return self._ownership_binding

    @property
    def execution_binding(self) -> FencedActorExecutionBinding | None:
        """Return the optional target-lease capability bound to this actor."""

        return self._execution_binding

    async def start(self) -> None:
        """Ensure durable state, recover stale claims, and start draining."""

        await self._start(legacy_recovery_permit=None)

    async def _start_for_legacy_recovery(
        self,
        permit: LegacyRecoveryPermit,
    ) -> None:
        """Start only under the registry's permit-owning recovery lifecycle."""

        if not isinstance(permit, LegacyRecoveryPermit):
            raise TypeError("permit must be a LegacyRecoveryPermit")
        await self._start(legacy_recovery_permit=permit)

    async def _start(
        self,
        *,
        legacy_recovery_permit: LegacyRecoveryPermit | None,
    ) -> None:
        """Create the drain task after one selected startup authority path."""

        async with self._start_lock:
            if self._started:
                return
            if self._closing:
                raise RuntimeError("a closed session actor cannot be restarted")
            ownership_binding = self._ownership_binding
            if legacy_recovery_permit is not None and ownership_binding is not None:
                raise RuntimeError(
                    "a legacy recovery actor cannot also bind a fenced ownership incarnation"
                )
            if legacy_recovery_permit is None and ownership_binding is None:
                await self._store.ensure(self.key)
                await self._store.recover(self.key, worker_id=self.worker_id)
            elif ownership_binding is not None:
                await self._store.ensure(
                    self.key,
                    ownership_generation=ownership_binding.ownership_generation,
                    ownership_binding=ownership_binding,
                    execution_binding=self._execution_binding,
                )
                await self._store.recover(
                    self.key,
                    worker_id=self.worker_id,
                    ownership_binding=ownership_binding,
                    execution_binding=self._execution_binding,
                )
            else:
                store = self._store
                if not isinstance(store, _LegacyRecoveryActorStore):
                    raise TypeError(
                        "session actor store does not support lifecycle-owned legacy recovery"
                    )
                await store.ensure_for_legacy_recovery(
                    self.key,
                    permit=legacy_recovery_permit,
                )
                await store.recover_for_legacy_recovery(
                    self.key,
                    worker_id=self.worker_id,
                    permit=legacy_recovery_permit,
                )
            self._task = asyncio.create_task(
                self._run(),
                name=(
                    "agent-session-actor:"
                    f"{self.key.profile_id or 'default'}:{self.key.session_id}"
                ),
            )
            self._started = True
            self.wake()

    def wake(self) -> None:
        """Notify the actor that durable mailbox work may be available."""

        if self._closing and not self._drain_on_shutdown:
            return
        self._idle_event.clear()
        self._wake_event.set()

    async def wait_idle(self) -> None:
        """Wait until the durable mailbox has been observed empty."""

        await self._idle_event.wait()

    async def shutdown(self, *, drain: bool = True) -> None:
        """Stop the actor, optionally draining all currently durable events.

        A failed event is released before shutdown and remains durable for the
        next recovery pass; graceful shutdown does not retry failures forever.

        Args:
            drain: Whether to finish the current event and drain pending work.
        """

        if self._closing:
            await self._stopped_event.wait()
            return
        self._closing = True
        self._drain_on_shutdown = drain
        self._cancel_retry()
        task = self._task
        if task is None:
            self._started = False
            self._idle_event.set()
            self._stopped_event.set()
            return
        if drain:
            self.wake()
        else:
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        try:
            while True:
                await self._wake_event.wait()
                self._wake_event.clear()
                if self._closing and not self._drain_on_shutdown:
                    break
                drained = await self._drain_mailbox()
                if self._closing:
                    break
                if not drained:
                    self._schedule_retry()
        except asyncio.CancelledError:
            claim = self._current_claim
            if claim is not None:
                await self._release_after_cancellation(claim)
            raise
        finally:
            self._cancel_retry()
            self._current_claim = None
            self._started = False
            self._idle_event.set()
            self._stopped_event.set()

    async def _drain_mailbox(self) -> bool:
        while not (self._closing and not self._drain_on_shutdown):
            try:
                claim = await self._claim_next()
            except Exception as exc:
                if self._is_ownership_binding_lost(exc):
                    self._stop_for_binding_loss(exc, event_id="", phase="claim")
                    return True
                self._record_error(exc, event_id="", phase="claim")
                return False
            if claim is None:
                if self._wake_event.is_set():
                    continue
                try:
                    has_pending = await self._has_pending_for_key()
                except Exception as exc:
                    if self._is_ownership_binding_lost(exc):
                        self._stop_for_binding_loss(
                            exc,
                            event_id="",
                            phase="pending_check",
                        )
                        return True
                    self._record_error(exc, event_id="", phase="pending_check")
                    return False
                if has_pending:
                    return False
                self._idle_event.set()
                if self._wake_event.is_set():
                    self._idle_event.clear()
                    continue
                return True
            if claim.key != self.key:
                error = RuntimeError("store returned a claim for a different session actor")
                await self._release_failed_claim(claim, error, phase="claim_validation")
                return False

            self._current_claim = claim
            try:
                aggregate = await self._load_aggregate()
            except asyncio.CancelledError:
                await self._release_after_cancellation(claim)
                self._current_claim = None
                raise
            except Exception as exc:
                if self._is_ownership_binding_lost(exc):
                    self._stop_for_binding_loss(
                        exc,
                        event_id=claim.envelope.event_id,
                        phase="load",
                    )
                    self._current_claim = None
                    return True
                terminal = await self._retry_or_fail_claim(
                    claim,
                    exc,
                    phase="load",
                )
                self._current_claim = None
                if terminal:
                    continue
                return False

            try:
                transition = self._handler(aggregate, claim.envelope)
                if isawaitable(transition):
                    close = getattr(transition, "close", None)
                    if callable(close):
                        close()
                    raise TypeError(
                        "session event handlers must be synchronous and return "
                        "declarative SessionTransition values"
                    )
                self._validate_transition(aggregate, transition)
            except Exception as exc:
                terminal = await self._fail_claim(claim, exc, phase="reduce")
                self._current_claim = None
                if terminal:
                    continue
                return False

            try:
                await self._commit_claim(
                    claim,
                    transition,
                    expected_revision=aggregate.state_revision,
                )
            except asyncio.CancelledError:
                await self._release_after_cancellation(claim)
                self._current_claim = None
                raise
            except RecoveryDeliveryClaimLost as exc:
                self._record_claim_lost(claim, exc)
                self._current_claim = None
                return False
            except Exception as exc:
                if self._is_ownership_binding_lost(exc):
                    self._stop_for_binding_loss(
                        exc,
                        event_id=claim.envelope.event_id,
                        phase="commit",
                    )
                    self._current_claim = None
                    return True
                terminal = await self._retry_or_fail_claim(
                    claim,
                    exc,
                    phase="commit",
                )
                self._current_claim = None
                if terminal:
                    continue
                return False
            self._current_claim = None
            self._last_error = None
        return True

    def _validate_transition(
        self,
        current: AgentSessionAggregate,
        transition: SessionTransition,
    ) -> None:
        validate_session_transition(
            current,
            transition,
            effect_contract_authority=self._effect_contract_authority,
        )

    async def _claim_next(self) -> ClaimedSessionEvent | None:
        """Claim work while preserving an optional immutable owner binding."""

        ownership_binding = self._ownership_binding
        if ownership_binding is None:
            return await self._store.claim_next(self.key, worker_id=self.worker_id)
        return await self._store.claim_next(
            self.key,
            worker_id=self.worker_id,
            ownership_binding=ownership_binding,
            execution_binding=self._execution_binding,
        )

    async def _has_pending_for_key(self) -> bool:
        """Check pending work without widening a bound actor to another owner."""

        ownership_binding = self._ownership_binding
        if ownership_binding is None:
            return await self._store.has_pending_for_key(self.key)
        return await self._store.has_pending_for_key(
            self.key,
            ownership_binding=ownership_binding,
            execution_binding=self._execution_binding,
        )

    async def _load_aggregate(self) -> AgentSessionAggregate:
        """Load state only while the actor's bound incarnation remains active."""

        ownership_binding = self._ownership_binding
        if ownership_binding is None:
            return await self._store.load(self.key)
        return await self._store.load(
            self.key,
            ownership_binding=ownership_binding,
            execution_binding=self._execution_binding,
        )

    async def _commit_claim(
        self,
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
        *,
        expected_revision: int,
    ) -> AgentSessionAggregate:
        """Commit one claim without dropping its optional ownership binding."""

        ownership_binding = self._ownership_binding
        if ownership_binding is None:
            return await self._store.commit(
                claim,
                transition,
                expected_revision=expected_revision,
            )
        return await self._store.commit(
            claim,
            transition,
            expected_revision=expected_revision,
            ownership_binding=ownership_binding,
            execution_binding=self._execution_binding,
        )

    async def _release_claim(self, claim: ClaimedSessionEvent, *, error: str) -> None:
        """Release a claim without widening a bound actor to another owner."""

        ownership_binding = self._ownership_binding
        if ownership_binding is None:
            await self._store.release(claim, error=error)
            return
        await self._store.release(
            claim,
            error=error,
            ownership_binding=ownership_binding,
            execution_binding=self._execution_binding,
        )

    async def _fail_claim_to_store(self, claim: ClaimedSessionEvent, *, error: str) -> None:
        """Dead-letter a claim only while its immutable owner remains active."""

        ownership_binding = self._ownership_binding
        if ownership_binding is None:
            await self._store.fail(claim, error=error)
            return
        await self._store.fail(
            claim,
            error=error,
            ownership_binding=ownership_binding,
            execution_binding=self._execution_binding,
        )

    async def _release_failed_claim(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> None:
        if self._preserve_unproven_typed_recovery(claim, exc, phase=phase):
            return
        self._record_error(exc, event_id=claim.envelope.event_id, phase=phase)
        try:
            await self._release_claim(claim, error=self._last_error or type(exc).__name__)
        except Exception as release_exc:
            if self._is_ownership_binding_lost(release_exc):
                self._stop_for_binding_loss(
                    release_exc,
                    event_id=claim.envelope.event_id,
                    phase="release",
                )
                return
            self._record_error(
                release_exc,
                event_id=claim.envelope.event_id,
                phase="release",
            )

    async def _retry_or_fail_claim(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> bool:
        if self._preserve_unproven_typed_recovery(claim, exc, phase=phase):
            return False
        if claim.attempt_count >= self._max_attempts:
            return await self._fail_claim(claim, exc, phase=f"{phase}_exhausted")
        await self._release_failed_claim(claim, exc, phase=phase)
        return False

    async def _fail_claim(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> bool:
        if self._preserve_unproven_typed_recovery(claim, exc, phase=phase):
            return False
        self._record_error(exc, event_id=claim.envelope.event_id, phase=phase)
        try:
            await self._fail_claim_to_store(
                claim,
                error=self._last_error or type(exc).__name__,
            )
        except Exception as fail_exc:
            if self._is_ownership_binding_lost(fail_exc):
                self._stop_for_binding_loss(
                    fail_exc,
                    event_id=claim.envelope.event_id,
                    phase="dead_letter",
                )
                return False
            self._record_error(
                fail_exc,
                event_id=claim.envelope.event_id,
                phase="dead_letter",
            )
            try:
                await self._release_claim(
                    claim,
                    error=self._last_error or type(fail_exc).__name__,
                )
            except Exception as release_exc:
                if self._is_ownership_binding_lost(release_exc):
                    self._stop_for_binding_loss(
                        release_exc,
                        event_id=claim.envelope.event_id,
                        phase="release_after_dead_letter_failure",
                    )
                    return False
                self._record_error(
                    release_exc,
                    event_id=claim.envelope.event_id,
                    phase="release_after_dead_letter_failure",
                )
            return False
        return True

    async def _release_after_cancellation(self, claim: ClaimedSessionEvent) -> None:
        release_task = asyncio.create_task(
            self._release_claim(claim, error="actor_cancelled"),
            name=f"agent-session-actor-release:{claim.envelope.event_id}",
        )
        try:
            await asyncio.shield(release_task)
        except asyncio.CancelledError:
            await release_task
        except Exception as exc:
            if self._is_ownership_binding_lost(exc):
                self._stop_for_binding_loss(
                    exc,
                    event_id=claim.envelope.event_id,
                    phase="release_after_cancel",
                )
                return
            self._record_error(
                exc,
                event_id=claim.envelope.event_id,
                phase="release_after_cancel",
            )

    def _record_error(self, exc: BaseException, *, event_id: str, phase: str) -> None:
        self._last_error = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "Agent session actor processing failed",
            extra={
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "event_id": event_id,
                "phase": phase,
            },
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    def _record_claim_lost(
        self,
        claim: ClaimedSessionEvent,
        exc: RecoveryDeliveryClaimLost,
    ) -> None:
        """Record a lost recovery claim without mutating another owner's work."""

        self._last_error = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "Agent session actor lost a typed recovery claim",
            extra={
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "event_id": claim.envelope.event_id,
                "phase": "commit_claim_lost",
                "recovery_claim_code": exc.code,
            },
        )

    def _is_ownership_binding_lost(self, exc: BaseException) -> bool:
        """Return whether a bound actor lost owner or target authority."""

        return (
            self._ownership_binding is not None
            and isinstance(
                exc,
                (ActorV2AdmissionFenceError, AgentRuntimeOwnershipError),
            )
        ) or (
            self._execution_binding is not None
            and isinstance(exc, FencedWakeTargetLeaseError)
        )

    def _stop_for_binding_loss(
        self,
        exc: BaseException,
        *,
        event_id: str,
        phase: str,
    ) -> None:
        """Stop a bound actor instead of retrying under a changed owner key."""

        self._record_error(exc, event_id=event_id, phase=f"ownership_binding_{phase}")
        self._closing = True
        self._drain_on_shutdown = False
        self._cancel_retry()

    def _preserve_unproven_typed_recovery(
        self,
        claim: ClaimedSessionEvent,
        exc: BaseException,
        *,
        phase: str,
    ) -> bool:
        """Keep a typed delivery out of generic retry and dead-letter handling.

        A scanner-owned recovery event may only change durable state through the
        coordinator's raw-proof transaction. Before that proof completes, an
        actor cannot safely release, fail, or advance it using ordinary mailbox
        semantics. A future scanner pass can record the raw finding or issue a
        proven blocker; this actor leaves the held claim unchanged.
        """

        envelope = claim.envelope
        if (
            envelope.kind != RECOVERY_DELIVERY_EVENT_KIND
            or envelope.source != RECOVERY_DELIVERY_EVENT_SOURCE
        ):
            return False
        self._last_error = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "Agent session actor preserved an unproven typed recovery delivery",
            extra={
                "profile_id": self.key.profile_id,
                "session_id": self.key.session_id,
                "event_id": envelope.event_id,
                "phase": f"typed_recovery_{phase}",
                "recovery_error_type": type(exc).__name__,
            },
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return True

    def _schedule_retry(self) -> None:
        if self._closing or self._retry_handle is not None:
            return
        loop = asyncio.get_running_loop()

        def _wake_for_retry() -> None:
            self._retry_handle = None
            self.wake()

        self._retry_handle = loop.call_later(
            self._retry_delay_seconds,
            _wake_for_retry,
        )

    def _cancel_retry(self) -> None:
        handle = self._retry_handle
        self._retry_handle = None
        if handle is not None:
            handle.cancel()


__all__ = [
    "AgentSessionActor",
    "SessionActorStore",
    "SessionEventHandler",
]
