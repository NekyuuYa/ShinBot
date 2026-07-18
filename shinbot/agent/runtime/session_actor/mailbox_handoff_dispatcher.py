"""Dormant pull-based dispatcher for exact Actor v2 mailbox handoffs.

The dispatcher is intentionally separate from the session actor registry.  It
claims immutable sidecar evidence, presents the complete lease-bound claim to
an explicitly bound target, and settles only an exact typed receipt.  It has
no startup hook, timer, registry fallback, or implicit target discovery.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import uuid
from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeRequest,
)
from shinbot.core.dispatch.mailbox_handoff import (
    FencedMailboxHandoffClaim,
    FencedMailboxHandoffPort,
    FencedMailboxHandoffReceipt,
    MailboxHandoffState,
    MailboxHandoffTarget,
)
from shinbot.persistence.repositories.actor_v2_mailbox_handoff import (
    MailboxHandoffDiscoveryCursor,
    MailboxHandoffDiscoveryPage,
    MailboxHandoffRecord,
)


class MailboxHandoffDispatchDisposition(StrEnum):
    """Result of one bounded exact handoff attempt."""

    ACCEPTED = "accepted"
    STALE = "stale"
    DEFERRED = "deferred"
    BUSY = "busy"
    NO_WORK = "no_work"
    FAILED = "failed"


@dataclass(slots=True, frozen=True)
class MailboxHandoffDispatchResult:
    """Describe one dispatcher attempt without projecting a session-level wake."""

    mailbox_id: int
    disposition: MailboxHandoffDispatchDisposition
    handoff_id: str = ""
    error: str = ""


@dataclass(slots=True, frozen=True)
class MailboxHandoffDispatchPage:
    """Results from one caller-driven keyset discovery and dispatch pass."""

    results: tuple[MailboxHandoffDispatchResult, ...]
    next_cursor: MailboxHandoffDiscoveryCursor | None
    has_more: bool

    def __post_init__(self) -> None:
        """Keep the dispatch page typed and keyset-compatible."""

        if not isinstance(self.results, tuple):
            raise TypeError("results must be a tuple")
        if any(
            not isinstance(result, MailboxHandoffDispatchResult)
            for result in self.results
        ):
            raise TypeError("results must contain MailboxHandoffDispatchResult values")
        if self.next_cursor is not None and not isinstance(
            self.next_cursor,
            MailboxHandoffDiscoveryCursor,
        ):
            raise TypeError("next_cursor must be a MailboxHandoffDiscoveryCursor or None")
        if not isinstance(self.has_more, bool):
            raise TypeError("has_more must be a bool")
        if self.has_more and self.next_cursor is None:
            raise ValueError("a page with more results requires a next cursor")


class MailboxHandoffRepositoryPort(Protocol):
    """Persistence surface required by the dormant dispatcher."""

    def read(self, mailbox_id: int) -> MailboxHandoffRecord | None:
        """Read one sidecar and its mutable delivery state."""

    def claim_fenced_handoff(
        self,
        mailbox_id: int,
        *,
        worker_id: str,
        target: MailboxHandoffTarget,
    ) -> FencedMailboxHandoffClaim | None:
        """Claim one pending or expired fenced sidecar."""

    def settle_fenced_claim(
        self,
        receipt: FencedMailboxHandoffReceipt,
    ) -> MailboxHandoffRecord:
        """Persist a target-bound terminal receipt."""

    def defer_fenced_claim(
        self,
        receipt: FencedMailboxHandoffReceipt,
    ) -> MailboxHandoffRecord:
        """Release one exact target-deferred claim back to pending."""


class MailboxHandoffDiscoveryRepositoryPort(MailboxHandoffRepositoryPort, Protocol):
    """Optional pull-discovery capability used only by explicit caller passes."""

    def discover_fenced_pending(
        self,
        *,
        limit: int = 100,
        after: MailboxHandoffDiscoveryCursor | None = None,
        profile_id: str | None = None,
        session_id: str | None = None,
        expected_request: FencedMailboxWakeRequest | None = None,
    ) -> MailboxHandoffDiscoveryPage:
        """Return one immutable keyset page of pending fenced sidecars."""


@dataclass(slots=True, frozen=True)
class _TargetBinding:
    """One target port and its immutable incarnation identity."""

    port: FencedMailboxHandoffPort
    identity: MailboxHandoffTarget
    epoch: int


class DurableMailboxHandoffDispatcher:
    """Dispatch exact fenced handoffs without activating Actor v2.

    The dispatcher is pull-based and inert until a caller explicitly binds a
    target and invokes :meth:`dispatch`.  A target timeout deliberately leaves
    the durable claim leased instead of guessing that delivery did not happen;
    the bounded lease then makes the sidecar eligible for a later redrive.
    """

    def __init__(
        self,
        repository: MailboxHandoffRepositoryPort,
        *,
        worker_id: str | None = None,
        target_timeout_seconds: float = 30.0,
        max_inflight: int = 32,
        hint_capacity: int = 256,
    ) -> None:
        """Initialize an unbound dispatcher with bounded local state.

        Args:
            repository: Durable sidecar repository shared with mailbox producers.
            worker_id: Stable lease owner identity for this dispatcher process.
            target_timeout_seconds: Maximum time to await one target response.
            max_inflight: Maximum number of claims awaiting target receipts.
            hint_capacity: Maximum number of advisory mailbox ids retained locally.

        Raises:
            ValueError: If a bound or local capacity is invalid.
        """

        if not _is_repository_port(repository):
            raise TypeError("repository must implement MailboxHandoffRepositoryPort")
        timeout = float(target_timeout_seconds)
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("target_timeout_seconds must be finite and positive")
        if isinstance(max_inflight, bool) or max_inflight < 1:
            raise ValueError("max_inflight must be at least one")
        if isinstance(hint_capacity, bool) or hint_capacity < 1:
            raise ValueError("hint_capacity must be at least one")
        self._repository = repository
        self._worker_id = str(worker_id or f"mailbox-handoff-dispatcher:{uuid.uuid4().hex}").strip()
        if not self._worker_id:
            raise ValueError("worker_id must not be empty")
        self._target_timeout_seconds = timeout
        self._max_inflight = max_inflight
        self._hint_capacity = hint_capacity
        self._target_binding: _TargetBinding | None = None
        self._target_epoch = 0
        self._closed = False
        self._hints: OrderedDict[int, None] = OrderedDict()
        self._inflight: dict[
            tuple[int, str, int],
            asyncio.Task[FencedMailboxHandoffReceipt],
        ] = {}

    @property
    def closed(self) -> bool:
        """Return whether this dispatcher has been closed for new work."""

        return self._closed

    @property
    def target_bound(self) -> bool:
        """Return whether a target port is explicitly bound."""

        return self._target_binding is not None and not self._closed

    @property
    def bound_target_identity(self) -> MailboxHandoffTarget | None:
        """Return the current target incarnation without exposing its port."""

        binding = self._target_binding
        return binding.identity if binding is not None and not self._closed else None

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain exposed by the handoff repository."""

        return getattr(self._repository, "persistence_domain", self._repository)

    @property
    def target_timeout_seconds(self) -> float:
        """Return the bounded wait applied to one target handoff attempt."""

        return self._target_timeout_seconds

    def bind_target(
        self,
        target: FencedMailboxHandoffPort,
        *,
        target_identity: MailboxHandoffTarget,
    ) -> int:
        """Bind a target incarnation and return its monotonically increasing epoch.

        Rebinding never transfers an existing claim.  In-flight work from the
        previous epoch is allowed to finish, but its receipt is discarded by
        :meth:`dispatch` and the lease remains durable for later redrive.
        """

        if self._closed:
            raise RuntimeError("mailbox handoff dispatcher is closed")
        if not isinstance(target_identity, MailboxHandoffTarget):
            raise TypeError("target_identity must be a MailboxHandoffTarget")
        if not callable(getattr(target, "wake_handoff", None)):
            raise TypeError("target must implement FencedMailboxHandoffPort")
        self._target_epoch += 1
        self._target_binding = _TargetBinding(target, target_identity, self._target_epoch)
        return self._target_epoch

    def unbind_target(self) -> None:
        """Remove the current target without touching durable handoff leases."""

        self._target_epoch += 1
        self._target_binding = None

    def notify(self, mailbox_id: int) -> None:
        """Retain one bounded advisory hint; durable sidecar remains authoritative."""

        normalized = _positive_mailbox_id(mailbox_id)
        self._hints.pop(normalized, None)
        self._hints[normalized] = None
        while len(self._hints) > self._hint_capacity:
            self._hints.popitem(last=False)

    def drain_hints(self, *, limit: int = 100) -> tuple[int, ...]:
        """Consume a bounded hint page without treating hints as authorization."""

        if isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be at least one")
        mailbox_ids: list[int] = []
        while self._hints and len(mailbox_ids) < limit:
            mailbox_ids.append(self._hints.popitem(last=False)[0])
        return tuple(mailbox_ids)

    async def dispatch(self, mailbox_id: int) -> MailboxHandoffDispatchResult:
        """Claim and deliver one exact fenced mailbox handoff.

        Target failures and timeouts leave the claim leased.  The dispatcher
        never calls a legacy ``wake(key)`` method and never synthesizes missing
        or non-fenced evidence.
        """

        normalized = _positive_mailbox_id(mailbox_id)
        binding = self._target_binding
        if self._closed or binding is None:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.DEFERRED,
            )
        if len(self._inflight) >= self._max_inflight:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.BUSY,
            )
        if any(key[0] == normalized for key in self._inflight):
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.BUSY,
            )
        record = self._repository.read(normalized)
        if record is None or record.state is MailboxHandoffState.SETTLED:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.NO_WORK,
            )
        try:
            claim = self._repository.claim_fenced_handoff(
                normalized,
                worker_id=self._worker_id,
                target=binding.identity,
            )
        except Exception as exc:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.FAILED,
                error=_error_text(exc),
            )
        if claim is None:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.NO_WORK,
                handoff_id=record.handoff_id,
            )
        inflight_key = (normalized, claim.claim_id, binding.epoch)
        task = asyncio.create_task(
            self._run_target(binding, claim),
            name=f"mailbox-handoff-target:{normalized}:{claim.claim_id}",
        )
        self._inflight[inflight_key] = task
        task.add_done_callback(
            lambda completed: self._finish_inflight(inflight_key, completed)
        )
        try:
            completed, _pending = await asyncio.wait(
                (task,),
                timeout=self._target_timeout_seconds,
            )
            if not completed:
                return MailboxHandoffDispatchResult(
                    normalized,
                    MailboxHandoffDispatchDisposition.FAILED,
                    handoff_id=claim.handoff_id,
                    error="target handoff timed out; lease retained",
                )
            receipt = task.result()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.FAILED,
                handoff_id=claim.handoff_id,
                error=_error_text(exc),
            )
        current = self._target_binding
        if current is None or current.epoch != binding.epoch or current.port is not binding.port:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.DEFERRED,
                handoff_id=claim.handoff_id,
                error="target incarnation changed; lease retained",
            )
        if receipt.claim != claim:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.FAILED,
                handoff_id=claim.handoff_id,
                error="target receipt claim does not match dispatched claim",
            )
        if receipt.disposition is FencedMailboxWakeDisposition.DEFERRED:
            try:
                self._repository.defer_fenced_claim(receipt)
            except Exception as exc:
                return MailboxHandoffDispatchResult(
                    normalized,
                    MailboxHandoffDispatchDisposition.FAILED,
                    handoff_id=claim.handoff_id,
                    error=_error_text(exc),
                )
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.DEFERRED,
                handoff_id=claim.handoff_id,
                error="target deferred; claim released",
            )
        if receipt.disposition not in {
            FencedMailboxWakeDisposition.ACCEPTED,
            FencedMailboxWakeDisposition.STALE,
        }:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.FAILED,
                handoff_id=claim.handoff_id,
                error="target returned an unsupported non-terminal disposition",
            )
        try:
            self._repository.settle_fenced_claim(receipt)
        except Exception as exc:
            return MailboxHandoffDispatchResult(
                normalized,
                MailboxHandoffDispatchDisposition.FAILED,
                handoff_id=claim.handoff_id,
                error=_error_text(exc),
            )
        return MailboxHandoffDispatchResult(
            normalized,
            (
                MailboxHandoffDispatchDisposition.ACCEPTED
                if receipt.disposition is FencedMailboxWakeDisposition.ACCEPTED
                else MailboxHandoffDispatchDisposition.STALE
            ),
            handoff_id=claim.handoff_id,
        )

    async def dispatch_many(
        self,
        mailbox_ids: Iterable[int],
    ) -> tuple[MailboxHandoffDispatchResult, ...]:
        """Dispatch a caller-selected bounded set sequentially."""

        results: list[MailboxHandoffDispatchResult] = []
        for mailbox_id in mailbox_ids:
            results.append(await self.dispatch(mailbox_id))
        return tuple(results)

    async def dispatch_pending(
        self,
        *,
        limit: int = 100,
        after: MailboxHandoffDiscoveryCursor | None = None,
        profile_id: str | None = None,
        session_id: str | None = None,
        expected_request: FencedMailboxWakeRequest | None = None,
    ) -> MailboxHandoffDispatchPage:
        """Discover and dispatch one bounded page of fenced sidecars.

        This is deliberately caller-driven.  Constructing or binding the
        dispatcher never starts a timer, a scanner, or a background task.
        Pagination retains each mailbox identity, including multiple events
        belonging to the same session.
        """

        discover = getattr(self._repository, "discover_fenced_pending", None)
        if not callable(discover):
            raise TypeError(
                "repository must implement discover_fenced_pending for pull dispatch"
            )
        if expected_request is not None and not isinstance(
            expected_request,
            FencedMailboxWakeRequest,
        ):
            raise TypeError("expected_request must be a FencedMailboxWakeRequest or None")
        if expected_request is None:
            page = discover(
                limit=limit,
                after=after,
                profile_id=profile_id,
                session_id=session_id,
            )
        else:
            page = discover(
                limit=limit,
                after=after,
                profile_id=profile_id,
                session_id=session_id,
                expected_request=expected_request,
            )
        if not isinstance(page, MailboxHandoffDiscoveryPage):
            raise TypeError("discovery repository returned an invalid handoff page")
        results = await self.dispatch_many(
            record.evidence.identity.mailbox_id for record in page.records
        )
        return MailboxHandoffDispatchPage(
            results=results,
            next_cursor=page.next_cursor,
            has_more=page.has_more,
        )

    async def close(self) -> None:
        """Stop new dispatches and cancel local target tasks without settling them."""

        self._closed = True
        self._target_epoch += 1
        self._target_binding = None
        for task in tuple(self._inflight.values()):
            if not task.done():
                task.cancel()
        self._hints.clear()

    async def _run_target(
        self,
        binding: _TargetBinding,
        claim: FencedMailboxHandoffClaim,
    ) -> FencedMailboxHandoffReceipt:
        result = binding.port.wake_handoff(claim)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, FencedMailboxHandoffReceipt):
            raise TypeError("target returned an invalid mailbox handoff receipt")
        return result

    def _finish_inflight(
        self,
        key: tuple[int, str, int],
        task: asyncio.Task[FencedMailboxHandoffReceipt],
    ) -> None:
        if self._inflight.get(key) is task:
            self._inflight.pop(key, None)
        if not task.cancelled():
            try:
                task.exception()
            except asyncio.CancelledError:
                pass


def _positive_mailbox_id(value: object) -> int:
    """Normalize one positive durable mailbox id."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("mailbox_id must be a positive integer")
    return value


def _error_text(exc: BaseException) -> str:
    """Return bounded, stable diagnostic text without retaining an exception."""

    message = str(exc).strip() or type(exc).__name__
    return message[:500]


def _is_repository_port(value: object) -> bool:
    """Check the small persistence capability set without importing a runtime graph."""

    return all(
        callable(getattr(value, method_name, None))
        for method_name in (
            "read",
            "claim_fenced_handoff",
            "defer_fenced_claim",
            "settle_fenced_claim",
        )
    )


__all__ = [
    "DurableMailboxHandoffDispatcher",
    "MailboxHandoffDiscoveryRepositoryPort",
    "MailboxHandoffDispatchDisposition",
    "MailboxHandoffDispatchPage",
    "MailboxHandoffDispatchResult",
    "MailboxHandoffRepositoryPort",
]
