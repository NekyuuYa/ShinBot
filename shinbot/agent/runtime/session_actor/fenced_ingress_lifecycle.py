"""Explicit ingress relay lifecycle for one already-published Actor v2 target.

The controller composes two deliberately narrow primitives:

* ``FencedNativeHistoryLifecycleController`` recovers and publishes one target;
* ``FencedDurableRoutingService`` relays only that target's complete fenced
  request into durable mailbox handoffs.

It does not acquire ownership, reserve an admission fence, start a core-drain
request, bind a global notifier, or make ``AgentRuntime`` a live Actor v2
consumer. Those authorities remain outside this unmounted controller.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.session_actor.fenced_native_history_lifecycle import (
    FencedNativeHistoryLifecycleSnapshot,
    FencedNativeHistoryLifecycleState,
)
from shinbot.core.dispatch.durable_routing_service import (
    DurableRoutingHealthSnapshot,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest


class FencedIngressHistoryLifecyclePort(Protocol):
    """Published target lifecycle required before scoped ingress can relay."""

    @property
    def snapshot(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Return the current target lifecycle diagnostics."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared by the published target."""

        ...

    async def activate(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Recover and publish the exact target before ingress begins."""

        ...

    async def verify_active(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Validate that the same target remains published and healthy."""

        ...

    async def shutdown(self) -> FencedNativeHistoryLifecycleSnapshot:
        """Retire the target after its ingress relay has stopped."""

        ...


class FencedIngressRelayPort(Protocol):
    """Exact durable relay service tied to one target request and database."""

    @property
    def request(self) -> FencedMailboxWakeRequest:
        """Return the only fenced request this relay may claim."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared with the target lifecycle."""

        ...

    @property
    def started(self) -> bool:
        """Return whether relay polling is currently active."""

        ...

    def health_snapshot(self) -> DurableRoutingHealthSnapshot:
        """Return relay health without exposing its mutable worker internals."""

        ...

    async def start(self) -> DurableRoutingHealthSnapshot:
        """Start scoped routing after the target has been published."""

        ...

    async def shutdown(self) -> None:
        """Stop relay polling and release any held durable claims."""

        ...


class FencedIngressLifecycleState(StrEnum):
    """Terminal-safe local state of one non-reusable fenced ingress lifetime."""

    READY = "ready"
    ACTIVE = "active"
    FAILED = "failed"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class FencedIngressLifecycleSnapshot:
    """Token-free diagnostics for the target and its exact ingress relay."""

    state: FencedIngressLifecycleState
    request: FencedMailboxWakeRequest
    history: FencedNativeHistoryLifecycleSnapshot
    relay: DurableRoutingHealthSnapshot
    persistence_domain_matches: bool
    cleanup_failed: bool
    error: str = ""

    def __post_init__(self) -> None:
        """Keep every diagnostic identity complete and bounded."""

        if not isinstance(self.state, FencedIngressLifecycleState):
            raise TypeError("state must be a FencedIngressLifecycleState")
        if not isinstance(self.request, FencedMailboxWakeRequest):
            raise TypeError("request must be a FencedMailboxWakeRequest")
        if not isinstance(self.history, FencedNativeHistoryLifecycleSnapshot):
            raise TypeError("history must be a FencedNativeHistoryLifecycleSnapshot")
        if self.history.request != self.request:
            raise ValueError("history request must match the fenced ingress request")
        if not isinstance(self.relay, DurableRoutingHealthSnapshot):
            raise TypeError("relay must be a DurableRoutingHealthSnapshot")
        if not isinstance(self.persistence_domain_matches, bool):
            raise TypeError("persistence_domain_matches must be a bool")
        if not isinstance(self.cleanup_failed, bool):
            raise TypeError("cleanup_failed must be a bool")
        object.__setattr__(self, "error", str(self.error or "").strip()[:500])


class FencedIngressLifecycleError(RuntimeError):
    """Raised when scoped ingress cannot prove its ordered lifecycle state."""


class FencedIngressLifecycleController:
    """Start exact relay only after target publication, then stop it first.

    The controller has no ownership or migration capability.  Its only
    authority is the request already held by both supplied components.  This
    ordering keeps a routing worker from replaying ingress before a target can
    pull the resulting durable handoff, and prevents target retirement while a
    relay can still create new sidecars for that target.
    """

    def __init__(
        self,
        *,
        history: FencedIngressHistoryLifecyclePort,
        relay: FencedIngressRelayPort,
    ) -> None:
        """Bind a new target lifecycle to one same-request relay service.

        Raises:
            TypeError: If a component lacks the narrow lifecycle port.
            ValueError: If the components have different requests, durable
                domains, or are already active.
        """

        _require_history_port(history)
        _require_relay_port(relay)
        history_snapshot = history.snapshot
        if not isinstance(history_snapshot, FencedNativeHistoryLifecycleSnapshot):
            raise TypeError("history must return a FencedNativeHistoryLifecycleSnapshot")
        if history_snapshot.state is not FencedNativeHistoryLifecycleState.READY:
            raise ValueError("fenced ingress lifecycle requires a ready target lifecycle")
        request = relay.request
        if not isinstance(request, FencedMailboxWakeRequest):
            raise TypeError("relay must expose a FencedMailboxWakeRequest")
        if not request.has_admission_fence:
            raise ValueError("fenced ingress lifecycle requires an admission-fenced request")
        if history_snapshot.request != request:
            raise ValueError("history and relay must retain the same fenced request")
        if history.persistence_domain is not relay.persistence_domain:
            raise ValueError("history and relay must share one persistence domain")
        if relay.started:
            raise ValueError("fenced ingress lifecycle requires an inactive relay")

        self._history = history
        self._relay = relay
        self._request = request
        self._persistence_domain = relay.persistence_domain
        self._lifecycle_lock = asyncio.Lock()
        self._active = False
        self._closed = False
        self._cleanup_failed = False
        self._error = ""

    @property
    def request(self) -> FencedMailboxWakeRequest:
        """Return the only request this lifecycle may activate."""

        return self._request

    @property
    def persistence_domain(self) -> object:
        """Return the immutable durable domain shared by target and relay."""

        return self._persistence_domain

    @property
    def snapshot(self) -> FencedIngressLifecycleSnapshot:
        """Return a token-free view of ordered target and relay state."""

        history = self._history.snapshot
        relay = self._relay.health_snapshot()
        domains_match = self._domains_match()
        if self._closed:
            state = FencedIngressLifecycleState.CLOSED
        elif self._active and self._components_active(history, relay, domains_match):
            state = FencedIngressLifecycleState.ACTIVE
        elif (
            self._cleanup_failed
            or self._active
            or history.state is not FencedNativeHistoryLifecycleState.READY
            or self._relay.started
            or not domains_match
        ):
            state = FencedIngressLifecycleState.FAILED
        else:
            state = FencedIngressLifecycleState.READY
        return FencedIngressLifecycleSnapshot(
            state=state,
            request=self._request,
            history=history,
            relay=relay,
            persistence_domain_matches=domains_match,
            cleanup_failed=self._cleanup_failed,
            error=self._error,
        )

    async def activate(self) -> FencedIngressLifecycleSnapshot:
        """Publish the target first, then start its exact ingress relay."""

        async with self._lifecycle_lock:
            if self._closed:
                raise FencedIngressLifecycleError(
                    "a closed fenced ingress lifecycle cannot activate"
                )
            if self._cleanup_failed:
                raise FencedIngressLifecycleError(
                    "fenced ingress cleanup failed; only shutdown may retry it"
                )
            if self._active:
                return await self._verify_active_locked()
            try:
                if not self._pre_activation_matches():
                    raise FencedIngressLifecycleError(
                        "fenced ingress target or relay changed before startup"
                    )
                history = await self._history.activate()
                self._require_active_history(history)
                relay = await self._relay.start()
                if not isinstance(relay, DurableRoutingHealthSnapshot):
                    raise TypeError("fenced ingress relay returned an invalid health snapshot")
                if not self._components_active(
                    self._history.snapshot,
                    relay,
                    self._domains_match(),
                ):
                    raise FencedIngressLifecycleError(
                        "fenced ingress target or relay is unhealthy after startup"
                    )
            except BaseException as exc:
                self._error = _error_text(exc)
                await self._terminate()
                raise
            self._active = True
            self._error = ""
            return self.snapshot

    async def verify_active(self) -> FencedIngressLifecycleSnapshot:
        """Verify that target publication and exact relay remain aligned."""

        async with self._lifecycle_lock:
            if self._closed:
                raise FencedIngressLifecycleError(
                    "a closed fenced ingress lifecycle cannot verify its target"
                )
            if self._cleanup_failed:
                raise FencedIngressLifecycleError(
                    "fenced ingress cleanup failed; only shutdown may retry it"
                )
            return await self._verify_active_locked()

    async def shutdown(self) -> FencedIngressLifecycleSnapshot:
        """Stop ingress relay before retiring the target publication."""

        async with self._lifecycle_lock:
            if self._closed:
                return self.snapshot
            await self._terminate()
            return self.snapshot

    async def _verify_active_locked(self) -> FencedIngressLifecycleSnapshot:
        """Validate exact target and relay identities under the lifecycle lock."""

        if not self._active:
            raise FencedIngressLifecycleError("fenced ingress lifecycle has not activated")
        try:
            history = await self._history.verify_active()
            self._require_active_history(history)
            relay = self._relay.health_snapshot()
            if not self._components_active(history, relay, self._domains_match()):
                raise FencedIngressLifecycleError(
                    "fenced ingress target or relay is no longer healthy"
                )
        except BaseException as exc:
            self._error = _error_text(exc)
            await self._terminate()
            raise
        return self.snapshot

    def _pre_activation_matches(self) -> bool:
        """Return whether both components still retain their original identities."""

        history = self._history.snapshot
        return (
            history.state is FencedNativeHistoryLifecycleState.READY
            and history.request == self._request
            and self._relay.request == self._request
            and not self._relay.started
            and self._domains_match()
        )

    def _components_active(
        self,
        history: FencedNativeHistoryLifecycleSnapshot,
        relay: DurableRoutingHealthSnapshot,
        domains_match: bool,
    ) -> bool:
        """Return whether every mutable component retains this exact request."""

        return (
            history.state is FencedNativeHistoryLifecycleState.ACTIVE
            and history.request == self._request
            and history.persistence_domain_matches
            and not history.cleanup_failed
            and self._relay.request == self._request
            and self._relay.started
            and relay.started
            and relay.fenced_request_scoped
            and relay.fenced_scope_live
            and domains_match
        )

    def _domains_match(self) -> bool:
        """Return whether both components still use the original durable domain."""

        return (
            self._history.persistence_domain is self._persistence_domain
            and self._relay.persistence_domain is self._persistence_domain
        )

    def _require_active_history(
        self,
        history: FencedNativeHistoryLifecycleSnapshot,
    ) -> None:
        """Reject a history activation result that differs from the bound target."""

        if not isinstance(history, FencedNativeHistoryLifecycleSnapshot):
            raise TypeError("history lifecycle returned an invalid snapshot")
        if (
            history.state is not FencedNativeHistoryLifecycleState.ACTIVE
            or history.request != self._request
        ):
            raise FencedIngressLifecycleError(
                "history lifecycle did not publish the exact fenced target"
            )

    async def _terminate(self) -> None:
        """Finish ordered cleanup even when the caller cancels this lifecycle."""

        task = asyncio.create_task(
            self._terminate_once(),
            name="agent-fenced-ingress-lifecycle-shutdown",
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
        """Stop relay first, then retire the exact target through its owner."""

        try:
            await self._relay.shutdown()
        except BaseException as exc:
            self._cleanup_failed = True
            self._error = _error_text(exc)
            raise
        if self._relay.started:
            self._cleanup_failed = True
            self._error = "fenced ingress relay did not stop before target retirement"
            raise FencedIngressLifecycleError(self._error)
        try:
            history = await self._history.shutdown()
        except BaseException as exc:
            self._cleanup_failed = True
            self._error = _error_text(exc)
            raise
        if (
            not isinstance(history, FencedNativeHistoryLifecycleSnapshot)
            or history.state is not FencedNativeHistoryLifecycleState.CLOSED
        ):
            self._cleanup_failed = True
            self._error = "fenced ingress target did not prove shutdown completion"
            raise FencedIngressLifecycleError(self._error)
        self._active = False
        self._cleanup_failed = False
        self._closed = True


def _require_history_port(history: object) -> None:
    """Validate target lifecycle capabilities before composition."""

    if not hasattr(history, "snapshot") or any(
        not callable(getattr(history, method_name, None))
        for method_name in ("activate", "verify_active", "shutdown")
    ) or not hasattr(history, "persistence_domain"):
        raise TypeError("history must implement the fenced ingress history lifecycle port")


def _require_relay_port(relay: object) -> None:
    """Validate scoped relay capabilities before composition."""

    required_properties = ("request", "persistence_domain", "started")
    required_methods = ("health_snapshot", "start", "shutdown")
    if any(not hasattr(relay, attribute) for attribute in required_properties) or any(
        not callable(getattr(relay, method_name, None)) for method_name in required_methods
    ):
        raise TypeError("relay must implement the fenced ingress relay port")


def _error_text(error: BaseException) -> str:
    """Return a bounded operator-visible error without retaining tracebacks."""

    return (str(error).strip() or type(error).__name__)[:500]


__all__ = [
    "FencedIngressLifecycleController",
    "FencedIngressLifecycleError",
    "FencedIngressLifecycleSnapshot",
    "FencedIngressLifecycleState",
]
