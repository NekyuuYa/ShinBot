"""Unmounted per-process discovery for barrier-bound core ingress drains.

The durable request is the cross-process delivery channel: every process polls
only requests that contain one of its frozen participant members, then delegates
the local freeze and acknowledgement to ``ActorV2CoreIngressDrainProcessWorker``.
This service never confirms a drain, changes ownership, or publishes an Actor
target. A controller must retain those separate authorities.
"""

from __future__ import annotations

import asyncio
import math
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from shinbot.agent.runtime.actor_v2_core_ingress_drain_worker import (
    ActorV2CoreIngressDrainWorkerOutcome,
    ActorV2CoreIngressDrainWorkerStatus,
)
from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceHealthSnapshot,
    supervised_backoff_seconds,
)
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainDiscoveryCursor,
    ActorV2CoreIngressDrainDiscoveryPage,
)
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="agent:core-ingress-drain", color="yellow")

MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH = 100
CORE_INGRESS_DRAIN_HEAD_RETRY_INTERVAL = 4


class CoreIngressDrainDiscoveryPort(Protocol):
    """Durable open-request discovery required by one local process service."""

    @property
    def persistence_domain(self) -> object:
        """Return the persistence domain shared with the local drain worker."""

        ...

    def discover_open_for_participant(
        self,
        participant_id: str,
        *,
        limit: int = MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH,
        after: ActorV2CoreIngressDrainDiscoveryCursor | None = None,
    ) -> ActorV2CoreIngressDrainDiscoveryPage:
        """Return one bounded page of unacknowledged local request members."""

        ...


class CoreIngressDrainWorkerPort(Protocol):
    """Local drain executor bound to one durable process incarnation."""

    @property
    def participant_id(self) -> str:
        """Return the exact process incarnation that owns local memberships."""

        ...

    @property
    def persistence_domain(self) -> object:
        """Return the durable domain shared with request discovery."""

        ...

    async def service_request(
        self,
        request_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> ActorV2CoreIngressDrainWorkerOutcome:
        """Attempt one exact local drain without exposing its freeze ticket."""

        ...


class ActorV2CoreIngressDrainServiceDisposition(StrEnum):
    """Safe outcome of one request observed by a local process service."""

    ACKNOWLEDGED = "acknowledged"
    AWAITING_LOCAL_DRAIN = "awaiting_local_drain"
    FAILED = "failed"


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainServiceResult:
    """Token-free result of servicing one discovered request."""

    request_id: str
    disposition: ActorV2CoreIngressDrainServiceDisposition
    outcome: ActorV2CoreIngressDrainWorkerOutcome | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    error_code: str = ""

    def __post_init__(self) -> None:
        """Require a worker result for success and a stable code for failure."""

        request_id = _identifier(self.request_id, "request_id")
        disposition = ActorV2CoreIngressDrainServiceDisposition(self.disposition)
        outcome = self.outcome
        error_code = str(self.error_code or "").strip()
        if disposition is ActorV2CoreIngressDrainServiceDisposition.FAILED:
            if outcome is not None or not error_code:
                raise ValueError("failed core ingress service result requires only error_code")
        else:
            if not isinstance(outcome, ActorV2CoreIngressDrainWorkerOutcome):
                raise TypeError("successful core ingress service result requires worker outcome")
            expected_status = (
                ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED
                if disposition is ActorV2CoreIngressDrainServiceDisposition.ACKNOWLEDGED
                else ActorV2CoreIngressDrainWorkerStatus.AWAITING_LOCAL_DRAIN
            )
            if outcome.request_id != request_id or outcome.status is not expected_status:
                raise ValueError("worker outcome differs from service result disposition")
            if error_code:
                raise ValueError("successful core ingress service result cannot retain error_code")
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(self, "error_code", error_code)


@dataclass(slots=True, frozen=True)
class ActorV2CoreIngressDrainServiceSummary:
    """One bounded pass over local durable core-drain work."""

    results: tuple[ActorV2CoreIngressDrainServiceResult, ...]
    has_more: bool = False

    def __post_init__(self) -> None:
        """Normalize request results without retaining local freeze capabilities."""

        results = tuple(self.results)
        if any(
            not isinstance(result, ActorV2CoreIngressDrainServiceResult)
            for result in results
        ):
            raise TypeError("core ingress service summary requires typed results")
        request_ids = tuple(result.request_id for result in results)
        if len(set(request_ids)) != len(request_ids):
            raise ValueError("core ingress service summary cannot repeat a request")
        if not isinstance(self.has_more, bool):
            raise TypeError("has_more must be a boolean")
        object.__setattr__(self, "results", results)

    @property
    def failed_count(self) -> int:
        """Return the number of requests left unacknowledged after a local error."""

        return sum(
            result.disposition is ActorV2CoreIngressDrainServiceDisposition.FAILED
            for result in self.results
        )


class ActorV2CoreIngressDrainServiceError(RuntimeError):
    """Aggregate local failures without exposing tickets or raw receipt data."""

    def __init__(self, request_ids: tuple[str, ...]) -> None:
        """Retain only stable durable identities in health state."""

        normalized = tuple(_identifier(request_id, "request_id") for request_id in request_ids)
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("core ingress drain service error requires unique request ids")
        self.request_ids = tuple(sorted(normalized))
        super().__init__("local core ingress drain failed for: " + ", ".join(self.request_ids))


class DurableActorV2CoreIngressDrainService:
    """Poll and service only one process incarnation's frozen drain members.

    This is deliberately an unmounted service. Constructing or starting it does
    not create a barrier, confirm a request, start an Actor, or resume ingress.
    It only supplies the durable request delivery missing from the direct local
    worker, while preserving a blocked request when the local process fails.
    """

    def __init__(
        self,
        *,
        repository: CoreIngressDrainDiscoveryPort,
        worker: CoreIngressDrainWorkerPort,
        tick_interval_seconds: float = 5.0,
        batch_limit: int = 25,
        local_drain_timeout_seconds: float | None = None,
        runtime_id: str | None = None,
    ) -> None:
        """Bind one process-local worker to durable request discovery."""

        if not callable(getattr(repository, "discover_open_for_participant", None)):
            raise TypeError("repository must implement core ingress drain discovery")
        if not callable(getattr(worker, "service_request", None)):
            raise TypeError("worker must implement service_request(request_id)")
        if not str(getattr(worker, "participant_id", "") or "").strip():
            raise ValueError("worker participant_id must not be empty")
        if worker.persistence_domain is not getattr(repository, "persistence_domain", None):
            raise ValueError("repository and worker must share one persistence domain")
        self._repository = repository
        self._worker = worker
        self._tick_interval_seconds = _positive_finite(
            tick_interval_seconds,
            "tick_interval_seconds",
        )
        if (
            isinstance(batch_limit, bool)
            or not isinstance(batch_limit, int)
            or not 1 <= batch_limit <= MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH
        ):
            raise ValueError(
                "batch_limit must be between 1 and "
                f"{MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH}"
            )
        self._batch_limit = batch_limit
        self._local_drain_timeout_seconds = _timeout(local_drain_timeout_seconds)
        self._runtime_id = str(
            runtime_id or f"core-ingress-drain:{uuid.uuid4().hex}"
        ).strip()
        if not self._runtime_id:
            raise ValueError("runtime_id must not be empty")
        self._task: asyncio.Task[None] | None = None
        self._run_lock = asyncio.Lock()
        self._health = RuntimeServiceHealth("durable_core_ingress_drain")
        self._last_summary = ActorV2CoreIngressDrainServiceSummary(results=())
        self._cursor: ActorV2CoreIngressDrainDiscoveryCursor | None = None
        # A main keyset scan can keep moving forward forever while new barriers
        # arrive. An independent head lap retries older non-quiescent requests
        # without resetting or losing the main cursor.
        self._head_retry_cursor: ActorV2CoreIngressDrainDiscoveryCursor | None = None
        self._head_retry_required = False
        self._head_retry_lap_has_pending = False
        self._pages_since_head_retry = 0
        self._lifecycle_epoch = 0

    @property
    def participant_id(self) -> str:
        """Return the exact registered process incarnation served by this loop."""

        return self._worker.participant_id

    @property
    def runtime_id(self) -> str:
        """Return the local task identity used in diagnostics and logs."""

        return self._runtime_id

    @property
    def last_summary(self) -> ActorV2CoreIngressDrainServiceSummary:
        """Return the most recent bounded discovery and local-service result."""

        return self._last_summary

    def health_snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return process-local service health without granting activation."""

        return self._health.snapshot()

    def start(self) -> None:
        """Start bounded local polling when an explicit lifecycle owner calls it."""

        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("agent.core_ingress_drain.start_skipped | no_running_loop")
            return
        self._health.start()
        self._task = loop.create_task(
            self._run_loop(),
            name=f"agent-core-ingress-drain:{self._runtime_id}",
        )

    async def shutdown(self) -> None:
        """Stop local polling without thawing, confirming, or retiring any request."""

        self._lifecycle_epoch += 1
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._health.stop()

    async def run_once(self) -> ActorV2CoreIngressDrainServiceSummary:
        """Discover and service one bounded page for this process incarnation."""

        lifecycle_epoch = self._lifecycle_epoch
        async with self._run_lock:
            if lifecycle_epoch != self._lifecycle_epoch:
                return self._last_summary
            self._health.scan_started()
            try:
                is_head_retry = self._next_head_retry()
                cursor = self._head_retry_cursor if is_head_retry else self._cursor
                page = self._repository.discover_open_for_participant(
                    self.participant_id,
                    limit=self._batch_limit,
                    after=cursor,
                )
                if not isinstance(page, ActorV2CoreIngressDrainDiscoveryPage):
                    raise TypeError("repository returned an invalid core ingress discovery page")
                results: list[ActorV2CoreIngressDrainServiceResult] = []
                failed_request_ids: list[str] = []
                has_unresolved_local_drain = False
                for request in page.requests:
                    if lifecycle_epoch != self._lifecycle_epoch:
                        return self._last_summary
                    try:
                        outcome = await self._worker.service_request(
                            request.request_id,
                            timeout_seconds=self._local_drain_timeout_seconds,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        failed_request_ids.append(request.request_id)
                        has_unresolved_local_drain = True
                        results.append(
                            ActorV2CoreIngressDrainServiceResult(
                                request_id=request.request_id,
                                disposition=ActorV2CoreIngressDrainServiceDisposition.FAILED,
                                error_code=type(exc).__name__,
                            )
                        )
                        logger.exception(
                            format_log_event(
                                "agent.core_ingress_drain.service_request_failed",
                                request_id=request.request_id,
                                participant_id=self.participant_id,
                                error_code=type(exc).__name__,
                            )
                        )
                        continue
                    if outcome.status is ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED:
                        disposition = ActorV2CoreIngressDrainServiceDisposition.ACKNOWLEDGED
                    elif (
                        outcome.status
                        is ActorV2CoreIngressDrainWorkerStatus.AWAITING_LOCAL_DRAIN
                    ):
                        has_unresolved_local_drain = True
                        disposition = (
                            ActorV2CoreIngressDrainServiceDisposition.AWAITING_LOCAL_DRAIN
                        )
                    else:
                        raise RuntimeError(
                            "core ingress drain worker returned an unsupported outcome status"
                        )
                    results.append(
                        ActorV2CoreIngressDrainServiceResult(
                            request_id=request.request_id,
                            disposition=disposition,
                            outcome=outcome,
                        )
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._health.failed(exc)
                raise
            if lifecycle_epoch != self._lifecycle_epoch:
                return self._last_summary
            self._advance_cursor(
                page,
                is_head_retry=is_head_retry,
                has_unresolved_local_drain=has_unresolved_local_drain,
            )
            summary = ActorV2CoreIngressDrainServiceSummary(
                results=tuple(results),
                has_more=page.has_more or self._head_retry_required,
            )
            self._last_summary = summary
            if failed_request_ids:
                self._health.failed(
                    ActorV2CoreIngressDrainServiceError(tuple(failed_request_ids))
                )
            else:
                self._health.succeeded()
            return summary

    async def _run_loop(self) -> None:
        """Run serialized local passes with bounded failure backoff."""

        delay = 0.0
        try:
            while True:
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    summary = await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "agent.core_ingress_drain.iteration_failed",
                            participant_id=self.participant_id,
                            error_code=type(exc).__name__,
                            consecutive_failures=(
                                self._health.snapshot().consecutive_failures
                            ),
                        )
                    )
                    delay = supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=self._health.snapshot().consecutive_failures,
                    )
                    continue
                delay = (
                    supervised_backoff_seconds(
                        base_seconds=self._tick_interval_seconds,
                        consecutive_failures=self._health.snapshot().consecutive_failures,
                    )
                    if summary.failed_count
                    else self._tick_interval_seconds
                )
        finally:
            self._health.stop()

    def _next_head_retry(self) -> bool:
        """Choose an independent old-work page after bounded main progress.

        The separate cursor is essential: rewinding the main cursor after a
        negative local drain would starve newer barriers, while never rewinding
        it could starve the frozen request under continuous append.
        """

        if not self._head_retry_required:
            return False
        self._pages_since_head_retry += 1
        if self._pages_since_head_retry < CORE_INGRESS_DRAIN_HEAD_RETRY_INTERVAL:
            return False
        self._pages_since_head_retry = 0
        if self._head_retry_cursor is None:
            self._head_retry_lap_has_pending = False
        return True

    def _advance_cursor(
        self,
        page: ActorV2CoreIngressDrainDiscoveryPage,
        *,
        is_head_retry: bool,
        has_unresolved_local_drain: bool,
    ) -> None:
        """Advance only the selected scan lane and retain retry demand safely."""

        if has_unresolved_local_drain:
            self._head_retry_required = True
        if not is_head_retry:
            self._cursor = page.next_cursor if page.has_more else None
            return

        head_lap_has_pending = (
            self._head_retry_lap_has_pending or has_unresolved_local_drain
        )
        if page.has_more:
            self._head_retry_cursor = page.next_cursor
            self._head_retry_lap_has_pending = head_lap_has_pending
            return

        self._head_retry_cursor = None
        self._head_retry_lap_has_pending = False
        if not head_lap_has_pending:
            self._head_retry_required = False


def _identifier(value: object, field_name: str) -> str:
    """Normalize one opaque durable identity without coercing absent values."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _positive_finite(value: object, field_name: str) -> float:
    """Require one finite positive polling interval."""

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be finite and positive")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized


def _timeout(value: float | None) -> float | None:
    """Normalize an optional finite non-negative per-request drain budget."""

    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("local_drain_timeout_seconds must be finite and non-negative")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError("local_drain_timeout_seconds must be finite and non-negative")
    return normalized


__all__ = [
    "ActorV2CoreIngressDrainServiceDisposition",
    "ActorV2CoreIngressDrainServiceError",
    "ActorV2CoreIngressDrainServiceResult",
    "ActorV2CoreIngressDrainServiceSummary",
    "CoreIngressDrainDiscoveryPort",
    "CoreIngressDrainWorkerPort",
    "CORE_INGRESS_DRAIN_HEAD_RETRY_INTERVAL",
    "DurableActorV2CoreIngressDrainService",
    "MAX_CORE_INGRESS_DRAIN_SERVICE_BATCH",
]
