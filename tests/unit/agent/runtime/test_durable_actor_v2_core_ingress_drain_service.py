"""Unit coverage for fair durable core-ingress drain supervision."""

from __future__ import annotations

import pytest

from shinbot.agent.runtime.actor_v2_core_ingress_drain_service import (
    CORE_INGRESS_DRAIN_HEAD_RETRY_INTERVAL,
    ActorV2CoreIngressDrainServiceDisposition,
    DurableActorV2CoreIngressDrainService,
)
from shinbot.agent.runtime.actor_v2_core_ingress_drain_worker import (
    ActorV2CoreIngressDrainWorkerOutcome,
    ActorV2CoreIngressDrainWorkerStatus,
)
from shinbot.core.dispatch.actor_v2_core_ingress_drain import (
    ActorV2CoreIngressDrainDiscoveryCursor,
    ActorV2CoreIngressDrainDiscoveryPage,
    ActorV2CoreIngressDrainMember,
    ActorV2CoreIngressDrainRequest,
    ActorV2CoreIngressDrainStatus,
)
from shinbot.core.dispatch.agent_identity import SessionKey


class _Repository:
    """Minimal durable page source retaining open local request membership."""

    def __init__(self) -> None:
        """Create one isolated discovery domain."""

        self.persistence_domain = self
        self.requests: dict[str, ActorV2CoreIngressDrainRequest] = {}
        self.pending_request_ids: set[str] = set()

    def add(self, request: ActorV2CoreIngressDrainRequest) -> None:
        """Publish one open request for the local process incarnation."""

        self.requests[request.request_id] = request
        self.pending_request_ids.add(request.request_id)

    def discover_open_for_participant(
        self,
        participant_id: str,
        *,
        limit: int,
        after: ActorV2CoreIngressDrainDiscoveryCursor | None = None,
    ) -> ActorV2CoreIngressDrainDiscoveryPage:
        """Return the current keyset page of this process's unacknowledged work."""

        candidates = tuple(
            request
            for request in sorted(
                self.requests.values(),
                key=lambda request: (request.created_at, request.request_id),
            )
            if request.request_id in self.pending_request_ids
            and any(member.participant_id == participant_id for member in request.members)
            and (
                after is None
                or (request.created_at, request.request_id)
                > (after.created_at, after.request_id)
            )
        )
        selected = candidates[:limit]
        has_more = len(candidates) > limit
        cursor = (
            ActorV2CoreIngressDrainDiscoveryCursor(
                created_at=selected[-1].created_at,
                request_id=selected[-1].request_id,
            )
            if has_more
            else None
        )
        return ActorV2CoreIngressDrainDiscoveryPage(
            requests=selected,
            next_cursor=cursor,
            has_more=has_more,
        )


class _Worker:
    """Controlled local worker that marks a request complete on acknowledgement."""

    participant_id = "process-a:incarnation-a"

    def __init__(
        self,
        repository: _Repository,
        outcomes: dict[str, list[ActorV2CoreIngressDrainWorkerStatus]],
    ) -> None:
        """Bind deterministic per-request local drain observations."""

        self.persistence_domain = repository.persistence_domain
        self._repository = repository
        self._outcomes = outcomes
        self.calls: list[str] = []

    async def service_request(
        self,
        request_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> ActorV2CoreIngressDrainWorkerOutcome:
        """Return the next local result and remove acknowledged durable work."""

        del timeout_seconds
        request = self._repository.requests[request_id]
        status = self._outcomes[request_id].pop(0)
        self.calls.append(request_id)
        member_ids = tuple(member.member_id for member in request.members)
        if status is ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED:
            self._repository.pending_request_ids.discard(request_id)
            return ActorV2CoreIngressDrainWorkerOutcome(
                request_id=request_id,
                barrier_id=request.barrier_id,
                participant_id=self.participant_id,
                member_ids=member_ids,
                status=status,
                core_ingress_quiescent=True,
                local_legacy_quiescent=True,
                acknowledged_member_ids=member_ids,
            )
        return ActorV2CoreIngressDrainWorkerOutcome(
            request_id=request_id,
            barrier_id=request.barrier_id,
            participant_id=self.participant_id,
            member_ids=member_ids,
            status=status,
            core_ingress_quiescent=False,
            local_legacy_quiescent=False,
            acknowledged_member_ids=(),
        )


def _request(request_id: str, created_at: float) -> ActorV2CoreIngressDrainRequest:
    """Build one open request served by the fake process incarnation."""

    member = ActorV2CoreIngressDrainMember(
        request_id=request_id,
        member_id="member-a",
        adapter_instance_id="adapter-a",
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
    )
    return ActorV2CoreIngressDrainRequest(
        request_id=request_id,
        barrier_id=f"barrier-{request_id}",
        key=SessionKey("profile-a", f"profile-a:group:{request_id}"),
        legacy_session_id=f"legacy-session-{request_id}",
        adapter_instance_ids=("adapter-a",),
        source_generation=1,
        migration_generation=2,
        status=ActorV2CoreIngressDrainStatus.OPEN,
        created_at=created_at,
        updated_at=created_at,
        drained_at=None,
        members=(member,),
    )


@pytest.mark.asyncio
async def test_head_retry_revisits_a_nonquiescent_request_under_continuous_append() -> None:
    """An old local drain cannot starve while the main keyset keeps advancing."""

    repository = _Repository()
    requests = (
        _request("request-old", 1.0),
        _request("request-main-a", 2.0),
        _request("request-main-b", 3.0),
        _request("request-main-c", 4.0),
        _request("request-main-d", 5.0),
        _request("request-main-e", 6.0),
        _request("request-main-f", 7.0),
    )
    for request in requests[:2]:
        repository.add(request)
    worker = _Worker(
        repository,
        {
            "request-old": [
                ActorV2CoreIngressDrainWorkerStatus.AWAITING_LOCAL_DRAIN,
                ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED,
            ],
            **{
                request.request_id: [ActorV2CoreIngressDrainWorkerStatus.ACKNOWLEDGED]
                for request in requests[1:]
            },
        },
    )
    service = DurableActorV2CoreIngressDrainService(
        repository=repository,
        worker=worker,
        batch_limit=1,
    )

    first = await service.run_once()
    assert first.results[0].request_id == "request-old"
    assert (
        first.results[0].disposition
        is ActorV2CoreIngressDrainServiceDisposition.AWAITING_LOCAL_DRAIN
    )

    for request in requests[2:6]:
        repository.add(request)
        await service.run_once()

    assert worker.calls[: CORE_INGRESS_DRAIN_HEAD_RETRY_INTERVAL + 1] == [
        "request-old",
        "request-main-a",
        "request-main-b",
        "request-main-c",
        "request-old",
    ]
    assert "request-old" not in repository.pending_request_ids

    repository.add(requests[6])
    resumed_main = await service.run_once()

    assert resumed_main.results[0].request_id == "request-main-d"
