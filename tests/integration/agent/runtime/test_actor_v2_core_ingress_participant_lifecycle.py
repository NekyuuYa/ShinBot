"""Integration coverage for the unmounted process-level core-drain participant."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from shinbot.agent.runtime.actor_v2_core_ingress_participant import (
    ActorV2CoreIngressParticipantLifecycle,
    ActorV2CoreIngressParticipantLifecycleState,
)
from shinbot.agent.runtime.services import install_agent_runtime
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.actor_v2_core_ingress_drain import ActorV2CoreIngressDrainStatus
from shinbot.core.dispatch.actor_v2_ingress_drain import ActorV2IngressParticipantStatus
from shinbot.core.dispatch.agent_identity import (
    DEFAULT_SESSION_ACTOR_PROFILE_ID,
    SessionKey,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode


@dataclass(slots=True)
class _CallbackIngress:
    """Minimal explicit adapter callback boundary for an unmounted integration test."""

    adapter_instance_id: str
    receiving_callbacks: bool = False
    starts: int = 0
    stops: int = 0

    async def start_receiving_callbacks(self) -> None:
        """Record callback admission after the lifecycle has registered membership."""

        self.starts += 1
        self.receiving_callbacks = True

    async def stop_receiving_callbacks(self) -> None:
        """Record callback cessation before member retirement."""

        self.stops += 1
        self.receiving_callbacks = False


@pytest.mark.asyncio
async def test_process_participant_drains_real_runtime_before_retiring_member(
    tmp_path: Path,
) -> None:
    """A frozen core request is acknowledged through the real local drain helper."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    database = bot.database
    assert database is not None
    callback = _CallbackIngress("adapter-a")
    lifecycle = ActorV2CoreIngressParticipantLifecycle(
        membership_repository=database.actor_v2_ingress_drains,
        core_drain_repository=database.actor_v2_core_ingress_drains,
        callback_ingresses={"adapter-a": callback},
        participant_id="process-a:incarnation-a",
        participant_epoch=1,
        legacy_drain=runtime.build_legacy_session_local_drain_participant(
            bot.message_ingress,
        ),
        heartbeat_interval_seconds=60.0,
        core_drain_tick_interval_seconds=60.0,
    )

    await lifecycle.activate()
    key = SessionKey(
        DEFAULT_SESSION_ACTOR_PROFILE_ID,
        "default:group:core-ingress-participant",
    )
    source = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="test process participant source",
        legacy_session_id="legacy-session-core-ingress-participant",
        requested_by="test",
    ).ownership
    barrier_grant = database.actor_v2_migration_barriers.start_legacy_to_actor_v2(
        key,
        expected_generation=source.generation,
        adapter_instance_ids=("adapter-a",),
        holder_id="cutover-controller-a",
        reason="test process participant core drain",
    )
    request = database.actor_v2_core_ingress_drains.begin_drain(barrier_grant)

    assert request.status is ActorV2CoreIngressDrainStatus.OPEN
    assert request.unacknowledged_members

    closed = await lifecycle.shutdown()
    drained_locally = database.actor_v2_core_ingress_drains.get(request.request_id)
    participant = database.actor_v2_ingress_drains.get_participant(
        request.members[0].member_id,
    )

    assert closed.state is ActorV2CoreIngressParticipantLifecycleState.CLOSED
    assert callback.starts == callback.stops == 1
    assert drained_locally is not None
    assert drained_locally.status is ActorV2CoreIngressDrainStatus.OPEN
    assert not drained_locally.unacknowledged_members
    assert participant is not None
    assert participant.status is ActorV2IngressParticipantStatus.RETIRED

    await runtime.shutdown()
