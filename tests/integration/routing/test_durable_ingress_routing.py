"""Production-path tests for actor-owned durable ingress routing."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import pytest

from shinbot.core.application.bot_routing import BotRuntimeRouter
from shinbot.core.application.bots_config import (
    BotAgentConfig,
    BotBindingConfig,
    BotServiceConfig,
)
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionFenceStatus
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    AgentEntryDispatcher,
    make_agent_entry_fallback_route_rule,
)
from shinbot.core.dispatch.durable_routing_service import (
    DurableRoutingService,
    DurableRoutingServiceStatus,
    FencedDurableRoutingService,
)
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.ingress import (
    MessageIngress,
    RouteDispatchContext,
    RouteTargetRegistry,
)
from shinbot.core.dispatch.legacy_ingress_quiescence import (
    LegacyIngressQuiescenceStatus,
)
from shinbot.core.dispatch.mailbox_handoff import (
    MailboxHandoffEvidenceState,
    MailboxHandoffState,
)
from shinbot.core.dispatch.message_context import WaitingInputRegistry
from shinbot.core.dispatch.routing import (
    RouteCondition,
    RouteMatchMode,
    RouteRule,
    RouteTable,
)
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.schema.elements import MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


class _Adapter(BaseAdapter):
    def __init__(self) -> None:
        super().__init__(instance_id="instance-a", platform="mock")

    async def start(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass

    async def send(
        self,
        target_session: str,
        elements: list[MessageElement],
    ) -> MessageHandle:
        return MessageHandle(message_id=target_session, adapter_ref=self)

    async def call_api(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        return {"method": method, "params": params}

    async def get_capabilities(self) -> dict[str, Any]:
        return {"elements": ["text"], "actions": [], "limits": {}}


def _event(
    *,
    message_id: str = "message-a",
    user_id: str = "user-a",
) -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        self_id="bot-self",
        platform="mock",
        user=User(id=user_id, name="Alice"),
        channel=Channel(id=user_id, type=1),
        message=MessagePayload(id=message_id, content="hello actor"),
    )


def _actor_key() -> SessionKey:
    return SessionKey("bot-a", "bot-a:private:user-a")


def _build_actor_ingress(
    tmp_path: Path,
    *,
    route_table: RouteTable | None = None,
    route_targets: RouteTargetRegistry | None = None,
    agent_handler: Any | None = None,
    claim_actor_owner: bool = True,
    waiting_registry: WaitingInputRegistry | None = None,
    durable_recovery_grace_seconds: float = 2.0,
    durable_routing_timeout_seconds: float = 20.0,
) -> tuple[MessageIngress, DatabaseManager, _Adapter]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    adapter = _Adapter()
    table = route_table or RouteTable()
    targets = route_targets or RouteTargetRegistry()
    if not table.rules:
        table.register(make_agent_entry_fallback_route_rule())
    if targets.get(AGENT_ENTRY_TARGET) is None:
        targets.register(
            AGENT_ENTRY_TARGET,
            AgentEntryDispatcher(handler=agent_handler),
        )
    bot = BotServiceConfig(
        id="bot-a",
        display_name="Bot A",
        agent=BotAgentConfig(mode="simple"),
        bindings=(
            BotBindingConfig(
                id="binding-a",
                adapter_instance_id=adapter.instance_id,
                session_patterns=("private:*",),
            ),
        ),
    )
    ingress = MessageIngress(
        session_manager=SessionManager(session_repo=database.sessions),
        permission_engine=PermissionEngine(),
        route_table=table,
        route_targets=targets,
        database=database,
        waiting_registry=waiting_registry,
        bot_router=BotRuntimeRouter((bot,)),
        durable_recovery_grace_seconds=durable_recovery_grace_seconds,
        durable_routing_timeout_seconds=durable_routing_timeout_seconds,
    )
    if claim_actor_owner:
        database.agent_runtime_ownership.claim(
            _actor_key(),
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="test activates actor owner",
            legacy_session_id="instance-a:private:user-a",
            requested_by="test",
        )
    return ingress, database, adapter


@pytest.mark.asyncio
async def test_actor_owner_commits_outbox_without_calling_legacy_agent_handler(
    tmp_path: Path,
) -> None:
    legacy_signals: list[Any] = []
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        agent_handler=legacy_signals.append,
    )

    result = await ingress.process_event(_event(), adapter)
    await asyncio.sleep(0)

    assert result.message_log_id is not None
    assert legacy_signals == []
    with database.connect() as conn:
        job = conn.execute(
            "SELECT status, decision_kind FROM message_routing_jobs"
        ).fetchone()
        outbox = conn.execute(
            "SELECT status, ownership_generation FROM agent_route_outbox"
        ).fetchone()
    assert tuple(job) == ("completed", "agent_deliveries")
    assert tuple(outbox) == ("pending", 1)
    assert database.message_logs.get(result.message_log_id)["routing_status"] == "dispatched"


@pytest.mark.asyncio
async def test_actor_owner_buffers_waiting_reply_without_resolving_legacy_future(
    tmp_path: Path,
) -> None:
    """An Actor owner wins admission before a legacy waiter can consume input."""

    waiting_registry = WaitingInputRegistry()
    waiting_future = waiting_registry.register("instance-a:private:user-a")
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        waiting_registry=waiting_registry,
    )

    event = _event()
    async with ingress._session_manager.session_lock("instance-a:private:user-a"):
        result = await asyncio.wait_for(ingress.process_event(event, adapter), timeout=0.5)
    duplicate = await ingress.process_event(event, adapter)

    assert result.message_log_id is not None
    assert duplicate.message_log_id == result.message_log_id
    assert result.matched_rules == []
    assert not waiting_future.done()
    assert waiting_registry.is_waiting("instance-a:private:user-a")
    with database.connect() as conn:
        job = conn.execute(
            "SELECT status, ownership_generation FROM message_routing_jobs"
        ).fetchone()
        outbox_count = conn.execute("SELECT COUNT(*) FROM agent_route_outbox").fetchone()[0]
        message_count = conn.execute("SELECT COUNT(*) FROM message_logs").fetchone()[0]
        job_count = conn.execute("SELECT COUNT(*) FROM message_routing_jobs").fetchone()[0]
    assert tuple(job) == ("pending", 1)
    assert outbox_count == 0
    assert (message_count, job_count) == (1, 1)
    waiting_registry.cancel("instance-a:private:user-a")


@pytest.mark.asyncio
async def test_actor_owner_durably_buffers_expired_waiting_reply(
    tmp_path: Path,
) -> None:
    """Expiry is decided after Actor-owned admission, not before the fence."""

    waiting_registry = WaitingInputRegistry()
    waiting_future = waiting_registry.register("instance-a:private:user-a")
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        waiting_registry=waiting_registry,
    )
    event = _event()
    event.timestamp = int((time.time() - 120.0) * 1000)

    result = await ingress.process_event(event, adapter)

    assert result.message_log_id is not None
    assert not waiting_future.done()
    with database.connect() as conn:
        job = conn.execute(
            "SELECT status, ownership_generation FROM message_routing_jobs"
        ).fetchone()
    assert tuple(job) == ("pending", 1)
    waiting_registry.cancel("instance-a:private:user-a")


@pytest.mark.asyncio
async def test_reserved_admission_buffers_ingress_without_legacy_target_execution(
    tmp_path: Path,
) -> None:
    """Reservation blocks implicit legacy ownership until one Actor owner commits."""

    legacy_signals: list[Any] = []
    waiting_registry = WaitingInputRegistry()
    waiting_future = waiting_registry.register("instance-a:private:user-a")
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        agent_handler=legacy_signals.append,
        claim_actor_owner=False,
        waiting_registry=waiting_registry,
    )
    key = _actor_key()
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="canary-a",
        ttl_seconds=30.0,
    )

    result = await ingress.process_event(_event(), adapter)
    await asyncio.sleep(0)

    assert result.message_log_id is not None
    assert result.matched_rules == []
    assert legacy_signals == []
    assert not waiting_future.done()
    with database.connect() as conn:
        job = conn.execute(
            """
            SELECT status, ownership_generation, admission_fence_id,
                   admission_fence_generation
            FROM message_routing_jobs
            """
        ).fetchone()
    assert tuple(job) == ("pending", 0, grant.fence.fence_id, grant.fence.generation)
    assert database.durable_routing.claim_next_job(worker_id="before-commit") is None

    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits reserved actor owner",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
        admission_grant=grant,
    ).ownership
    fence = database.actor_v2_admission_fences.get(key)
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED
    assert owner.admission_fence_id == fence.fence_id
    with database.connect() as conn:
        committed_job = conn.execute(
            """
            SELECT routing_job_id, ownership_generation, admission_fence_id,
                   admission_fence_generation
            FROM message_routing_jobs
            """
        ).fetchone()
    assert committed_job["ownership_generation"] == owner.generation
    assert committed_job["admission_fence_id"] == fence.fence_id
    assert committed_job["admission_fence_generation"] == fence.generation
    assert database.durable_routing.claim_job(
        str(committed_job["routing_job_id"]),
        worker_id="after-commit",
        ignore_available_at=True,
    ) is not None
    assert not waiting_future.done()
    waiting_registry.cancel("instance-a:private:user-a")


@pytest.mark.asyncio
async def test_local_ingress_freeze_uses_existing_reservation_for_new_messages(
    tmp_path: Path,
) -> None:
    """A post-freeze event is buffered by its fence instead of entering legacy."""

    legacy_signals: list[Any] = []
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        agent_handler=legacy_signals.append,
        claim_actor_owner=False,
    )
    grant = database.actor_v2_admission_fences.reserve(
        _actor_key(),
        holder_id="local-ingress-freeze",
        ttl_seconds=30.0,
    )
    ticket = ingress.freeze_legacy_ingress_session(
        "instance-a:private:user-a",
        cutover_id="cutover-a",
    )

    result = await ingress.process_event(_event(), adapter)
    receipt = await ingress.await_legacy_ingress_quiescent(
        ticket,
        timeout_seconds=0.0,
    )

    assert result.message_log_id is not None
    assert result.matched_rules == []
    assert legacy_signals == []
    assert receipt.status is LegacyIngressQuiescenceStatus.QUIESCENT
    assert ingress.thaw_legacy_ingress_session(ticket) is True
    with database.connect() as conn:
        job = conn.execute(
            """
            SELECT ownership_generation, admission_fence_id,
                   admission_fence_generation
            FROM message_routing_jobs
            """
        ).fetchone()
        ownership_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_runtime_ownership"
        ).fetchone()[0]
    assert tuple(job) == (0, grant.fence.fence_id, grant.fence.generation)
    assert ownership_count == 0


@pytest.mark.asyncio
async def test_local_ingress_freeze_uses_migrating_ownership_for_delayed_message(
    tmp_path: Path,
) -> None:
    """A callback delayed before core ingress cannot re-enter legacy after freeze.

    This models an adapter event that was already queued locally but invokes
    ``MessageIngress`` only after the durable ownership migration and local
    core-ingress freeze have both happened. No adapter pause is needed for the
    normalized message to become a fenced durable routing job.
    """

    legacy_signals: list[Any] = []
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        agent_handler=legacy_signals.append,
        claim_actor_owner=False,
    )
    key = _actor_key()
    legacy = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy source before migration barrier",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
    ).ownership
    migrating = database.agent_runtime_ownership.begin_migration(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        expected_generation=legacy.generation,
        reason="durable core ingress migration barrier",
        requested_by="test",
    )
    ticket = ingress.freeze_legacy_ingress_session(
        "instance-a:private:user-a",
        cutover_id="cutover-migrating-a",
    )

    result = await ingress.process_event(
        _event(message_id="adapter-queue-delayed-message"),
        adapter,
    )
    receipt = await ingress.await_legacy_ingress_quiescent(
        ticket,
        timeout_seconds=0.0,
    )

    assert result.message_log_id is not None
    assert result.matched_rules == []
    assert legacy_signals == []
    assert receipt.status is LegacyIngressQuiescenceStatus.QUIESCENT
    assert ingress.thaw_legacy_ingress_session(ticket) is True
    with database.connect() as conn:
        job = conn.execute(
            """
            SELECT status, ownership_generation, admission_fence_id,
                   admission_fence_generation
            FROM message_routing_jobs
            """
        ).fetchone()
        ownership = conn.execute(
            """
            SELECT mode, status, generation
            FROM agent_session_runtime_ownership
            """
        ).fetchone()
    assert tuple(job) == ("pending", migrating.generation, "", 0)
    assert tuple(ownership) == ("legacy", "migrating", migrating.generation)


@pytest.mark.asyncio
async def test_route_decision_commits_before_observer_target_is_invoked(
    tmp_path: Path,
) -> None:
    table = RouteTable()
    targets = RouteTargetRegistry()
    observed_state: list[tuple[str, int]] = []
    database_ref: DatabaseManager | None = None

    def observer(_context: RouteDispatchContext, _rule: RouteRule) -> None:
        assert database_ref is not None
        with database_ref.connect() as conn:
            job_status = conn.execute(
                "SELECT status FROM message_routing_jobs"
            ).fetchone()[0]
            outbox_count = conn.execute(
                "SELECT COUNT(*) FROM agent_route_outbox"
            ).fetchone()[0]
        observed_state.append((str(job_status), int(outbox_count)))

    table.register(
        RouteRule(
            id="observer.audit",
            priority=100,
            condition=RouteCondition(event_types=frozenset({"message-created"})),
            target="observer",
            match_mode=RouteMatchMode.OBSERVE,
        )
    )
    table.register(make_agent_entry_fallback_route_rule())
    targets.register("observer", observer)
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        route_table=table,
        route_targets=targets,
    )
    database_ref = database

    await ingress.process_event(_event(), adapter)

    assert observed_state == [("completed", 1)]


@pytest.mark.asyncio
async def test_duplicate_platform_event_reuses_message_job_and_delivery(
    tmp_path: Path,
) -> None:
    ingress, database, adapter = _build_actor_ingress(tmp_path)
    event = _event()

    first = await ingress.process_event(event, adapter)
    second = await ingress.process_event(event, adapter)

    assert second.message_log_id == first.message_log_id
    with database.connect() as conn:
        counts = tuple(
            conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM message_logs),
                    (SELECT COUNT(*) FROM message_routing_jobs),
                    (SELECT COUNT(*) FROM agent_route_outbox)
                """
            ).fetchone()
        )
    assert counts == (1, 1, 1)


@pytest.mark.asyncio
async def test_live_ingress_claims_job_before_running_slow_hook(
    tmp_path: Path,
) -> None:
    ingress, database, adapter = _build_actor_ingress(tmp_path)
    hook_entered = asyncio.Event()
    release_hook = asyncio.Event()

    async def slow_hook(_context: RouteDispatchContext) -> None:
        hook_entered.set()
        await release_hook.wait()

    ingress.add_pre_route_hook(slow_hook)
    process_task = asyncio.create_task(ingress.process_event(_event(), adapter))
    await asyncio.wait_for(hook_entered.wait(), timeout=1.0)

    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, claim_id, lease_owner FROM message_routing_jobs"
        ).fetchone()
    assert row["status"] == "processing"
    assert row["claim_id"]
    assert str(row["lease_owner"]).startswith("message-ingress:")
    assert database.durable_routing.claim_next_job(worker_id="recovery") is None

    release_hook.set()
    await process_task


@pytest.mark.asyncio
async def test_live_routing_timeout_releases_job_for_recovery(tmp_path: Path) -> None:
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        durable_recovery_grace_seconds=0.0,
        durable_routing_timeout_seconds=0.02,
    )

    async def blocked_hook(_context: RouteDispatchContext) -> None:
        await asyncio.Event().wait()

    ingress.add_pre_route_hook(blocked_hook)

    with pytest.raises(TimeoutError):
        await ingress.process_event(_event(), adapter)

    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, claim_id, lease_owner, last_error_code FROM message_routing_jobs"
        ).fetchone()
    assert tuple(row) == ("pending", "", "", "TimeoutError")


class _WakeTarget:
    def __init__(self) -> None:
        self.accepting = True
        self.woken: list[SessionKey] = []
        self.recover_count = 0

    async def wake(self, key: SessionKey) -> None:
        self.woken.append(key)

    async def recover(self) -> int:
        self.recover_count += 1
        return 0

class _MailboxHandoffNotifier:
    """Capture advisory mailbox hints without acting as a handoff target."""

    def __init__(self, *, fail: bool = False) -> None:
        self.mailbox_ids: list[int] = []
        self._fail = fail

    async def notify(self, mailbox_id: int) -> None:
        self.mailbox_ids.append(mailbox_id)
        if self._fail:
            raise RuntimeError("synthetic mailbox handoff notifier failure")


async def _wait_for_relay_count(
    service: DurableRoutingService,
    expected: int,
    *,
    timeout: float = 2.0,
) -> None:
    """Wait until the route service has committed the expected relay count."""

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if service.health_snapshot().relayed_delivery_count >= expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"route relay count did not reach {expected}")


@pytest.mark.asyncio
async def test_fenced_route_relay_notifies_exact_mailbox_handoff_id(
    tmp_path: Path,
) -> None:
    """A live fenced relay only publishes its durable mailbox identity."""

    ingress, database, adapter = _build_actor_ingress(tmp_path, claim_actor_owner=False)
    key = _actor_key()
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="route-mailbox-notifier",
        ttl_seconds=30.0,
    )
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits fenced actor owner",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
        admission_grant=grant,
    ).ownership
    notifier = _MailboxHandoffNotifier()
    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: adapter,
        mailbox_handoff_notifier=notifier,
        poll_interval_seconds=0.01,
    )
    ingress.set_durable_routing_wake_callback(service.wake)

    try:
        await service.start()
        await ingress.process_event(_event(), adapter)
        await _wait_for_relay_count(service, 1)

        assert len(notifier.mailbox_ids) == 1
        mailbox_id = notifier.mailbox_ids[0]
        handoff = database.actor_v2_mailbox_handoffs.read(mailbox_id)
        assert handoff is not None
        assert handoff.state is MailboxHandoffState.PENDING
        assert handoff.evidence.state is MailboxHandoffEvidenceState.FENCED
        assert handoff.evidence.identity.mailbox_id == mailbox_id
        assert handoff.evidence.identity.key == key
        assert handoff.evidence.identity.ownership_generation == owner.generation
        assert handoff.evidence.admission_fence_id == grant.fence.fence_id
        assert handoff.evidence.admission_fence_generation == grant.fence.generation
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_fenced_route_notifier_failure_never_falls_back_to_legacy_wake(
    tmp_path: Path,
) -> None:
    """A failed hint leaves durable handoff debt instead of waking by session key."""

    ingress, database, adapter = _build_actor_ingress(tmp_path, claim_actor_owner=False)
    key = _actor_key()
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="route-notifier-failure",
        ttl_seconds=30.0,
    )
    database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits fenced actor owner",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
        admission_grant=grant,
    )
    legacy_target = _WakeTarget()
    notifier = _MailboxHandoffNotifier(fail=True)
    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: adapter,
        actor_wake_target=legacy_target,
        mailbox_handoff_notifier=notifier,
        poll_interval_seconds=0.01,
    )
    ingress.set_durable_routing_wake_callback(service.wake)

    try:
        await service.start()
        recover_count_before_relay = legacy_target.recover_count
        await ingress.process_event(_event(), adapter)
        await _wait_for_relay_count(service, 1)
        await asyncio.sleep(0.03)

        assert legacy_target.woken == []
        assert legacy_target.recover_count == recover_count_before_relay
        assert len(notifier.mailbox_ids) == 1
        handoff = database.actor_v2_mailbox_handoffs.read(notifier.mailbox_ids[0])
        assert handoff is not None
        assert handoff.state is MailboxHandoffState.PENDING
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_fenced_route_without_notifier_stays_durable_and_blocks_legacy_recovery(
    tmp_path: Path,
) -> None:
    """Legacy recovery cannot consume a fenced route when no hint sink exists."""

    ingress, database, adapter = _build_actor_ingress(tmp_path, claim_actor_owner=False)
    key = _actor_key()
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="route-no-notifier",
        ttl_seconds=30.0,
    )
    database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits fenced actor owner",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
        admission_grant=grant,
    )
    legacy_target = _WakeTarget()
    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: adapter,
        actor_wake_target=legacy_target,
        poll_interval_seconds=0.01,
    )
    ingress.set_durable_routing_wake_callback(service.wake)

    try:
        await service.start()
        recover_count_before_relay = legacy_target.recover_count
        await ingress.process_event(_event(), adapter)
        await _wait_for_relay_count(service, 1)
        await asyncio.sleep(0.03)

        assert legacy_target.woken == []
        assert legacy_target.recover_count == recover_count_before_relay
        health = service.health_snapshot()
        assert health.degraded_reason == "mailbox_handoff_notifier_unavailable"
        with database.connect() as conn:
            row = conn.execute(
                "SELECT mailbox_id FROM agent_session_mailbox"
            ).fetchone()
        assert row is not None
        handoff = database.actor_v2_mailbox_handoffs.read(int(row["mailbox_id"]))
        assert handoff is not None
        assert handoff.state is MailboxHandoffState.PENDING
    finally:
        await service.shutdown()


@pytest.mark.asyncio
async def test_scoped_fenced_relay_persists_only_its_target_handoff_without_notifier(
    tmp_path: Path,
) -> None:
    """An explicit target relay cannot claim a sibling fenced request."""

    ingress, database, adapter = _build_actor_ingress(tmp_path, claim_actor_owner=False)
    first_key = _actor_key()
    second_key = SessionKey("bot-a", "bot-a:private:user-b")
    first_grant = database.actor_v2_admission_fences.reserve(
        first_key,
        holder_id="scoped-fenced-relay-first",
        ttl_seconds=30.0,
    )
    first_owner = database.agent_runtime_ownership.claim(
        first_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits first fenced actor owner",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
        admission_grant=first_grant,
    ).ownership
    second_grant = database.actor_v2_admission_fences.reserve(
        second_key,
        holder_id="scoped-fenced-relay-second",
        ttl_seconds=30.0,
    )
    database.agent_runtime_ownership.claim(
        second_key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits second fenced actor owner",
        legacy_session_id="instance-a:private:user-b",
        requested_by="test",
        admission_grant=second_grant,
    )
    request = FencedMailboxWakeRequest(
        key=first_key,
        ownership_generation=first_owner.generation,
        admission_fence_id=first_owner.admission_fence_id,
        admission_fence_generation=first_owner.admission_fence_generation,
    )
    await ingress.process_event(_event(message_id="first-message", user_id="user-a"), adapter)
    await ingress.process_event(_event(message_id="second-message", user_id="user-b"), adapter)
    service = FencedDurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: adapter,
        request=request,
        poll_interval_seconds=0.01,
    )

    try:
        started = await service.start()
        await _wait_for_relay_count(service, 1)
        await asyncio.sleep(0.03)

        assert service.persistence_domain is database
        assert started.actor_consumer_ready is False
        assert started.ready_for_actor_traffic is False
        assert started.fenced_request_scoped is True
        assert started.fenced_scope_live is True
        assert started.status is DurableRoutingServiceStatus.RUNNING
        with database.connect() as conn:
            outbox_rows = conn.execute(
                """
                SELECT profile_id, session_id, status
                FROM agent_route_outbox
                ORDER BY session_id
                """
            ).fetchall()
            mailbox_rows = conn.execute(
                """
                SELECT mailbox_id, profile_id, session_id
                FROM agent_session_mailbox
                ORDER BY session_id
                """
            ).fetchall()
        assert [tuple(row) for row in outbox_rows] == [
            ("bot-a", first_key.session_id, "completed"),
            ("bot-a", second_key.session_id, "pending"),
        ]
        assert [tuple(row)[1:] for row in mailbox_rows] == [
            ("bot-a", first_key.session_id),
        ]
        handoff = database.actor_v2_mailbox_handoffs.read(int(mailbox_rows[0]["mailbox_id"]))
        assert handoff is not None
        assert handoff.evidence.as_fenced_wake_request() == request
    finally:
        await service.shutdown()



async def _wait_for_job_status(
    database: DatabaseManager,
    expected: str,
    *,
    timeout: float = 2.0,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        with database.connect() as conn:
            row = conn.execute(
                "SELECT status FROM message_routing_jobs"
            ).fetchone()
        if row is not None and row["status"] == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"routing job did not reach {expected!r}")


@pytest.mark.asyncio
async def test_missing_actor_consumer_keeps_outbox_pending_and_reports_degraded(
    tmp_path: Path,
) -> None:
    ingress, database, adapter = _build_actor_ingress(tmp_path)
    await ingress.process_event(_event(), adapter)
    replay_calls: list[str] = []

    async def replay(_claim: Any, _adapter: BaseAdapter) -> None:
        replay_calls.append("called")

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=replay,
        adapter_resolver=lambda _instance_id: adapter,
    )

    prepared = await service.prepare()
    started = await service.start()
    await asyncio.sleep(0.03)

    assert prepared.pending_delivery_count == 1
    assert started.degraded_reason == "actor_consumer_unavailable"
    assert started.ready_for_actor_traffic is False
    assert replay_calls == []
    with database.connect() as conn:
        outbox_status = conn.execute(
            "SELECT status FROM agent_route_outbox"
        ).fetchone()[0]
        mailbox_count = conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0]
    assert outbox_status == "pending"
    assert mailbox_count == 0
    await service.shutdown()


@pytest.mark.asyncio
async def test_missing_actor_consumer_does_not_claim_pending_actor_job(
    tmp_path: Path,
) -> None:
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        durable_recovery_grace_seconds=0.0,
        durable_routing_timeout_seconds=0.02,
    )

    async def blocked_hook(_context: RouteDispatchContext) -> None:
        await asyncio.Event().wait()

    ingress.add_pre_route_hook(blocked_hook)
    with pytest.raises(TimeoutError):
        await ingress.process_event(_event(), adapter)
    with database.connect() as conn:
        before = conn.execute(
            "SELECT status, attempt_count, claim_id, lease_owner FROM message_routing_jobs"
        ).fetchone()

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: adapter,
        poll_interval_seconds=0.01,
    )
    await service.start()
    await asyncio.sleep(0.05)

    with database.connect() as conn:
        after = conn.execute(
            "SELECT status, attempt_count, claim_id, lease_owner FROM message_routing_jobs"
        ).fetchone()
    assert tuple(after) == tuple(before) == ("pending", 1, "", "")
    assert service.health_snapshot().degraded_reason == "actor_consumer_unavailable"
    await service.shutdown()


@pytest.mark.asyncio
async def test_prepare_commits_fenced_sidecar_without_publishing_advisory_hint(
    tmp_path: Path,
) -> None:
    """Prepare may relay durable fenced work without impersonating a dispatcher."""

    ingress, database, adapter = _build_actor_ingress(tmp_path, claim_actor_owner=False)
    key = _actor_key()
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="prepare-fenced-sidecar",
        ttl_seconds=30.0,
    )
    database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="test commits fenced actor owner",
        legacy_session_id="instance-a:private:user-a",
        requested_by="test",
        admission_grant=grant,
    )
    await ingress.process_event(_event(), adapter)
    notifier = _MailboxHandoffNotifier()

    async def replay(_claim: Any, _adapter: BaseAdapter) -> None:
        raise AssertionError("prepare must not replay undecided routing jobs")

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=replay,
        adapter_resolver=lambda _instance_id: adapter,
        mailbox_handoff_notifier=notifier,
        poll_interval_seconds=0.01,
    )

    prepared = await service.prepare()

    assert prepared.pending_delivery_count == 0
    assert prepared.wake_debt_count == 0
    assert notifier.mailbox_ids == []
    with database.connect() as conn:
        assert conn.execute(
            "SELECT status FROM agent_route_outbox"
        ).fetchone()[0] == "completed"
        mailbox_id = int(
            conn.execute("SELECT mailbox_id FROM agent_session_mailbox").fetchone()[0]
        )
    handoff = database.actor_v2_mailbox_handoffs.read(mailbox_id)
    assert handoff is not None
    assert handoff.state is MailboxHandoffState.PENDING

    await service.start()
    assert notifier.mailbox_ids == []
    await service.shutdown()


@pytest.mark.asyncio
async def test_prepare_does_not_execute_undecided_routing_job(tmp_path: Path) -> None:
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        durable_recovery_grace_seconds=0.0,
        durable_routing_timeout_seconds=0.02,
    )
    first_call = True

    async def one_slow_hook(_context: RouteDispatchContext) -> None:
        nonlocal first_call
        if first_call:
            first_call = False
            await asyncio.Event().wait()

    ingress.add_pre_route_hook(one_slow_hook)
    with pytest.raises(TimeoutError):
        await ingress.process_event(_event(), adapter)

    replay_calls: list[str] = []

    async def replay(_claim: Any, _adapter: BaseAdapter) -> None:
        replay_calls.append("called")

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=replay,
        adapter_resolver=lambda _instance_id: adapter,
        actor_wake_target=_WakeTarget(),
    )

    await service.prepare()

    assert replay_calls == []
    with database.connect() as conn:
        assert conn.execute(
            "SELECT status FROM message_routing_jobs"
        ).fetchone()[0] == "pending"
    await service.shutdown()


@pytest.mark.asyncio
async def test_adapter_becoming_available_allows_bounded_recovery(
    tmp_path: Path,
) -> None:
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        durable_recovery_grace_seconds=0.0,
        durable_routing_timeout_seconds=0.02,
    )
    first_call = True

    async def one_slow_hook(_context: RouteDispatchContext) -> None:
        nonlocal first_call
        if first_call:
            first_call = False
            await asyncio.Event().wait()

    ingress.add_pre_route_hook(one_slow_hook)
    with pytest.raises(TimeoutError):
        await ingress.process_event(_event(), adapter)

    available = False
    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: adapter if available else None,
        actor_wake_target=_WakeTarget(),
        poll_interval_seconds=0.01,
        retry_base_seconds=0.01,
        retry_max_seconds=0.02,
        max_attempts=4,
    )
    await service.start()
    while True:
        with database.connect() as conn:
            attempt_count = conn.execute(
                "SELECT attempt_count FROM message_routing_jobs"
            ).fetchone()[0]
        if attempt_count >= 2:
            break
        await asyncio.sleep(0.005)
    available = True
    service.wake()

    await _wait_for_job_status(database, "completed")

    with database.connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM agent_route_outbox"
        ).fetchone()[0] == 1
    await service.shutdown()


@pytest.mark.asyncio
async def test_retry_exhaustion_marks_job_and_message_failed(tmp_path: Path) -> None:
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        durable_recovery_grace_seconds=0.0,
        durable_routing_timeout_seconds=0.02,
    )

    async def blocked_hook(_context: RouteDispatchContext) -> None:
        await asyncio.Event().wait()

    ingress.add_pre_route_hook(blocked_hook)
    with pytest.raises(TimeoutError):
        await ingress.process_event(_event(), adapter)

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=ingress.replay_claimed_routing_job,
        adapter_resolver=lambda _instance_id: None,
        actor_wake_target=_WakeTarget(),
        poll_interval_seconds=0.01,
        retry_base_seconds=0.0,
        retry_max_seconds=0.01,
        max_attempts=2,
    )
    await service.start()

    await _wait_for_job_status(database, "failed")

    with database.connect() as conn:
        message_status = conn.execute(
            "SELECT routing_status FROM message_logs"
        ).fetchone()[0]
    assert message_status == "failed"
    await service.shutdown()


@pytest.mark.asyncio
async def test_shutdown_releases_inflight_job_claim(tmp_path: Path) -> None:
    ingress, database, adapter = _build_actor_ingress(
        tmp_path,
        durable_recovery_grace_seconds=0.0,
        durable_routing_timeout_seconds=0.02,
    )
    first_call = True

    async def one_slow_hook(_context: RouteDispatchContext) -> None:
        nonlocal first_call
        if first_call:
            first_call = False
            await asyncio.Event().wait()

    ingress.add_pre_route_hook(one_slow_hook)
    with pytest.raises(TimeoutError):
        await ingress.process_event(_event(), adapter)

    replay_started = asyncio.Event()

    async def blocked_replay(_claim: Any, _adapter: BaseAdapter) -> None:
        replay_started.set()
        await asyncio.Event().wait()

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=blocked_replay,
        adapter_resolver=lambda _instance_id: adapter,
        actor_wake_target=_WakeTarget(),
        poll_interval_seconds=0.01,
    )
    await service.start()
    await asyncio.wait_for(replay_started.wait(), timeout=1.0)

    await service.shutdown()

    with database.connect() as conn:
        row = conn.execute(
            "SELECT status, claim_id, lease_owner, last_error_code FROM message_routing_jobs"
        ).fetchone()
    assert tuple(row) == ("pending", "", "", "service_shutdown")
