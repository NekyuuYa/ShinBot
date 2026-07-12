"""Production-path tests for actor-owned durable ingress routing."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from shinbot.core.application.bot_routing import BotRuntimeRouter
from shinbot.core.application.bots_config import (
    BotAgentConfig,
    BotBindingConfig,
    BotServiceConfig,
)
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.dispatchers import (
    AGENT_ENTRY_TARGET,
    AgentEntryDispatcher,
    make_agent_entry_fallback_route_rule,
)
from shinbot.core.dispatch.durable_routing_service import DurableRoutingService
from shinbot.core.dispatch.ingress import (
    MessageIngress,
    RouteDispatchContext,
    RouteTargetRegistry,
)
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


def _event(*, message_id: str = "message-a") -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        self_id="bot-self",
        platform="mock",
        user=User(id="user-a", name="Alice"),
        channel=Channel(id="user-a", type=1),
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
        bot_router=BotRuntimeRouter((bot,)),
        durable_recovery_grace_seconds=durable_recovery_grace_seconds,
        durable_routing_timeout_seconds=durable_routing_timeout_seconds,
    )
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
async def test_consumer_mount_relays_before_start_then_recovers_wake_debt(
    tmp_path: Path,
) -> None:
    ingress, database, adapter = _build_actor_ingress(tmp_path)
    await ingress.process_event(_event(), adapter)
    target = _WakeTarget()

    async def replay(_claim: Any, _adapter: BaseAdapter) -> None:
        raise AssertionError("prepare must not replay undecided routing jobs")

    service = DurableRoutingService(
        repository=database.durable_routing,
        replay=replay,
        adapter_resolver=lambda _instance_id: adapter,
        actor_wake_target=target,
        poll_interval_seconds=0.01,
    )

    prepared = await service.prepare()

    assert prepared.pending_delivery_count == 0
    assert prepared.wake_debt_count == 1
    assert target.woken == []
    with database.connect() as conn:
        assert conn.execute(
            "SELECT status FROM agent_route_outbox"
        ).fetchone()[0] == "completed"
        assert conn.execute(
            "SELECT COUNT(*) FROM agent_session_mailbox"
        ).fetchone()[0] == 1

    started = await service.start()

    assert started.ready_for_actor_traffic is True
    assert target.recover_count >= 1
    assert service.health_snapshot().wake_debt_count == 0
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
