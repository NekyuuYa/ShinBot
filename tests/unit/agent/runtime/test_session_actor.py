from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, replace

import pytest

from shinbot.agent.runtime.session_actor.actor import AgentSessionActor, SessionActorStore
from shinbot.agent.runtime.session_actor.aggregate import (
    AgentSessionAggregate,
    SessionKey,
)
from shinbot.agent.runtime.session_actor.effect_contracts import (
    EffectContractAuthority,
    EffectExecutionContract,
    EffectLane,
    builtin_effect_contract,
    builtin_session_actor_effect_contracts,
)
from shinbot.agent.runtime.session_actor.events import (
    ClaimedSessionEvent,
    EventEnqueueResult,
    MailboxEventStatus,
    ReviewScheduleStatus,
    SessionEffect,
    SessionEventEnvelope,
    SessionOperation,
    SessionOperationStatus,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.key_factory import (
    DEFAULT_SESSION_ACTOR_PROFILE_ID,
    SessionKeyFactory,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
)
from shinbot.agent.runtime.session_actor.recovery_commit import (
    RecoveryDeliveryClaimLost,
)
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.agent_signals import (
    AgentSignal,
    AgentSignalKind,
    AgentSignalSource,
)
from shinbot.persistence import DatabaseManager

_TEST_EFFECT_CONTRACTS = {
    kind: EffectExecutionContract(
        effect_kind=kind,
        version=1,
        lane=EffectLane.DEFAULT,
        completion_event_kind="TestEffectCompleted",
    )
    for kind in ("record", "run_review")
}
_TEST_EFFECT_AUTHORITY = EffectContractAuthority(
    (*builtin_session_actor_effect_contracts(), *_TEST_EFFECT_CONTRACTS.values())
)


def _schedule_outcome_metadata(
    *,
    plan_revision: int,
    applied_delay_seconds: float,
) -> dict[str, object]:
    return {
        "plan_revision": plan_revision,
        "schedule_outcome": {
            "active_reply_threshold": {},
            "applied_delay_seconds": applied_delay_seconds,
            "fallback_reason": "",
            "kind": "",
            "mention_sensitivity": "normal",
            "model_execution_id": "",
            "prompt_signature": "",
            "reason": "",
            "requested_delay_seconds": None,
            "source": "",
        },
    }


@dataclass(slots=True)
class _EventRecord:
    envelope: SessionEventEnvelope
    status: MailboxEventStatus = MailboxEventStatus.PENDING
    attempt_count: int = 0
    claim_id: str = ""
    worker_id: str = ""


class _MemorySessionActorStore(SessionActorStore):
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.aggregates: dict[SessionKey, AgentSessionAggregate] = {}
        self.records: dict[tuple[SessionKey, str], _EventRecord] = {}
        self.order: dict[SessionKey, list[str]] = {}
        self.effects: list[SessionEffect] = []
        self.operations: list[SessionOperation] = []
        self.review_schedules: list[SessionReviewSchedule] = []
        self.review_schedule_events: list[SessionReviewScheduleEvent] = []
        self.release_errors: list[str] = []
        self.failed_errors: list[str] = []

    @property
    def persistence_domain(self) -> object:
        return self

    @property
    def effect_contract_authority(self) -> EffectContractAuthority:
        return _TEST_EFFECT_AUTHORITY

    async def enqueue(self, envelope: SessionEventEnvelope) -> EventEnqueueResult:
        async with self._lock:
            record_key = (envelope.key, envelope.event_id)
            existing = self.records.get(record_key)
            if existing is not None:
                return EventEnqueueResult(
                    event_id=envelope.event_id,
                    key=envelope.key,
                    inserted=False,
                    status=existing.status,
                )
            self.records[record_key] = _EventRecord(envelope=envelope)
            self.order.setdefault(envelope.key, []).append(envelope.event_id)
            self.aggregates.setdefault(
                envelope.key,
                AgentSessionAggregate(key=envelope.key),
            )
            return EventEnqueueResult(
                event_id=envelope.event_id,
                key=envelope.key,
                inserted=True,
            )

    async def ensure(self, key: SessionKey) -> AgentSessionAggregate:
        async with self._lock:
            return self.aggregates.setdefault(key, AgentSessionAggregate(key=key))

    async def load(self, key: SessionKey) -> AgentSessionAggregate:
        async with self._lock:
            return self.aggregates[key]

    async def claim_next(
        self,
        key: SessionKey,
        *,
        worker_id: str,
    ) -> ClaimedSessionEvent | None:
        async with self._lock:
            for event_id in self.order.get(key, []):
                record = self.records[(key, event_id)]
                if record.status != MailboxEventStatus.PENDING:
                    continue
                record.status = MailboxEventStatus.PROCESSING
                record.attempt_count += 1
                record.claim_id = uuid.uuid4().hex
                record.worker_id = worker_id
                return ClaimedSessionEvent(
                    claim_id=record.claim_id,
                    envelope=record.envelope,
                    worker_id=worker_id,
                    attempt_count=record.attempt_count,
                )
            return None

    async def commit(
        self,
        claim: ClaimedSessionEvent,
        transition: SessionTransition,
        *,
        expected_revision: int,
    ) -> AgentSessionAggregate:
        async with self._lock:
            record = self.records[(claim.key, claim.envelope.event_id)]
            assert record.status == MailboxEventStatus.PROCESSING
            assert record.claim_id == claim.claim_id
            current = self.aggregates[claim.key]
            if current.state_revision != expected_revision:
                raise RuntimeError("aggregate revision conflict")
            self.aggregates[claim.key] = transition.aggregate
            self.effects.extend(transition.effects)
            self.operations.extend(transition.operations)
            self.review_schedules.extend(transition.review_schedules)
            self.review_schedule_events.extend(transition.review_schedule_events)
            record.status = MailboxEventStatus.COMPLETED
            return transition.aggregate

    async def release(
        self,
        claim: ClaimedSessionEvent,
        *,
        error: str,
    ) -> None:
        async with self._lock:
            record = self.records[(claim.key, claim.envelope.event_id)]
            if record.status != MailboxEventStatus.PROCESSING:
                return
            if record.claim_id != claim.claim_id:
                return
            record.status = MailboxEventStatus.PENDING
            record.claim_id = ""
            record.worker_id = ""
            self.release_errors.append(error)

    async def fail(
        self,
        claim: ClaimedSessionEvent,
        *,
        error: str,
    ) -> None:
        async with self._lock:
            record = self.records[(claim.key, claim.envelope.event_id)]
            if record.status != MailboxEventStatus.PROCESSING:
                return
            if record.claim_id != claim.claim_id:
                return
            record.status = MailboxEventStatus.FAILED
            record.claim_id = ""
            record.worker_id = ""
            self.aggregates[claim.key] = self.aggregates[claim.key].advance(
                state_changed=False
            )
            self.failed_errors.append(error)

    async def recover(self, key: SessionKey, *, worker_id: str) -> int:
        del worker_id
        recovered = 0
        async with self._lock:
            for event_id in self.order.get(key, []):
                record = self.records[(key, event_id)]
                if record.status != MailboxEventStatus.PROCESSING:
                    continue
                record.status = MailboxEventStatus.PENDING
                record.claim_id = ""
                record.worker_id = ""
                recovered += 1
        return recovered

    async def pending_keys(self) -> list[SessionKey]:
        async with self._lock:
            return sorted(
                {
                    key
                    for (key, _event_id), record in self.records.items()
                    if record.status == MailboxEventStatus.PENDING
                }
            )

    async def has_pending_for_key(self, key: SessionKey) -> bool:
        async with self._lock:
            return any(
                record_key == key
                and record.status
                in {MailboxEventStatus.PENDING, MailboxEventStatus.PROCESSING}
                for (record_key, _event_id), record in self.records.items()
            )


def _event(
    event_id: str,
    *,
    profile_id: str = "profile-a",
    session_id: str = "bot:group:room",
) -> SessionEventEnvelope:
    return SessionEventEnvelope(
        event_id=event_id,
        key=SessionKey(profile_id=profile_id, session_id=session_id),
        kind="test",
        payload={"event_id": event_id},
    )


def _typed_recovery_event(event_id: str) -> SessionEventEnvelope:
    """Build a scanner-owned delivery for actor failure-boundary tests."""

    return SessionEventEnvelope(
        event_id=event_id,
        key=SessionKey(profile_id="profile-a", session_id="bot:group:room"),
        kind=RECOVERY_DELIVERY_EVENT_KIND,
        source=RECOVERY_DELIVERY_EVENT_SOURCE,
        payload={"event_id": event_id},
        ownership_generation=1,
    )


def test_session_key_rejects_empty_profile_identity() -> None:
    with pytest.raises(ValueError, match="profile_id"):
        SessionKey(profile_id="", session_id="bot:group:room")


def test_session_aggregate_deeply_freezes_json_state() -> None:
    source_items = [{"value": 1}]
    source_data = {"nested": {"items": source_items}}
    aggregate = AgentSessionAggregate(
        key=SessionKey("profile-a", "session-a"),
        data=source_data,
    )

    source_items[0]["value"] = 2
    source_items.append({"value": 3})

    assert aggregate.data == {"nested": {"items": [{"value": 1}]}}
    with pytest.raises(TypeError, match="immutable"):
        aggregate.data["nested"]["items"].append({"value": 4})
    with pytest.raises(TypeError, match="immutable"):
        aggregate.data["nested"]["items"][0]["value"] = 5


def test_session_aggregate_advance_enforces_authoritative_fences() -> None:
    aggregate = AgentSessionAggregate(
        key=SessionKey("profile-a", "session-a"),
        activity_generation=3,
        active_epoch=2,
        updated_at=10.0,
    )

    observed = aggregate.advance(state_changed=False, updated_at=11.0)

    assert observed.state_revision == aggregate.state_revision
    assert observed.event_sequence == aggregate.event_sequence + 1
    assert observed.updated_at == 11.0
    with pytest.raises(ValueError, match="state_changed=False"):
        aggregate.advance(state_changed=False, state="review")
    with pytest.raises(ValueError, match="active_epoch cannot move backwards"):
        aggregate.advance(active_epoch=1)
    with pytest.raises(ValueError, match="activity_generation cannot move backwards"):
        aggregate.advance(activity_generation=2)
    with pytest.raises(ValueError, match="updated_at cannot move backwards"):
        aggregate.advance(updated_at=9.0)

    planned = aggregate.advance(
        current_plan_id="plan-1",
        review_plan_revision=1,
    )
    assert planned.current_plan_id == "plan-1"
    assert planned.review_plan_revision == 1
    with pytest.raises(ValueError, match="must advance together"):
        planned.advance(current_plan_id="plan-2")
    with pytest.raises(ValueError, match="must advance together"):
        planned.advance(review_plan_revision=2)
    with pytest.raises(ValueError, match="advance exactly once"):
        planned.advance(current_plan_id="plan-3", review_plan_revision=3)


def test_session_key_factory_prefers_stable_routing_identity() -> None:
    factory = SessionKeyFactory()

    key = factory.create(
        bot_config_id=" bot-config-a ",
        bot_id="bot-config-a",
        bot_session_id=" bot-config-a:group:guild:room ",
        base_session_id="adapter-instance:group:guild:room",
    )

    assert key == SessionKey(
        profile_id="bot-config-a",
        session_id="bot-config-a:group:guild:room",
    )


def test_session_key_factory_has_one_canonical_fallback() -> None:
    factory = SessionKeyFactory()

    bot_key = factory.create(
        bot_config_id="bot-config-a",
        bot_id="bot-config-a",
        base_session_id="adapter-instance:group:room",
    )
    default_key = factory.create(base_session_id="adapter-instance:private:user")

    assert bot_key == SessionKey(
        profile_id="bot-config-a",
        session_id="bot-config-a:adapter-instance:group:room",
    )
    assert default_key == SessionKey(
        profile_id=DEFAULT_SESSION_ACTOR_PROFILE_ID,
        session_id=(
            f"{DEFAULT_SESSION_ACTOR_PROFILE_ID}:adapter-instance:private:user"
        ),
    )
    with pytest.raises(ValueError, match="base_session_id"):
        factory.create()
    with pytest.raises(ValueError, match="reserved"):
        factory.create(
            bot_config_id=DEFAULT_SESSION_ACTOR_PROFILE_ID,
            base_session_id="session",
        )


def test_session_key_factory_uses_signal_bot_config_identity() -> None:
    factory = SessionKeyFactory()
    bot_signal = AgentSignal(
        signal_id="bot-signal",
        kind=AgentSignalKind.MESSAGE,
        source=AgentSignalSource.MESSAGE_INGRESS,
        session_id="adapter-instance:group:room",
        occurred_at=1.0,
        bot_id="bot-config-a",
        bot_session_id="bot-config-a:group:room",
    )
    default_signal = replace(
        bot_signal,
        signal_id="default-signal",
        bot_id="",
        bot_session_id="",
    )

    assert factory.from_signal(bot_signal) == SessionKey(
        "bot-config-a",
        "bot-config-a:group:room",
    )
    assert factory.from_signal(default_signal) == SessionKey(
        DEFAULT_SESSION_ACTOR_PROFILE_ID,
        f"{DEFAULT_SESSION_ACTOR_PROFILE_ID}:adapter-instance:group:room",
    )


@pytest.mark.asyncio
async def test_session_actor_handles_one_session_serially() -> None:
    store = _MemorySessionActorStore()
    handled: list[str] = []

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        handled.append(f"start:{envelope.event_id}")
        handled.append(f"end:{envelope.event_id}")
        return SessionTransition(
            aggregate=aggregate.advance(
                data={"handled": [*aggregate.data.get("handled", []), envelope.event_id]}
            ),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    try:
        for event_id in ("one", "two", "three"):
            await registry.submit(_event(event_id))
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert handled == [
            "start:one",
            "end:one",
            "start:two",
            "end:two",
            "start:three",
            "end:three",
        ]
        aggregate = store.aggregates[SessionKey("profile-a", "bot:group:room")]
        assert aggregate.state_revision == 3
        assert aggregate.event_sequence == 3
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_keeps_concurrent_submissions_without_lost_updates() -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(
                data={"count": int(aggregate.data.get("count", 0)) + 1}
            ),
            disposition="event_recorded",
            effects=(
                SessionEffect(
                    effect_id=f"effect:{envelope.event_id}",
                    kind="record",
                    contract_signature=_TEST_EFFECT_CONTRACTS["record"].signature,
                ),
            ),
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    try:
        results = await asyncio.gather(
            *(registry.submit(_event(f"event-{index}")) for index in range(40))
        )
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert all(result.inserted for result in results)
        aggregate = store.aggregates[SessionKey("profile-a", "bot:group:room")]
        assert aggregate.data["count"] == 40
        assert aggregate.event_sequence == 40
        assert len(store.effects) == 40
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_rejects_malformed_v2_effect_before_store_commit() -> None:
    store = _MemorySessionActorStore()
    contract = builtin_effect_contract("run_idle_review_planning")

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"handled": envelope.event_id}),
            disposition="invalid_effect",
            effects=(
                SessionEffect(
                    effect_id="invalid-effect",
                    kind=contract.effect_kind,
                    contract_version=contract.version,
                    contract_signature=contract.signature,
                    payload={"input_watermark": 10},
                ),
            ),
        )

    registry = AgentSessionActorRegistry(
        store=store,
        handler=handler,
        max_attempts=1,
    )
    envelope = _event("invalid-effect-event")
    try:
        await registry.submit(envelope)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        record = store.records[(envelope.key, envelope.event_id)]
        assert record.status is MailboxEventStatus.FAILED
        assert store.effects == []
        assert "missing declared outcome fence fields" in store.failed_errors[0]
        assert "handled" not in store.aggregates[envelope.key].data
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_transition_carries_only_declarative_durable_work() -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        next_aggregate = aggregate.advance(
            review_operation_id="operation-1",
            current_plan_id="plan-1",
            review_plan_revision=1,
            review_plan={
                "plan_id": "plan-1",
                "plan_revision": 1,
                "applied_delay_seconds": 30.0,
            },
        )
        return SessionTransition(
            aggregate=next_aggregate,
            disposition="review_scheduled",
            caused_operation_id="operation-1",
            caused_plan_id="plan-1",
            effects=(
                SessionEffect(
                    effect_id="effect-1",
                    idempotency_key="send:operation-1:0",
                    kind="run_review",
                    contract_signature=_TEST_EFFECT_CONTRACTS["run_review"].signature,
                    operation_id="operation-1",
                ),
            ),
            operations=(
                SessionOperation(
                    operation_id="operation-1",
                    kind="review",
                    status=SessionOperationStatus.PENDING,
                    launched_by_event_id=envelope.event_id,
                    state_revision=next_aggregate.state_revision,
                ),
            ),
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-1",
                    plan_revision=1,
                    applied_delay_seconds=30.0,
                    status=ReviewScheduleStatus.SCHEDULED,
                ),
            ),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id="schedule-event-1",
                    event_type="scheduled",
                    plan_id="plan-1",
                    applied_delay_seconds=30.0,
                    committed_state_revision=next_aggregate.state_revision,
                    metadata=_schedule_outcome_metadata(
                        plan_revision=1,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    try:
        await registry.submit(_event("declarative-work"))
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert [effect.effect_id for effect in store.effects] == ["effect-1"]
        assert [operation.operation_id for operation in store.operations] == ["operation-1"]
        assert [schedule.plan_id for schedule in store.review_schedules] == ["plan-1"]
        assert store.review_schedules[0].scheduled_from is None
        assert store.review_schedules[0].next_review_at is None
        assert [
            event.schedule_event_id for event in store.review_schedule_events
        ] == ["schedule-event-1"]
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_rejects_plan_advance_without_schedule() -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        del envelope
        return SessionTransition(
            aggregate=aggregate.advance(
                current_plan_id="orphan-plan",
                review_plan_revision=1,
                review_plan={"plan_id": "orphan-plan", "plan_revision": 1},
            ),
            disposition="invalid_plan_advance",
            caused_plan_id="orphan-plan",
        )

    registry = AgentSessionActorRegistry(
        store=store,
        handler=handler,
        max_attempts=1,
    )
    envelope = _event("orphan-plan-event")
    try:
        await registry.submit(envelope)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        record = store.records[(envelope.key, envelope.event_id)]
        assert record.status is MailboxEventStatus.FAILED
        assert len(store.failed_errors) == 1
        assert "requires exactly one schedule" in store.failed_errors[0]
        assert store.review_schedules == []
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_rejects_plan_revision_jump_with_matching_schedule() -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        del envelope
        return SessionTransition(
            aggregate=replace(
                aggregate,
                current_plan_id="jumped-plan",
                review_plan_revision=99,
                review_plan={
                    "plan_id": "jumped-plan",
                    "plan_revision": 99,
                    "applied_delay_seconds": 30.0,
                },
                state_revision=aggregate.state_revision + 1,
                event_sequence=aggregate.event_sequence + 1,
            ),
            disposition="invalid_plan_jump",
            caused_plan_id="jumped-plan",
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="jumped-plan",
                    plan_revision=99,
                    applied_delay_seconds=30.0,
                ),
            ),
        )

    registry = AgentSessionActorRegistry(
        store=store,
        handler=handler,
        max_attempts=1,
    )
    envelope = _event("jumped-plan-event")
    try:
        await registry.submit(envelope)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        record = store.records[(envelope.key, envelope.event_id)]
        assert record.status is MailboxEventStatus.FAILED
        assert len(store.failed_errors) == 1
        assert "revision must advance by exactly one" in store.failed_errors[0]
        assert store.review_schedules == []
    finally:
        await registry.shutdown()


@pytest.mark.parametrize(
    "status",
    (
        ReviewScheduleStatus.COMPLETED,
        ReviewScheduleStatus.FAILED,
        ReviewScheduleStatus.SUPERSEDED,
    ),
)
@pytest.mark.asyncio
async def test_session_actor_rejects_terminal_schedule_for_new_plan(
    status: ReviewScheduleStatus,
) -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        del envelope
        schedule = SessionReviewSchedule(
            plan_id="terminal-plan",
            plan_revision=1,
            applied_delay_seconds=30.0,
            status=status,
        )
        return SessionTransition(
            aggregate=aggregate.advance(
                current_plan_id="terminal-plan",
                review_plan_revision=1,
                review_plan={
                    "plan_id": "terminal-plan",
                    "plan_revision": 1,
                    "applied_delay_seconds": 30.0,
                },
            ),
            disposition="invalid_terminal_plan",
            caused_plan_id="terminal-plan",
            review_schedules=(schedule,),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id=f"terminal-plan:{status.value}:scheduled",
                    event_type="scheduled",
                    plan_id="terminal-plan",
                    applied_delay_seconds=30.0,
                    metadata=_schedule_outcome_metadata(
                        plan_revision=1,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
        )

    registry = AgentSessionActorRegistry(
        store=store,
        handler=handler,
        max_attempts=1,
    )
    envelope = _event(f"terminal-plan:{status.value}")
    try:
        await registry.submit(envelope)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        record = store.records[(envelope.key, envelope.event_id)]
        assert record.status is MailboxEventStatus.FAILED
        assert len(store.failed_errors) == 1
        assert "must start scheduled" in store.failed_errors[0]
        assert store.review_schedules == []
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_deduplicates_event_ids_durably() -> None:
    store = _MemorySessionActorStore()
    call_count = 0

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        nonlocal call_count
        call_count += 1
        return SessionTransition(
            aggregate=aggregate.advance(),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    envelope = _event("same-event")
    try:
        first, second = await asyncio.gather(
            registry.submit(envelope),
            registry.submit(envelope),
        )
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert sorted([first.inserted, second.inserted]) == [False, True]
        assert call_count == 1
        assert store.aggregates[envelope.key].event_sequence == 1
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_releases_and_retries_infrastructure_failure() -> None:
    class _FailOnceCommitStore(_MemorySessionActorStore):
        def __init__(self) -> None:
            super().__init__()
            self.failed_once = False

        async def commit(
            self,
            claim: ClaimedSessionEvent,
            transition: SessionTransition,
            *,
            expected_revision: int,
        ) -> AgentSessionAggregate:
            if not self.failed_once:
                self.failed_once = True
                raise ConnectionError("database unavailable")
            return await super().commit(
                claim,
                transition,
                expected_revision=expected_revision,
            )

    store = _FailOnceCommitStore()
    attempts = 0

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        nonlocal attempts
        attempts += 1
        return SessionTransition(
            aggregate=aggregate.advance(),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(
        store=store,
        handler=handler,
        retry_delay_seconds=0.001,
    )
    envelope = _event("retry-event")
    try:
        await registry.submit(envelope)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        record = store.records[(envelope.key, envelope.event_id)]
        assert attempts == 2
        assert record.attempt_count == 2
        assert record.status == MailboxEventStatus.COMPLETED
        assert store.release_errors == ["ConnectionError: database unavailable"]
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_leaves_a_lost_recovery_claim_untouched() -> None:
    """A stale typed recovery claim must never enter release or dead-letter logic."""

    class _ClaimLostCommitStore(_MemorySessionActorStore):
        async def commit(
            self,
            claim: ClaimedSessionEvent,
            transition: SessionTransition,
            *,
            expected_revision: int,
        ) -> AgentSessionAggregate:
            del claim, transition, expected_revision
            raise RecoveryDeliveryClaimLost(
                "recovery_delivery_claim_attempt_count_changed"
            )

    store = _ClaimLostCommitStore()
    envelope = _event("lost-recovery-claim")
    await store.enqueue(envelope)

    def handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="recovery_claim_test",
            reason=event.kind,
        )

    actor = AgentSessionActor(
        key=envelope.key,
        store=store,
        handler=handler,
        worker_id="lost-recovery-claim-worker",
        max_attempts=1,
    )

    assert await actor._drain_mailbox() is False

    record = store.records[(envelope.key, envelope.event_id)]
    assert record.status is MailboxEventStatus.PROCESSING
    assert store.release_errors == []
    assert store.failed_errors == []
    assert store.aggregates[envelope.key].event_sequence == 0


@pytest.mark.parametrize("phase", ["reduce", "commit"])
@pytest.mark.asyncio
async def test_session_actor_preserves_unproven_typed_recovery_delivery(
    phase: str,
) -> None:
    """Typed decoder and proof failures must not enter generic dead-lettering."""

    class _CommitRejectedStore(_MemorySessionActorStore):
        async def commit(
            self,
            claim: ClaimedSessionEvent,
            transition: SessionTransition,
            *,
            expected_revision: int,
        ) -> AgentSessionAggregate:
            if phase == "commit":
                del claim, transition, expected_revision
                raise RuntimeError("recovery authority proof rejected")
            return await super().commit(
                claim,
                transition,
                expected_revision=expected_revision,
            )

    store = _CommitRejectedStore()
    envelope = _typed_recovery_event(f"unproven-recovery-{phase}")
    await store.enqueue(envelope)

    def handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        if phase == "reduce":
            return AgentSessionReducer().reduce(aggregate, event)
        return SessionTransition(
            aggregate=aggregate.advance(state_changed=False),
            disposition="recovery_claim_test",
            reason=event.kind,
        )

    actor = AgentSessionActor(
        key=envelope.key,
        store=store,
        handler=handler,
        worker_id=f"unproven-recovery-{phase}-worker",
        max_attempts=1,
    )

    assert await actor._drain_mailbox() is False

    record = store.records[(envelope.key, envelope.event_id)]
    assert record.status is MailboxEventStatus.PROCESSING
    assert store.release_errors == []
    assert store.failed_errors == []
    assert store.aggregates[envelope.key].event_sequence == 0


@pytest.mark.parametrize("failure_mode", ["handler", "transition"])
@pytest.mark.asyncio
async def test_poison_event_is_failed_without_blocking_next_event(
    failure_mode: str,
) -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        if envelope.event_id == "poison":
            if failure_mode == "handler":
                raise ValueError("invalid event payload")
            return SessionTransition(
                aggregate=aggregate,
                disposition="invalid_transition",
            )
        return SessionTransition(
            aggregate=aggregate.advance(data={"handled": envelope.event_id}),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    poison = _event("poison")
    healthy = _event("healthy")
    try:
        await asyncio.gather(registry.submit(poison), registry.submit(healthy))
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert store.records[(poison.key, poison.event_id)].status == (
            MailboxEventStatus.FAILED
        )
        assert store.records[(healthy.key, healthy.event_id)].status == (
            MailboxEventStatus.COMPLETED
        )
        assert store.aggregates[healthy.key].data == {"handled": "healthy"}
        assert store.aggregates[healthy.key].event_sequence == 2
        assert len(store.failed_errors) == 1
        assert store.release_errors == []
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_infrastructure_failure_is_failed_after_max_attempts() -> None:
    class _FailPoisonCommitStore(_MemorySessionActorStore):
        async def commit(
            self,
            claim: ClaimedSessionEvent,
            transition: SessionTransition,
            *,
            expected_revision: int,
        ) -> AgentSessionAggregate:
            if claim.envelope.event_id == "poison":
                raise OSError("database write failed")
            return await super().commit(
                claim,
                transition,
                expected_revision=expected_revision,
            )

    store = _FailPoisonCommitStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"handled": envelope.event_id}),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(
        store=store,
        handler=handler,
        retry_delay_seconds=0.001,
        max_attempts=2,
    )
    poison = _event("poison")
    healthy = _event("healthy")
    try:
        await asyncio.gather(registry.submit(poison), registry.submit(healthy))
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        poison_record = store.records[(poison.key, poison.event_id)]
        assert poison_record.status == MailboxEventStatus.FAILED
        assert poison_record.attempt_count == 2
        assert store.records[(healthy.key, healthy.event_id)].status == (
            MailboxEventStatus.COMPLETED
        )
        assert store.aggregates[healthy.key].data == {"handled": "healthy"}
        assert store.aggregates[healthy.key].event_sequence == 2
        assert store.release_errors == ["OSError: database write failed"]
        assert store.failed_errors == ["OSError: database write failed"]
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_session_actor_isolates_same_session_id_across_profiles() -> None:
    store = _MemorySessionActorStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"profile": envelope.key.profile_id}),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    first = _event("same-id", profile_id="profile-a")
    second = _event("same-id", profile_id="profile-b")
    try:
        await asyncio.gather(registry.submit(first), registry.submit(second))
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert store.aggregates[first.key].data == {"profile": "profile-a"}
        assert store.aggregates[second.key].data == {"profile": "profile-b"}
        assert registry.actor_for(first.key) is not registry.actor_for(second.key)
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_registry_recovers_pending_events_and_rejects_after_shutdown() -> None:
    store = _MemorySessionActorStore()
    envelope = _event("recover-me")
    await store.enqueue(envelope)

    def handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"event": event.event_id}),
            disposition="event_recorded",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    assert await registry.recover() == 1
    await asyncio.wait_for(registry.wait_idle(), timeout=1.0)
    await registry.shutdown()

    assert store.aggregates[envelope.key].data == {"event": "recover-me"}
    with pytest.raises(RuntimeError, match="shutting down"):
        await registry.submit(_event("too-late"))


def test_registry_does_not_expose_short_lived_permit_recovery() -> None:
    """Long-lived actors cannot be created through a pass-scoped permit API."""

    registry = AgentSessionActorRegistry(
        store=_MemorySessionActorStore(),
        handler=lambda aggregate, _event: SessionTransition(
            aggregate=aggregate.advance(),
            disposition="unexpected",
        ),
    )

    assert not hasattr(registry, "recover_with_legacy_recovery_permit")


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_registry_wake_drains_an_already_durable_event_without_reenqueue() -> None:
    store = _MemorySessionActorStore()
    envelope = _event("already-durable")
    await store.enqueue(envelope)

    def handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"event": event.event_id}),
            disposition="externally_committed_event",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    try:
        await registry.wake(envelope.key)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)

        assert list(store.records) == [(envelope.key, envelope.event_id)]
        assert store.aggregates[envelope.key].data == {"event": "already-durable"}
    finally:
        await registry.shutdown()


@pytest.mark.asyncio
async def test_registry_shutdown_waits_for_in_flight_recovery_scan() -> None:
    class _BlockingRecoveryStore(_MemorySessionActorStore):
        def __init__(self) -> None:
            super().__init__()
            self.scan_started = asyncio.Event()
            self.allow_scan = asyncio.Event()

        async def pending_keys(self) -> list[SessionKey]:
            self.scan_started.set()
            await self.allow_scan.wait()
            return await super().pending_keys()

    store = _BlockingRecoveryStore()
    envelope = _event("recover-during-shutdown")
    await store.enqueue(envelope)

    def handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"event": event.event_id}),
            disposition="recovered",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    recover_task = asyncio.create_task(registry.recover())
    await asyncio.wait_for(store.scan_started.wait(), timeout=1.0)
    shutdown_task = asyncio.create_task(registry.shutdown())
    await asyncio.sleep(0)
    assert shutdown_task.done() is False

    store.allow_scan.set()
    assert await asyncio.wait_for(recover_task, timeout=1.0) == 1
    await asyncio.wait_for(shutdown_task, timeout=1.0)

    assert store.records[(envelope.key, envelope.event_id)].status == (
        MailboxEventStatus.COMPLETED
    )


@pytest.mark.asyncio
async def test_immediate_shutdown_releases_in_flight_event_for_recovery() -> None:
    class _BlockingCommitStore(_MemorySessionActorStore):
        def __init__(self) -> None:
            super().__init__()
            self.commit_started = asyncio.Event()
            self.allow_commit = asyncio.Event()

        async def commit(
            self,
            claim: ClaimedSessionEvent,
            transition: SessionTransition,
            *,
            expected_revision: int,
        ) -> AgentSessionAggregate:
            self.commit_started.set()
            await self.allow_commit.wait()
            return await super().commit(
                claim,
                transition,
                expected_revision=expected_revision,
            )

    store = _BlockingCommitStore()

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(),
            disposition="event_recorded",
        )

    envelope = _event("interrupted")
    first_registry = AgentSessionActorRegistry(store=store, handler=handler)
    await first_registry.submit(envelope)
    await asyncio.wait_for(store.commit_started.wait(), timeout=1.0)
    await first_registry.shutdown(drain=False)
    store.allow_commit.set()

    record = store.records[(envelope.key, envelope.event_id)]
    assert record.status == MailboxEventStatus.PENDING
    assert store.release_errors == ["actor_cancelled"]

    def recovered_handler(
        aggregate: AgentSessionAggregate,
        event: SessionEventEnvelope,
    ) -> SessionTransition:
        return SessionTransition(
            aggregate=aggregate.advance(data={"event": event.event_id}),
            disposition="event_recorded",
        )

    second_registry = AgentSessionActorRegistry(store=store, handler=recovered_handler)
    try:
        assert await second_registry.recover() == 1
        await asyncio.wait_for(second_registry.wait_idle(), timeout=1.0)
        assert record.status == MailboxEventStatus.COMPLETED
        assert record.attempt_count == 2
    finally:
        await second_registry.shutdown()


@pytest.mark.asyncio
async def test_sqlite_actor_commit_persists_transition_contract_atomically(tmp_path) -> None:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    store = SQLiteSessionActorStore(
        database,
        retry_delay_seconds=0.0,
        clock=lambda: 100.0,
        effect_contract_authority=_TEST_EFFECT_AUTHORITY,
    )

    def handler(
        aggregate: AgentSessionAggregate,
        envelope: SessionEventEnvelope,
    ) -> SessionTransition:
        next_aggregate = aggregate.advance(
            state="review",
            review_operation_id="operation-sqlite",
            current_plan_id="plan-sqlite",
            review_plan_revision=1,
            review_plan={
                "plan_id": "plan-sqlite",
                "plan_revision": 1,
                "applied_delay_seconds": 30.0,
            },
        )
        return SessionTransition(
            aggregate=next_aggregate,
            disposition="review_started",
            caused_operation_id="operation-sqlite",
            caused_plan_id="plan-sqlite",
            effects=(
                SessionEffect(
                    effect_id="effect-sqlite",
                    idempotency_key="effect-key-sqlite",
                    kind="run_review",
                    contract_signature=_TEST_EFFECT_CONTRACTS["run_review"].signature,
                    operation_id="operation-sqlite",
                ),
            ),
            operations=(
                SessionOperation(
                    operation_id="operation-sqlite",
                    kind="review",
                    launched_by_event_id=envelope.event_id,
                ),
            ),
            review_schedules=(
                SessionReviewSchedule(
                    plan_id="plan-sqlite",
                    plan_revision=1,
                    applied_delay_seconds=30.0,
                ),
            ),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id="schedule-event-sqlite",
                    event_type="scheduled",
                    plan_id="plan-sqlite",
                    applied_delay_seconds=30.0,
                    metadata=_schedule_outcome_metadata(
                        plan_revision=1,
                        applied_delay_seconds=30.0,
                    ),
                ),
            ),
            result={"accepted": True},
            reason="review_due",
        )

    registry = AgentSessionActorRegistry(store=store, handler=handler)
    envelope = _event("sqlite-event")
    ownership = database.agent_runtime_ownership.claim(
        envelope.key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="sqlite actor unit test",
        legacy_session_id="legacy:sqlite-actor-unit-test",
    ).ownership
    envelope = replace(envelope, ownership_generation=ownership.generation)
    try:
        await registry.submit(envelope)
        await asyncio.wait_for(registry.wait_idle(), timeout=1.0)
    finally:
        await registry.shutdown()

    aggregate = await store.load(envelope.key)
    assert aggregate.state == "review"
    assert aggregate.state_revision == 1
    assert aggregate.event_sequence == 1
    with database.connect() as conn:
        mailbox = conn.execute(
            """
            SELECT status FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (envelope.key.profile_id, envelope.key.session_id, envelope.event_id),
        ).fetchone()
        operation = conn.execute(
            "SELECT status FROM agent_session_operations WHERE operation_id = ?",
            ("operation-sqlite",),
        ).fetchone()
        schedule = conn.execute(
            """
            SELECT status, applied_delay_seconds, scheduled_from, next_review_at
            FROM agent_review_schedules WHERE plan_id = ?
            """,
            ("plan-sqlite",),
        ).fetchone()
        schedule_event = conn.execute(
            """
            SELECT event_type FROM agent_review_schedule_events
            WHERE schedule_event_id = ?
            """,
            ("schedule-event-sqlite",),
        ).fetchone()
        effect = conn.execute(
            """
            SELECT status, idempotency_key FROM agent_effect_outbox
            WHERE effect_id = ?
            """,
            ("effect-sqlite",),
        ).fetchone()
        transition = conn.execute(
            """
            SELECT trigger, state_revision FROM agent_state_transitions
            WHERE profile_id = ? AND session_id = ?
            """,
            (envelope.key.profile_id, envelope.key.session_id),
        ).fetchone()

    assert mailbox is not None and mailbox["status"] == "completed"
    assert operation is not None and operation["status"] == "pending"
    assert schedule is not None and dict(schedule) == {
        "status": "scheduled",
        "applied_delay_seconds": 30.0,
        "scheduled_from": 100.0,
        "next_review_at": 130.0,
    }
    assert schedule_event is not None and schedule_event["event_type"] == "scheduled"
    assert effect is not None and dict(effect) == {
        "status": "pending",
        "idempotency_key": "effect-key-sqlite",
    }
    assert transition is not None and dict(transition) == {
        "trigger": "review_due",
        "state_revision": 1,
    }
