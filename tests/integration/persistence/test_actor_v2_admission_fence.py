"""Integration coverage for the durable Actor v2 admission reservation."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest

from shinbot.core.dispatch.actor_v2_admission import (
    ActorV2AdmissionFenceConflict,
    ActorV2AdmissionFenceExpired,
    ActorV2AdmissionFenceReserved,
    ActorV2AdmissionFenceStatus,
)
from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.durable_routing import MessageRoutingJobEnvelope
from shinbot.persistence import DatabaseManager, MessageLogRecord
from shinbot.persistence.repositories.actor_v2_admission_fence import (
    ActorV2AdmissionFenceRepository,
)
from shinbot.persistence.repositories.agent_runtime_ownership import (
    AgentRuntimeOwnershipRepository,
)
from shinbot.persistence.repositories.durable_routing import (
    DurableMessageRoutingRepository,
)


def _database(tmp_path: Path) -> DatabaseManager:
    """Build one initialized durable domain for admission-fence tests."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _record() -> MessageLogRecord:
    """Build one durable message record for admission-routing tests."""

    return MessageLogRecord(
        session_id="instance-a:group:room",
        platform_msg_id="message-a",
        sender_id="user-a",
        sender_name="Alice",
        content_json='[{"type":"text","attrs":{"text":"hello"}}]',
        raw_text="hello",
        role="user",
        is_mentioned=True,
        created_at=1_000.0,
    )


def _reserved_envelope(
    key: SessionKey,
    *,
    fence_id: str,
    fence_generation: int,
) -> MessageRoutingJobEnvelope:
    """Build one job that is durably buffered behind an admission reservation."""

    return MessageRoutingJobEnvelope(
        job_id="fenced-routing-job",
        idempotency_key="fenced-routing-key",
        trace_id="fenced-trace",
        correlation_id="fenced-correlation",
        causation_id="message-a",
        profile_id=key.profile_id,
        session_id=key.session_id,
        ownership_generation=0,
        admission_fence_id=fence_id,
        admission_fence_generation=fence_generation,
        occurred_at=1_000.0,
        available_at=1_000.0,
        payload={"kind": "fenced-ingress"},
    )


def _delivery(key: SessionKey, *, message_log_id: int) -> AgentRouteDelivery:
    """Build one actor delivery whose canonical key matches the reserved job."""

    return AgentRouteDelivery(
        session_key=key,
        bot_id="profile-a",
        bot_binding_id="binding-a",
        base_session_id="instance-a:group:room",
        bot_session_id=key.session_id,
        message_log_id=message_log_id,
        sender_id="user-a",
        instance_id="instance-a",
        platform="mock",
        self_id="bot-self",
        is_private=False,
        is_mentioned=True,
        is_mention_to_other=False,
        is_reply_to_bot=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        already_handled=False,
        is_stopped=False,
        trace_id="fenced-trace",
        observed_at=1_000.0,
        route_rule_id="agent-entry",
    )


def test_reservation_persists_token_free_snapshot_and_rejects_reuse(tmp_path: Path) -> None:
    """A fence capability is returned once while durable history remains visible."""

    database = _database(tmp_path)
    repository = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: 10.0,
        fence_id_factory=lambda: "fence-a",
        holder_token_factory=lambda: "holder-secret-a",
    )
    key = SessionKey("profile-a", "profile-a:group:room")

    grant = repository.reserve(key, holder_id="canary-a", ttl_seconds=30.0)
    restored = repository.get(key)

    assert grant.fence.fence_id == "fence-a"
    assert grant.fence.generation == 1
    assert grant.fence.status is ActorV2AdmissionFenceStatus.RESERVED
    assert restored == grant.fence
    with database.connect() as conn:
        persisted = conn.execute(
            "SELECT holder_token_digest FROM agent_session_actor_v2_admission_fences"
        ).fetchone()
    assert persisted is not None
    assert persisted["holder_token_digest"] != grant.holder_token

    with pytest.raises(ActorV2AdmissionFenceConflict, match="history already exists"):
        repository.reserve(key, holder_id="canary-b", ttl_seconds=30.0)


def test_reservation_requires_an_unowned_session(tmp_path: Path) -> None:
    """A normal ownership decision cannot be overwritten by a future canary."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy session already selected",
    )

    with pytest.raises(ActorV2AdmissionFenceConflict, match="unowned session"):
        database.actor_v2_admission_fences.reserve(
            key,
            holder_id="canary-a",
            ttl_seconds=30.0,
        )


def test_live_holder_can_renew_then_commit_inside_one_transaction(tmp_path: Path) -> None:
    """The raw holder token is required for both lease extension and commitment."""

    now = [10.0]
    database = _database(tmp_path)
    repository = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "fence-a",
        holder_token_factory=lambda: "holder-secret-a",
    )
    key = SessionKey("profile-a", "profile-a:group:room")
    grant = repository.reserve(key, holder_id="canary-a", ttl_seconds=5.0)

    now[0] = 12.0
    renewed = repository.renew(grant, ttl_seconds=20.0)
    assert renewed.fence.expires_at == 32.0

    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        committed = repository.commit_in_transaction(conn, renewed)
        verified = repository.require_committed_in_transaction(
            conn,
            key=key,
            fence_id=renewed.fence.fence_id,
            generation=renewed.fence.generation,
        )

    assert committed.status is ActorV2AdmissionFenceStatus.COMMITTED
    assert verified == committed


def test_expired_or_revoked_fence_cannot_be_used_for_commitment(tmp_path: Path) -> None:
    """Lost holder liveness fails closed instead of allowing an implicit takeover."""

    now = [10.0]
    database = _database(tmp_path)
    repository = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "fence-a",
        holder_token_factory=lambda: "holder-secret-a",
    )
    key = SessionKey("profile-a", "profile-a:group:room")
    grant = repository.reserve(key, holder_id="canary-a", ttl_seconds=1.0)

    now[0] = 11.0
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(ActorV2AdmissionFenceExpired):
            repository.require_reserved_in_transaction(conn, grant)

    revoked = repository.revoke(grant, reason="holder timed out")
    assert revoked.status is ActorV2AdmissionFenceStatus.REVOKED
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(ActorV2AdmissionFenceConflict, match="revoked"):
            repository.require_reserved_in_transaction(conn, grant)


def test_concurrent_reservation_leaves_exactly_one_durable_holder(tmp_path: Path) -> None:
    """Concurrent would-be canaries cannot share a session admission fence."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    workers = 4
    barrier = threading.Barrier(workers)

    def reserve(index: int) -> str:
        repository = ActorV2AdmissionFenceRepository(database)
        barrier.wait()
        try:
            grant = repository.reserve(
                key,
                holder_id=f"canary-{index}",
                ttl_seconds=30.0,
            )
        except ActorV2AdmissionFenceConflict:
            return "conflict"
        return grant.fence.holder_id

    with ThreadPoolExecutor(max_workers=workers) as executor:
        outcomes = list(executor.map(reserve, range(workers)))

    assert outcomes.count("conflict") == workers - 1
    restored = database.actor_v2_admission_fences.get(key)
    assert restored is not None
    assert outcomes.count(restored.holder_id) == 1


def test_reservation_and_default_legacy_claim_race_choose_one_admission_path(
    tmp_path: Path,
) -> None:
    """A first legacy claim cannot cross a concurrently committed reservation."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    barrier = threading.Barrier(2)

    def reserve() -> str:
        barrier.wait()
        try:
            database.actor_v2_admission_fences.reserve(
                key,
                holder_id="canary-a",
                ttl_seconds=30.0,
            )
        except ActorV2AdmissionFenceConflict:
            return "reservation_conflict"
        return "reserved"

    def select_legacy() -> str:
        barrier.wait()
        try:
            database.agent_runtime_ownership.claim(
                key,
                AgentRuntimeOwnershipMode.LEGACY,
                reason="concurrent default legacy selection",
                legacy_session_id="instance-a:group:room",
            )
        except ActorV2AdmissionFenceReserved:
            return "legacy_blocked"
        return "legacy_claimed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        reservation, legacy = list(executor.map(lambda fn: fn(), (reserve, select_legacy)))

    assert (reservation, legacy) in {
        ("reserved", "legacy_blocked"),
        ("reservation_conflict", "legacy_claimed"),
    }
    fence = database.actor_v2_admission_fences.get(key)
    ownership = database.agent_runtime_ownership.get(key)
    if reservation == "reserved":
        assert fence is not None
        assert fence.status is ActorV2AdmissionFenceStatus.RESERVED
        assert ownership is None
    else:
        assert fence is None
        assert ownership is not None
        assert ownership.mode is AgentRuntimeOwnershipMode.LEGACY


def test_reservation_and_unfenced_actor_claim_race_choose_one_admission_path(
    tmp_path: Path,
) -> None:
    """A generic Actor v2 first claim cannot bypass a concurrent reservation."""

    database = _database(tmp_path)
    key = SessionKey("profile-a", "profile-a:group:room")
    barrier = threading.Barrier(2)

    def reserve() -> str:
        barrier.wait()
        try:
            database.actor_v2_admission_fences.reserve(
                key,
                holder_id="canary-a",
                ttl_seconds=30.0,
            )
        except ActorV2AdmissionFenceConflict:
            return "reservation_conflict"
        return "reserved"

    def select_unfenced_actor() -> str:
        barrier.wait()
        try:
            database.agent_runtime_ownership.claim(
                key,
                AgentRuntimeOwnershipMode.ACTOR_V2,
                reason="concurrent unfenced actor selection",
                legacy_session_id="instance-a:group:room",
            )
        except ActorV2AdmissionFenceConflict:
            return "actor_blocked"
        return "actor_claimed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        reservation, actor = list(
            executor.map(lambda fn: fn(), (reserve, select_unfenced_actor))
        )

    assert (reservation, actor) in {
        ("reserved", "actor_blocked"),
        ("reservation_conflict", "actor_claimed"),
    }
    fence = database.actor_v2_admission_fences.get(key)
    ownership = database.agent_runtime_ownership.get(key)
    if reservation == "reserved":
        assert fence is not None
        assert ownership is None
    else:
        assert fence is None
        assert ownership is not None
        assert ownership.mode is AgentRuntimeOwnershipMode.ACTOR_V2


def test_fenced_routing_survives_restart_then_expiry_blocks_relay(tmp_path: Path) -> None:
    """One committed fence retargets buffered work and fences it again at expiry."""

    now = [1_000.0]
    database = _database(tmp_path)
    database.actor_v2_admission_fences = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "fence-restart",
        holder_token_factory=lambda: "holder-restart",
    )
    database.agent_runtime_ownership = AgentRuntimeOwnershipRepository(
        database,
        clock=lambda: now[0],
    )
    key = SessionKey("profile-a", "profile-a:group:room")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="canary-a",
        ttl_seconds=5.0,
    )
    store = DurableMessageRoutingRepository(database, clock=lambda: now[0])
    persisted = store.persist_message_and_job(
        _record(),
        _reserved_envelope(
            key,
            fence_id=grant.fence.fence_id,
            fence_generation=grant.fence.generation,
        ),
    )

    assert store.claim_next_job(worker_id="before-commit") is None
    with pytest.raises(ActorV2AdmissionFenceReserved):
        database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            reason="must not select legacy behind an admission fence",
            legacy_session_id="instance-a:group:room",
        )
    with pytest.raises(ActorV2AdmissionFenceConflict, match="matching admission grant"):
        database.agent_runtime_ownership.claim(
            key,
            AgentRuntimeOwnershipMode.ACTOR_V2,
            reason="must not bypass an admission fence",
            legacy_session_id="instance-a:group:room",
        )

    restarted = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    restarted.initialize()
    restarted.actor_v2_admission_fences = ActorV2AdmissionFenceRepository(
        restarted,
        clock=lambda: now[0],
    )
    restarted.agent_runtime_ownership = AgentRuntimeOwnershipRepository(
        restarted,
        clock=lambda: now[0],
    )
    restarted_store = DurableMessageRoutingRepository(restarted, clock=lambda: now[0])
    owner = restarted.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="commit fenced Actor v2 owner after restart",
        legacy_session_id="instance-a:group:room",
        admission_grant=grant,
    ).ownership

    fence = restarted.actor_v2_admission_fences.get(key)
    assert fence is not None
    assert fence.status is ActorV2AdmissionFenceStatus.COMMITTED
    assert owner.admission_fence_id == fence.fence_id
    assert owner.admission_fence_generation == fence.generation
    with restarted.connect() as conn:
        row = conn.execute(
            """
            SELECT ownership_generation, admission_fence_id,
                   admission_fence_generation, status
            FROM message_routing_jobs
            WHERE routing_job_id = ?
            """,
            (persisted.routing_job_id,),
        ).fetchone()
    assert tuple(row) == (owner.generation, fence.fence_id, fence.generation, "pending")

    late_envelope = replace(
        _reserved_envelope(
            key,
            fence_id=grant.fence.fence_id,
            fence_generation=grant.fence.generation,
        ),
        job_id="fenced-routing-job-after-commit",
        idempotency_key="fenced-routing-key-after-commit",
        causation_id="message-after-commit",
    )
    late = restarted_store.persist_message_and_job(_record(), late_envelope)
    with restarted.connect() as conn:
        late_row = conn.execute(
            """
            SELECT ownership_generation, admission_fence_id,
                   admission_fence_generation
            FROM message_routing_jobs
            WHERE routing_job_id = ?
            """,
            (late.routing_job_id,),
        ).fetchone()
    assert tuple(late_row) == (owner.generation, fence.fence_id, fence.generation)

    claim = restarted_store.claim_next_job(worker_id="after-commit")
    assert claim is not None
    delivery = _delivery(key, message_log_id=persisted.message_log_id)
    restarted_store.complete_with_agent_deliveries(
        claim,
        [delivery],
        expected_ownership_generations={key: owner.generation},
    )
    with restarted.connect() as conn:
        outbox = conn.execute(
            """
            SELECT admission_fence_id, admission_fence_generation
            FROM agent_route_outbox
            """
        ).fetchone()
    assert tuple(outbox) == (fence.fence_id, fence.generation)

    now[0] = 1_005.0
    assert restarted_store.claim_next_delivery(worker_id="expired-relay") is None
    with restarted.connect() as conn:
        with pytest.raises(ActorV2AdmissionFenceExpired):
            restarted.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=owner.generation,
                expected_admission_fence_id=fence.fence_id,
                expected_admission_fence_generation=fence.generation,
            )


def test_revoked_committed_fence_blocks_actor_ownership_validation(tmp_path: Path) -> None:
    """Revocation never turns a committed Actor v2 owner back into legacy work."""

    now = [1_000.0]
    database = _database(tmp_path)
    database.actor_v2_admission_fences = ActorV2AdmissionFenceRepository(
        database,
        clock=lambda: now[0],
        fence_id_factory=lambda: "fence-revoked",
        holder_token_factory=lambda: "holder-revoked",
    )
    database.agent_runtime_ownership = AgentRuntimeOwnershipRepository(
        database,
        clock=lambda: now[0],
    )
    key = SessionKey("profile-a", "profile-a:group:room")
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="canary-a",
        ttl_seconds=30.0,
    )
    owner = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="commit revocable actor owner",
        legacy_session_id="instance-a:group:room",
        admission_grant=grant,
    ).ownership
    with pytest.raises(ActorV2AdmissionFenceConflict, match="generic legacy transition"):
        database.agent_runtime_ownership.begin_migration(
            key,
            AgentRuntimeOwnershipMode.LEGACY,
            expected_generation=owner.generation,
            reason="must not bypass fenced admission through migration",
        )
    database.actor_v2_admission_fences.revoke(grant, reason="operator stop proof failed")

    with database.connect() as conn:
        with pytest.raises(ActorV2AdmissionFenceConflict, match="not committed"):
            database.agent_runtime_ownership.require_actor_v2_in_transaction(
                conn,
                key,
                expected_generation=owner.generation,
                expected_admission_fence_id=owner.admission_fence_id,
                expected_admission_fence_generation=owner.admission_fence_generation,
            )
