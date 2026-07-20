"""Integration coverage for non-review model execution witnesses."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.effect_store import SQLiteDurableEffectStore
from shinbot.agent.runtime.session_actor.model_execution_witness import (
    MODEL_EXECUTION_UNKNOWN_EVENT_KIND,
    MODEL_EXECUTION_UNKNOWN_EVENT_SOURCE,
    ModelExecutionClaim,
    ModelExecutionPermitDisposition,
    ModelExecutionUnknownNotice,
    SQLiteModelExecutionWitnessStore,
    mark_expired_model_execution_unknown,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.actor_v2_admission import ActorV2AdmissionGrant
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.core.dispatch.fenced_wake import FencedMailboxWakeRequest
from shinbot.core.dispatch.fenced_wake_target_lease import (
    FencedActorExecutionBinding,
    FencedWakeTargetLeaseError,
)
from shinbot.core.dispatch.mailbox_handoff import MailboxHandoffTarget
from shinbot.persistence import DatabaseManager


def _json(value: dict[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


async def _make_stores(
    tmp_path: Path,
    now: list[float],
    *,
    admission_grants: list[ActorV2AdmissionGrant] | None = None,
) -> tuple[
    DatabaseManager,
    SessionKey,
    SQLiteDurableEffectStore,
    SQLiteModelExecutionWitnessStore,
]:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    key = SessionKey("profile-model-witness", "bot:group:model-witness")
    admission_grant = None
    if admission_grants is not None:
        admission_grant = database.actor_v2_admission_fences.reserve(
            key,
            holder_id="fenced-model-expiry-test",
            ttl_seconds=3600.0,
        )
        admission_grants.append(admission_grant)
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="model execution witness test",
        admission_grant=admission_grant,
    ).ownership
    await SQLiteSessionActorStore(database, clock=lambda: now[0]).ensure(
        key,
        ownership_generation=ownership.generation,
    )
    return (
        database,
        key,
        SQLiteDurableEffectStore(database, lease_seconds=10.0, clock=lambda: now[0]),
        SQLiteModelExecutionWitnessStore(database, clock=lambda: now[0]),
    )


def _execution_binding(
    database: DatabaseManager,
    *,
    key: SessionKey,
    ownership_generation: int,
    admission_grant: ActorV2AdmissionGrant,
    target_incarnation_id: str,
) -> FencedActorExecutionBinding:
    """Acquire one exact target capability for a model witness test."""

    request = FencedMailboxWakeRequest(
        key=key,
        ownership_generation=ownership_generation,
        admission_fence_id=admission_grant.fence.fence_id,
        admission_fence_generation=admission_grant.fence.generation,
    )
    target_lease = database.actor_v2_fenced_wake_target_leases.acquire(
        request,
        target=MailboxHandoffTarget(
            "model-witness-test-target",
            target_incarnation_id,
        ),
        ttl_seconds=60.0,
    )
    return FencedActorExecutionBinding(request=request, target_lease=target_lease)


def _seed_active_reply_effect(
    database: DatabaseManager,
    *,
    key: SessionKey,
    now: float,
) -> tuple[str, str]:
    effect_id = "model-effect-active-reply"
    operation_id = "model-operation-active-reply"
    contract = builtin_effect_contract("run_active_reply_workflow", version=2)
    payload = {
        "plan_id": "plan-a",
        "active_epoch": 1,
        "activity_generation": 1,
        "input_watermark": 1,
        "input_ledger_sequence": 1,
        "completion_event_id": "model-completion-active-reply",
        "failure_event_id": "model-failure-active-reply",
    }
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, metadata_json
            ) VALUES (?, ?, ?, 1, 'active_reply', 'pending', 'source:model',
                      1, 1, 1, 1, 1, ?, '{}')
            """,
            (operation_id, key.profile_id, key.session_id, now),
        )
        conn.execute(
            """
            INSERT INTO agent_effect_outbox (
                effect_id, idempotency_key, profile_id, session_id,
                ownership_generation, event_id, operation_id, kind,
                contract_version, contract_signature, payload_json,
                status, attempt_count, available_at, claim_id, lease_owner,
                lease_until, created_at, updated_at, completed_at, last_error
            ) VALUES (?, ?, ?, ?, 1, 'source:model', ?,
                      'run_active_reply_workflow', ?, ?, ?, 'pending', 0,
                      ?, '', '', NULL, ?, ?, NULL, '')
            """,
            (
                effect_id,
                effect_id,
                key.profile_id,
                key.session_id,
                operation_id,
                contract.version,
                contract.signature,
                _json(payload),
                now,
                now,
                now,
            ),
        )
    return effect_id, operation_id


def _claim_from_effect(claim) -> ModelExecutionClaim:
    effect = claim.effect
    return ModelExecutionClaim(
        key=effect.key,
        ownership_generation=effect.ownership_generation,
        effect_id=effect.effect_id,
        operation_id=effect.operation_id,
        effect_kind=effect.kind,
        contract_version=effect.contract_version,
        contract_signature=effect.contract_signature,
        claim_id=claim.claim_id,
        worker_id=claim.worker_id,
    )


@pytest.mark.asyncio
async def test_witness_records_start_finish_and_refuses_a_second_task(
    tmp_path: Path,
) -> None:
    """A finished but unsettled task remains a no-replay durable fact."""

    now = [100.0]
    database, key, effects, witnesses = await _make_stores(tmp_path, now)
    effect_id, _operation_id = _seed_active_reply_effect(database, key=key, now=now[0])
    claim = await effects.claim_next(
        worker_id="model-worker-a",
        effect_contracts=(("run_active_reply_workflow", 2),),
    )
    assert claim is not None
    execution = _claim_from_effect(claim)

    started = await witnesses.begin_execution(execution)
    finished = await witnesses.finish_execution(execution)
    replay = await witnesses.begin_execution(execution)

    assert started.disposition is ModelExecutionPermitDisposition.STARTED
    assert finished.disposition is ModelExecutionPermitDisposition.STARTED
    assert replay.disposition is ModelExecutionPermitDisposition.DEFERRED
    assert replay.blocker_code == "model_execution_witness_finished"
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT execution_status, finished_at, unknown_at, unknown_reason
            FROM agent_model_execution_runs WHERE effect_id = ?
            """,
            (effect_id,),
        ).fetchone()
    assert tuple(row) == ("finished", 100.0, None, "")


@pytest.mark.asyncio
async def test_fenced_witness_rejects_lost_target_before_start_and_finish(
    tmp_path: Path,
) -> None:
    """A stale target cannot create or finalize model execution evidence."""

    now = [100.0]
    admission_grants: list[ActorV2AdmissionGrant] = []
    database, key, effects, witnesses = await _make_stores(
        tmp_path,
        now,
        admission_grants=admission_grants,
    )
    _seed_active_reply_effect(database, key=key, now=now[0])
    claim = await effects.claim_next(
        worker_id="fenced-model-worker",
        effect_contracts=(("run_active_reply_workflow", 2),),
    )
    assert claim is not None
    execution = _claim_from_effect(claim)
    first_binding = _execution_binding(
        database,
        key=key,
        ownership_generation=claim.effect.ownership_generation,
        admission_grant=admission_grants[0],
        target_incarnation_id="model-witness-incarnation-a",
    )
    database.actor_v2_fenced_wake_target_leases.release(first_binding.target_lease)

    with pytest.raises(FencedWakeTargetLeaseError):
        await witnesses.begin_execution(
            execution,
            execution_binding=first_binding,
        )

    second_binding = _execution_binding(
        database,
        key=key,
        ownership_generation=claim.effect.ownership_generation,
        admission_grant=admission_grants[0],
        target_incarnation_id="model-witness-incarnation-b",
    )
    started = await witnesses.begin_execution(
        execution,
        execution_binding=second_binding,
    )
    assert started.disposition is ModelExecutionPermitDisposition.STARTED
    database.actor_v2_fenced_wake_target_leases.release(second_binding.target_lease)

    with pytest.raises(FencedWakeTargetLeaseError):
        await witnesses.finish_execution(
            execution,
            execution_binding=second_binding,
        )
    with database.connect() as conn:
        status = conn.execute(
            """
            SELECT execution_status FROM agent_model_execution_runs
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, execution.effect_id),
        ).fetchone()["execution_status"]
    assert status == "running"


@pytest.mark.asyncio
async def test_unknown_witness_is_stable_and_has_a_fenced_mailbox_notice(
    tmp_path: Path,
) -> None:
    """Lease expiry cannot turn a started model request into replayable work."""

    now = [100.0]
    database, key, effects, witnesses = await _make_stores(tmp_path, now)
    effect_id, operation_id = _seed_active_reply_effect(database, key=key, now=now[0])
    claim = await effects.claim_next(
        worker_id="model-worker-a",
        effect_contracts=(("run_active_reply_workflow", 2),),
    )
    assert claim is not None
    execution = _claim_from_effect(claim)
    started = await witnesses.begin_execution(execution)
    assert started.disposition is ModelExecutionPermitDisposition.STARTED

    now[0] = 120.0
    with database.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        marked = mark_expired_model_execution_unknown(
            conn,
            key=key,
            ownership_generation=1,
            effect_id=effect_id,
            claim_id=execution.claim_id,
            worker_id=execution.worker_id,
            now=now[0],
            reason="model_execution_lease_expired_before_handler_terminal",
        )
    assert marked is True

    notice = ModelExecutionUnknownNotice(
        claim=execution,
        attempt_count=claim.attempt_count,
        unknown_at=now[0],
        unknown_reason="model_execution_lease_expired_before_handler_terminal",
    )
    decoded = ModelExecutionUnknownNotice.from_payload(
        notice.to_payload(),
        event_id=notice.event_id,
        key=key,
        ownership_generation=1,
    )
    replay = await witnesses.begin_execution(execution)
    late_finish = await witnesses.finish_execution(execution)

    assert decoded == notice
    assert replay.disposition is ModelExecutionPermitDisposition.DEFERRED
    assert replay.blocker_code == "model_execution_witness_unknown"
    assert late_finish.disposition is ModelExecutionPermitDisposition.DEFERRED
    assert late_finish.blocker_code == "model_execution_witness_unknown"
    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT execution_status, finished_at, unknown_at, unknown_reason
            FROM agent_model_execution_runs WHERE effect_id = ?
            """,
            (effect_id,),
        ).fetchone()
    assert tuple(row) == (
        "unknown",
        None,
        120.0,
        "model_execution_lease_expired_before_handler_terminal",
    )
    assert execution.operation_id == operation_id


@pytest.mark.asyncio
async def test_new_claim_cannot_replace_a_running_witness_for_the_same_effect(
    tmp_path: Path,
) -> None:
    """A changed lease identity is deferred instead of becoming a second call."""

    now = [100.0]
    database, key, effects, witnesses = await _make_stores(tmp_path, now)
    effect_id, _operation_id = _seed_active_reply_effect(database, key=key, now=now[0])
    claim = await effects.claim_next(
        worker_id="model-worker-a",
        effect_contracts=(("run_active_reply_workflow", 2),),
    )
    assert claim is not None
    execution = _claim_from_effect(claim)
    await witnesses.begin_execution(execution)

    replacement = ModelExecutionClaim(
        key=execution.key,
        ownership_generation=execution.ownership_generation,
        effect_id=execution.effect_id,
        operation_id=execution.operation_id,
        effect_kind=execution.effect_kind,
        contract_version=execution.contract_version,
        contract_signature=execution.contract_signature,
        claim_id="replacement-claim",
        worker_id="model-worker-b",
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_effect_outbox
            SET claim_id = ?, lease_owner = ?, lease_until = ?
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (
                replacement.claim_id,
                replacement.worker_id,
                130.0,
                key.profile_id,
                key.session_id,
                effect_id,
            ),
        )

    result = await witnesses.begin_execution(replacement)

    assert result.disposition is ModelExecutionPermitDisposition.DEFERRED
    assert result.blocker_code == "model_execution_witness_identity_conflict"


@pytest.mark.asyncio
async def test_expired_witness_becomes_an_unknown_mailbox_fence_without_replay(
    tmp_path: Path,
) -> None:
    """Recovery preserves a potentially started model call as unknown evidence."""

    now = [100.0]
    database, key, effects, witnesses = await _make_stores(tmp_path, now)
    effect_id, operation_id = _seed_active_reply_effect(database, key=key, now=now[0])
    claim = await effects.claim_next(
        worker_id="model-worker-a",
        effect_contracts=(("run_active_reply_workflow", 2),),
    )
    assert claim is not None
    execution = _claim_from_effect(claim)
    started = await witnesses.begin_execution(execution)
    assert started.disposition is ModelExecutionPermitDisposition.STARTED

    now[0] = 120.0
    recovered = await effects.recover_expired(worker_id="model-recovery")

    assert recovered == 0
    assert await effects.claim_next(
        worker_id="model-worker-b",
        effect_contracts=(("run_active_reply_workflow", 2),),
    ) is None
    notice = ModelExecutionUnknownNotice(
        claim=execution,
        attempt_count=claim.attempt_count,
        unknown_at=now[0],
        unknown_reason="model_execution_lease_expired_before_handler_terminal",
    )
    with database.connect() as conn:
        effect = conn.execute(
            """
            SELECT status, claim_id, lease_owner, lease_until, last_error
            FROM agent_effect_outbox
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT kind, source, payload_json, causation_id, correlation_id
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ? AND event_id = ?
            """,
            (key.profile_id, key.session_id, notice.event_id),
        ).fetchone()
        witness = conn.execute(
            """
            SELECT execution_status, unknown_at, unknown_reason
            FROM agent_model_execution_runs
            WHERE profile_id = ? AND session_id = ? AND effect_id = ?
            """,
            (key.profile_id, key.session_id, effect_id),
        ).fetchone()

    assert tuple(effect) == (
        "processing",
        execution.claim_id,
        execution.worker_id,
        110.0,
        "model_execution_lease_expired_before_handler_terminal",
    )
    assert tuple(witness) == (
        "unknown",
        120.0,
        "model_execution_lease_expired_before_handler_terminal",
    )
    assert mailbox is not None
    assert tuple(mailbox) == (
        MODEL_EXECUTION_UNKNOWN_EVENT_KIND,
        MODEL_EXECUTION_UNKNOWN_EVENT_SOURCE,
        _json(notice.to_payload()),
        "source:model",
        operation_id,
    )


@pytest.mark.asyncio
async def test_expired_model_notice_returns_exact_fenced_wake_request(
    tmp_path: Path,
) -> None:
    """Model-expiry diagnostics retain the full final admission identity."""

    now = [100.0]
    admission_grants: list[ActorV2AdmissionGrant] = []
    database, key, effects, witnesses = await _make_stores(
        tmp_path,
        now,
        admission_grants=admission_grants,
    )
    assert len(admission_grants) == 1
    grant = admission_grants[0]
    effect_id, _operation_id = _seed_active_reply_effect(database, key=key, now=now[0])
    claim = await effects.claim_next(
        worker_id="fenced-model-worker",
        effect_contracts=(("run_active_reply_workflow", 2),),
    )
    assert claim is not None
    execution = _claim_from_effect(claim)
    assert (
        await witnesses.begin_execution(execution)
    ).disposition is ModelExecutionPermitDisposition.STARTED

    now[0] = 120.0
    binding = _execution_binding(
        database,
        key=key,
        ownership_generation=1,
        admission_grant=grant,
        target_incarnation_id="fenced-model-recovery-incarnation",
    )
    recovered = await effects.recover_expired_fenced(
        worker_id="fenced-model-recovery",
        execution_binding=binding,
    )

    assert recovered.recovered_count == 0
    notifications = recovered.notifications
    assert len(notifications) == 1
    assert notifications[0].status.value == "committed"
    assert notifications[0].effect_id == effect_id
    assert notifications[0].wake_request == FencedMailboxWakeRequest(
        key=key,
        ownership_generation=1,
        admission_fence_id=grant.fence.fence_id,
        admission_fence_generation=grant.fence.generation,
    )
    assert await effects.drain_quarantine_notifications() == ()
