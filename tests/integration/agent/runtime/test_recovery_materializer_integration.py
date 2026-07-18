"""SQLite end-to-end coverage for concrete no-replay recovery materializers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.effect_contracts import (
    builtin_effect_contract,
)
from shinbot.agent.runtime.session_actor.recovery import (
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
)
from shinbot.agent.runtime.session_actor.recovery_commit_coordinator import (
    SQLiteRecoveryCommitCoordinator,
)
from shinbot.agent.runtime.session_actor.recovery_graph_reader import (
    SQLiteRecoveryGraphReader,
)
from shinbot.agent.runtime.session_actor.recovery_materializers import (
    builtin_recovery_materializers,
)
from shinbot.agent.runtime.session_actor.recovery_scanner import (
    RecoveryScanDisposition,
    SQLiteRecoveryGraphScanner,
)
from shinbot.agent.runtime.session_actor.reducer import AgentSessionReducer
from shinbot.agent.runtime.session_actor.registry import AgentSessionActorRegistry
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode
from shinbot.persistence import DatabaseManager
from tests.agent_runtime_helpers import wait_for_session_actor_idle


def _make_database(tmp_path: Path) -> DatabaseManager:
    """Create one initialized SQLite persistence domain."""

    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _workflow_contract(kind: str) -> tuple[int, str]:
    """Return the durable contract identity for one seeded effect kind."""

    contract = builtin_effect_contract(kind)
    return contract.version, contract.signature


def _operation_fence(
    *,
    operation_id: str,
    operation_kind: str,
    effect_id: str,
    effect_kind: str,
    contract_version: int,
    contract_signature: str,
    message_log_ids: list[int] | None = None,
) -> dict[str, object]:
    """Build a complete terminal-workflow fence for one recovery root."""

    fence: dict[str, object] = {
        "operation_id": operation_id,
        "operation_kind": operation_kind,
        "source_event_id": f"source:{operation_id}",
        "effect_id": effect_id,
        "effect_kind": effect_kind,
        "idempotency_key": effect_id,
        "completion_event_id": f"completed:{effect_id}",
        "failure_event_id": f"failed:{effect_id}",
        "ownership_generation": 1,
        "plan_id": "",
        "active_epoch": 0,
        "activity_generation": 0,
        "input_watermark": 0,
        "input_ledger_sequence": 0,
        "contract_version": contract_version,
        "contract_signature": contract_signature,
    }
    if message_log_ids is not None:
        fence["message_log_ids"] = message_log_ids
    return fence


async def _seed_orphaned_state(
    database: DatabaseManager,
    *,
    key: SessionKey,
    state: str,
) -> int:
    """Persist one exact terminal-effect shape for the recovery scanner."""

    generation = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="recovery materializer integration test",
    ).ownership.generation
    assert generation == 1
    await SQLiteSessionActorStore(database, clock=lambda: 10.0).ensure(
        key,
        ownership_generation=generation,
    )
    operation_id = f"{state}-operation"
    data: dict[str, Any] = {"pending_high_priority_message_log_ids": [101]}
    active_chat_state: dict[str, object] = {}
    aggregate_fields = {
        "review_operation_id": "",
        "active_reply_operation_id": "",
        "active_chat_round_operation_id": "",
        "idle_planning_operation_id": "",
    }
    effects: list[tuple[str, str, int, str]] = []
    operation_kind: str
    if state == "review":
        operation_kind = "review"
        effect_kind = "run_review_workflow"
        effect_id = "review-effect"
        version, signature = _workflow_contract(effect_kind)
        data["operation_fences"] = {
            operation_id: _operation_fence(
                operation_id=operation_id,
                operation_kind=operation_kind,
                effect_id=effect_id,
                effect_kind=effect_kind,
                contract_version=version,
                contract_signature=signature,
            )
        }
        aggregate_fields["review_operation_id"] = operation_id
        effects.append((effect_id, effect_kind, version, signature))
    elif state == "active_reply":
        operation_kind = "active_reply"
        effect_kind = "run_active_reply_workflow"
        effect_id = "active-reply-effect"
        version, signature = _workflow_contract(effect_kind)
        data["operation_fences"] = {
            operation_id: _operation_fence(
                operation_id=operation_id,
                operation_kind=operation_kind,
                effect_id=effect_id,
                effect_kind=effect_kind,
                contract_version=version,
                contract_signature=signature,
            )
        }
        aggregate_fields["active_reply_operation_id"] = operation_id
        effects.append((effect_id, effect_kind, version, signature))
    elif state == "active_chat_bootstrap":
        operation_kind = "active_chat_bootstrap"
        effect_kind = "run_active_chat_bootstrap"
        effect_id = "active-chat-bootstrap-effect"
        version, signature = _workflow_contract(effect_kind)
        data["operation_fences"] = {
            operation_id: _operation_fence(
                operation_id=operation_id,
                operation_kind=operation_kind,
                effect_id=effect_id,
                effect_kind=effect_kind,
                contract_version=version,
                contract_signature=signature,
            )
        }
        active_chat_state = {
            "bootstrap_status": "pending",
            "bootstrap_operation_id": operation_id,
        }
        effects.append((effect_id, effect_kind, version, signature))
    elif state == "active_chat_round":
        operation_kind = "active_chat_round"
        effect_kind = "run_active_chat_round"
        effect_id = "active-chat-round-effect"
        version, signature = _workflow_contract(effect_kind)
        data["operation_fences"] = {
            operation_id: _operation_fence(
                operation_id=operation_id,
                operation_kind=operation_kind,
                effect_id=effect_id,
                effect_kind=effect_kind,
                contract_version=version,
                contract_signature=signature,
                message_log_ids=[101],
            )
        }
        data["effect_control_intents"] = {
            "enqueue_active_chat_round_due": {
                "status": "completed",
                "effect_kind": "enqueue_active_chat_round_due",
                "operation_id": "",
                "plan_id": "",
                "ownership_generation": generation,
                "active_epoch": 0,
                "activity_generation": 0,
            }
        }
        aggregate_fields["active_chat_round_operation_id"] = operation_id
        active_chat_state = {
            "bootstrap_status": "completed",
            "bootstrap_operation_id": "",
            "round_operation_id": operation_id,
            "pending_message_log_ids": [101],
        }
        effects.append((effect_id, effect_kind, version, signature))
    elif state == "active_chat_settling":
        operation_kind = "idle_review_planning"
        successor_plan_id = "recovery-settling-successor"
        planner_id = "idle-planner-effect"
        deadline_id = "idle-deadline-effect"
        planner_version, planner_signature = _workflow_contract(
            "run_idle_review_planning"
        )
        deadline_version, deadline_signature = _workflow_contract(
            "enqueue_idle_review_planning_deadline"
        )
        data["operation_fences"] = {
            operation_id: {
                "operation_id": operation_id,
                "operation_kind": operation_kind,
                "source_event_id": f"source:{operation_id}",
                "ownership_generation": generation,
                "plan_id": successor_plan_id,
                "active_epoch": 0,
                "activity_generation": 0,
                "input_watermark": 0,
                "input_ledger_sequence": 0,
            }
        }
        data["idle_exit"] = {
            "operation_id": operation_id,
            "plan_id": successor_plan_id,
            "ownership_generation": generation,
            "active_epoch": 0,
            "activity_generation": 0,
            "planner_effect_id": planner_id,
            "planner_idempotency_key": planner_id,
            "planner_contract_version": planner_version,
            "planner_contract_signature": planner_signature,
            "deadline_effect_id": deadline_id,
            "deadline_idempotency_key": deadline_id,
            "deadline_contract_version": deadline_version,
            "deadline_contract_signature": deadline_signature,
        }
        data["effect_control_intents"] = {
            "enqueue_active_chat_exit_request": {
                "status": "completed",
                "effect_kind": "enqueue_active_chat_exit_request",
                "operation_id": "",
                "plan_id": "",
                "ownership_generation": generation,
                "active_epoch": 0,
                "activity_generation": 0,
            }
        }
        aggregate_fields["idle_planning_operation_id"] = operation_id
        active_chat_state = {
            "bootstrap_status": "completed",
            "interest_value": 22,
            "pending_message_log_ids": [101],
        }
        effects.extend(
            (
                (
                    planner_id,
                    "run_idle_review_planning",
                    planner_version,
                    planner_signature,
                ),
                (
                    deadline_id,
                    "enqueue_idle_review_planning_deadline",
                    deadline_version,
                    deadline_signature,
                ),
            )
        )
    else:
        raise AssertionError(f"unknown recovery state: {state}")

    aggregate_state = (
        "active_chat"
        if state in {"active_chat_bootstrap", "active_chat_round"}
        else state
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET state = ?, state_revision = 1, event_sequence = 0,
                active_chat_state_json = ?, data_json = ?, updated_at = 10,
                review_operation_id = ?, active_reply_operation_id = ?,
                active_chat_round_operation_id = ?, idle_planning_operation_id = ?
            WHERE profile_id = ? AND session_id = ? AND ownership_generation = ?
            """,
            (
                aggregate_state,
                json.dumps(active_chat_state, separators=(",", ":"), sort_keys=True),
                json.dumps(data, separators=(",", ":"), sort_keys=True),
                aggregate_fields["review_operation_id"],
                aggregate_fields["active_reply_operation_id"],
                aggregate_fields["active_chat_round_operation_id"],
                aggregate_fields["idle_planning_operation_id"],
                key.profile_id,
                key.session_id,
                generation,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_session_operations (
                operation_id, profile_id, session_id, ownership_generation,
                kind, status, launched_by_event_id, state_revision,
                active_epoch, activity_generation, input_watermark,
                input_ledger_sequence, started_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, 'pending', ?, 1, 0, 0, 0, 0, 10, '{}')
            """,
            (
                operation_id,
                key.profile_id,
                key.session_id,
                generation,
                operation_kind,
                f"source:{operation_id}",
            ),
        )
        for effect_id, effect_kind, version, signature in effects:
            conn.execute(
                """
                INSERT INTO agent_effect_outbox (
                    effect_id, idempotency_key, profile_id, session_id,
                    ownership_generation, event_id, operation_id, kind,
                    contract_version, contract_signature, payload_json, status,
                    attempt_count, available_at, claim_id, lease_owner, lease_until,
                    created_at, updated_at, completed_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', 'failed', 1, 10,
                          '', '', NULL, 10, 10, 10,
                          'worker_lost_before_completion_delivery')
                """,
                (
                    effect_id,
                    effect_id,
                    key.profile_id,
                    key.session_id,
                    generation,
                    f"source:{operation_id}",
                    operation_id,
                    effect_kind,
                    version,
                    signature,
                ),
            )
    return generation


@pytest.mark.parametrize(
    "state",
    ("review", "active_reply", "active_chat_bootstrap", "active_chat_round", "active_chat_settling"),
)
async def test_materializers_commit_no_replay_recovery_for_each_state(
    tmp_path: Path,
    state: str,
) -> None:
    """Typed scan through commit settles each supported non-idle state safely."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", f"bot:group:{state}")
    generation = await _seed_orphaned_state(database, key=key, state=state)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    scan = scanner.scan()
    assert scan.delivered_count == 1

    coordinator = SQLiteRecoveryCommitCoordinator(
        SQLiteRecoveryGraphReader(database),
        materializers=builtin_recovery_materializers(delay_seconds=15.0),
    )
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: 100.0,
        recovery_commit_coordinator=coordinator,
    )
    claim = await store.claim_next(key, worker_id="recovery-worker")
    assert claim is not None
    before = await store.load(key)
    transition = AgentSessionReducer().reduce(before, claim.envelope)
    recovered = await store.commit(
        claim,
        transition,
        expected_revision=before.state_revision,
    )

    assert recovered.state == "idle"
    assert recovered.ownership_generation == generation
    assert recovered.review_plan_revision == 1
    assert recovered.current_plan_id
    assert recovered.data["pending_high_priority_message_log_ids"] == [101]
    assert "operation_fences" not in recovered.data
    assert "effect_control_intents" not in recovered.data
    assert "idle_exit" not in recovered.data
    operation_kind = {
        "review": "review",
        "active_reply": "active_reply",
        "active_chat_bootstrap": "active_chat_bootstrap",
        "active_chat_round": "active_chat_round",
        "active_chat_settling": "idle_review_planning",
    }[state]
    with database.connect() as conn:
        operation = conn.execute(
            """
            SELECT status, failure_code
            FROM agent_session_operations
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        schedule = conn.execute(
            """
            SELECT status, applied_delay_seconds
            FROM agent_review_schedules
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        consumption_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM agent_message_ledger_consumptions
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
        recovery_case = conn.execute(
            """
            SELECT status
            FROM agent_session_recovery_cases
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert operation is not None
    assert tuple(operation) == (
        "failed",
        f"orphaned_operation_recovered:{operation_kind}",
    )
    assert schedule is not None
    assert tuple(schedule) == ("scheduled", 15.0)
    assert consumption_count is not None
    assert int(consumption_count[0]) == 0
    assert recovery_case is not None
    assert recovery_case[0] == "applied"
    assert scanner.scan().results == ()


@pytest.mark.parametrize(
    "state",
    (
        "review",
        "active_reply",
        "active_chat_bootstrap",
        "active_chat_round",
        "active_chat_settling",
    ),
)
async def test_typed_recovery_registry_handoff_converges_without_legacy_fallback(
    tmp_path: Path,
    state: str,
) -> None:
    """A typed scanner delivery reaches the real actor commit boundary once."""

    database = _make_database(tmp_path)
    key = SessionKey("profile-a", f"bot:group:registry-{state}")
    generation = await _seed_orphaned_state(database, key=key, state=state)
    scanner = SQLiteRecoveryGraphScanner(database, clock=lambda: 100.0)
    coordinator = SQLiteRecoveryCommitCoordinator(
        scanner.graph_reader,
        materializers=builtin_recovery_materializers(delay_seconds=15.0),
    )
    store = SQLiteSessionActorStore(
        database,
        clock=lambda: 100.0,
        recovery_commit_coordinator=coordinator,
    )
    scan = scanner.scan()
    redrive_scan = scanner.scan()
    assert scan.delivered_count == 1
    assert scan.results[0].key == key
    assert redrive_scan.results[0].disposition is RecoveryScanDisposition.ALREADY_DELIVERED
    with database.connect() as conn:
        pending_delivery = conn.execute(
            """
            SELECT kind, source, status
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()
    assert tuple(pending_delivery) == (
        RECOVERY_DELIVERY_EVENT_KIND,
        RECOVERY_DELIVERY_EVENT_SOURCE,
        "pending",
    )

    # Construct the registry only after the scanner has committed its delivery,
    # which models a process restart between discovery and the post-commit wake.
    registry = AgentSessionActorRegistry(
        store=store,
        handler=AgentSessionReducer().reduce,
    )
    try:
        assert scanner.persistence_domain is registry.persistence_domain

        await registry.wake(key)
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint=f"typed recovery registry handoff:{state}",
        )

        recovered = await store.load(key)
        assert recovered.state == "idle"
        assert recovered.ownership_generation == generation
        assert recovered.review_plan_revision == 1
        assert recovered.current_plan_id
        operation_kind = {
            "review": "review",
            "active_reply": "active_reply",
            "active_chat_bootstrap": "active_chat_bootstrap",
            "active_chat_round": "active_chat_round",
            "active_chat_settling": "idle_review_planning",
        }[state]
        with database.connect() as conn:
            mailbox_rows = conn.execute(
                """
                SELECT kind, source, status
                FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ?
                ORDER BY mailbox_id
                """,
                (key.profile_id, key.session_id),
            ).fetchall()
            recovery_case = conn.execute(
                """
                SELECT status
                FROM agent_session_recovery_cases
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            operation = conn.execute(
                """
                SELECT status, failure_code
                FROM agent_session_operations
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()
            transition_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM agent_state_transitions
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()[0]
        assert [tuple(row) for row in mailbox_rows] == [
            (
                RECOVERY_DELIVERY_EVENT_KIND,
                RECOVERY_DELIVERY_EVENT_SOURCE,
                "completed",
            )
        ]
        assert recovery_case is not None
        assert recovery_case[0] == "applied"
        assert operation is not None
        assert tuple(operation) == (
            "failed",
            f"orphaned_operation_recovered:{operation_kind}",
        )
        assert transition_count == 1
        assert scanner.scan().results == ()

        # A duplicate wake sees no mailbox work and cannot synthesize a legacy
        # recovery event or a second materialization transition.
        await registry.wake(key)
        await wait_for_session_actor_idle(
            database,
            registry,
            key,
            checkpoint=f"typed recovery duplicate wake:{state}",
        )
        with database.connect() as conn:
            transition_count_after = conn.execute(
                """
                SELECT COUNT(*)
                FROM agent_state_transitions
                WHERE profile_id = ? AND session_id = ?
                """,
                (key.profile_id, key.session_id),
            ).fetchone()[0]
            legacy_recovery_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM agent_session_mailbox
                WHERE profile_id = ? AND session_id = ?
                  AND source = 'session_actor_recovery'
                """,
                (key.profile_id, key.session_id),
            ).fetchone()[0]
        assert transition_count_after == 1
        assert legacy_recovery_count == 0
    finally:
        await registry.shutdown(drain=False)
