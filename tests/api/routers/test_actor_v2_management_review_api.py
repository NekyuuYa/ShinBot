"""API coverage for ownership-aware manual Actor v2 review admission."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.events import (
    SessionEventEnvelope,
    SessionReviewSchedule,
    SessionReviewScheduleEvent,
    SessionTransition,
)
from shinbot.agent.runtime.session_actor.store import SQLiteSessionActorStore
from shinbot.agent.scheduler import AgentState
from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode


class _BootStub:
    """Minimal authenticated API boot state for management-route tests."""

    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "runtime": {"model": False, "agent": True},
            "adapter_instances": [],
            "plugins": [],
            "bots": [],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.bot_service_configs = ()

    def save_config(self) -> bool:
        """Match the application boot controller persistence port."""

        return True


class _LegacyReviewScheduler:
    """Minimal legacy scheduler that exposes direct-review mutation attempts."""

    def __init__(self, session_id: str) -> None:
        self._session_id = session_id
        self.bring_forward_calls = 0
        self.run_due_calls = 0

    def list_session_ids(self) -> list[str]:
        """Return only the test session in this profile."""

        return [self._session_id]

    def state_for(self, session_id: str) -> AgentState:
        """Expose an idle session that can accept a manual review."""

        assert session_id == self._session_id
        return AgentState.IDLE

    def bring_review_plan_forward(
        self,
        session_id: str,
        *,
        next_review_at: float,
        now: float,
        reason: str,
    ) -> None:
        """Record the direct legacy transition attempt."""

        assert session_id == self._session_id
        assert next_review_at == now
        assert reason == "manual_trigger"
        self.bring_forward_calls += 1

    async def run_due_review(
        self,
        session_id: str,
        *,
        now: float,
    ) -> SimpleNamespace:
        """Return a deterministic successful legacy review decision."""

        assert session_id == self._session_id
        assert now > 0.0
        self.run_due_calls += 1
        return SimpleNamespace(
            review_workflow_started=True,
            state=AgentState.REVIEW,
        )


def _auth_headers(app: object) -> dict[str, str]:
    """Return an authenticated admin header for one test application."""

    auth_config = app.state.auth_config
    return {"Authorization": f"Bearer {auth_config.create_token()}"}


async def _seed_fenced_review_schedule(
    bot: ShinBot,
    *,
    key: SessionKey,
    plan_id: str,
) -> None:
    """Create one current scheduled review under committed Actor v2 ownership."""

    assert bot.database is not None
    database = bot.database
    grant = database.actor_v2_admission_fences.reserve(
        key,
        holder_id="management-review-api-test",
        ttl_seconds=3600.0,
    )
    ownership = database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="management review API test",
        legacy_session_id=f"legacy:{key.profile_id}:{key.session_id}",
        admission_grant=grant,
    ).ownership
    store = SQLiteSessionActorStore(database)
    await store.ensure(key, ownership_generation=ownership.generation)
    await store.enqueue(
        SessionEventEnvelope(
            event_id=f"seed-schedule:{plan_id}",
            key=key,
            kind="SeedReviewSchedule",
            ownership_generation=ownership.generation,
            source="test",
            occurred_at=1.0,
            trace_id=f"trace:{plan_id}",
            available_at=1.0,
            created_at=1.0,
        )
    )
    claim = await store.claim_next(key, worker_id="management-review-api-seeder")
    assert claim is not None
    aggregate = await store.load(key)
    target = aggregate.advance(
        current_plan_id=plan_id,
        review_plan_revision=aggregate.review_plan_revision + 1,
        review_plan={
            "plan_id": plan_id,
            "plan_revision": aggregate.review_plan_revision + 1,
            "applied_delay_seconds": 0.0,
            "trigger": "test",
            "kind": "planned",
            "source": "test",
            "reason": "test",
        },
    )
    schedule = SessionReviewSchedule(
        plan_id=plan_id,
        plan_revision=target.review_plan_revision,
        applied_delay_seconds=0.0,
        trigger="test",
        outcome="planned",
        source="test",
        reason="test",
    )
    await store.commit(
        claim,
        SessionTransition(
            aggregate=target,
            disposition="review_schedule_seeded",
            caused_plan_id=plan_id,
            review_schedules=(schedule,),
            review_schedule_events=(
                SessionReviewScheduleEvent(
                    schedule_event_id=f"seed-scheduled:{plan_id}",
                    event_type="scheduled",
                    plan_id=plan_id,
                    trigger=schedule.trigger,
                    outcome=schedule.outcome,
                    source=schedule.source,
                    applied_delay_seconds=schedule.applied_delay_seconds,
                    reason=schedule.reason,
                    metadata={
                        "plan_revision": schedule.plan_revision,
                        "schedule_outcome": {
                            "active_reply_threshold": {},
                            "applied_delay_seconds": 0.0,
                            "fallback_reason": "",
                            "kind": "planned",
                            "mention_sensitivity": "normal",
                            "model_execution_id": "",
                            "prompt_signature": "",
                            "reason": "test",
                            "requested_delay_seconds": None,
                            "source": "test",
                        },
                    },
                ),
            ),
        ),
        expected_revision=aggregate.state_revision,
    )


def test_profile_manual_review_admits_only_fenced_actor_mailbox(tmp_path: Path) -> None:
    """An Actor v2 management request queues work without activating a target."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={"bot-a": {}},
    )
    key = SessionKey("bot-a", "instance-a:group:room")
    asyncio.run(
        _seed_fenced_review_schedule(
            bot,
            key=key,
            plan_id="management-review-plan",
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    path = (
        "/api/v1/agent-runtime/profiles/bot-a/sessions/"
        "instance-a:group:room/trigger-review"
    )
    headers = {
        **_auth_headers(app),
        "Idempotency-Key": "operator-review-request-1",
    }

    with TestClient(app) as client:
        first = client.post(path, headers=headers)
        duplicate = client.post(path, headers=headers)

    assert first.status_code == 200
    first_data = first.json()["data"]
    assert first_data["profileId"] == key.profile_id
    assert first_data["sessionId"] == key.session_id
    assert first_data["success"] is True
    assert first_data["runtimeKind"] == "actor_v2"
    assert first_data["disposition"] == "admitted"
    assert first_data["requestId"] == "operator-review-request-1"
    assert first_data["eventId"]
    assert first_data["mailboxId"] is not None

    assert duplicate.status_code == 200
    duplicate_data = duplicate.json()["data"]
    assert duplicate_data["success"] is True
    assert duplicate_data["runtimeKind"] == "actor_v2"
    assert duplicate_data["disposition"] == "duplicate"
    assert duplicate_data["eventId"] == first_data["eventId"]
    assert duplicate_data["mailboxId"] == first_data["mailboxId"]

    assert bot.database is not None
    with bot.database.connect() as conn:
        mailbox_rows = conn.execute(
            """
            SELECT mailbox_id, status, payload_json
            FROM agent_session_mailbox
            WHERE profile_id = ? AND session_id = ?
              AND kind = 'ManualReviewRequested'
              AND source = 'manual_review_admission'
            """,
            (key.profile_id, key.session_id),
        ).fetchall()
        handoff = conn.execute(
            """
            SELECT evidence_state, state
            FROM agent_session_mailbox_handoffs
            WHERE mailbox_id = ?
            """,
            (first_data["mailboxId"],),
        ).fetchone()
        aggregate = conn.execute(
            """
            SELECT state
            FROM agent_session_aggregates
            WHERE profile_id = ? AND session_id = ?
            """,
            (key.profile_id, key.session_id),
        ).fetchone()

    assert len(mailbox_rows) == 1
    assert str(mailbox_rows[0]["status"]) == "pending"
    payload = json.loads(str(mailbox_rows[0]["payload_json"]))
    assert payload["requested_by"] == "admin"
    assert payload["request_id"] == "operator-review-request-1"
    assert handoff is not None
    assert str(handoff["evidence_state"]) == "fenced"
    assert str(handoff["state"]) == "pending"
    assert aggregate is not None
    assert str(aggregate["state"]) == "idle"
    assert runtime.actor_v2_diagnostics is not None
    assert runtime.actor_v2_diagnostics.effects_running is False


def test_profile_manual_review_rejects_unowned_session_without_claiming(tmp_path: Path) -> None:
    """The management route cannot choose a runtime owner for a new session."""

    bot = ShinBot(data_dir=tmp_path)
    install_agent_runtime(bot, agent_configs_by_bot_id={"bot-a": {}})
    app = create_api_app(bot, _BootStub(tmp_path))
    session_id = "instance-a:group:unowned"
    path = f"/api/v1/agent-runtime/profiles/bot-a/sessions/{session_id}/trigger-review"

    with TestClient(app) as client:
        response = client.post(path, headers=_auth_headers(app))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["success"] is False
    assert data["runtimeKind"] == "unavailable"
    assert data["disposition"] == "ownership_missing"
    assert bot.database is not None
    assert bot.database.agent_runtime_ownership.get(SessionKey("bot-a", session_id)) is None


def test_profile_legacy_manual_review_enters_the_local_freeze_boundary(
    tmp_path: Path,
) -> None:
    """Legacy management work is tracked and blocked by the same local drain gate."""

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={"bot-a": {}},
    )
    assert bot.database is not None
    key = SessionKey("bot-a", "instance-a:group:legacy")
    bot.database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy management review API test",
        legacy_session_id=key.session_id,
    )
    scheduler = _LegacyReviewScheduler(key.session_id)
    profile = runtime.agent_profile_for_bot("bot-a")
    profile.agent_scheduler = scheduler  # type: ignore[assignment]

    admitted = asyncio.run(
        runtime.request_review_for_profile(
            key.profile_id,
            key.session_id,
            request_id="legacy-management-request",
            requested_by="operator-a",
        )
    )

    assert admitted.success is True
    assert admitted.runtime_kind == "legacy"
    assert admitted.disposition == "triggered"
    assert scheduler.bring_forward_calls == 1
    assert scheduler.run_due_calls == 1

    ticket = runtime.freeze_legacy_session_signal_admission(
        key.session_id,
        cutover_id="legacy-management-review-freeze",
    )
    frozen = asyncio.run(
        runtime.request_review_for_profile(
            key.profile_id,
            key.session_id,
            request_id="legacy-management-frozen-request",
            requested_by="operator-a",
        )
    )

    assert frozen.success is False
    assert frozen.runtime_kind == "legacy"
    assert frozen.disposition == "not_triggered"
    assert scheduler.bring_forward_calls == 1
    assert scheduler.run_due_calls == 1
    assert runtime.thaw_legacy_session_signal_admission(ticket) is True


def test_profile_manual_review_does_not_bypass_reserved_actor_admission(
    tmp_path: Path,
) -> None:
    """An unresolved Actor v2 fence blocks legacy management mutation too."""

    bot = ShinBot(data_dir=tmp_path)
    install_agent_runtime(bot, agent_configs_by_bot_id={"bot-a": {}})
    assert bot.database is not None
    key = SessionKey("bot-a", "instance-a:group:reserved")
    bot.database.actor_v2_admission_fences.reserve(
        key,
        holder_id="management-review-reservation-test",
        ttl_seconds=3600.0,
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    path = (
        "/api/v1/agent-runtime/profiles/bot-a/sessions/"
        "instance-a:group:reserved/trigger-review"
    )

    with TestClient(app) as client:
        response = client.post(path, headers=_auth_headers(app))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["success"] is False
    assert data["runtimeKind"] == "unavailable"
    assert data["disposition"] == "admission_fence_reserved"
    assert bot.database.agent_runtime_ownership.get(key) is None
