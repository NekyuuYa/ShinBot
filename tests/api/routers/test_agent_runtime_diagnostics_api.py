"""API contract tests for canonical Agent runtime diagnostics."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.runtime.services import install_agent_runtime
from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.agent_identity import (
    DEFAULT_SESSION_ACTOR_PROFILE_ID,
    SessionKey,
)
from shinbot.core.dispatch.agent_ownership import AgentRuntimeOwnershipMode


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
            "runtime": {"model": False, "agent": False},
            "adapter_instances": [],
            "plugins": [],
            "bots": [],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.bot_service_configs = ()

    def save_config(self) -> bool:
        return True


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def test_agent_runtime_session_diagnostics_requires_auth_and_returns_envelope(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    assert bot.database is not None
    key = SessionKey("bot-main", "bot-main:group:room")
    bot.database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="API diagnostics fixture",
    )
    app = create_api_app(bot, _BootStub(tmp_path))
    path = "/api/v1/agent-runtime/profiles/bot-main/sessions/bot-main:group:room"

    with TestClient(app) as client:
        unauthorized = client.get(path)
        response = client.get(path, headers=_auth_headers(app))

    assert unauthorized.status_code == 401
    assert unauthorized.json()["error"]["code"] == "AUTH_TOKEN_MISSING"
    assert response.status_code == 200
    envelope = response.json()
    assert envelope["success"] is True
    assert envelope["error"] is None
    assert envelope["data"]["profileId"] == key.profile_id
    assert envelope["data"]["sessionId"] == key.session_id
    assert envelope["data"]["runtimeKind"] == "actor_v2"
    assert envelope["data"]["actorDataStatus"] == "not_initialized"
    assert envelope["data"]["externalActions"] == {
        "status": "ok",
        "attentionRequired": False,
        "unknownCount": 0,
        "abandonedBeforeDispatchCount": 0,
        "liveCount": 0,
        "outboundBlocker": None,
        "receipts": {"total": 0, "byStatus": {}, "recent": []},
        "attempts": {"total": 0, "byStatus": {}, "recent": []},
    }


def test_actor_v2_readiness_exposes_only_diagnostic_activation_state(
    tmp_path: Path,
) -> None:
    """Operators can distinguish incomplete Actor v2 wiring from live ownership."""

    bot = ShinBot(data_dir=tmp_path)
    install_agent_runtime(bot)
    app = create_api_app(bot, _BootStub(tmp_path))
    path = "/api/v1/agent-runtime/actor-v2/readiness"

    with TestClient(app) as client:
        unauthorized = client.get(path)
        response = client.get(path, headers=_auth_headers(app))

    assert unauthorized.status_code == 401
    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["runtimeMode"] == "diagnostic_only"
    assert payload["activationPermitted"] is False
    assert payload["activationBlockers"] == [
        "actor_v2_complete_history_handler_graph_incomplete",
        "actor_v2_diagnostic_assembly_unmounted",
        "actor_v2_durable_isolation_lease_unavailable",
        "actor_v2_ownership_ingress_cutover_controller_unavailable",
        "actor_v2_legacy_state_handoff_manifest_unavailable",
        "actor_v2_base_session_migration_scope_unresolved",
        "actor_v2_wake_target_unpublished",
        "actor_v2_recovery_and_timer_supervision_unmounted",
        "actor_v2_management_mailbox_admission_unavailable",
    ]
    assert payload["handlerGraphComplete"] is False
    assert payload["cleanSessionHandlerGraphComplete"] is True
    assert payload["cleanSessionHandlerFailures"] == []
    assert payload["effectsRunning"] is False
    assert payload["actorWakeTargetAvailable"] is False
    assert payload["closed"] is False
    assert payload["shutdownComplete"] is False
    assert payload["profileIds"] == [DEFAULT_SESSION_ACTOR_PROFILE_ID]
    assert payload["recoveryMaterializationStates"] == [
        "active_chat",
        "active_chat_settling",
        "active_reply",
        "review",
    ]
    assert [service["serviceName"] for service in payload["backgroundServices"]] == [
        "durable_recovery_scanner",
        "durable_review_due_scanner",
    ]
    assert [service["status"] for service in payload["backgroundServices"]] == [
        "stopped",
        "stopped",
    ]
    missing = {
        (failure["effectKind"], failure["contractVersion"])
        for failure in payload["handlerFailures"]
    }
    assert ("cancel_model_execution", 3) not in missing
    assert ("cancel_review_workflow", 1) in missing
    assert len(missing) == 13


def test_agent_runtime_diagnostics_highlights_redacted_unknown_actions(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    assert bot.database is not None
    key = SessionKey("bot-main", "bot-main:group:room")
    owner = bot.database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="unknown action diagnostics fixture",
    ).ownership
    with bot.database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id,
                session_id, ownership_generation, action_ordinal, action_kind,
                contract_version, request_digest, request_json, status,
                attempt_count, claim_id, lease_owner, lease_until,
                platform_result_json, rejection_json, unknown_json,
                assistant_message_log_id, prepared_at,
                execution_started_at, settled_at, updated_at
            ) VALUES (
                'action-key', 'action-effect', 'operation-1', ?, ?, ?, 0,
                'send_poke', 1, ?,
                '{"operation_id":"operation-1","text":"private-reply","api_token":"private-token"}',
                'unknown', 1, 'action-claim', 'action-worker', NULL,
                '{"result":"private-result"}', '{}',
                '{"reason_code":"lost_ack","error":"private-error"}',
                NULL, 1.0, 2.0, 3.0, 3.0
            )
            """,
            (key.profile_id, key.session_id, owner.generation, "b" * 64),
        )
        conn.execute(
            """
            INSERT INTO agent_external_action_attempts (
                idempotency_key, attempt_count, claim_id, lease_owner,
                claimed_at, lease_until, status, platform_result_json,
                rejection_json, unknown_json, assistant_message_log_id,
                settled_at
            ) VALUES (
                'action-key', 1, 'action-claim', 'action-worker', 2.0, 30.0,
                'unknown', '{"result":"private-result"}', '{}',
                '{"reason_code":"lost_ack","error":"private-error"}',
                NULL, 3.0
            )
            """
        )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/agent-runtime/profiles/bot-main/sessions/bot-main:group:room",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    actions = response.json()["data"]["externalActions"]
    assert actions["status"] == "attention_required"
    assert actions["attentionRequired"] is True
    assert actions["unknownCount"] == 1
    assert actions["abandonedBeforeDispatchCount"] == 0
    assert actions["receipts"]["byStatus"] == {"unknown": 1}
    assert actions["attempts"]["byStatus"] == {"unknown": 1}
    assert actions["outboundBlocker"] is None
    serialized = repr(actions)
    assert "private-reply" not in serialized
    assert "private-token" not in serialized
    assert "private-result" not in serialized
    assert "private-error" not in serialized


def test_agent_runtime_diagnostics_exposes_abandoned_pre_dispatch_actions(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    assert bot.database is not None
    key = SessionKey("bot-main", "bot-main:group:room")
    owner = bot.database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="abandoned action diagnostics fixture",
    ).ownership
    with bot.database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_external_action_receipts (
                idempotency_key, effect_id, operation_id, profile_id,
                session_id, ownership_generation, action_ordinal, action_kind,
                contract_version, request_digest, request_json, status,
                attempt_count, claim_id, lease_owner, lease_until,
                platform_result_json, rejection_json, unknown_json,
                assistant_message_log_id, prepared_at,
                execution_started_at, settled_at, updated_at
            ) VALUES (
                'abandoned-action-key', 'abandoned-action-effect', 'operation-1',
                ?, ?, ?, 0, 'send_poke', 1, ?,
                '{"operation_id":"operation-1","text":"private-reply"}',
                'abandoned_before_dispatch', 0, '', '', NULL,
                '{}', '{}', '{}', NULL, 1.0, NULL, 2.0, 2.0
            )
            """,
            (key.profile_id, key.session_id, owner.generation, "c" * 64),
        )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/agent-runtime/profiles/bot-main/sessions/bot-main:group:room",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    actions = response.json()["data"]["externalActions"]
    assert actions["status"] == "ok"
    assert actions["attentionRequired"] is False
    assert actions["unknownCount"] == 0
    assert actions["abandonedBeforeDispatchCount"] == 1
    assert actions["liveCount"] == 0
    assert actions["receipts"]["byStatus"] == {"abandoned_before_dispatch": 1}
    assert actions["outboundBlocker"] is None
    assert "private-reply" not in repr(actions)


def test_agent_runtime_diagnostics_projects_safe_actor_outbound_blocker(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    assert bot.database is not None
    key = SessionKey("bot-main", "bot-main:group:room")
    owner = bot.database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.ACTOR_V2,
        reason="outbound blocker diagnostics fixture",
    ).ownership
    with bot.database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation, data_json,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 1.0, 1.0)
            """,
            (
                key.profile_id,
                key.session_id,
                owner.generation,
                (
                    '{"outbound_blocked":{"kind":"effect_failed",'
                    '"effect_id":"external-action:abc",'
                    '"operation_id":"round:7",'
                    '"failure_event_id":"effect-failed:abc",'
                    '"failure_code":"ExternalActionRetryRequired",'
                    '"private_message":"must-not-leak"}}'
                ),
            ),
        )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/agent-runtime/profiles/bot-main/sessions/bot-main:group:room",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    actions = response.json()["data"]["externalActions"]
    assert actions["status"] == "attention_required"
    assert actions["attentionRequired"] is True
    assert actions["outboundBlocker"] == {
        "effectId": "external-action:abc",
        "failureCode": "ExternalActionRetryRequired",
        "failureEventId": "effect-failed:abc",
        "kind": "effect_failed",
        "operationId": "round:7",
    }
    assert "must-not-leak" not in repr(actions)


def test_agent_runtime_session_diagnostics_uses_explicit_404_and_strict_paths(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    app = create_api_app(bot, _BootStub(tmp_path))
    headers = _auth_headers(app)

    with TestClient(app) as client:
        missing = client.get(
            "/api/v1/agent-runtime/profiles/missing/sessions/missing:group:room",
            headers=headers,
        )
        invalid = client.get(
            "/api/v1/agent-runtime/profiles/not%20safe/sessions/valid:group:room",
            headers=headers,
        )
        overlong = client.get(
            "/api/v1/agent-runtime/profiles/" + ("a" * 129) + "/sessions/valid:group:room",
            headers=headers,
        )
        encoded_slash = client.get(
            "/api/v1/agent-runtime/profiles/profile-a/sessions/group%2Froom",
            headers=headers,
        )

    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "AGENT_RUNTIME_SESSION_NOT_FOUND"
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "VALIDATION_ERROR"
    assert overlong.status_code == 422
    assert overlong.json()["error"]["code"] == "VALIDATION_ERROR"
    assert encoded_slash.status_code == 404


def test_agent_runtime_session_diagnostics_projects_legacy_only_state(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    assert bot.database is not None
    key = SessionKey("bot-legacy", "bot-legacy:group:room")
    legacy_session_id = "instance-legacy:group:room"
    bot.database.agent_runtime_ownership.claim(
        key,
        AgentRuntimeOwnershipMode.LEGACY,
        reason="legacy API fixture",
        legacy_session_id=legacy_session_id,
    )
    with bot.database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_scheduler_states (session_id, state, updated_at)
            VALUES (?, 'idle', 1.0)
            """,
            (legacy_session_id,),
        )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/agent-runtime/profiles/bot-legacy/sessions/bot-legacy:group:room",
            headers=_auth_headers(app),
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["runtimeKind"] == "legacy"
    assert payload["actorDataStatus"] == "not_applicable"
    assert payload["aggregate"] is None
    assert payload["legacy"]["canonical"] is True
    assert payload["legacy"]["schedulerState"]["state"] == "idle"
