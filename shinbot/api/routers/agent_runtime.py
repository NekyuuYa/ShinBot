"""Agent runtime overview API."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel, Field

from shinbot.admin.agent_runtime_diagnostics import (
    AGENT_RUNTIME_DIAGNOSTIC_SEGMENT_PATTERN,
    AGENT_RUNTIME_PROFILE_ID_MAX_LENGTH,
    AGENT_RUNTIME_SESSION_ID_MAX_LENGTH,
    AgentRuntimeDiagnosticsInvalidKey,
    AgentRuntimeDiagnosticsNotFound,
)
from shinbot.admin.agent_runtime_diagnostics import (
    get_agent_runtime_session_diagnostics as read_agent_runtime_session_diagnostics,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, Envelope, ok

router = APIRouter(
    prefix="/agent-runtime",
    tags=["agent-runtime"],
    dependencies=AuthRequired,
)


class AgentRuntimeBinding(BaseModel):
    adapterInstanceId: str = ""
    sessionPatterns: list[str] = Field(default_factory=list)
    enabled: bool = False
    priority: int = 0
    platformState: dict[str, bool] = Field(
        default_factory=lambda: {"running": False, "connected": False, "available": False}
    )


class AgentRuntimeTask(BaseModel):
    key: str = ""
    name: str = ""
    done: bool = False
    cancelled: bool = False
    error: str | None = None


class AgentRuntimeServiceHealth(BaseModel):
    """Process-local supervision health for one legacy timer loop."""

    serviceName: str = ""
    sessionId: str = ""
    status: str = "stopped"
    startedAt: float = 0.0
    lastScanStartedAt: float = 0.0
    lastSuccessAt: float = 0.0
    lastErrorAt: float = 0.0
    lastErrorCode: str = ""
    lastErrorMessage: str = ""
    consecutiveFailures: int = 0
    scanCount: int = 0
    successCount: int = 0


class AgentRuntimeTimerHealth(BaseModel):
    """Process-local health snapshots for one profile's legacy timers."""

    reviewDueTimer: AgentRuntimeServiceHealth | None = None
    activeChatTimers: list[AgentRuntimeServiceHealth] = Field(default_factory=list)


class AgentRuntimeSession(BaseModel):
    sessionId: str = ""
    adapterInstanceId: str = ""
    platformState: dict[str, bool] = Field(
        default_factory=lambda: {"running": False, "connected": False, "available": False}
    )
    state: str = ""
    reviewPlan: dict[str, Any] | None = None
    activeChatState: dict[str, Any] | None = None
    unreadCount: int = 0
    highPriorityCount: int = 0
    latestReviewRun: dict[str, Any] | None = None
    latestReviewSummary: dict[str, Any] | None = None
    latestAudit: dict[str, Any] | None = None


class AgentRuntimeProfile(BaseModel):
    profileId: str = ""
    botId: str = ""
    botName: str = ""
    enabled: bool = False
    agentMode: str = ""
    agentConfig: str = ""
    bindings: list[AgentRuntimeBinding] = Field(default_factory=list)
    tasks: list[AgentRuntimeTask] = Field(default_factory=list)
    taskFailures: list[AgentRuntimeTask] = Field(default_factory=list)
    timerHealth: AgentRuntimeTimerHealth = Field(default_factory=AgentRuntimeTimerHealth)
    sessions: list[AgentRuntimeSession] = Field(default_factory=list)


class AgentRuntimeDiagnosticCollection(BaseModel):
    """Bounded recent rows plus complete status counts."""

    total: int = 0
    byStatus: dict[str, int] = Field(default_factory=dict)
    recent: list[dict[str, Any]] = Field(default_factory=list)


class AgentRuntimeReviewScheduleDiagnostics(AgentRuntimeDiagnosticCollection):
    """Current schedule resolution and recent planning provenance."""

    currentPlanId: str = ""
    resolution: Literal["resolved", "missing", "not_set"] = "not_set"
    current: dict[str, Any] | None = None


class AgentRuntimeExternalActionDiagnostics(BaseModel):
    """Durable external-action outcomes with explicit unknown attention state."""

    status: Literal["ok", "active", "attention_required"]
    attentionRequired: bool
    unknownCount: int = Field(ge=0)
    abandonedBeforeDispatchCount: int = Field(ge=0)
    liveCount: int = Field(ge=0)
    outboundBlocker: dict[str, str] | None = None
    receipts: AgentRuntimeDiagnosticCollection
    attempts: AgentRuntimeDiagnosticCollection


class AgentRuntimeLegacyDiagnostics(BaseModel):
    """Legacy scheduler evidence addressed by the ownership alias."""

    sessionId: str
    canonical: bool
    dataStatus: Literal["available", "empty"]
    schedulerState: dict[str, Any] | None = None
    unreadMessages: dict[str, int] = Field(default_factory=dict)
    unreadRanges: dict[str, int] = Field(default_factory=dict)


class AgentRuntimeSessionDiagnosticsData(BaseModel):
    """Canonical durable diagnostics for one profile-scoped session."""

    profileId: str
    sessionId: str
    sensitiveDataPolicy: Literal["redacted"]
    runtimeKind: Literal["legacy", "actor_v2", "unowned"]
    actorCanonical: bool
    actorDataStatus: Literal["available", "not_initialized", "not_applicable"]
    ownership: dict[str, Any] | None = None
    ownershipEvents: list[dict[str, Any]] = Field(default_factory=list)
    aggregate: dict[str, Any] | None = None
    mailbox: AgentRuntimeDiagnosticCollection
    operations: AgentRuntimeDiagnosticCollection
    effects: AgentRuntimeDiagnosticCollection
    externalActions: AgentRuntimeExternalActionDiagnostics
    reviewSchedule: AgentRuntimeReviewScheduleDiagnostics
    routeDeliveries: AgentRuntimeDiagnosticCollection
    routingJobs: list[dict[str, Any]] = Field(default_factory=list)
    recentTransitions: list[dict[str, Any]] = Field(default_factory=list)
    recentScheduleEvents: list[dict[str, Any]] = Field(default_factory=list)
    legacy: AgentRuntimeLegacyDiagnostics | None = None


AgentRuntimeProfileIdPath = Annotated[
    str,
    Path(
        min_length=1,
        max_length=AGENT_RUNTIME_PROFILE_ID_MAX_LENGTH,
        pattern=AGENT_RUNTIME_DIAGNOSTIC_SEGMENT_PATTERN,
    ),
]
AgentRuntimeSessionIdPath = Annotated[
    str,
    Path(
        min_length=1,
        max_length=AGENT_RUNTIME_SESSION_ID_MAX_LENGTH,
        pattern=AGENT_RUNTIME_DIAGNOSTIC_SEGMENT_PATTERN,
    ),
]


def _parse_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _review_plan_payload(plan: Any) -> dict[str, Any] | None:
    if plan is None:
        return None
    return {
        "sessionId": plan.session_id,
        "nextReviewAt": plan.next_review_at,
        "reason": plan.reason,
        "mentionSensitivity": plan.mention_sensitivity.value,
        "activeReplyThreshold": {
            "atCount": plan.active_reply_threshold.at_count,
            "windowSeconds": plan.active_reply_threshold.window_seconds,
        },
        "updatedAt": plan.updated_at,
    }


def _active_chat_state_payload(state: Any) -> dict[str, Any] | None:
    if state is None:
        return None
    return {
        "sessionId": state.session_id,
        "interestValue": state.interest_value,
        "decayHalfLifeSeconds": state.decay_half_life_seconds,
        "enteredAt": state.entered_at,
        "updatedAt": state.updated_at,
        "tickCount": state.tick_count,
        "activeEpoch": state.active_epoch,
        "bootstrapApplied": state.bootstrap_applied,
        "bootstrapDisposition": getattr(state.bootstrap_disposition, "value", None),
    }


def _summary_payload(record: Any) -> dict[str, Any] | None:
    if record is None:
        return None
    metadata = _parse_json(record.metadata_json, {})
    return {
        "id": record.id,
        "sessionId": record.session_id,
        "startMsgLogId": record.msg_log_start,
        "endMsgLogId": record.msg_log_end,
        "messageCount": record.msg_count,
        "summary": record.content,
        "reason": metadata.get("reason", ""),
        "createdAt": record.created_at,
    }


def _review_run_payload(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": str(row["id"]),
        "sessionId": str(row["session_id"]),
        "startedAt": float(row["started_at"] or 0.0),
        "finishedAt": row["finished_at"],
        "batchSize": int(row["batch_size"] or 0),
        "replied": bool(row["replied"]),
        "responseSummary": str(row["response_summary"] or ""),
        "finishReason": str(row["finish_reason"] or ""),
    }


def _platform_state_payload(bot: Any, instance_id: str) -> dict[str, bool]:
    """Build runtime availability flags for an adapter instance.

    Args:
        bot: The running ShinBot application.
        instance_id: Adapter instance identifier.

    Returns:
        A runtime availability payload.
    """
    manager = getattr(bot, "adapter_manager", None)
    if manager is None or not instance_id:
        return {"running": False, "connected": False, "available": False}
    return {
        "running": bool(manager.is_running(instance_id)),
        "connected": bool(manager.is_connected(instance_id)),
        "available": bool(manager.is_available(instance_id)),
    }


def _session_overview(bot: Any, bot_id: str) -> list[dict[str, Any]]:
    agent_runtime = getattr(bot, "agent_runtime", None)
    database = getattr(bot, "database", None)
    if agent_runtime is None or database is None:
        return []

    scheduler = agent_runtime.agent_profile_for_bot(bot_id).agent_scheduler
    # The scheduler is already isolated per bot profile, while session ids are
    # anchored to adapter instance ids. Filtering again by bot id would hide
    # valid sessions for the selected profile.
    session_ids = scheduler.list_session_ids() or []
    result: list[dict[str, Any]] = []
    for session_id in session_ids:
        instance_id = str(session_id.split(":", 1)[0] or "")
        review_plan = scheduler.review_plan_for(session_id)
        active_chat_state = scheduler.active_chat_state_for(session_id)
        latest_review_summary = database.agent_summaries.get_latest_by_session(session_id)
        latest_audit = database.audit.get_latest_by_session(session_id)
        with database.connect() as conn:
            run_row = conn.execute(
                """
                SELECT *
                FROM workflow_runs
                WHERE session_id = ?
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        result.append(
            {
                "sessionId": session_id,
                "adapterInstanceId": instance_id,
                "platformState": _platform_state_payload(bot, instance_id),
                "state": scheduler.state_for(session_id).value,
                "reviewPlan": _review_plan_payload(review_plan),
                "activeChatState": _active_chat_state_payload(active_chat_state),
                "unreadCount": scheduler.count_unread_messages(session_id),
                "highPriorityCount": len(scheduler.high_priority_events(session_id)),
                "latestReviewRun": _review_run_payload(run_row),
                "latestReviewSummary": _summary_payload(latest_review_summary),
                "latestAudit": latest_audit,
            }
        )
    return result


def _task_snapshot_payload(snapshot: Any) -> dict[str, Any]:
    return {
        "key": snapshot.key,
        "name": snapshot.name,
        "done": snapshot.done,
        "cancelled": snapshot.cancelled,
        "error": snapshot.error,
    }


def _task_overview(bot: Any, bot_id: str) -> list[dict[str, Any]]:
    agent_runtime = getattr(bot, "agent_runtime", None)
    if agent_runtime is None:
        return []
    profile = agent_runtime.agent_profile_for_bot(bot_id)
    task_manager = getattr(agent_runtime, "task_manager", None)
    if task_manager is None:
        return []
    namespace = f"agent:{profile.bot_id or profile.profile_id}:"
    return [
        _task_snapshot_payload(snapshot)
        for snapshot in task_manager.snapshots(prefix=namespace)
    ]


def _task_failure_overview(bot: Any, bot_id: str) -> list[dict[str, Any]]:
    agent_runtime = getattr(bot, "agent_runtime", None)
    if agent_runtime is None:
        return []
    profile = agent_runtime.agent_profile_for_bot(bot_id)
    task_manager = getattr(agent_runtime, "task_manager", None)
    if task_manager is None:
        return []
    namespace = f"agent:{profile.bot_id or profile.profile_id}:"
    return [
        _task_snapshot_payload(snapshot)
        for snapshot in task_manager.failures(prefix=namespace)
    ]


def _service_health_payload(snapshot: Any, *, session_id: str = "") -> dict[str, Any]:
    status = getattr(snapshot.status, "value", snapshot.status)
    return {
        "serviceName": snapshot.service_name,
        "sessionId": session_id,
        "status": str(status),
        "startedAt": snapshot.started_at,
        "lastScanStartedAt": snapshot.last_scan_started_at,
        "lastSuccessAt": snapshot.last_success_at,
        "lastErrorAt": snapshot.last_error_at,
        "lastErrorCode": snapshot.last_error_code,
        "lastErrorMessage": snapshot.last_error_message,
        "consecutiveFailures": snapshot.consecutive_failures,
        "scanCount": snapshot.scan_count,
        "successCount": snapshot.success_count,
    }


def _timer_health_overview(bot: Any, bot_id: str) -> dict[str, Any]:
    agent_runtime = getattr(bot, "agent_runtime", None)
    if agent_runtime is None:
        return {"reviewDueTimer": None, "activeChatTimers": []}
    profile = agent_runtime.agent_profile_for_bot(bot_id)

    review_timer = getattr(profile, "review_due_timer", None)
    review_snapshot = (
        review_timer.health_snapshot()
        if review_timer is not None and callable(getattr(review_timer, "health_snapshot", None))
        else None
    )

    active_chat_timer = getattr(profile, "active_chat_timer", None)
    active_snapshots = (
        active_chat_timer.health_snapshots()
        if active_chat_timer is not None
        and callable(getattr(active_chat_timer, "health_snapshots", None))
        else []
    )
    active_payloads = []
    prefix = "active_chat_timer:"
    for snapshot in active_snapshots:
        service_name = str(snapshot.service_name)
        session_id = service_name.removeprefix(prefix) if service_name.startswith(prefix) else ""
        active_payloads.append(
            _service_health_payload(snapshot, session_id=session_id)
        )
    return {
        "reviewDueTimer": (
            _service_health_payload(review_snapshot)
            if review_snapshot is not None
            else None
        ),
        "activeChatTimers": active_payloads,
    }


@router.get("", response_model=Envelope[list[AgentRuntimeProfile]])
def get_agent_runtime_overview(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Return an overview of all agent runtime profiles, sessions, and tasks."""
    profiles: list[AgentRuntimeProfile] = []
    for bot_config in getattr(boot, "bot_service_configs", ()):
        bindings = [
            AgentRuntimeBinding(
                adapterInstanceId=binding.adapter_instance_id,
                sessionPatterns=list(binding.session_patterns),
                enabled=binding.enabled,
                priority=binding.priority,
                platformState=_platform_state_payload(bot, binding.adapter_instance_id),
            )
            for binding in bot_config.bindings
        ]
        profiles.append(
            AgentRuntimeProfile(
                profileId=bot_config.id,
                botId=bot_config.id,
                botName=bot_config.display_name,
                enabled=bot_config.enabled,
                agentMode=bot_config.agent.mode,
                agentConfig=bot_config.agent.config,
                bindings=bindings,
                tasks=_task_overview(bot, bot_config.id),
                taskFailures=_task_failure_overview(bot, bot_config.id),
                timerHealth=_timer_health_overview(bot, bot_config.id),
                sessions=_session_overview(bot, bot_config.id),
            )
        )
    return ok([profile.model_dump() for profile in profiles])


@router.get(
    "/profiles/{profile_id}/sessions/{session_id}",
    response_model=Envelope[AgentRuntimeSessionDiagnosticsData],
)
def get_agent_runtime_session_diagnostics(
    profile_id: AgentRuntimeProfileIdPath,
    session_id: AgentRuntimeSessionIdPath,
    bot: Any = BotDep,
) -> dict[str, Any]:
    """Return canonical durable diagnostics for one stable session key."""

    database = getattr(bot, "database", None)
    if database is None:
        raise HTTPException(
            status_code=503,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent runtime storage is not available",
            },
        )
    try:
        diagnostics = read_agent_runtime_session_diagnostics(
            database,
            profile_id=profile_id,
            session_id=session_id,
        )
    except AgentRuntimeDiagnosticsInvalidKey as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": "VALIDATION_ERROR", "message": str(exc)},
        ) from exc
    except AgentRuntimeDiagnosticsNotFound as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.AGENT_RUNTIME_SESSION_NOT_FOUND,
                "message": str(exc),
            },
        ) from exc
    return ok(diagnostics.to_payload())


class _ManualActionData(BaseModel):
    """Response payload for a manual scheduler action."""

    sessionId: str
    success: bool


@router.post(
    "/sessions/{session_id:path}/trigger-review",
    response_model=Envelope[_ManualActionData],
)
async def trigger_session_review(session_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Manually trigger a review for a session by bringing the review plan forward to now."""
    agent_runtime = getattr(bot, "agent_runtime", None)
    if agent_runtime is None:
        return ok({"sessionId": session_id, "success": False})
    triggered = await agent_runtime.trigger_review(session_id)
    return ok({"sessionId": session_id, "success": triggered})


@router.post(
    "/sessions/{session_id:path}/force-idle",
    response_model=Envelope[_ManualActionData],
)
async def force_session_idle(session_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Force a session back to IDLE from any active state."""
    agent_runtime = getattr(bot, "agent_runtime", None)
    if agent_runtime is None:
        return ok({"sessionId": session_id, "success": False})
    changed = await agent_runtime.force_idle(session_id)
    return ok({"sessionId": session_id, "success": changed})
