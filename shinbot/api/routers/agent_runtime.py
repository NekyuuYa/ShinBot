"""Agent runtime overview API."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

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
    botId: str = ""
    botName: str = ""
    enabled: bool = False
    agentMode: str = ""
    agentConfig: str = ""
    bindings: list[AgentRuntimeBinding] = Field(default_factory=list)
    tasks: list[AgentRuntimeTask] = Field(default_factory=list)
    sessions: list[AgentRuntimeSession] = Field(default_factory=list)


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
        {
            "key": snapshot.key,
            "name": snapshot.name,
            "done": snapshot.done,
            "cancelled": snapshot.cancelled,
            "error": snapshot.error,
        }
        for snapshot in task_manager.snapshots(prefix=namespace)
    ]


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
                botId=bot_config.id,
                botName=bot_config.display_name,
                enabled=bot_config.enabled,
                agentMode=bot_config.agent.mode,
                agentConfig=bot_config.agent.config,
                bindings=bindings,
                tasks=_task_overview(bot, bot_config.id),
                sessions=_session_overview(bot, bot_config.id),
            )
        )
    return ok([profile.model_dump() for profile in profiles])


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
