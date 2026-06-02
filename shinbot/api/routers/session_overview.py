"""Session management overview API."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, Envelope, ok

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/session-overview",
    tags=["session-overview"],
    dependencies=AuthRequired,
)


class SessionInfoData(BaseModel):
    id: str
    instanceId: str
    sessionType: str
    platform: str
    guildId: Any = None
    channelId: str
    displayName: str
    permissionGroup: str
    createdAt: float
    lastActive: float


class PlatformStateData(BaseModel):
    """Stable runtime availability state for an adapter instance."""

    running: bool = False
    connected: bool = False
    available: bool = False


class SessionConfigData(BaseModel):
    prefixes: list[str]
    llmEnabled: bool
    isMuted: bool
    auditEnabled: bool
    updatedAt: float


class MessageLogData(BaseModel):
    id: int
    sessionId: str
    platformMsgId: str
    senderId: str
    senderName: str
    content: Any
    rawText: str
    role: str
    isRead: bool
    isMentioned: bool
    createdAt: float
    routingStatus: str
    routedAt: Any = None
    routingSkipReason: Any = None


class AuditLogData(BaseModel):
    id: int
    timestamp: str
    entryType: str
    commandName: str
    pluginId: str
    userId: str
    sessionId: str
    instanceId: str
    permissionRequired: str
    permissionGranted: bool
    executionTimeMs: float
    success: bool
    error: str
    metadata: dict[str, Any]


class ReviewSummaryData(BaseModel):
    id: Any = None
    sessionId: str
    summaryType: str
    startMsgLogId: Any = None
    endMsgLogId: Any = None
    messageCount: Any = None
    summary: str
    reason: str
    createdAt: Any = None


class WorkflowRunData(BaseModel):
    id: str
    sessionId: str
    instanceId: str
    responseProfile: str
    batchStartMsgId: Any = None
    batchEndMsgId: Any = None
    batchSize: int
    triggerAttention: float
    effectiveThreshold: float
    toolCalls: list[Any]
    replied: bool
    responseSummary: str
    finishReason: str
    startedAt: float
    finishedAt: Any = None


class AgentStateData(BaseModel):
    state: str
    reviewPlan: dict[str, Any] | None = None
    activeChatState: dict[str, Any] | None = None
    unreadCount: int
    highPriorityCount: int


class SessionOverviewItem(BaseModel):
    session: SessionInfoData
    platformState: PlatformStateData
    config: SessionConfigData | None = None
    history: list[MessageLogData]
    latestMessage: MessageLogData | None = None
    latestAudit: AuditLogData | None = None
    latestReviewSummary: ReviewSummaryData | None = None
    latestActiveChatSummary: ReviewSummaryData | None = None
    latestOverflowSummary: ReviewSummaryData | None = None
    latestWorkflowRun: WorkflowRunData | None = None
    agent: AgentStateData | None = None
    messageCount: int
    auditCount: int


class SessionDeletedData(BaseModel):
    """Response payload returned after deleting a session."""

    sessionId: str
    deleted: bool


class SessionClearedData(BaseModel):
    """Response payload returned after clearing part of a session."""

    sessionId: str
    scope: str
    cleared: bool


class SessionBatchActionRequest(BaseModel):
    """Request payload for batch session management actions."""

    sessionIds: list[str]


class SessionBatchActionData(BaseModel):
    """Response payload returned after a batch session action."""

    action: str
    requestedCount: int
    processedCount: int
    processedSessionIds: list[str]
    missingSessionIds: list[str]


def _parse_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    import json

    try:
        return json.loads(value)
    except Exception:
        return default


def _session_config_payload(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "prefixes": _parse_json(row["prefixes_json"], ["/"]),
        "llmEnabled": bool(row["llm_enabled"]) if row["llm_enabled"] is not None else True,
        "isMuted": bool(row["is_muted"]) if row["is_muted"] is not None else False,
        "auditEnabled": bool(row["audit_enabled"]) if row["audit_enabled"] is not None else False,
        "updatedAt": float(row["updated_at"] or 0.0),
    }


def _message_payload(row: Any) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "sessionId": str(row["session_id"] or ""),
        "platformMsgId": str(row["platform_msg_id"] or ""),
        "senderId": str(row["sender_id"] or ""),
        "senderName": str(row["sender_name"] or ""),
        "content": _parse_json(row["content_json"], []),
        "rawText": str(row["raw_text"] or ""),
        "role": str(row["role"] or ""),
        "isRead": bool(row["is_read"]),
        "isMentioned": bool(row["is_mentioned"]),
        "createdAt": float(row["created_at"] or 0.0),
        "routingStatus": str(row["routing_status"] or "pending"),
        "routedAt": row["routed_at"],
        "routingSkipReason": row["routing_skip_reason"],
    }


def _audit_payload(row: Any) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "timestamp": str(row["timestamp"] or ""),
        "entryType": str(row["entry_type"] or ""),
        "commandName": str(row["command_name"] or ""),
        "pluginId": str(row["plugin_id"] or ""),
        "userId": str(row["user_id"] or ""),
        "sessionId": str(row["session_id"] or ""),
        "instanceId": str(row["instance_id"] or ""),
        "permissionRequired": str(row["permission_required"] or ""),
        "permissionGranted": bool(row["permission_granted"]),
        "executionTimeMs": float(row["execution_time_ms"] or 0.0),
        "success": bool(row["success"]),
        "error": str(row["error"] or ""),
        "metadata": _parse_json(row["metadata_json"], {}),
    }


def _review_summary_payload(record: Any) -> dict[str, Any] | None:
    if record is None:
        return None
    metadata = _parse_json(record.metadata_json, {})
    return {
        "id": record.id,
        "sessionId": record.session_id,
        "summaryType": str(record.summary_type),
        "startMsgLogId": record.msg_log_start,
        "endMsgLogId": record.msg_log_end,
        "messageCount": record.msg_count,
        "summary": record.content,
        "reason": metadata.get("reason", ""),
        "createdAt": record.created_at,
    }


def _workflow_run_payload(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": str(row["id"] or ""),
        "sessionId": str(row["session_id"] or ""),
        "instanceId": str(row["instance_id"] or ""),
        "responseProfile": str(row["response_profile"] or ""),
        "batchStartMsgId": row["batch_start_msg_id"],
        "batchEndMsgId": row["batch_end_msg_id"],
        "batchSize": int(row["batch_size"] or 0),
        "triggerAttention": float(row["trigger_attention"] or 0.0),
        "effectiveThreshold": float(row["effective_threshold"] or 0.0),
        "toolCalls": _parse_json(row["tool_calls_json"], []),
        "replied": bool(row["replied"]),
        "responseSummary": str(row["response_summary"] or ""),
        "finishReason": str(row["finish_reason"] or ""),
        "startedAt": float(row["started_at"] or 0.0),
        "finishedAt": row["finished_at"],
    }


def _platform_state_payload(bot: Any, instance_id: str) -> dict[str, Any]:
    """Build runtime availability flags for a session's adapter instance.

    Args:
        bot: The running ShinBot application.
        instance_id: Adapter instance ID extracted from the session.

    Returns:
        A serializable runtime state payload.
    """
    manager = getattr(bot, "adapter_manager", None)
    if manager is None or not instance_id:
        return {"running": False, "connected": False, "available": False}
    return {
        "running": bool(manager.is_running(instance_id)),
        "connected": bool(manager.is_connected(instance_id)),
        "available": bool(manager.is_available(instance_id)),
    }


def _latest_summary_of_type(database: Any, session_id: str, summary_type: str) -> Any:
    repo = getattr(database, "agent_summaries", None)
    if repo is None:
        return None
    try:
        from shinbot.agent.services.summaries.models import SummaryType

        return repo.get_latest_by_session(session_id, summary_type=SummaryType(summary_type))
    except Exception:
        return None


def _session_storage(bot: Any) -> Any:
    database = getattr(bot, "database", None)
    if database is None:
        raise HTTPException(
            status_code=503,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Session storage is not available",
            },
        )
    return database


def _existing_session_or_404(database: Any, session_id: str) -> dict[str, Any]:
    session = database.sessions.get(session_id)
    if session is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.SESSION_NOT_FOUND,
                "message": f"Session not found: {session_id}",
            },
        )
    return session


def _reset_session_runtime(bot: Any, boot: Any, session_id: str, *, instance_id: str = "") -> None:
    agent_runtime = getattr(bot, "agent_runtime", None)
    if agent_runtime is None:
        return

    seen_profiles: set[int] = set()
    bot_ids = [str(config.id or "").strip() for config in getattr(boot, "bot_service_configs", ())]
    if instance_id:
        bot_ids.append(str(instance_id).strip())
    for bot_id in bot_ids:
        profile = agent_runtime.agent_profile_for_bot(bot_id)
        marker = id(profile)
        if marker in seen_profiles:
            continue
        seen_profiles.add(marker)
        profile.active_chat_workflow.stop_active_chat(session_id)
        profile.active_chat_timer.cancel(session_id)

    context_manager = getattr(agent_runtime, "context_manager", None)
    if context_manager is None:
        return
    try:
        context_manager.delete_session_state(session_id)
    except Exception:
        logger.exception("Failed to delete context state for session %s", session_id)


def _unique_session_ids(session_ids: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for session_id in session_ids:
        cleaned = str(session_id or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _resolve_session_batch(database: Any, session_ids: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    existing_sessions: list[dict[str, Any]] = []
    missing_session_ids: list[str] = []
    for session_id in _unique_session_ids(session_ids):
        session = database.sessions.get(session_id)
        if session is None:
            missing_session_ids.append(session_id)
            continue
        existing_sessions.append(session)
    return existing_sessions, missing_session_ids


def _validated_batch_request(database: Any, body: SessionBatchActionRequest) -> tuple[list[dict[str, Any]], list[str]]:
    existing_sessions, missing_session_ids = _resolve_session_batch(database, body.sessionIds)
    if not existing_sessions:
        detail: dict[str, Any] = {
            "code": EC.SESSION_NOT_FOUND,
            "message": "No matching sessions were found",
        }
        if missing_session_ids:
            detail["missingSessionIds"] = missing_session_ids
        raise HTTPException(status_code=404, detail=detail)
    return existing_sessions, missing_session_ids


@router.get("", response_model=Envelope[list[SessionOverviewItem]])
def get_session_overview(bot=BotDep, boot=BootDep):
    """Get a full overview of all sessions including history, audit logs, agent state, and summaries."""
    database = getattr(bot, "database", None)
    if database is None:
        return ok([])

    sessions: list[dict[str, Any]] = []
    with database.connect() as conn:
        session_rows = conn.execute(
            """
            SELECT
                s.id,
                s.instance_id,
                s.session_type,
                s.platform,
                s.guild_id,
                s.channel_id,
                s.display_name,
                s.permission_group,
                s.created_at,
                s.last_active,
                c.prefixes_json,
                c.llm_enabled,
                c.is_muted,
                c.audit_enabled,
                c.updated_at
            FROM sessions AS s
            LEFT JOIN session_configs AS c ON c.session_id = s.id
            ORDER BY s.last_active DESC, s.id ASC
            """
        ).fetchall()

    agent_runtime = getattr(bot, "agent_runtime", None)
    agent_profiles: dict[str, Any] = {}
    if agent_runtime is not None:
        for bot_config in getattr(boot, "bot_service_configs", ()):
            agent_profiles[bot_config.id] = agent_runtime.agent_profile_for_bot(bot_config.id)

    for row in session_rows:
        session_id = str(row["id"] or "")
        instance_id = str(row["instance_id"] or "")
        with database.connect() as conn:
            history_rows = conn.execute(
                """
                SELECT *
                FROM message_logs
                WHERE session_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 40
                """,
                (session_id,),
            ).fetchall()
            audit_rows = conn.execute(
                """
                SELECT *
                FROM audit_logs
                WHERE session_id = ?
                ORDER BY timestamp DESC, id DESC
                LIMIT 20
                """,
                (session_id,),
            ).fetchall()
            workflow_row = conn.execute(
                """
                SELECT *
                FROM workflow_runs
                WHERE session_id = ?
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()

        message_rows = [_message_payload(item) for item in reversed(history_rows)]
        latest_message = message_rows[-1] if message_rows else None
        latest_review_summary = _latest_summary_of_type(database, session_id, "block_digest")
        latest_active_chat_summary = _latest_summary_of_type(database, session_id, "active_chat")
        latest_overflow_summary = _latest_summary_of_type(database, session_id, "overflow_compression")
        latest_audit = _audit_payload(audit_rows[0]) if audit_rows else None
        profile = next(
            (
                candidate
                for candidate in agent_profiles.values()
                if session_id in (candidate.agent_scheduler.list_session_ids() or [])
            ),
            None,
        )
        review_plan = profile.agent_scheduler.review_plan_for(session_id) if profile else None
        active_chat_state = profile.agent_scheduler.active_chat_state_for(session_id) if profile else None
        agent_state = profile.agent_scheduler.state_for(session_id).value if profile else ""

        sessions.append(
            {
                "session": {
                    "id": session_id,
                    "instanceId": instance_id,
                    "sessionType": str(row["session_type"] or ""),
                    "platform": str(row["platform"] or ""),
                    "guildId": row["guild_id"],
                    "channelId": str(row["channel_id"] or ""),
                    "displayName": str(row["display_name"] or ""),
                    "permissionGroup": str(row["permission_group"] or "default"),
                    "createdAt": float(row["created_at"] or 0.0),
                    "lastActive": float(row["last_active"] or 0.0),
                },
                "platformState": _platform_state_payload(bot, instance_id),
                "config": _session_config_payload(row),
                "history": message_rows,
                "latestMessage": latest_message,
                "latestAudit": latest_audit,
                "latestReviewSummary": _review_summary_payload(latest_review_summary),
                "latestActiveChatSummary": _review_summary_payload(latest_active_chat_summary),
                "latestOverflowSummary": _review_summary_payload(latest_overflow_summary),
                "latestWorkflowRun": _workflow_run_payload(workflow_row),
                "agent": (
                    {
                        "state": agent_state,
                        "reviewPlan": (
                            None
                            if review_plan is None
                            else {
                                "sessionId": review_plan.session_id,
                                "nextReviewAt": review_plan.next_review_at,
                                "reason": review_plan.reason,
                                "mentionSensitivity": review_plan.mention_sensitivity.value,
                                "activeReplyThreshold": {
                                    "atCount": review_plan.active_reply_threshold.at_count,
                                    "windowSeconds": review_plan.active_reply_threshold.window_seconds,
                                },
                                "updatedAt": review_plan.updated_at,
                            }
                        ),
                        "activeChatState": (
                            None
                            if active_chat_state is None
                            else {
                                "sessionId": active_chat_state.session_id,
                                "interestValue": active_chat_state.interest_value,
                                "decayHalfLifeSeconds": active_chat_state.decay_half_life_seconds,
                                "enteredAt": active_chat_state.entered_at,
                                "updatedAt": active_chat_state.updated_at,
                                "tickCount": active_chat_state.tick_count,
                                "activeEpoch": active_chat_state.active_epoch,
                                "bootstrapApplied": active_chat_state.bootstrap_applied,
                                "bootstrapDisposition": getattr(
                                    active_chat_state.bootstrap_disposition, "value", None
                                ),
                            }
                        ),
                        "unreadCount": profile.agent_scheduler.count_unread_messages(session_id),
                        "highPriorityCount": len(profile.agent_scheduler.high_priority_events(session_id)),
                    }
                    if profile is not None
                    else None
                ),
                "messageCount": len(message_rows),
                "auditCount": len(audit_rows),
            }
        )

    return ok(sessions)


@router.delete("/{session_id:path}/history", response_model=Envelope[SessionClearedData])
def clear_session_history(session_id: str, bot=BotDep, boot=BootDep):
    """Clear message history and message-derived state while keeping the session shell."""
    database = _session_storage(bot)
    existing = _existing_session_or_404(database, session_id)
    _reset_session_runtime(
        bot,
        boot,
        session_id,
        instance_id=str(existing.get("instance_id") or ""),
    )
    database.sessions.clear_history(session_id)
    return ok({"sessionId": session_id, "scope": "history", "cleared": True})


@router.delete("/{session_id:path}/audit-logs", response_model=Envelope[SessionClearedData])
def clear_session_audit_logs(session_id: str, bot=BotDep):
    """Clear audit logs for a session while keeping all other records."""
    database = _session_storage(bot)
    _existing_session_or_404(database, session_id)
    database.sessions.clear_audit_logs(session_id)
    return ok({"sessionId": session_id, "scope": "audit_logs", "cleared": True})


@router.post("/batch/history", response_model=Envelope[SessionBatchActionData])
def clear_session_history_batch(body: SessionBatchActionRequest, bot=BotDep, boot=BootDep):
    """Clear message history and derived state for multiple sessions."""
    database = _session_storage(bot)
    existing_sessions, missing_session_ids = _validated_batch_request(database, body)
    processed_session_ids = [str(item["id"]) for item in existing_sessions]
    for session in existing_sessions:
        _reset_session_runtime(
            bot,
            boot,
            str(session["id"]),
            instance_id=str(session.get("instance_id") or ""),
        )
    database.sessions.clear_history_many(processed_session_ids)
    return ok(
        {
            "action": "history",
            "requestedCount": len(body.sessionIds),
            "processedCount": len(processed_session_ids),
            "processedSessionIds": processed_session_ids,
            "missingSessionIds": missing_session_ids,
        }
    )


@router.post("/batch/audit-logs", response_model=Envelope[SessionBatchActionData])
def clear_session_audit_logs_batch(body: SessionBatchActionRequest, bot=BotDep):
    """Clear audit logs for multiple sessions."""
    database = _session_storage(bot)
    existing_sessions, missing_session_ids = _validated_batch_request(database, body)
    processed_session_ids = [str(item["id"]) for item in existing_sessions]
    database.sessions.clear_audit_logs_many(processed_session_ids)
    return ok(
        {
            "action": "audit_logs",
            "requestedCount": len(body.sessionIds),
            "processedCount": len(processed_session_ids),
            "processedSessionIds": processed_session_ids,
            "missingSessionIds": missing_session_ids,
        }
    )


@router.post("/batch/delete", response_model=Envelope[SessionBatchActionData])
def delete_session_overview_batch(body: SessionBatchActionRequest, bot=BotDep, boot=BootDep):
    """Delete multiple sessions and their related persisted/runtime state."""
    database = _session_storage(bot)
    existing_sessions, missing_session_ids = _validated_batch_request(database, body)
    processed_session_ids = [str(item["id"]) for item in existing_sessions]
    for session in existing_sessions:
        session_id = str(session["id"])
        _reset_session_runtime(
            bot,
            boot,
            session_id,
            instance_id=str(session.get("instance_id") or ""),
        )
        bot.session_manager.remove(session_id, delete_persisted=False)
    database.sessions.delete_many(processed_session_ids)
    return ok(
        {
            "action": "delete",
            "requestedCount": len(body.sessionIds),
            "processedCount": len(processed_session_ids),
            "processedSessionIds": processed_session_ids,
            "missingSessionIds": missing_session_ids,
        }
    )


@router.delete("/{session_id:path}", response_model=Envelope[SessionDeletedData])
def delete_session_overview_entry(session_id: str, bot=BotDep, boot=BootDep):
    """Delete a session and its related persisted/runtime state."""
    database = _session_storage(bot)
    existing = _existing_session_or_404(database, session_id)
    _reset_session_runtime(
        bot,
        boot,
        session_id,
        instance_id=str(existing.get("instance_id") or ""),
    )
    database.sessions.delete(session_id)
    bot.session_manager.remove(session_id, delete_persisted=False)
    return ok({"sessionId": session_id, "deleted": True})
