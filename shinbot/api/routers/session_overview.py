"""Session management overview API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

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


def _latest_summary_of_type(database: Any, session_id: str, summary_type: str) -> Any:
    repo = getattr(database, "agent_summaries", None)
    if repo is None:
        return None
    try:
        from shinbot.agent.services.summaries.models import SummaryType

        return repo.get_latest_by_session(session_id, summary_type=SummaryType(summary_type))
    except Exception:
        return None


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
        profile = agent_profiles.get(str(row["instance_id"] or ""))
        review_plan = profile.agent_scheduler.review_plan_for(session_id) if profile else None
        active_chat_state = profile.agent_scheduler.active_chat_state_for(session_id) if profile else None
        agent_state = profile.agent_scheduler.state_for(session_id).value if profile else ""

        sessions.append(
            {
                "session": {
                    "id": session_id,
                    "instanceId": str(row["instance_id"] or ""),
                    "sessionType": str(row["session_type"] or ""),
                    "platform": str(row["platform"] or ""),
                    "guildId": row["guild_id"],
                    "channelId": str(row["channel_id"] or ""),
                    "displayName": str(row["display_name"] or ""),
                    "permissionGroup": str(row["permission_group"] or "default"),
                    "createdAt": float(row["created_at"] or 0.0),
                    "lastActive": float(row["last_active"] or 0.0),
                },
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
