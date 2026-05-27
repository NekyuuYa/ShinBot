"""Structured run trace helpers for platform-sim E2E scenarios."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any

from shinbot.agent.scheduler import AgentScheduler
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.agent_signals import AgentSignal


@dataclass(slots=True)
class ScenarioCheck:
    name: str
    passed: bool
    detail: str = ""


@dataclass(slots=True)
class ScenarioAnalysisReport:
    label: str
    checks: list[ScenarioCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def summary(self) -> str:
        failures = [check for check in self.checks if not check.passed]
        if not failures:
            return f"{self.label}: passed"
        details = "; ".join(
            f"{check.name}: {check.detail or 'failed'}" for check in failures
        )
        return f"{self.label}: {details}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "passed": self.passed,
            "checks": [
                {"name": check.name, "passed": check.passed, "detail": check.detail}
                for check in self.checks
            ],
        }

    def raise_for_failures(self) -> None:
        if not self.passed:
            raise AssertionError(self.summary())


class PlatformScenarioTraceRecorder:
    """Collect a compact transcript of one platform-sim E2E scenario."""

    def __init__(self, scenario_name: str, *, data_dir: Path) -> None:
        self._scenario_name = scenario_name
        self._path = data_dir / "e2e-traces" / f"{scenario_name}.json"
        self._events: list[dict[str, Any]] = []
        self._started_at = time.time()

    def record_event(self, kind: str, payload: dict[str, Any] | None = None) -> None:
        event = {"kind": kind, "at": time.time()}
        if payload:
            event.update(_jsonable(payload))
        self._events.append(event)

    def record_snapshot(
        self,
        label: str,
        bot: ShinBot,
        adapter: Any,
        *,
        analysis: ScenarioAnalysisReport | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        snapshot = {
            "kind": "snapshot",
            "label": label,
            "at": time.time(),
            "state": _build_state_snapshot(bot, adapter),
        }
        if analysis is not None:
            snapshot["analysis"] = analysis.to_dict()
        if payload:
            snapshot["payload"] = _jsonable(payload)
        self._events.append(snapshot)

    def record_analysis(self, analysis: ScenarioAnalysisReport) -> None:
        self._events.append(
            {
                "kind": "analysis",
                "at": time.time(),
                **analysis.to_dict(),
            }
        )

    def write(self) -> Path:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "scenario": self._scenario_name,
            "startedAt": self._started_at,
            "finishedAt": time.time(),
            "events": self._events,
        }
        self._path.write_text(
            json.dumps(_jsonable(payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return self._path

    @property
    def path(self) -> Path:
        return self._path


async def analyze_expectations(
    bot: ShinBot,
    adapter: Any,
    expect: dict[str, Any],
    *,
    label: str,
    hooks: dict[str, Callable[[], None] | Callable[[], Awaitable[None]]],
) -> ScenarioAnalysisReport:
    checks: list[ScenarioCheck] = []

    for key, hook in hooks.items():
        if key not in expect:
            continue
        checks.append(await _run_check(key, hook))

    report = ScenarioAnalysisReport(label=label, checks=checks)
    return report


async def _run_check(
    name: str,
    hook: Callable[[], None] | Callable[[], Awaitable[None]],
) -> ScenarioCheck:
    try:
        result = hook()
        if asyncio.iscoroutine(result) or hasattr(result, "__await__"):
            await result
    except AssertionError as exc:
        return ScenarioCheck(name=name, passed=False, detail=str(exc))
    except Exception as exc:  # pragma: no cover - defensive trace capture
        return ScenarioCheck(name=name, passed=False, detail=f"{type(exc).__name__}: {exc}")
    return ScenarioCheck(name=name, passed=True)


def _build_state_snapshot(bot: ShinBot, adapter: Any) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "adapter": {
            "started": bool(getattr(adapter, "started", False)),
            "stopped": bool(getattr(adapter, "stopped", False)),
            "sent": [_summarize_sent_message(item) for item in getattr(adapter, "sent", [])],
            "apiCalls": [
                {"method": method, "params": _jsonable(params)}
                for method, params in getattr(adapter, "api_calls", [])
            ],
            "noticeEvents": list(getattr(adapter, "notice_events", [])),
            "agentEntrySignals": [
                _summarize_agent_signal(signal)
                for signal in getattr(adapter, "agent_entry_signals", [])
            ],
        },
        "sessions": [_summarize_session(session) for session in bot.session_manager.all_sessions],
    }
    scheduler = getattr(adapter, "agent_scheduler", None)
    if scheduler is not None:
        snapshot["scheduler"] = _summarize_scheduler(scheduler)
    if bot.database is not None:
        snapshot["database"] = _summarize_database(bot)
    return snapshot


def _summarize_sent_message(item: Any) -> dict[str, Any]:
    return {
        "sessionId": str(getattr(item, "session_id", "")),
        "messageId": str(getattr(item, "message_id", "")),
        "text": str(getattr(item, "text", "")),
        "elements": _jsonable(getattr(item, "elements", [])),
    }


def _summarize_agent_signal(signal: AgentSignal) -> dict[str, Any]:
    return {
        "signalId": signal.signal_id,
        "kind": signal.kind.value,
        "source": signal.source.value,
        "sessionId": signal.session_id,
        "occurredAt": signal.occurred_at,
        "botId": signal.bot_id,
        "botBindingId": signal.bot_binding_id,
        "botSessionId": signal.bot_session_id,
        "message": _jsonable(signal.message) if signal.message is not None else None,
        "timer": _jsonable(signal.timer) if signal.timer is not None else None,
        "activeChatBootstrap": (
            _jsonable(signal.active_chat_bootstrap)
            if signal.active_chat_bootstrap is not None
            else None
        ),
        "meta": _jsonable(signal.meta),
    }


def _summarize_scheduler(scheduler: AgentScheduler) -> dict[str, Any]:
    session_ids = list(scheduler.list_session_ids())
    sessions: list[dict[str, Any]] = []
    for session_id in session_ids:
        review_plan = scheduler.review_plan_for(session_id)
        active_chat_state = scheduler.active_chat_state_for(session_id)
        sessions.append(
            {
                "sessionId": session_id,
                "state": scheduler.state_for(session_id).value,
                "unreadCount": scheduler.count_unread_messages(session_id),
                "reviewPlan": _jsonable(review_plan) if review_plan is not None else None,
                "activeChatState": (
                    _jsonable(active_chat_state) if active_chat_state is not None else None
                ),
            }
        )
    return {"sessionIds": session_ids, "sessions": sessions}


def _summarize_session(session: Any) -> dict[str, Any]:
    config = getattr(session, "config", None)
    return {
        "id": getattr(session, "id", ""),
        "instanceId": getattr(session, "instance_id", ""),
        "type": getattr(session, "session_type", ""),
        "platform": getattr(session, "platform", ""),
        "guildId": getattr(session, "guild_id", None),
        "channelId": getattr(session, "channel_id", ""),
        "displayName": getattr(session, "display_name", ""),
        "isMuted": bool(getattr(config, "is_muted", False)) if config is not None else False,
        "prefixes": list(getattr(config, "prefixes", [])) if config is not None else [],
    }


def _summarize_database(bot: ShinBot) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    try:
        summary["modelExecutions"] = _list_model_executions(bot)
    except Exception:
        summary["modelExecutions"] = []
    try:
        summary["workflowRuns"] = _list_workflow_runs(bot)
    except Exception:
        summary["workflowRuns"] = []
    return summary


def _list_model_executions(bot: ShinBot) -> list[dict[str, Any]]:
    records = bot.database.model_executions.list_recent(limit=5)
    return [
        {
            "id": record.get("id"),
            "caller": record.get("caller"),
            "purpose": record.get("purpose"),
            "modelId": record.get("model_id"),
            "providerId": record.get("provider_id"),
            "success": bool(record.get("success")),
            "createdAt": record.get("created_at"),
        }
        for record in records
    ]


def _list_workflow_runs(bot: ShinBot) -> list[dict[str, Any]]:
    with bot.database.connect() as conn:
        rows = conn.execute(
            """
            SELECT id, session_id, instance_id, response_profile, replied,
                   finish_reason, started_at, finished_at
            FROM workflow_runs
            ORDER BY started_at DESC, id DESC
            LIMIT 5
            """
        ).fetchall()
    return [
        {
            "id": row["id"],
            "sessionId": row["session_id"],
            "instanceId": row["instance_id"],
            "responseProfile": row["response_profile"],
            "replied": bool(row["replied"]),
            "finishReason": row["finish_reason"],
            "startedAt": row["started_at"],
            "finishedAt": row["finished_at"],
        }
        for row in rows
    ]


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if hasattr(value, "model_dump"):
        try:
            return _jsonable(value.model_dump())
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return _jsonable(vars(value))
        except Exception:
            pass
    return str(value)
