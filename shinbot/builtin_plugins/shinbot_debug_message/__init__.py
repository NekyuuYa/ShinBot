"""Builtin plugin: capture and persist all events for debugging."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shinbot.core.plugins.plugin import PluginContext, PluginRole
from shinbot.models.elements import Message
from shinbot.models.events import UnifiedEvent

__plugin_name__ = "Message Debug Plugin"
__plugin_version__ = "1.0.0"
__plugin_author__ = "ShinBot Team"
__plugin_description__ = "Logs event summaries and writes raw + AST events to JSONL."
__plugin_role__ = PluginRole.LOGIC

_WRITE_QUEUE: asyncio.Queue[tuple[Path, dict[str, Any]]] | None = None
_WRITER_TASK: asyncio.Task[None] | None = None


async def _writer_loop() -> None:
    assert _WRITE_QUEUE is not None
    while True:
        target_file, payload = await _WRITE_QUEUE.get()
        if payload.get("_stop"):
            break
        line = json.dumps(payload, ensure_ascii=False)
        await asyncio.to_thread(_append_line, target_file, line)


def _append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file_obj:
        file_obj.write(line + "\n")


def _extract_event_payload(event_obj: Any) -> tuple[str, str, str, dict[str, Any]]:
    if hasattr(event_obj, "event"):
        event = event_obj.event
        if isinstance(event, UnifiedEvent):
            return event.type, event.platform, event.self_id, event.model_dump(mode="json")
    if isinstance(event_obj, UnifiedEvent):
        return (
            event_obj.type,
            event_obj.platform,
            event_obj.self_id,
            event_obj.model_dump(mode="json"),
        )
    return "unknown", "unknown", "", {"raw": repr(event_obj)}


def _extract_unified_event(event_obj: Any) -> UnifiedEvent | None:
    if hasattr(event_obj, "event") and isinstance(event_obj.event, UnifiedEvent):
        return event_obj.event
    if isinstance(event_obj, UnifiedEvent):
        return event_obj
    return None


def _build_raw_record(event_obj: Any) -> dict[str, Any]:
    event_type, platform, self_id, payload = _extract_event_payload(event_obj)
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "event_type": event_type,
        "platform": platform,
        "self_id": self_id,
        "payload": payload,
    }


def _build_ast_record(event_obj: Any) -> dict[str, Any]:
    event = _extract_unified_event(event_obj)
    if event is None:
        raw_record = _build_raw_record(event_obj)
        return {
            "timestamp": raw_record["timestamp"],
            "event_type": raw_record["event_type"],
            "platform": raw_record["platform"],
            "self_id": raw_record["self_id"],
            "event": raw_record["payload"],
            "message_ast": None,
        }

    message_ast: dict[str, Any] | None = None
    if event.message is not None and event.message.content:
        message = Message.from_xml(event.message.content)
        message_ast = {
            "id": event.message.id,
            "content_xml": event.message.content,
            "text": message.text,
            "elements": [element.model_dump(mode="json") for element in message.elements],
        }
    elif event.message is not None:
        message_ast = {
            "id": event.message.id,
            "content_xml": event.message.content,
            "text": "",
            "elements": [],
        }

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "event_type": event.type,
        "platform": event.platform,
        "self_id": event.self_id,
        "event": event.model_dump(mode="json"),
        "message_ast": message_ast,
    }


def _enqueue_record(target_file: Path, payload: dict[str, Any]) -> None:
    if _WRITE_QUEUE is None:
        return
    _WRITE_QUEUE.put_nowait((target_file, payload))


def setup(ctx: PluginContext) -> None:
    global _WRITE_QUEUE, _WRITER_TASK

    raw_target_file = ctx.data_dir / "raw_events.jsonl"
    ast_target_file = ctx.data_dir / "ast_events.jsonl"
    _WRITE_QUEUE = asyncio.Queue(maxsize=2000)
    _WRITER_TASK = asyncio.create_task(_writer_loop())

    @ctx.on_event("*")
    async def _on_any_event(event_obj: Any) -> None:
        event_type, platform, self_id, _ = _extract_event_payload(event_obj)
        ctx.logger.info(
            "[debug_message] type=%s platform=%s self_id=%s",
            event_type,
            platform,
            self_id,
        )
        try:
            _enqueue_record(raw_target_file, _build_raw_record(event_obj))
            _enqueue_record(ast_target_file, _build_ast_record(event_obj))
        except asyncio.QueueFull:
            ctx.logger.warning("[debug_message] queue full, dropping event %s", event_type)


async def on_disable(ctx: PluginContext) -> None:
    global _WRITE_QUEUE, _WRITER_TASK

    if _WRITE_QUEUE is not None:
        await _WRITE_QUEUE.put((Path(""), {"_stop": True}))

    if _WRITER_TASK is not None:
        try:
            await _WRITER_TASK
        except Exception:
            ctx.logger.exception("[debug_message] writer task terminated unexpectedly")

    _WRITE_QUEUE = None
    _WRITER_TASK = None
