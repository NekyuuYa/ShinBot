"""Builtin plugin: capture and persist model runtime request/response events."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shinbot.core.plugins.context import Plugin

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


def _enqueue_record(target_file: Path, payload: dict[str, Any]) -> None:
    if _WRITE_QUEUE is None:
        return
    _WRITE_QUEUE.put_nowait((target_file, payload))


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"type": "bytes", "length": len(value)}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump())
    if hasattr(value, "__dict__"):
        return _json_safe(dict(value.__dict__))
    return str(value)


def _extract_usage_and_cache(payload: dict[str, Any]) -> tuple[dict[str, int], dict[str, Any]]:
    usage_raw = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    cache_read_tokens = _to_int(
        usage_raw.get("cache_read_tokens") or payload.get("cache_read_tokens")
    )
    cache_write_tokens = _to_int(
        usage_raw.get("cache_write_tokens") or payload.get("cache_write_tokens")
    )
    usage = {
        "input_tokens": _to_int(
            usage_raw.get("input_tokens")
            or usage_raw.get("prompt_tokens")
            or payload.get("input_tokens")
        ),
        "output_tokens": _to_int(
            usage_raw.get("output_tokens")
            or usage_raw.get("completion_tokens")
            or payload.get("output_tokens")
        ),
        "cache_read_tokens": cache_read_tokens,
        "cache_write_tokens": cache_write_tokens,
    }
    cache = {
        "hit": bool(payload.get("cache_hit")) or cache_read_tokens > 0,
        "read_tokens": cache_read_tokens,
        "write_tokens": cache_write_tokens,
    }
    return usage, cache


def _build_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("request"), dict):
        request = dict(payload.get("request") or {})
    else:
        request = {
            "messages": payload.get("messages"),
            "tools": payload.get("tools"),
            "response_format": payload.get("response_format"),
            "input_data": payload.get("input_data"),
            "params": payload.get("params"),
            "kwargs": payload.get("kwargs"),
            "metadata": payload.get("metadata"),
            "prompt_snapshot_id": payload.get("prompt_snapshot_id"),
        }
    return request


def _build_response_payload(payload: dict[str, Any]) -> dict[str, Any]:
    usage, cache = _extract_usage_and_cache(payload)
    latency_ms = payload.get("latency_ms")
    try:
        latency_value = float(latency_ms) if latency_ms is not None else None
    except (TypeError, ValueError):
        latency_value = None
    return {
        "status": str(payload.get("status", "success")),
        "latency_ms": latency_value,
        "usage": usage,
        "cache": cache,
        "return": payload.get("return"),
        "response": payload.get("response"),
        "error": payload.get("error"),
        "metadata": payload.get("metadata"),
        "prompt_snapshot_id": payload.get("prompt_snapshot_id"),
    }


def _build_model_record(payload: dict[str, Any]) -> dict[str, Any]:
    event_type = str(payload.get("event", "model_runtime.request"))
    record: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event_type": event_type,
        "phase": "response" if event_type == "model_runtime.response" else "request",
        "mode": str(payload.get("mode", "")),
        "caller": str(payload.get("caller", "")),
        "purpose": str(payload.get("purpose", "")),
        "session_id": str(payload.get("session_id", "")),
        "instance_id": str(payload.get("instance_id", "")),
        "route_id": str(payload.get("route_id", "")),
        "provider_id": str(payload.get("provider_id", "")),
        "provider_type": str(payload.get("provider_type", "")),
        "model_id": str(payload.get("model_id", "")),
        "litellm_model": str(payload.get("litellm_model", "")),
        "execution_id": str(payload.get("execution_id", "")),
        "strategy": str(payload.get("strategy", "")),
    }
    if event_type == "model_runtime.response":
        record["response"] = _build_response_payload(payload)
    else:
        record["request"] = _build_request_payload(payload)
    return _json_safe(record)


def setup(plg: Plugin) -> None:
    global _WRITE_QUEUE, _WRITER_TASK

    target_file = plg.data_dir / "model_requests.jsonl"
    _WRITE_QUEUE = asyncio.Queue(maxsize=2000)
    _WRITER_TASK = asyncio.create_task(_writer_loop())

    async def _on_model_runtime_event(payload: dict[str, Any]) -> None:
        event_type = str(payload.get("event", "model_runtime.request"))
        if event_type == "model_runtime.response":
            usage, cache = _extract_usage_and_cache(payload)
            plg.logger.info(
                "[debug_model] event=%s mode=%s status=%s provider=%s model=%s caller=%s input=%s output=%s cache_hit=%s",
                event_type,
                payload.get("mode", ""),
                payload.get("status", ""),
                payload.get("provider_id", ""),
                payload.get("model_id", ""),
                payload.get("caller", ""),
                usage["input_tokens"],
                usage["output_tokens"],
                cache["hit"],
            )
        else:
            plg.logger.info(
                "[debug_model] event=%s mode=%s provider=%s model=%s caller=%s",
                event_type,
                payload.get("mode", ""),
                payload.get("provider_id", ""),
                payload.get("model_id", ""),
                payload.get("caller", ""),
            )
        try:
            _enqueue_record(target_file, _build_model_record(payload))
        except asyncio.QueueFull:
            plg.logger.warning(
                "[debug_model] queue full, dropping event=%s execution=%s",
                event_type,
                payload.get("execution_id", ""),
            )

    plg.register_model_runtime_observer(_on_model_runtime_event)


async def on_disable(plg: Plugin) -> None:
    global _WRITE_QUEUE, _WRITER_TASK

    if _WRITE_QUEUE is not None:
        await _WRITE_QUEUE.put((Path(""), {"_stop": True}))

    if _WRITER_TASK is not None:
        try:
            await _WRITER_TASK
        except Exception:
            plg.logger.exception("[debug_model] writer task terminated unexpectedly")

    _WRITE_QUEUE = None
    _WRITER_TASK = None
