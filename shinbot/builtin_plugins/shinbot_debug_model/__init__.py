"""Builtin plugin: capture and persist all model runtime requests for debugging."""

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


def _build_model_record(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "event_type": str(payload.get("event", "model_runtime.request")),
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
        "request": {
            "messages": payload.get("messages"),
            "tools": payload.get("tools"),
            "response_format": payload.get("response_format"),
            "input_data": payload.get("input_data"),
            "params": payload.get("params"),
            "kwargs": payload.get("kwargs"),
            "metadata": payload.get("metadata"),
            "prompt_snapshot_id": payload.get("prompt_snapshot_id"),
        },
    }


def setup(plg: Plugin) -> None:
    global _WRITE_QUEUE, _WRITER_TASK

    target_file = plg.data_dir / "model_requests.jsonl"
    _WRITE_QUEUE = asyncio.Queue(maxsize=2000)
    _WRITER_TASK = asyncio.create_task(_writer_loop())

    async def _on_model_request(payload: dict[str, Any]) -> None:
        plg.logger.info(
            "[debug_model] mode=%s provider=%s model=%s caller=%s",
            payload.get("mode", ""),
            payload.get("provider_id", ""),
            payload.get("model_id", ""),
            payload.get("caller", ""),
        )
        try:
            _enqueue_record(target_file, _build_model_record(payload))
        except asyncio.QueueFull:
            plg.logger.warning(
                "[debug_model] queue full, dropping execution %s",
                payload.get("execution_id", ""),
            )

    plg.register_model_runtime_observer(_on_model_request)


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
