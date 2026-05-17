"""Builtin plugin: time-based attention threshold scheduler (Sleepy).

Runs a background loop that periodically checks the current local time against
user-configured schedule slots and adjusts Agent active chat thresholds
accordingly.

  - Positive threshold_delta  → higher threshold → bot replies less actively
  - Negative threshold_delta  → lower threshold  → bot replies more actively

Multiple overlapping slots have their deltas summed.
"""

from __future__ import annotations

import asyncio
import sys
import tomllib
from collections.abc import Sequence
from datetime import datetime
from datetime import time as dt_time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from shinbot.core.plugins.config import plugin_config_block
from shinbot.core.plugins.context import Plugin
from shinbot.utils.logger import get_logger

logger = get_logger(__name__, source="plugin:sleepy", color="bright_magenta")

# Background loop interval (seconds)
_LOOP_INTERVAL = 60


# ── Config models ────────────────────────────────────────────────────────


class TimeSlot(BaseModel):
    """A single time-based threshold adjustment rule."""

    name: str = Field(default="Sleep", title="Name")
    start_time: str = Field(
        default="23:00",
        title="Start",
        description="HH:MM format (24 h)",
    )
    end_time: str = Field(
        default="08:00",
        title="End",
        description="HH:MM format (24 h); wrap past midnight is supported",
    )
    threshold_delta: float = Field(
        default=5.0,
        title="Delta",
        description="Added to base threshold (+ = less active, − = more active)",
    )
    enabled: bool = Field(default=True, title="Active")


class SleepyPluginConfig(BaseModel):
    enabled: bool = Field(default=True, title="Enabled")
    schedules: list[TimeSlot] = Field(
        default_factory=lambda: [
            TimeSlot(
                name="Sleep",
                start_time="23:00",
                end_time="08:00",
                threshold_delta=5.0,
                enabled=True,
            )
        ],
        title="Time Schedules",
        description=(
            "Each row defines a period during which the attention threshold is shifted. "
            "Overlapping slots are summed. Positive delta = harder to trigger."
        ),
        json_schema_extra={"ui_component": "schedule_table"},
    )


__plugin_config_class__ = SleepyPluginConfig


# ── Config loading ────────────────────────────────────────────────────────


def _resolve_config_path(argv: Sequence[str] | None = None) -> Path:
    args = list(sys.argv[1:] if argv is None else argv)
    for index, value in enumerate(args):
        if value == "--config" and index + 1 < len(args):
            return Path(args[index + 1])
        if value.startswith("--config="):
            return Path(value.split("=", 1)[1])
    return Path("config.toml")


def _load_plugin_config(plugin_id: str) -> SleepyPluginConfig:
    path = _resolve_config_path()
    raw: dict[str, Any] = {}
    try:
        if path.exists():
            with path.open("rb") as fh:
                payload = tomllib.load(fh)
            raw = plugin_config_block(payload, plugin_id)
    except Exception:
        raw = {}
    try:
        return SleepyPluginConfig.model_validate(raw)
    except ValidationError:
        return SleepyPluginConfig()


# ── Schedule logic ────────────────────────────────────────────────────────


def _parse_hhmm(value: str) -> dt_time | None:
    try:
        h, m = map(int, value.split(":"))
        return dt_time(h, m)
    except (ValueError, AttributeError):
        return None


def _is_slot_active(slot: TimeSlot) -> bool:
    if not slot.enabled:
        return False
    start = _parse_hhmm(slot.start_time)
    end = _parse_hhmm(slot.end_time)
    if start is None or end is None or start == end:
        return False
    now = datetime.now().time().replace(second=0, microsecond=0)
    if start < end:
        return start <= now < end
    # Crosses midnight
    return now >= start or now < end


def _compute_delta(slots: list[TimeSlot]) -> float:
    return sum(s.threshold_delta for s in slots if _is_slot_active(s))


# ── Runtime delta application ─────────────────────────────────────────────


def _apply_schedule(plugin_id: str, agent_runtime: Any) -> float:
    """Apply the current schedule delta to Agent active chat profiles."""
    config = _load_plugin_config(plugin_id)
    target_delta = _compute_delta(config.schedules) if config.enabled else 0.0

    setter = getattr(agent_runtime, "set_active_chat_threshold_delta", None)
    if setter is None:
        raise RuntimeError("Agent runtime does not support active chat threshold updates")
    setter(float(target_delta), source=plugin_id)
    logger.debug("Sleepy: applied active chat threshold delta %.2f", target_delta)
    return float(target_delta)


# ── Background loop ───────────────────────────────────────────────────────


async def _schedule_loop(plugin_id: str, agent_runtime: Any) -> None:
    while True:
        await asyncio.sleep(_LOOP_INTERVAL)
        try:
            _apply_schedule(plugin_id, agent_runtime)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Sleepy: unhandled error in schedule loop")


# ── Plugin entry points ───────────────────────────────────────────────────

_loop_task: asyncio.Task[None] | None = None


def setup(plg: Plugin) -> None:
    global _loop_task

    agent_runtime = plg.agent_runtime
    if agent_runtime is None:
        plg.logger.warning(
            "Sleepy: no Agent runtime available — schedule loop will not run. "
            "Enable Agent mode for at least one bot before loading this plugin."
        )
        return

    # Apply immediately so the first window doesn't require waiting 60 s
    try:
        _apply_schedule(plg.plugin_id, agent_runtime)
    except Exception:
        plg.logger.exception("Sleepy: error applying schedule on startup")

    _loop_task = asyncio.create_task(
        _schedule_loop(plg.plugin_id, agent_runtime),
        name=f"{plg.plugin_id}.schedule_loop",
    )
    plg.logger.info("Sleepy plugin loaded — schedule loop started (interval=%ds)", _LOOP_INTERVAL)


def on_disable(plg: Plugin) -> None:
    global _loop_task
    if _loop_task is not None and not _loop_task.done():
        _loop_task.cancel()
        _loop_task = None

    # Clear sleepy overrides when the plugin is disabled.
    agent_runtime = plg.agent_runtime
    if agent_runtime is not None:
        try:
            setter = getattr(agent_runtime, "set_active_chat_threshold_delta", None)
            if setter is not None:
                setter(0.0, source=plg.plugin_id)
        except Exception:
            plg.logger.exception("Sleepy: failed to clear runtime threshold delta")

    plg.logger.info("Sleepy plugin disabled — schedule loop stopped")
