"""Builtin plugin: time-based attention threshold scheduler (Sleepy).

Runs a background loop that periodically checks the current local time against
user-configured schedule slots and adjusts the base threshold for all active
sessions accordingly.

  - Positive threshold_delta  → higher threshold → bot replies less actively
  - Negative threshold_delta  → lower threshold  → bot replies more actively

Multiple overlapping slots have their deltas summed.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tomllib
from collections.abc import Sequence
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from shinbot.core.plugins.context import Plugin
from shinbot.utils.logger import get_logger

logger = get_logger(__name__)

# ── Metadata keys written into session_attention_states.metadata_json ──
_KEY_ORIGINAL_BASE = "sleepy_original_base_threshold"
_KEY_ACTIVE_DELTA = "sleepy_active_delta"
_KEY_FIXED_BASE = "fixed_base_threshold"  # consumed by AttentionEngine

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
            block = payload.get("plugin_configs", {}).get(plugin_id, {})
            if isinstance(block, dict):
                raw = block
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


# ── Per-session delta application ─────────────────────────────────────────


def _apply_delta_to_session(
    repo: Any,
    db_connect: Any,
    session_id: str,
    target_delta: float,
) -> None:
    """Update (or remove) the sleepy threshold override for one session."""
    state = repo.get_attention(session_id)
    if state is None:
        return

    current_delta: float = float(state.metadata.get(_KEY_ACTIVE_DELTA, 0.0))
    if abs(target_delta - current_delta) < 1e-6:
        return  # nothing to do

    if target_delta == 0.0:
        # Deactivate — restore original base_threshold if we saved it
        original = state.metadata.get(_KEY_ORIGINAL_BASE)
        new_meta = {
            k: v
            for k, v in state.metadata.items()
            if k not in (_KEY_ORIGINAL_BASE, _KEY_ACTIVE_DELTA, _KEY_FIXED_BASE)
        }
        if original is not None:
            with db_connect() as conn:
                conn.execute(
                    """
                    UPDATE session_attention_states
                    SET base_threshold = ?, metadata_json = ?
                    WHERE session_id = ?
                    """,
                    (float(original), json.dumps(new_meta, ensure_ascii=False), session_id),
                )
        else:
            repo.update_metadata(session_id, new_meta)
        logger.debug("Sleepy: deactivated for session %s", session_id)
    else:
        # Activate or change delta
        if current_delta == 0.0:
            original = state.base_threshold
        else:
            original = float(state.metadata.get(_KEY_ORIGINAL_BASE, state.base_threshold))
        fixed = original + target_delta
        new_meta = {
            **state.metadata,
            _KEY_ORIGINAL_BASE: original,
            _KEY_ACTIVE_DELTA: target_delta,
            _KEY_FIXED_BASE: fixed,
        }
        repo.update_metadata(session_id, new_meta)
        logger.debug(
            "Sleepy: session=%s delta=%.2f→%.2f fixed_base=%.2f",
            session_id,
            current_delta,
            target_delta,
            fixed,
        )


def _apply_schedule(plugin_id: str, db: Any) -> None:
    """Apply the current schedule delta to all sessions in the DB."""
    config = _load_plugin_config(plugin_id)
    target_delta = _compute_delta(config.schedules) if config.enabled else 0.0

    try:
        with db.connect() as conn:
            rows = conn.execute(
                "SELECT session_id FROM session_attention_states"
            ).fetchall()
        session_ids = [row["session_id"] for row in rows]
    except Exception:
        logger.exception("Sleepy: failed to list sessions")
        return

    for sid in session_ids:
        try:
            _apply_delta_to_session(db.attention, db.connect, sid, target_delta)
        except Exception:
            logger.exception("Sleepy: error applying delta to session %s", sid)


# ── Background loop ───────────────────────────────────────────────────────


async def _schedule_loop(plugin_id: str, db: Any) -> None:
    while True:
        await asyncio.sleep(_LOOP_INTERVAL)
        try:
            _apply_schedule(plugin_id, db)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Sleepy: unhandled error in schedule loop")


# ── Plugin entry points ───────────────────────────────────────────────────

_loop_task: asyncio.Task[None] | None = None


def setup(plg: Plugin) -> None:
    global _loop_task

    db = plg.database
    if db is None:
        plg.logger.warning(
            "Sleepy: no database available — schedule loop will not run. "
            "Ensure ShinBot is started with a data_dir."
        )
        return

    # Apply immediately so the first window doesn't require waiting 60 s
    try:
        _apply_schedule(plg.plugin_id, db)
    except Exception:
        plg.logger.exception("Sleepy: error applying schedule on startup")

    _loop_task = asyncio.create_task(
        _schedule_loop(plg.plugin_id, db),
        name=f"{plg.plugin_id}.schedule_loop",
    )
    plg.logger.info("Sleepy plugin loaded — schedule loop started (interval=%ds)", _LOOP_INTERVAL)


def on_disable(plg: Plugin) -> None:
    global _loop_task
    if _loop_task is not None and not _loop_task.done():
        _loop_task.cancel()
        _loop_task = None

    # Clear all sleepy overrides when the plugin is disabled
    db = plg.database
    if db is not None:
        try:
            with db.connect() as conn:
                rows = conn.execute(
                    "SELECT session_id FROM session_attention_states"
                ).fetchall()
            for row in rows:
                try:
                    _apply_delta_to_session(db.attention, db.connect, row["session_id"], 0.0)
                except Exception:
                    pass
        except Exception:
            pass

    plg.logger.info("Sleepy plugin disabled — schedule loop stopped")
