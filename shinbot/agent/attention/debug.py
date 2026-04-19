"""Debug tracer for the attention system.

When enabled via ``AttentionConfig.debug = True``, prints structured
traces to a dedicated logger so operators can follow attention value
mutations, sender weight evolution, trigger decisions, and workflow
dispatch events in real time from the console.

Usage:
    config = AttentionConfig(debug=True)
    # All subsequent engine/scheduler operations will emit traces.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.agent.attention.models import (
        SenderWeightState,
        SessionAttentionState,
    )

# Dedicated logger — independent of the module-level loggers so it can be
# enabled/disabled and formatted separately.  When debug mode is active we
# force it to DEBUG; otherwise it stays at WARNING (silent).
_debug_logger = logging.getLogger("shinbot.attention.debug")

# ── ANSI helpers ────────────────────────────────────────────────────────

_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_CYAN = "\033[36m"
_YELLOW = "\033[33m"
_GREEN = "\033[32m"
_RED = "\033[31m"
_MAGENTA = "\033[35m"
_BLUE = "\033[34m"


def _c(color: str, text: str) -> str:
    return f"{color}{text}{_RESET}"


def _bar(ratio: float, width: int = 20) -> str:
    """Render a simple text progress bar."""
    filled = int(min(ratio, 1.0) * width)
    overflow = ratio > 1.0
    bar_char = "█" if not overflow else "▓"
    empty_char = "░"
    bar_str = bar_char * filled + empty_char * (width - filled)
    if overflow:
        return _c(_RED, bar_str)
    if ratio >= 0.7:
        return _c(_YELLOW, bar_str)
    return _c(_GREEN, bar_str)


class AttentionDebugTracer:
    """Structured console tracer for attention system internals."""

    def __init__(self, *, enabled: bool = False) -> None:
        self._enabled = enabled
        self._configure_logger()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value
        self._configure_logger()

    def _configure_logger(self) -> None:
        if self._enabled:
            _debug_logger.setLevel(logging.DEBUG)
            # Ensure at least one handler exists so output is visible
            if not _debug_logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(
                    logging.Formatter(
                        f"{_DIM}%(asctime)s{_RESET} {_CYAN}[ATN-DBG]{_RESET} %(message)s",
                        datefmt="%H:%M:%S",
                    )
                )
                _debug_logger.addHandler(handler)
            _debug_logger.propagate = False
        else:
            _debug_logger.setLevel(logging.WARNING)

    # ── Trace: attention update ─────────────────────────────────────

    def trace_update(
        self,
        session_id: str,
        sender_id: str,
        *,
        value_before_decay: float,
        value_after_decay: float,
        contribution: float,
        value_after: float,
        threshold: float,
        triggered: bool,
        sender_factor: float,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        burst_count: int,
    ) -> None:
        if not self._enabled:
            return

        ratio = value_after / threshold if threshold > 0 else 0.0
        decay_delta = value_after_decay - value_before_decay
        trigger_mark = _c(_RED + _BOLD, "▶ TRIGGERED") if triggered else _c(_DIM, "  not triggered")

        lines = [
            "",
            _c(_BOLD, f"┌─ Attention Update: {session_id} ─────────────"),
            f"│ Sender:       {_c(_CYAN, sender_id)}  (factor={sender_factor:.3f})",
            f"│ Flags:        mention={is_mentioned}  reply={is_reply_to_bot}  burst={burst_count}",
            f"│ Decay:        {value_before_decay:.4f} → {value_after_decay:.4f}  ({decay_delta:+.4f})",
            f"│ Contribution: {_c(_YELLOW, f'+{contribution:.4f}')}",
            f"│ Value:        {value_after_decay:.4f} → {_c(_BOLD, f'{value_after:.4f}')}",
            f"│ Threshold:    {threshold:.4f}",
            f"│ Ratio:        {ratio:.2%}  {_bar(ratio)}",
            f"│ Result:       {trigger_mark}",
            _c(_BOLD, "└────────────────────────────────────────────"),
        ]
        _debug_logger.debug("\n".join(lines))

    # ── Trace: time decay detail ────────────────────────────────────

    def trace_decay(
        self,
        session_id: str,
        *,
        dt: float,
        value_before: float,
        value_after: float,
        offset_before: float,
        offset_after: float,
    ) -> None:
        if not self._enabled:
            return
        _debug_logger.debug(
            "%s decay  dt=%.2fs  value: %.4f→%.4f  offset: %.4f→%.4f",
            _c(_DIM, f"[{session_id}]"),
            dt,
            value_before,
            value_after,
            offset_before,
            offset_after,
        )

    # ── Trace: sender weight change ─────────────────────────────────

    def trace_sender_weight(
        self,
        session_id: str,
        sender_id: str,
        *,
        stable_before: float,
        stable_after: float,
        runtime_before: float,
        runtime_after: float,
        combined_score: float,
        factor: float,
    ) -> None:
        if not self._enabled:
            return
        _debug_logger.debug(
            "%s sender-weight %s  stable: %.3f→%.3f  "
            "runtime: %.3f→%.3f  combined=%.3f  factor=%.3f",
            _c(_DIM, f"[{session_id}]"),
            _c(_CYAN, sender_id),
            stable_before,
            stable_after,
            runtime_before,
            runtime_after,
            combined_score,
            factor,
        )

    # ── Trace: batch claim ──────────────────────────────────────────

    def trace_batch_claim(
        self,
        session_id: str,
        *,
        batch_size: int,
        cursor_before: int | None,
        cursor_after: int | None,
        residual_attention: float,
    ) -> None:
        if not self._enabled:
            return
        _debug_logger.debug(
            "%s batch-claim  size=%d  cursor: %s→%s  residual=%.4f",
            _c(_MAGENTA, f"[{session_id}]"),
            batch_size,
            cursor_before,
            cursor_after,
            residual_attention,
        )

    # ── Trace: semantic wait / trigger ──────────────────────────────

    def trace_semantic_wait(
        self,
        session_id: str,
        *,
        action: str,
        sender_id: str = "",
        wait_ms: float = 0,
        profile: str = "",
    ) -> None:
        if not self._enabled:
            return
        detail = ""
        if action == "armed":
            detail = f"sender={sender_id} wait={wait_ms:.0f}ms profile={profile}"
        elif action == "reset":
            detail = f"sender={sender_id} (still typing)"
        elif action == "skipped_running":
            detail = "workflow already running"
        elif action == "skipped_different_sender":
            detail = f"different sender ({sender_id}), letting timer run"
        _debug_logger.debug(
            "%s timer %s  %s",
            _c(_BLUE, f"[{session_id}]"),
            _c(_YELLOW if action == "armed" else _DIM, action.upper()),
            detail,
        )

    # ── Trace: workflow dispatch ─────────────────────────────────────

    def trace_dispatch(
        self,
        session_id: str,
        *,
        action: str,
        batch_size: int = 0,
        attention_value: float = 0,
        run_id: str = "",
    ) -> None:
        if not self._enabled:
            return
        if action == "start":
            _debug_logger.debug(
                "%s dispatch START  batch=%d  attention=%.4f  run=%s",
                _c(_GREEN, f"[{session_id}]"),
                batch_size,
                attention_value,
                run_id[:8],
            )
        elif action == "empty":
            _debug_logger.debug(
                "%s dispatch SKIP (empty batch)", _c(_DIM, f"[{session_id}]"),
            )
        elif action == "error":
            _debug_logger.debug(
                "%s dispatch ERROR  run=%s",
                _c(_RED, f"[{session_id}]"),
                run_id[:8],
            )

    # ── Trace: reply fatigue ────────────────────────────────────────

    def trace_fatigue(
        self,
        session_id: str,
        *,
        offset_before: float,
        offset_after: float,
        cooldown_until: float,
    ) -> None:
        if not self._enabled:
            return
        remaining = max(cooldown_until - time.time(), 0)
        _debug_logger.debug(
            "%s fatigue  offset: %.3f→%.3f  cooldown=%.1fs remaining",
            _c(_RED, f"[{session_id}]"),
            offset_before,
            offset_after,
            remaining,
        )

    # ── Trace: workflow result ──────────────────────────────────────

    def trace_workflow_result(
        self,
        session_id: str,
        *,
        run_id: str,
        replied: bool,
        tool_count: int,
        iterations: int,
        duration_ms: float,
    ) -> None:
        if not self._enabled:
            return
        outcome = _c(_GREEN, "REPLIED") if replied else _c(_YELLOW, "NO_REPLY")
        _debug_logger.debug(
            "%s workflow DONE  run=%s  %s  tools=%d  iters=%d  %.0fms",
            _c(_MAGENTA, f"[{session_id}]"),
            run_id[:8],
            outcome,
            tool_count,
            iterations,
            duration_ms,
        )

    # ── Trace: threshold adjustment ─────────────────────────────────

    def trace_threshold_adjust(
        self,
        session_id: str,
        *,
        offset_delta: float,
        effective_before: float,
        effective_after: float,
        status: str,
    ) -> None:
        if not self._enabled:
            return
        _debug_logger.debug(
            "%s threshold-adjust  delta=%+.3f  effective: %.3f→%.3f  %s",
            _c(_BLUE, f"[{session_id}]"),
            offset_delta,
            effective_before,
            effective_after,
            _c(_YELLOW, status) if status != "applied" else status,
        )

    # ── Trace: weight adjustment (tool call) ────────────────────────

    def trace_weight_adjust(
        self,
        session_id: str,
        sender_id: str,
        *,
        stable_delta: float,
        runtime_delta: float,
        result: dict[str, Any],
    ) -> None:
        if not self._enabled:
            return
        _debug_logger.debug(
            "%s weight-adjust  %s  stable_delta=%+.3f  runtime_delta=%+.3f  "
            "band=%s  %s",
            _c(_BLUE, f"[{session_id}]"),
            _c(_CYAN, sender_id),
            stable_delta,
            runtime_delta,
            result.get("current_band", {}),
            result.get("hint", ""),
        )
