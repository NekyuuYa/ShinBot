"""Attention engine — core decay, contribution and threshold logic."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from shinbot.agent.attention.models import SenderWeightState, SessionAttentionState
from shinbot.agent.attention.repository import AttentionRepository
from shinbot.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class AttentionConfig:
    """Tunable parameters for the attention system."""

    # Threshold
    base_threshold: float = 5.0
    threshold_min: float = 1.0
    threshold_max: float = 20.0

    # Exponential decay constants (per second)
    decay_k: float = 0.05
    runtime_weight_decay_k: float = 0.01
    runtime_threshold_decay_k: float = 0.008

    # Contribution
    base_gain: float = 1.0
    mention_bonus: float = 1.5
    reply_bonus: float = 1.2

    # Burst detection (Robust Interrupt)
    burst_window_seconds: float = 10.0
    burst_exponent: float = 1.5

    # Reply fatigue
    fatigue_increment: float = 1.0
    fatigue_decay_k: float = 0.02
    cooldown_seconds: float = 3.0

    # Semantic boundary wait
    semantic_wait_ms: float = 1000.0

    # Sender weight bounds
    weight_min: float = -2.0
    weight_max: float = 2.0


def _clamp(value: float, lo: float, hi: float) -> tuple[float, str]:
    """Clamp value and report status."""
    if value <= lo:
        return lo, "clamped_to_min"
    if value >= hi:
        return hi, "clamped_to_max"
    return value, "applied"


def weight_curve(sender_score: float) -> float:
    """Map sender_score to a multiplicative factor.

    Neutral point: score=0 -> factor=1.0
    Positive scores amplify; negative scores attenuate.
    """
    return math.pow(2.0, sender_score)


class AttentionEngine:
    """Stateless computation engine for session attention."""

    def __init__(self, config: AttentionConfig, repository: AttentionRepository) -> None:
        self.config = config
        self.repo = repository

    # ── Time decay ──────────────────────────────────────────────────

    def apply_time_decay(
        self,
        state: SessionAttentionState,
        now: float | None = None,
    ) -> SessionAttentionState:
        """Apply exponential decay to attention_value and regression to runtime offsets."""
        if now is None:
            now = time.time()
        dt = max(now - state.last_update_at, 0.0)
        if dt <= 0:
            return state

        # Exponential decay on attention value
        state.attention_value *= math.exp(-self.config.decay_k * dt)

        # Smooth regression of runtime_threshold_offset toward 0
        if state.runtime_threshold_offset != 0.0:
            decay_factor = math.exp(-self.config.runtime_threshold_decay_k * dt)
            state.runtime_threshold_offset *= decay_factor

        state.last_update_at = now
        return state

    def apply_sender_weight_decay(
        self,
        sw: SenderWeightState,
        now: float | None = None,
    ) -> SenderWeightState:
        """Regress runtime_weight toward 0."""
        if now is None:
            now = time.time()
        dt = max(now - sw.last_runtime_adjust_at, 0.0)
        if dt > 0 and sw.runtime_weight != 0.0:
            sw.runtime_weight *= math.exp(-self.config.runtime_weight_decay_k * dt)
            sw.last_runtime_adjust_at = now
        return sw

    # ── Sender factor ───────────────────────────────────────────────

    def compute_sender_factor(self, sw: SenderWeightState) -> float:
        score = sw.stable_weight + sw.runtime_weight
        score, _ = _clamp(score, self.config.weight_min, self.config.weight_max)
        return weight_curve(score)

    # ── Contribution ────────────────────────────────────────────────

    def compute_contribution(
        self,
        *,
        sender_factor: float,
        is_mentioned: bool,
        is_reply_to_bot: bool,
        recent_mention_count: int,
    ) -> float:
        """Compute a single message's contribution to session attention.

        recent_mention_count: how many mentions/replies occurred in the burst window.
        """
        base = self.config.base_gain * sender_factor

        feature_bonus = 0.0
        if is_mentioned:
            feature_bonus += self.config.mention_bonus
        if is_reply_to_bot:
            feature_bonus += self.config.reply_bonus

        # Burst amplification: non-linear growth when multiple mentions occur
        if recent_mention_count > 1:
            burst_factor = math.pow(recent_mention_count, self.config.burst_exponent)
            feature_bonus *= burst_factor

        return base + feature_bonus

    # ── Effective threshold ─────────────────────────────────────────

    def effective_threshold(self, state: SessionAttentionState) -> float:
        raw = state.base_threshold + state.runtime_threshold_offset
        clamped, _ = _clamp(raw, self.config.threshold_min, self.config.threshold_max)
        return clamped

    # ── Core update flow ────────────────────────────────────────────

    def update_attention(
        self,
        session_id: str,
        *,
        sender_id: str,
        msg_log_id: int,
        base_threshold: float | None = None,
        is_mentioned: bool = False,
        is_reply_to_bot: bool = False,
        recent_mention_count: int = 0,
        now: float | None = None,
    ) -> tuple[SessionAttentionState, bool]:
        """Process a new message and return (updated_state, should_trigger).

        This is the main entry point called on each incoming group message.
        """
        if now is None:
            now = time.time()

        # Load states
        state = self.repo.get_or_create_attention(
            session_id,
            base_threshold=base_threshold or self.config.base_threshold,
        )
        sw = self.repo.get_or_create_sender_weight(session_id, sender_id)

        if base_threshold is not None:
            state.base_threshold = base_threshold

        # Apply time decay
        state = self.apply_time_decay(state, now)
        sw = self.apply_sender_weight_decay(sw, now)

        # Compute contribution
        sender_factor = self.compute_sender_factor(sw)
        contribution = self.compute_contribution(
            sender_factor=sender_factor,
            is_mentioned=is_mentioned,
            is_reply_to_bot=is_reply_to_bot,
            recent_mention_count=recent_mention_count,
        )

        state.attention_value += contribution
        state.last_update_at = now

        # Persist sender weight (may have decayed)
        self.repo.save_sender_weight(sw)

        # Check trigger condition
        threshold = self.effective_threshold(state)
        triggered = state.attention_value >= threshold and not state.is_cooling_down

        # Persist attention state
        self.repo.save_attention(state)

        logger.debug(
            "Attention update: session=%s sender=%s contribution=%.3f "
            "value=%.3f threshold=%.3f triggered=%s",
            session_id,
            sender_id,
            contribution,
            state.attention_value,
            threshold,
            triggered,
        )

        return state, triggered

    # ── Post-trigger operations ─────────────────────────────────────

    def consume_batch(self, state: SessionAttentionState) -> SessionAttentionState:
        """Deduct threshold from attention after triggering, preserving residual."""
        threshold = self.effective_threshold(state)
        state.attention_value = max(state.attention_value - threshold, 0.0)
        return state

    def apply_reply_fatigue(self, state: SessionAttentionState) -> SessionAttentionState:
        """Increase threshold offset after a reply to prevent self-reinforcing storms."""
        state.runtime_threshold_offset += self.config.fatigue_increment
        state.cooldown_until = time.time() + self.config.cooldown_seconds
        self.repo.save_attention(state)
        return state

    # ── Weight adjustment (for tools) ───────────────────────────────

    def adjust_sender_weight(
        self,
        session_id: str,
        sender_id: str,
        *,
        stable_delta: float = 0.0,
        runtime_delta: float = 0.0,
    ) -> dict[str, Any]:
        """Adjust sender weight and return structured feedback with clamp status."""
        sw = self.repo.get_or_create_sender_weight(session_id, sender_id)
        sw = self.apply_sender_weight_decay(sw)

        new_stable = sw.stable_weight + stable_delta
        new_stable, stable_status = _clamp(
            new_stable, self.config.weight_min, self.config.weight_max
        )
        sw.stable_weight = new_stable

        new_runtime = sw.runtime_weight + runtime_delta
        new_runtime, runtime_status = _clamp(
            new_runtime, self.config.weight_min, self.config.weight_max
        )
        sw.runtime_weight = new_runtime
        sw.last_runtime_adjust_at = time.time()

        self.repo.save_sender_weight(sw)

        # Compute band labels
        def _band(value: float) -> str:
            if value <= -1.0:
                return "very_low"
            if value < -0.3:
                return "low"
            if value <= 0.3:
                return "neutral"
            if value < 1.0:
                return "high"
            return "very_high"

        hint_parts = []
        if stable_status == "clamped_to_max":
            hint_parts.append("稳定权重已达上限，继续增加将不再生效。")
        elif stable_status == "clamped_to_min":
            hint_parts.append("稳定权重已达下限，继续降低将不再生效。")
        if runtime_status == "clamped_to_max":
            hint_parts.append("临时权重已达上限。")
        elif runtime_status == "clamped_to_min":
            hint_parts.append("临时权重已达下限。")

        return {
            "target": f"sender:{sender_id}",
            "applied": {"stable": stable_status, "runtime": runtime_status},
            "current_band": {
                "stable": _band(sw.stable_weight),
                "runtime": _band(sw.runtime_weight),
            },
            "hint": " ".join(hint_parts) if hint_parts else "调整已生效。",
        }

    def adjust_session_threshold(
        self,
        session_id: str,
        *,
        offset_delta: float,
    ) -> dict[str, Any]:
        """Adjust session threshold offset and return feedback."""
        state = self.repo.get_or_create_attention(
            session_id,
            base_threshold=self.config.base_threshold,
        )

        new_offset = state.runtime_threshold_offset + offset_delta
        effective_raw = state.base_threshold + new_offset
        _, status = _clamp(effective_raw, self.config.threshold_min, self.config.threshold_max)

        # Clamp the offset such that effective stays within bounds
        if effective_raw < self.config.threshold_min:
            new_offset = self.config.threshold_min - state.base_threshold
        elif effective_raw > self.config.threshold_max:
            new_offset = self.config.threshold_max - state.base_threshold

        state.runtime_threshold_offset = new_offset
        self.repo.save_attention(state)

        return {
            "applied": status,
            "effective_threshold": self.effective_threshold(state),
            "runtime_offset": state.runtime_threshold_offset,
            "hint": (
                "阈值已达边界。" if status != "applied" else "阈值调整已生效。"
            ),
        }

    # ── Inspection (for tools) ──────────────────────────────────────

    def inspect_state(self, session_id: str) -> dict[str, Any]:
        """Return a human-readable summary for the inspect tool."""
        state = self.repo.get_or_create_attention(
            session_id,
            base_threshold=self.config.base_threshold,
        )
        state = self.apply_time_decay(state)
        self.repo.save_attention(state)

        threshold = self.effective_threshold(state)
        ratio = state.attention_value / threshold if threshold > 0 else 0.0

        if ratio < 0.3:
            attention_band = "low"
        elif ratio < 0.7:
            attention_band = "neutral"
        elif ratio < 1.0:
            attention_band = "high"
        else:
            attention_band = "very_high"

        # Sender weight summary
        weights = self.repo.list_sender_weights(session_id)
        weight_summary = []
        for w in weights[:10]:
            score = w.stable_weight + w.runtime_weight
            weight_summary.append({
                "sender_id": w.sender_id,
                "combined_score": round(score, 3),
                "factor": round(weight_curve(score), 3),
            })

        return {
            "session_id": session_id,
            "attention_value": round(state.attention_value, 3),
            "effective_threshold": round(threshold, 3),
            "attention_band": attention_band,
            "attention_ratio": round(ratio, 3),
            "runtime_threshold_offset": round(state.runtime_threshold_offset, 3),
            "cooldown_active": state.is_cooling_down,
            "last_consumed_msg_log_id": state.last_consumed_msg_log_id,
            "sender_weights": weight_summary,
        }
