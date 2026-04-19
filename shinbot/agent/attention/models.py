"""Data models for the attention-driven conversation workflow."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SessionAttentionState:
    """Global attention state for a single session."""

    session_id: str
    attention_value: float = 0.0
    base_threshold: float = 5.0
    runtime_threshold_offset: float = 0.0
    cooldown_until: float = 0.0
    last_update_at: float = field(default_factory=time.time)
    last_consumed_msg_log_id: int | None = None
    last_trigger_msg_log_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def effective_threshold(self) -> float:
        """Compute the effective threshold with runtime offset applied."""
        return self.base_threshold + self.runtime_threshold_offset

    @property
    def is_cooling_down(self) -> bool:
        return time.time() < self.cooldown_until


@dataclass(slots=True)
class SenderWeightState:
    """Per-sender weight state within a session."""

    session_id: str
    sender_id: str
    stable_weight: float = 0.0
    runtime_weight: float = 0.0
    last_runtime_adjust_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class WorkflowRunRecord:
    """Audit record for a single workflow execution."""

    id: str
    session_id: str
    instance_id: str = ""
    response_profile: str = "balanced"
    batch_start_msg_id: int | None = None
    batch_end_msg_id: int | None = None
    batch_size: int = 0
    trigger_attention: float = 0.0
    effective_threshold: float = 0.0
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    replied: bool = False
    response_summary: str = ""
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
