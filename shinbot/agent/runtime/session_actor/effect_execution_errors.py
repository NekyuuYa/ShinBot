"""Cycle-free control-flow exceptions shared by durable effect components."""

from __future__ import annotations

import math


class EffectExecutionDeferred(RuntimeError):
    """Request a durable retry that does not consume an effect attempt budget.

    A handler uses this only when it has observed a durable blocker that can
    clear without re-running the handler's external work, such as an actor
    control gate waiting for an already-running task to acknowledge
    cancellation. It is deliberately distinct from ordinary exceptions, which
    retain bounded retry and terminal-failure semantics.
    """

    def __init__(self, reason: str, *, delay_seconds: float = 1.0) -> None:
        """Capture a stable deferred reason and a bounded next observation time."""

        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("deferred effect reason must not be empty")
        if (
            isinstance(delay_seconds, bool)
            or not isinstance(delay_seconds, (int, float))
            or not math.isfinite(float(delay_seconds))
            or float(delay_seconds) <= 0
        ):
            raise ValueError("deferred effect delay_seconds must be finite and positive")
        self.reason = normalized_reason
        self.delay_seconds = float(delay_seconds)
        super().__init__(normalized_reason)


class EffectExecutionCancelled(RuntimeError):
    """Signal a durable control gate terminalized the current effect claim."""

    def __init__(self, reason: str) -> None:
        """Require a stable audit reason without fabricating a mailbox failure."""

        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("cancelled effect reason must not be empty")
        self.reason = normalized_reason
        super().__init__(normalized_reason)


__all__ = ["EffectExecutionCancelled", "EffectExecutionDeferred"]
