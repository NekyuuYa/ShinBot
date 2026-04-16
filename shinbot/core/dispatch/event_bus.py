"""Async event bus for internal event dispatch.

Provides a lightweight pub/sub mechanism for decoupled communication
between the core pipeline, plugins, and adapters.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)

EventHandler = Callable[..., Coroutine[Any, Any, Any]]

# ── Circuit-breaker constants ────────────────────────────────────────
# A handler that raises on every invocation floods logs and wastes CPU.
# After _CB_THRESHOLD consecutive failures the circuit opens and the
# handler is skipped.  After _CB_RESET_SEC seconds the circuit enters
# half-open state: the handler is tried once more.  Success closes it;
# another failure re-opens it and resets the cooldown timer.

_CB_THRESHOLD = 5       # consecutive failures before opening
_CB_RESET_SEC = 60.0    # seconds before half-open retry


class _CircuitState:
    """Per-handler circuit-breaker state (closed → open → half-open → …)."""

    __slots__ = ("consecutive_failures", "tripped_at")

    def __init__(self) -> None:
        self.consecutive_failures: int = 0
        self.tripped_at: float | None = None  # None = closed

    @property
    def is_open(self) -> bool:
        """True when the circuit is open and the handler should be skipped.

        Side-effect: transitions open → half-open (sets tripped_at = None)
        when the reset window has elapsed so the next call attempt is allowed.
        """
        if self.tripped_at is None:
            return False
        if time.monotonic() - self.tripped_at >= _CB_RESET_SEC:
            # Half-open: give the handler one more chance.
            self.tripped_at = None
            return False
        return True

    def on_success(self) -> None:
        self.consecutive_failures = 0
        self.tripped_at = None

    def on_failure(self) -> bool:
        """Record a failure.  Returns True if the circuit just opened."""
        self.consecutive_failures += 1
        if self.consecutive_failures >= _CB_THRESHOLD and self.tripped_at is None:
            self.tripped_at = time.monotonic()
            return True
        return False


class EventBus:
    """Async event bus with priority-ordered handlers.

    Handlers are registered for event types (strings like "message-created").
    When an event is emitted, all matching handlers are called in priority
    order (lower number = higher priority).
    """

    def __init__(self) -> None:
        # event_type → [(priority, handler, owner_id)]
        self._handlers: dict[str, list[tuple[int, EventHandler, str | None]]] = defaultdict(list)
        # handler id → circuit-breaker state (keyed by id() — safe because
        # handlers are held in _handlers, preventing GC until explicitly removed)
        self._circuit: dict[int, _CircuitState] = {}

    def on(
        self,
        event_type: str,
        handler: EventHandler,
        priority: int = 100,
        owner: str | None = None,
    ) -> None:
        """Register a handler for an event type.

        Args:
            event_type: Event type string (e.g. "message-created", "*" for all).
            handler: Async callable to invoke.
            priority: Lower = earlier execution. Default 100.
            owner: Optional owner ID (plugin ID) for cleanup on unload.
        """
        entry = (priority, handler, owner)
        self._handlers[event_type].append(entry)
        self._handlers[event_type].sort(key=lambda x: x[0])

    def off(self, event_type: str, handler: EventHandler) -> None:
        """Remove a specific handler."""
        entries = self._handlers.get(event_type, [])
        self._handlers[event_type] = [(p, h, o) for p, h, o in entries if h is not handler]
        self._circuit.pop(id(handler), None)

    def off_all(self, owner: str) -> int:
        """Remove all handlers owned by a specific owner (plugin unload).

        Returns the count of removed handlers.
        """
        removed = 0
        for event_type in list(self._handlers.keys()):
            before = len(self._handlers[event_type])
            kept = [(p, h, o) for p, h, o in self._handlers[event_type] if o != owner]
            for _p, h, o in self._handlers[event_type]:
                if o == owner:
                    self._circuit.pop(id(h), None)
            self._handlers[event_type] = kept
            removed += before - len(kept)
        return removed

    async def emit(self, event_type: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Emit an event, calling all registered handlers in priority order.

        Handlers for the specific event_type AND wildcard "*" handlers are
        both invoked (specific first, then wildcard).

        If a handler raises StopPropagation, subsequent handlers are skipped.

        Returns a list of handler return values (excluding None).
        """
        results: list[Any] = []

        handlers: list[tuple[int, EventHandler, str | None]] = []
        handlers.extend(self._handlers.get(event_type, []))
        if event_type != "*":
            handlers.extend(self._handlers.get("*", []))
        handlers.sort(key=lambda x: x[0])

        for _priority, handler, owner in handlers:
            cb = self._circuit.setdefault(id(handler), _CircuitState())
            if cb.is_open:
                continue
            try:
                result = await handler(*args, **kwargs)
                cb.on_success()
                if result is not None:
                    results.append(result)
            except StopPropagation:
                cb.on_success()
                logger.debug(
                    "Propagation stopped by handler %s (owner=%s) for event %s",
                    handler.__name__,
                    owner,
                    event_type,
                )
                break
            except Exception:
                just_opened = cb.on_failure()
                if just_opened:
                    logger.warning(
                        "Circuit breaker OPEN for handler %s (owner=%s) after %d consecutive failures",
                        handler.__name__,
                        owner,
                        _CB_THRESHOLD,
                    )
                logger.exception(
                    "Error in event handler %s (owner=%s) for event %s",
                    handler.__name__,
                    owner,
                    event_type,
                )

        return results

    def handler_count(self, event_type: str | None = None) -> int:
        if event_type is not None:
            return len(self._handlers.get(event_type, []))
        return sum(len(h) for h in self._handlers.values())


class StopPropagation(Exception):
    """Raise in a handler to stop event propagation to lower-priority handlers."""
