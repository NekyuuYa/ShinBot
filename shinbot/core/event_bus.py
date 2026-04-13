"""Async event bus for internal event dispatch.

Provides a lightweight pub/sub mechanism for decoupled communication
between the core pipeline, plugins, and adapters.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)

EventHandler = Callable[..., Coroutine[Any, Any, Any]]


class EventBus:
    """Async event bus with priority-ordered handlers.

    Handlers are registered for event types (strings like "message-created").
    When an event is emitted, all matching handlers are called in priority
    order (lower number = higher priority).
    """

    def __init__(self) -> None:
        # event_type → [(priority, handler, owner_id)]
        self._handlers: dict[str, list[tuple[int, EventHandler, str | None]]] = defaultdict(list)

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

    def off_all(self, owner: str) -> int:
        """Remove all handlers owned by a specific owner (plugin unload).

        Returns the count of removed handlers.
        """
        removed = 0
        for event_type in list(self._handlers.keys()):
            before = len(self._handlers[event_type])
            self._handlers[event_type] = [
                (p, h, o) for p, h, o in self._handlers[event_type] if o != owner
            ]
            removed += before - len(self._handlers[event_type])
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
            try:
                result = await handler(*args, **kwargs)
                if result is not None:
                    results.append(result)
            except StopPropagation:
                logger.debug(
                    "Propagation stopped by handler %s (owner=%s) for event %s",
                    handler.__name__,
                    owner,
                    event_type,
                )
                break
            except Exception:
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
