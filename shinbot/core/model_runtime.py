"""Lower-layer model runtime contracts used by core integrations."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

ModelRuntimeObserver = Callable[[dict[str, Any]], Awaitable[None] | None]


class ModelRuntimeObserverRegistry(Protocol):
    """Observer registration surface required by plugin lifecycle code."""

    def register_observer(self, observer: ModelRuntimeObserver) -> None:
        """Register a callback for model runtime events."""

    def unregister_observer(self, observer: ModelRuntimeObserver) -> None:
        """Remove a previously registered model runtime callback."""
