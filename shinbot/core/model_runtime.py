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


class ModelRuntimeExtensionRegistrar:
    """Plugin-facing registrar for model backend/provider extensions."""

    def register_backend_factory(
        self,
        name: str,
        factory: Callable[[], Any],
        *,
        descriptor: Any | None = None,
    ) -> None:
        """Register a model backend factory by name."""

        from shinbot.agent.services.model_runtime.backends import register_backend

        register_backend(name, factory, descriptor=descriptor)

    def register_provider_descriptor(self, descriptor: Any) -> None:
        """Register a model provider descriptor."""

        from shinbot.agent.services.model_runtime.providers import register_provider_descriptor

        register_provider_descriptor(descriptor)
