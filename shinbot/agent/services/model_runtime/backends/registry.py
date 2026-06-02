"""Backend registry for model runtime integrations."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from shinbot.agent.services.model_runtime.backends.protocol import ModelBackend

BackendFactory = Callable[[], ModelBackend]


@dataclass(slots=True, frozen=True)
class ModelBackendDescriptor:
    """Management metadata for one model backend implementation."""

    name: str
    display_name: str = ""
    description: str = ""
    kind: str = "runtime"
    supported_provider_types: frozenset[str] = frozenset()


class ModelBackendRegistry:
    """In-memory registry of backend factories."""

    def __init__(self) -> None:
        self._factories: dict[str, BackendFactory] = {}
        self._descriptors: dict[str, ModelBackendDescriptor] = {}

    def register(
        self,
        name: str,
        factory: BackendFactory,
        *,
        descriptor: ModelBackendDescriptor | None = None,
    ) -> None:
        """Register or replace a backend factory."""

        normalized = str(name or "").strip()
        if not normalized:
            raise ValueError("Backend name cannot be empty")
        self._factories[normalized] = factory
        if descriptor is not None:
            self._descriptors[normalized] = descriptor
        elif normalized not in self._descriptors:
            self._descriptors[normalized] = ModelBackendDescriptor(name=normalized)

    def create(self, name: str) -> ModelBackend:
        """Create a backend instance by name."""

        normalized = str(name or "").strip()
        factory = self._factories.get(normalized)
        if factory is None:
            supported = ", ".join(sorted(self._factories))
            raise ValueError(
                f"Unsupported model backend {normalized!r}; supported backends: {supported}"
            )
        return factory()

    def names(self) -> frozenset[str]:
        """Return registered backend names."""

        return frozenset(self._factories)

    def get_descriptor(self, name: str) -> ModelBackendDescriptor | None:
        """Return backend metadata by name, if present."""

        normalized = str(name or "").strip()
        if not normalized:
            return None
        return self._descriptors.get(normalized)

    def descriptors(self) -> Iterable[ModelBackendDescriptor]:
        """Iterate over registered backend descriptors."""

        return tuple(self._descriptors.values())

    def factories(self) -> Iterable[tuple[str, BackendFactory]]:
        """Iterate over registered backend factories."""

        return tuple(self._factories.items())


_DEFAULT_BACKEND_REGISTRY = ModelBackendRegistry()


def register_backend(
    name: str,
    factory: BackendFactory,
    *,
    descriptor: ModelBackendDescriptor | None = None,
) -> None:
    """Register a backend factory in the default registry."""

    _DEFAULT_BACKEND_REGISTRY.register(name, factory, descriptor=descriptor)


def create_registered_backend(name: str) -> ModelBackend:
    """Create a backend from the default registry."""

    return _DEFAULT_BACKEND_REGISTRY.create(name)


def supported_backend_names() -> frozenset[str]:
    """Return supported backend names from the default registry."""

    return _DEFAULT_BACKEND_REGISTRY.names()


def get_backend_descriptor(name: str) -> ModelBackendDescriptor | None:
    """Return backend metadata from the default registry, if present."""

    return _DEFAULT_BACKEND_REGISTRY.get_descriptor(name)


def registered_backend_descriptors() -> tuple[ModelBackendDescriptor, ...]:
    """Return all backend descriptors from the default registry."""

    return tuple(_DEFAULT_BACKEND_REGISTRY.descriptors())
