"""BaseAdapter abstract class and AdapterManager.

Implements the adapter interface specification (09_adapter_interface_spec.md).
Adapters are protocol translators — they convert platform-native payloads
to/from the ShinBot UnifiedEvent/MessageElement AST.

The core engine never contains platform-specific logic. All platform
interaction goes through the BaseAdapter interface.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from shinbot.models.elements import MessageElement

logger = logging.getLogger(__name__)


class MessageHandle:
    """Handle returned after sending a message, supporting edit/recall."""

    __slots__ = ("message_id", "adapter_ref", "_platform_data")

    def __init__(
        self,
        message_id: str,
        adapter_ref: BaseAdapter | None = None,
        platform_data: dict[str, Any] | None = None,
    ):
        self.message_id = message_id
        self.adapter_ref = adapter_ref
        self._platform_data = platform_data or {}

    async def edit(self, elements: list[MessageElement]) -> None:
        if self.adapter_ref is None:
            raise RuntimeError("No adapter reference for edit operation")
        await self.adapter_ref.call_api(
            "message.update",
            {"message_id": self.message_id, "elements": elements},
        )

    async def recall(self) -> None:
        if self.adapter_ref is None:
            raise RuntimeError("No adapter reference for recall operation")
        await self.adapter_ref.call_api(
            "message.delete",
            {"message_id": self.message_id},
        )

    def __repr__(self) -> str:
        return f"MessageHandle(message_id={self.message_id!r})"


class BaseAdapter(ABC):
    """Abstract base class for all platform adapters.

    Each adapter instance represents a single bot account / connection endpoint.
    Adapters must implement bidirectional translation:
      - Ingress: Raw Payload → UnifiedEvent (Satori AST)
      - Egress: ShinBot call_api / send → Platform Native API
    """

    def __init__(self, instance_id: str, platform: str):
        self.instance_id = instance_id
        self.platform = platform
        self._event_callback: Callable | None = None

    def set_event_callback(self, callback: Callable) -> None:
        """Set the callback that the adapter calls when it receives events.

        The callback signature should be:
            async def on_event(event: UnifiedEvent) -> None
        """
        self._event_callback = callback

    @abstractmethod
    async def start(self) -> None:
        """Establish connection and begin listening for events."""
        ...

    @abstractmethod
    async def shutdown(self) -> None:
        """Safely disconnect and clean up resources."""
        ...

    @abstractmethod
    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        """Send a message to the specified session.

        Args:
            target_session: Session ID (URN format) to send to.
            elements: MessageElement AST array to send.

        Returns:
            MessageHandle for subsequent edit/recall operations.
        """
        ...

    @abstractmethod
    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        """Call a Satori standard API or platform internal API.

        Standard methods: message.create, message.delete, message.get,
            member.kick, member.mute, guild.get, friend.list, etc.
        Internal methods: internal.{platform}.{action}
        """
        ...

    @abstractmethod
    async def get_capabilities(self) -> dict[str, Any]:
        """Return the adapter's capability manifest.

        Returns:
            {
                "elements": ["text", "at", "img", ...],
                "actions": ["message.create", "member.kick", ...],
                "limits": {"max_file_size": 10485760, ...}
            }
        """
        ...

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} instance={self.instance_id!r} platform={self.platform!r}>"
        )


class AdapterManager:
    """Registry and lifecycle manager for adapter instances.

    Supports:
      - Factory registration: adapter plugins register their class by platform name
      - Dynamic instance creation from config
      - Lifecycle management (start/shutdown all adapters)
      - Lookup by instance_id or platform
    """

    def __init__(self) -> None:
        self._factories: dict[str, Callable] = {}
        self._instances: dict[str, BaseAdapter] = {}
        self._running: set[str] = set()

    # ── Factory registration ─────────────────────────────────────────

    def register_adapter(self, platform: str, factory: Callable) -> None:
        """Register a factory callable for a platform name.

        The factory must be callable with signature:
            factory(instance_id, platform, **kwargs) -> BaseAdapter

        Called by adapter plugins at load time via PluginContext.register_adapter_factory().
        """
        if not callable(factory):
            raise TypeError(f"{factory!r} must be a callable factory")
        if platform in self._factories:
            logger.warning("Overriding adapter factory for platform %r", platform)
        self._factories[platform] = factory
        logger.info("Registered adapter factory for platform: %s", platform)

    def unregister_adapter(self, platform: str) -> None:
        """Remove a registered adapter factory."""
        self._factories.pop(platform, None)

    @property
    def registered_platforms(self) -> list[str]:
        return list(self._factories.keys())

    # ── Instance management ──────────────────────────────────────────

    def create_instance(
        self,
        instance_id: str,
        platform: str,
        event_callback: Callable | None = None,
        **kwargs: Any,
    ) -> BaseAdapter:
        """Create an adapter instance from a registered factory.

        Args:
            instance_id: Unique identifier for this bot account/connection.
            platform: Platform name matching a registered factory.
            event_callback: Async callback for incoming events.
            **kwargs: Additional arguments passed to the adapter constructor.
        """
        if platform not in self._factories:
            available = ", ".join(self._factories.keys()) or "(none)"
            raise ValueError(
                f"No adapter registered for platform {platform!r}. Available: {available}"
            )
        if instance_id in self._instances:
            raise ValueError(f"Instance {instance_id!r} already exists")

        adapter_cls = self._factories[platform]
        adapter = adapter_cls(instance_id=instance_id, platform=platform, **kwargs)

        if event_callback is not None:
            adapter.set_event_callback(event_callback)

        self._instances[instance_id] = adapter
        logger.info("Created adapter instance: %s (platform=%s)", instance_id, platform)
        return adapter

    def get_instance(self, instance_id: str) -> BaseAdapter | None:
        return self._instances.get(instance_id)

    def get_instances_by_platform(self, platform: str) -> list[BaseAdapter]:
        return [a for a in self._instances.values() if a.platform == platform]

    @property
    def all_instances(self) -> list[BaseAdapter]:
        return list(self._instances.values())

    def remove_instance(self, instance_id: str) -> BaseAdapter | None:
        return self._instances.pop(instance_id, None)

    # ── Lifecycle ────────────────────────────────────────────────────

    def is_running(self, instance_id: str) -> bool:
        """Return True if the adapter instance is currently running."""
        return instance_id in self._running

    async def start_instance(self, instance_id: str) -> None:
        """Start a single adapter instance by ID."""
        adapter = self._instances.get(instance_id)
        if adapter is None:
            raise ValueError(f"No instance registered with id {instance_id!r}")
        await adapter.start()
        self._running.add(instance_id)
        logger.info("Started adapter: %s", instance_id)

    async def stop_instance(self, instance_id: str) -> None:
        """Stop a single adapter instance by ID."""
        adapter = self._instances.get(instance_id)
        if adapter is None:
            raise ValueError(f"No instance registered with id {instance_id!r}")
        await adapter.shutdown()
        self._running.discard(instance_id)
        logger.info("Stopped adapter: %s", instance_id)

    async def delete_instance(self, instance_id: str) -> bool:
        """Stop (if running) and remove an adapter instance."""
        if instance_id in self._running:
            await self.stop_instance(instance_id)
        return self._instances.pop(instance_id, None) is not None

    async def start_all(self) -> None:
        """Start all registered adapter instances."""
        for instance_id in list(self._instances):
            try:
                await self.start_instance(instance_id)
            except Exception:
                logger.exception("Failed to start adapter: %s", instance_id)

    async def shutdown_all(self) -> None:
        """Shutdown all adapter instances gracefully."""
        for instance_id in list(self._instances):
            try:
                await self.stop_instance(instance_id)
            except Exception:
                logger.exception("Error shutting down adapter: %s", instance_id)
        self._instances.clear()
        self._running.clear()

    # ── Capability queries ───────────────────────────────────────────

    async def get_capabilities(self, instance_id: str) -> dict[str, Any] | None:
        adapter = self.get_instance(instance_id)
        if adapter is None:
            return None
        return await adapter.get_capabilities()
