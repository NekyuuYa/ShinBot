"""BaseAdapter abstract class and AdapterManager.

Implements the adapter interface specification (09_adapter_interface_spec.md).
Adapters are protocol translators — they convert platform-native payloads
to/from the ShinBot UnifiedEvent/MessageElement AST.

The core engine never contains platform-specific logic. All platform
interaction goes through the BaseAdapter interface.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from shinbot.schema.elements import MessageElement
from shinbot.utils.logger import format_log_event, get_logger

logger = get_logger(__name__, source="adapter", color="green")


@dataclass(slots=True)
class AdapterConnectionState:
    """Observed connection state for one adapter instance."""

    connected: bool = False
    last_connected_at: float | None = None
    last_disconnected_at: float | None = None


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
        """Edit this message by replacing its content with new elements.

        Args:
            elements: New message element list to replace the current content.

        Raises:
            RuntimeError: If no adapter reference is available.
        """
        if self.adapter_ref is None:
            raise RuntimeError("No adapter reference for edit operation")
        params: dict[str, Any] = {"message_id": self.message_id, "elements": elements}
        if "session_id" in self._platform_data:
            params["session_id"] = self._platform_data["session_id"]
        await self.adapter_ref.call_api(
            "message.update",
            params,
        )

    async def recall(self) -> None:
        """Recall (delete) this message from the platform.

        Raises:
            RuntimeError: If no adapter reference is available.
        """
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
        self._connection_state_callback: Callable[[bool], None] | None = None

    def set_event_callback(self, callback: Callable) -> None:
        """Set the callback that the adapter calls when it receives events.

        The callback signature should be:
            async def on_event(event: UnifiedEvent) -> None
        """
        self._event_callback = callback

    def set_connection_state_callback(self, callback: Callable[[bool], None]) -> None:
        """Set the callback used to report explicit connection lifecycle changes.

        Args:
            callback: Invoked with ``True`` when the adapter becomes connected
                and ``False`` when the adapter explicitly disconnects.
        """
        self._connection_state_callback = callback

    def _notify_connection_state(self, connected: bool) -> None:
        """Report an explicit connection lifecycle transition to the manager."""
        if self._connection_state_callback is None:
            return
        self._connection_state_callback(bool(connected))

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

    def __init__(self, *, offline_grace_seconds: float = 15.0) -> None:
        self._factories: dict[str, Callable] = {}
        self._instances: dict[str, BaseAdapter] = {}
        self._running: set[str] = set()
        self._connection_states: dict[str, AdapterConnectionState] = {}
        self._offline_grace_seconds = max(0.0, float(offline_grace_seconds))

    # ── Factory registration ─────────────────────────────────────────

    def register_adapter(self, platform: str, factory: Callable) -> None:
        """Register a factory callable for a platform name.

        The factory must be callable with signature:
            factory(instance_id, platform, **kwargs) -> BaseAdapter

        Called by adapter plugins at load time via Plugin.register_adapter_factory().
        """
        if not callable(factory):
            raise TypeError(f"{factory!r} must be a callable factory")
        if platform in self._factories:
            logger.warning(
                format_log_event(
                    "adapter.factory.override",
                    platform=platform,
                )
            )
        self._factories[platform] = factory
        logger.info(format_log_event("adapter.factory.registered", platform=platform))

    def unregister_adapter(self, platform: str) -> None:
        """Remove a registered adapter factory."""
        self._factories.pop(platform, None)

    @property
    def registered_platforms(self) -> list[str]:
        """Platform names that have a registered adapter factory."""
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
        adapter.set_connection_state_callback(
            lambda connected, *, _instance_id=instance_id: self._handle_connection_state_change(
                _instance_id,
                connected,
            )
        )

        self._instances[instance_id] = adapter
        self._connection_states[instance_id] = AdapterConnectionState()
        logger.info(
            format_log_event(
                "adapter.instance.created",
                instance_id=instance_id,
                platform=platform,
            )
        )
        return adapter

    def get_instance(self, instance_id: str) -> BaseAdapter | None:
        """Retrieve an adapter instance by its unique ID.

        Args:
            instance_id: Unique identifier for the adapter instance.

        Returns:
            The adapter, or None if no instance with that ID exists.
        """
        return self._instances.get(instance_id)

    def get_instances_by_platform(self, platform: str) -> list[BaseAdapter]:
        """Return all adapter instances for a given platform name.

        Args:
            platform: Platform name (e.g. ``onebot_v11``).
        """
        return [a for a in self._instances.values() if a.platform == platform]

    def get_instance_by_session(self, session_id: str) -> BaseAdapter | None:
        """Look up an adapter instance by a session URN.

        Session URN format: ``{instance_id}:{type}:{target}``
        The leading segment before the first colon is the instance_id.
        """
        instance_id = session_id.split(":", 1)[0]
        return self._instances.get(instance_id)

    @property
    def all_instances(self) -> list[BaseAdapter]:
        """List of every registered adapter instance."""
        return list(self._instances.values())

    def remove_instance(self, instance_id: str) -> BaseAdapter | None:
        """Remove an adapter instance without stopping it.

        Args:
            instance_id: Unique identifier for the adapter instance.

        Returns:
            The removed adapter, or None if not found.
        """
        self._connection_states.pop(instance_id, None)
        return self._instances.pop(instance_id, None)

    # ── Lifecycle ────────────────────────────────────────────────────

    def is_running(self, instance_id: str) -> bool:
        """Return True if the adapter instance is currently running."""
        return instance_id in self._running

    def is_connected(self, instance_id: str) -> bool:
        """Return True when the adapter is explicitly connected right now.

        Args:
            instance_id: Unique identifier for the adapter instance.
        """
        if instance_id not in self._running:
            return False
        state = self._connection_states.get(instance_id)
        return bool(state is not None and state.connected)

    def is_available(
        self,
        instance_id: str,
        *,
        now: float | None = None,
        offline_grace_seconds: float | None = None,
    ) -> bool:
        """Return True when the adapter should be treated as stably available.

        This differs from :meth:`is_connected` by allowing a short grace window
        after an explicit disconnect, preventing brief reconnect jitter from
        immediately flipping Agent scheduling on and off.

        Args:
            instance_id: Unique identifier for the adapter instance.
            now: Optional monotonic timestamp override for tests.
            offline_grace_seconds: Optional grace-window override for tests.
        """
        if instance_id not in self._running:
            return False
        state = self._connection_states.get(instance_id)
        if state is None:
            return False
        if state.connected:
            return True

        grace_seconds = (
            self._offline_grace_seconds
            if offline_grace_seconds is None
            else max(0.0, float(offline_grace_seconds))
        )
        if grace_seconds <= 0:
            return False
        if state.last_connected_at is None or state.last_disconnected_at is None:
            return False

        current_time = time.monotonic() if now is None else float(now)
        return current_time - state.last_disconnected_at <= grace_seconds

    async def start_instance(self, instance_id: str) -> None:
        """Start a single adapter instance by ID."""
        adapter = self._instances.get(instance_id)
        if adapter is None:
            raise ValueError(f"No instance registered with id {instance_id!r}")
        self._reset_connection_state(instance_id)
        logger.info(
            format_log_event(
                "adapter.instance.starting",
                instance_id=instance_id,
                platform=adapter.platform,
            )
        )
        await adapter.start()
        self._running.add(instance_id)
        logger.info(
            format_log_event(
                "adapter.instance.started",
                instance_id=instance_id,
                platform=adapter.platform,
            )
        )

    async def stop_instance(self, instance_id: str) -> None:
        """Stop a single adapter instance by ID."""
        adapter = self._instances.get(instance_id)
        if adapter is None:
            raise ValueError(f"No instance registered with id {instance_id!r}")
        logger.info(
            format_log_event(
                "adapter.instance.stopping",
                instance_id=instance_id,
                platform=adapter.platform,
            )
        )
        await adapter.shutdown()
        self._running.discard(instance_id)
        self._reset_connection_state(instance_id)
        logger.info(
            format_log_event(
                "adapter.instance.stopped",
                instance_id=instance_id,
                platform=adapter.platform,
            )
        )

    async def delete_instance(self, instance_id: str) -> bool:
        """Stop (if running) and remove an adapter instance."""
        if instance_id in self._running:
            await self.stop_instance(instance_id)
        self._connection_states.pop(instance_id, None)
        return self._instances.pop(instance_id, None) is not None

    async def start_all(self) -> None:
        """Start all registered adapter instances."""
        for instance_id in list(self._instances):
            try:
                await self.start_instance(instance_id)
            except Exception as exc:
                adapter = self._instances.get(instance_id)
                logger.exception(
                    format_log_event(
                        "adapter.instance.start_failed",
                        instance_id=instance_id,
                        platform=adapter.platform if adapter is not None else "",
                        error_code=type(exc).__name__,
                    )
                )

    async def shutdown_all(self) -> None:
        """Shutdown all adapter instances gracefully."""
        for instance_id in list(self._instances):
            try:
                await self.stop_instance(instance_id)
            except Exception as exc:
                adapter = self._instances.get(instance_id)
                logger.exception(
                    format_log_event(
                        "adapter.instance.stop_failed",
                        instance_id=instance_id,
                        platform=adapter.platform if adapter is not None else "",
                        error_code=type(exc).__name__,
                    )
                )
        self._instances.clear()
        self._running.clear()
        self._connection_states.clear()

    def mark_connected(self, instance_id: str, *, at: float | None = None) -> None:
        """Record that an adapter instance reached an explicit connected state.

        Args:
            instance_id: Unique identifier for the adapter instance.
            at: Optional monotonic timestamp override for tests.
        """
        self._set_connection_state(instance_id, connected=True, at=at)

    def mark_disconnected(self, instance_id: str, *, at: float | None = None) -> None:
        """Record that an adapter instance explicitly disconnected.

        Args:
            instance_id: Unique identifier for the adapter instance.
            at: Optional monotonic timestamp override for tests.
        """
        self._set_connection_state(instance_id, connected=False, at=at)

    # ── Capability queries ───────────────────────────────────────────

    async def get_capabilities(self, instance_id: str) -> dict[str, Any] | None:
        """Query the capability manifest of a running adapter instance.

        Args:
            instance_id: Unique identifier for the adapter instance.

        Returns:
            Capability dict from the adapter, or None if instance not found.
        """
        adapter = self.get_instance(instance_id)
        if adapter is None:
            return None
        return await adapter.get_capabilities()

    def _handle_connection_state_change(self, instance_id: str, connected: bool) -> None:
        if instance_id not in self._instances:
            return
        if connected:
            self.mark_connected(instance_id)
            return
        self.mark_disconnected(instance_id)

    def _reset_connection_state(self, instance_id: str) -> None:
        self._connection_states[instance_id] = AdapterConnectionState()

    def _set_connection_state(
        self,
        instance_id: str,
        *,
        connected: bool,
        at: float | None = None,
    ) -> None:
        state = self._connection_states.setdefault(instance_id, AdapterConnectionState())
        previous = state.connected
        timestamp = time.monotonic() if at is None else float(at)
        state.connected = connected
        if connected:
            state.last_connected_at = timestamp
        else:
            state.last_disconnected_at = timestamp
        if previous == connected:
            return
        logger.debug(
            format_log_event(
                "adapter.connection.state_changed",
                instance_id=instance_id,
                connected=connected,
            )
        )
