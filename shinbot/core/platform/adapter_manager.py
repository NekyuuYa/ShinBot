"""BaseAdapter abstract class and AdapterManager.

Implements the adapter interface specification (09_adapter_interface_spec.md).
Adapters are protocol translators — they convert platform-native payloads
to/from the ShinBot UnifiedEvent/MessageElement AST.

The core engine never contains platform-specific logic. All platform
interaction goes through the BaseAdapter interface.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import AsyncExitStack
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


@dataclass(slots=True, frozen=True)
class _AdapterFactoryRegistration:
    factory: Callable[..., Any]
    owner: str | None = None


@dataclass(slots=True)
class _AdapterInstanceSpec:
    instance_id: str
    platform: str
    event_callback: Callable[..., Any] | None
    kwargs: dict[str, Any]
    owner: str | None
    suspended: bool = False
    resume_running: bool = False
    quarantined_adapter: BaseAdapter | None = None


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
        self._event_callback: Callable[..., Any] | None = None
        self._connection_state_callback: Callable[[bool], None] | None = None

    def set_event_callback(self, callback: Callable[..., Any]) -> None:
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
        self._factories: dict[str, Callable[..., Any]] = {}
        self._factory_registrations: dict[str, list[_AdapterFactoryRegistration]] = {}
        self._instances: dict[str, BaseAdapter] = {}
        self._instance_specs: dict[str, _AdapterInstanceSpec] = {}
        self._instance_locks: dict[str, asyncio.Lock] = {}
        self._blocked_instance_owners: set[str] = set()
        self._running: set[str] = set()
        self._connection_states: dict[str, AdapterConnectionState] = {}
        self._offline_grace_seconds = max(0.0, float(offline_grace_seconds))

    # ── Factory registration ─────────────────────────────────────────

    def register_adapter(
        self,
        platform: str,
        factory: Callable[..., Any],
        *,
        owner: str | None = None,
    ) -> None:
        """Register a factory callable for a platform name.

        The factory must be callable with signature:
            factory(instance_id, platform, **kwargs) -> BaseAdapter

        Called by adapter plugins at load time via Plugin.register_adapter_factory().

        Args:
            platform: Platform name used for adapter instance creation.
            factory: Callable that creates an adapter instance.
            owner: Optional registration owner. Owned registrations form an
                override stack so removing one owner can restore the previous
                factory without disturbing later overrides.
        """
        if not callable(factory):
            raise TypeError(f"{factory!r} must be a callable factory")
        if platform in self._factories:
            logger.warning(
                format_log_event(
                    "adapter.factory.override",
                    platform=platform,
                    owner=owner or "",
                )
            )
        registrations = self._factory_registrations.setdefault(platform, [])
        registrations.append(_AdapterFactoryRegistration(factory=factory, owner=owner))
        self._factories[platform] = factory
        logger.info(
            format_log_event(
                "adapter.factory.registered",
                platform=platform,
                owner=owner or "",
            )
        )

    def unregister_adapter(self, platform: str, *, owner: str | None = None) -> None:
        """Remove adapter factory registrations for a platform.

        Args:
            platform: Platform name whose registration should be removed.
            owner: Registration owner to remove. When omitted, all registrations
                are removed to preserve the legacy unowned API behavior.
        """
        if owner is None:
            self._factory_registrations.pop(platform, None)
            self._factories.pop(platform, None)
            return

        registrations = self._factory_registrations.get(platform)
        if not registrations:
            return
        remaining = [
            registration for registration in registrations if registration.owner != owner
        ]
        if len(remaining) == len(registrations):
            return
        if remaining:
            self._factory_registrations[platform] = remaining
            self._factories[platform] = remaining[-1].factory
            return
        self._factory_registrations.pop(platform, None)
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
        event_callback: Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> BaseAdapter:
        """Create an adapter instance from a registered factory.

        Args:
            instance_id: Unique identifier for this bot account/connection.
            platform: Platform name matching a registered factory.
            event_callback: Async callback for incoming events.
            **kwargs: Additional arguments passed to the adapter constructor.
        """
        registration = self._active_factory_registration(platform)
        if registration is None:
            available = ", ".join(self._factories.keys()) or "(none)"
            raise ValueError(
                f"No adapter registered for platform {platform!r}. Available: {available}"
            )
        if registration.owner is not None and registration.owner in self._blocked_instance_owners:
            raise RuntimeError(
                f"Adapter factory owner {registration.owner!r} is inactive"
            )
        if instance_id in self._instance_specs:
            raise ValueError(f"Instance {instance_id!r} already exists")

        spec = _AdapterInstanceSpec(
            instance_id=instance_id,
            platform=platform,
            event_callback=event_callback,
            kwargs=dict(kwargs),
            owner=registration.owner,
        )
        adapter = self._instantiate_instance(spec, registration)
        self._instance_specs[instance_id] = spec
        logger.info(
            format_log_event(
                "adapter.instance.created",
                instance_id=instance_id,
                platform=platform,
                owner=registration.owner or "",
            )
        )
        return adapter

    def get_instance_owner(self, instance_id: str) -> str | None:
        """Return the factory owner captured when an instance was created."""

        spec = self._instance_specs.get(instance_id)
        return spec.owner if spec is not None else None

    def instance_ids_for_owner(
        self,
        owner: str,
        *,
        include_suspended: bool = True,
    ) -> list[str]:
        """Return instance IDs created from factories registered by ``owner``."""

        return sorted(
            spec.instance_id
            for spec in self._instance_specs.values()
            if spec.owner == owner and (include_suspended or not spec.suspended)
        )

    def has_instance_spec(self, instance_id: str) -> bool:
        """Return whether a live or suspended runtime instance spec exists."""

        return instance_id in self._instance_specs

    def update_instance_kwargs(self, instance_id: str, patch: dict[str, Any]) -> bool:
        """Update saved constructor kwargs used when an instance is rebuilt."""

        spec = self._instance_specs.get(instance_id)
        if spec is None:
            return False
        spec.kwargs.update(patch)
        return True

    async def suspend_owner_instances(self, owner: str) -> list[str]:
        """Shutdown and detach instances owned by a plugin, retaining rebuild specs."""

        self._blocked_instance_owners.add(owner)
        specs = [
            spec
            for spec in self._instance_specs.values()
            if spec.owner == owner and not spec.suspended and spec.instance_id in self._instances
        ]
        if not specs:
            return []

        async with AsyncExitStack() as stack:
            for spec in sorted(specs, key=lambda item: item.instance_id):
                await stack.enter_async_context(self._instance_lock(spec.instance_id))

            specs = [
                spec
                for spec in specs
                if self._instance_specs.get(spec.instance_id) is spec
                and not spec.suspended
                and spec.instance_id in self._instances
            ]
            was_running = {
                spec.instance_id: spec.instance_id in self._running
                for spec in specs
            }
            shutdown_errors: list[tuple[str, Exception]] = []
            for spec in specs:
                adapter = self._instances[spec.instance_id]
                try:
                    await self._stop_instance_locked(spec.instance_id)
                except Exception as exc:
                    spec.quarantined_adapter = adapter
                    shutdown_errors.append((spec.instance_id, exc))
                    logger.exception(
                        format_log_event(
                            "adapter.instance.owner_suspend_stop_failed",
                            instance_id=spec.instance_id,
                            platform=spec.platform,
                            owner=owner,
                            error_code=type(exc).__name__,
                        )
                    )
                else:
                    spec.quarantined_adapter = None

            for spec in specs:
                instance_id = spec.instance_id
                self._instances.pop(instance_id, None)
                self._running.discard(instance_id)
                self._connection_states.pop(instance_id, None)
                spec.suspended = True
                spec.resume_running = was_running[instance_id]
        if shutdown_errors:
            logger.error(
                format_log_event(
                    "adapter.owner.suspended_with_errors",
                    owner=owner,
                    failed_instance_ids=",".join(
                        instance_id for instance_id, _exc in shutdown_errors
                    ),
                    error_codes=",".join(
                        type(exc).__name__ for _instance_id, exc in shutdown_errors
                    ),
                )
            )
        logger.info(
            format_log_event(
                "adapter.owner.suspended",
                owner=owner,
                instance_ids=",".join(sorted(spec.instance_id for spec in specs)),
            )
        )
        return sorted(spec.instance_id for spec in specs)

    async def resume_owner_instances(self, owner: str) -> list[str]:
        """Rebuild suspended owner instances and restore their prior running state."""

        specs = sorted(
            (
                spec
                for spec in self._instance_specs.values()
                if spec.owner == owner and spec.suspended
            ),
            key=lambda item: item.instance_id,
        )
        if not specs:
            self._blocked_instance_owners.discard(owner)
            return []

        async with AsyncExitStack() as stack:
            for spec in specs:
                await stack.enter_async_context(self._instance_lock(spec.instance_id))

            specs = [
                spec
                for spec in specs
                if self._instance_specs.get(spec.instance_id) is spec
                and spec.suspended
                and spec.instance_id not in self._instances
            ]
            for spec in specs:
                quarantined = spec.quarantined_adapter
                if quarantined is None:
                    continue
                try:
                    await quarantined.shutdown()
                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "adapter.instance.quarantine_stop_failed",
                            instance_id=spec.instance_id,
                            platform=spec.platform,
                            owner=owner,
                            error_code=type(exc).__name__,
                        )
                    )
                    raise RuntimeError(
                        f"Cannot restore adapter instance {spec.instance_id!r}: "
                        "the previous adapter is still active"
                    ) from exc
                spec.quarantined_adapter = None

            registrations: dict[str, _AdapterFactoryRegistration] = {}
            for spec in specs:
                registration = self._active_factory_registration(spec.platform)
                if registration is None or registration.owner != owner:
                    raise RuntimeError(
                        f"Cannot restore adapter instance {spec.instance_id!r}: active factory "
                        f"for {spec.platform!r} is not owned by {owner!r}"
                    )
                registrations[spec.instance_id] = registration

            rebuilt: list[_AdapterInstanceSpec] = []
            try:
                for spec in specs:
                    self._instantiate_instance(spec, registrations[spec.instance_id])
                    rebuilt.append(spec)
                for spec in specs:
                    if spec.resume_running:
                        await self._start_instance_locked(spec.instance_id)
            except BaseException:
                for spec in reversed(rebuilt):
                    adapter = self._instances.get(spec.instance_id)
                    try:
                        await self._stop_instance_locked(spec.instance_id)
                    except Exception:
                        spec.quarantined_adapter = adapter
                        logger.exception(
                            "Failed to shutdown adapter instance %s after owner restore failed",
                            spec.instance_id,
                        )
                    else:
                        spec.quarantined_adapter = None
                    self._instances.pop(spec.instance_id, None)
                    self._running.discard(spec.instance_id)
                    self._connection_states.pop(spec.instance_id, None)
                raise

            for spec in specs:
                spec.suspended = False
                spec.resume_running = False
        self._blocked_instance_owners.discard(owner)
        logger.info(
            format_log_event(
                "adapter.owner.resumed",
                owner=owner,
                instance_ids=",".join(spec.instance_id for spec in specs),
            )
        )
        return [spec.instance_id for spec in specs]

    def discard_owner_instance_snapshots(
        self,
        owner: str,
        *,
        preserve_instance_ids: set[str] | frozenset[str] = frozenset(),
    ) -> int:
        """Discard suspended instance specs for an owner except an explicit baseline."""

        to_remove = [
            spec.instance_id
            for spec in self._instance_specs.values()
            if spec.owner == owner
            and spec.suspended
            and spec.quarantined_adapter is None
            and spec.instance_id not in preserve_instance_ids
        ]
        for instance_id in to_remove:
            self._instance_specs.pop(instance_id, None)
        return len(to_remove)

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
        self._running.discard(instance_id)
        self._instance_specs.pop(instance_id, None)
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
        if state.last_connected_at is None and state.last_disconnected_at is None:
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
        async with self._instance_lock(instance_id):
            await self._start_instance_locked(instance_id)

    async def _start_instance_locked(self, instance_id: str) -> None:
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
        async with self._instance_lock(instance_id):
            await self._stop_instance_locked(instance_id)

    async def _stop_instance_locked(self, instance_id: str) -> None:
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
        async with self._instance_lock(instance_id):
            adapter = self._instances.get(instance_id)
            spec = self._instance_specs.get(instance_id)
            if adapter is not None:
                await self._stop_instance_locked(instance_id)
            elif spec is not None and spec.quarantined_adapter is not None:
                await spec.quarantined_adapter.shutdown()
                spec.quarantined_adapter = None
            self._connection_states.pop(instance_id, None)
            self._running.discard(instance_id)
            removed_adapter = self._instances.pop(instance_id, None)
            removed_spec = self._instance_specs.pop(instance_id, None)
            return removed_adapter is not None or removed_spec is not None

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
        self._instance_specs.clear()
        self._blocked_instance_owners.clear()
        self._running.clear()
        self._connection_states.clear()

    def _active_factory_registration(
        self,
        platform: str,
    ) -> _AdapterFactoryRegistration | None:
        registrations = self._factory_registrations.get(platform)
        if not registrations:
            return None
        return registrations[-1]

    def _instance_lock(self, instance_id: str) -> asyncio.Lock:
        return self._instance_locks.setdefault(instance_id, asyncio.Lock())

    def _instantiate_instance(
        self,
        spec: _AdapterInstanceSpec,
        registration: _AdapterFactoryRegistration,
    ) -> BaseAdapter:
        adapter = registration.factory(
            instance_id=spec.instance_id,
            platform=spec.platform,
            **spec.kwargs,
        )
        if spec.event_callback is not None:
            callback = spec.event_callback

            async def handle_event(*args: Any, **kwargs: Any) -> Any:
                if self._instances.get(spec.instance_id) is not adapter:
                    return None
                result = callback(*args, **kwargs)
                if inspect.isawaitable(result):
                    return await result
                return result

            adapter.set_event_callback(handle_event)
        adapter.set_connection_state_callback(
            lambda connected, *, _instance_id=spec.instance_id, _adapter=adapter: (
                self._handle_connection_state_change(
                    _instance_id,
                    connected,
                    expected_adapter=_adapter,
                )
            )
        )
        self._instances[spec.instance_id] = adapter
        self._connection_states[spec.instance_id] = AdapterConnectionState()
        return adapter

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

    def _handle_connection_state_change(
        self,
        instance_id: str,
        connected: bool,
        *,
        expected_adapter: BaseAdapter,
    ) -> None:
        if self._instances.get(instance_id) is not expected_adapter:
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
