"""Shared pytest fixtures for ShinBot test suite."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.message_routes.command import CommandRegistry
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.plugins.manager import PluginManager
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.schema.elements import MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, User

LAYER_MARKERS = {"unit", "api", "integration", "e2e"}


def classify_test_layer(path: Path) -> str:
    """Infer a default test layer from the current suite layout."""

    normalized = path.as_posix()
    name = path.name
    parts = set(path.parts)

    if "unit" in parts:
        return "unit"
    if "api" in parts:
        return "api"
    if "integration" in parts:
        return "integration"
    if "e2e" in parts:
        return "e2e"
    if name.endswith("_api.py") or "_api_" in name:
        return "api"
    if any(
        token in normalized
        for token in (
            "runtime",
            "boot",
            "plugin",
            "adapter",
            "ingress",
            "routing",
            "workflow",
            "media",
            "persistence",
            "operator_cli",
            "system_update",
        )
    ):
        return "integration"
    return "unit"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Apply default layer markers so marker-based test runs stay useful."""

    for item in items:
        existing = {marker.name for marker in item.iter_markers()}
        if existing & LAYER_MARKERS:
            continue
        item.add_marker(getattr(pytest.mark, classify_test_layer(Path(str(item.path)))))

# ── Mock adapter ─────────────────────────────────────────────────────────────


class MockAdapter(BaseAdapter):
    """Test double for BaseAdapter — records sent messages."""

    def __init__(self, instance_id: str = "test-inst", platform: str = "mock"):
        super().__init__(instance_id=instance_id, platform=platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def shutdown(self) -> None:
        self.stopped = True

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        self.sent.append((target_session, elements))
        return MessageHandle(
            message_id=f"mock-{len(self.sent)}",
            adapter_ref=self,
        )

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        return {"ok": True, "method": method}

    async def get_capabilities(self) -> dict[str, Any]:
        return {
            "elements": ["text", "at", "img"],
            "actions": ["message.create", "message.delete"],
            "limits": {},
        }


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_adapter() -> MockAdapter:
    return MockAdapter()


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def command_registry() -> CommandRegistry:
    return CommandRegistry()


@pytest.fixture
def session_manager() -> SessionManager:
    return SessionManager()


@pytest.fixture
def permission_engine() -> PermissionEngine:
    return PermissionEngine()


@pytest.fixture
def adapter_manager() -> AdapterManager:
    mgr = AdapterManager()
    mgr.register_adapter("mock", MockAdapter)
    return mgr


@pytest.fixture
def plugin_manager(command_registry, event_bus, tmp_path) -> PluginManager:
    return PluginManager(command_registry=command_registry, event_bus=event_bus, data_dir=tmp_path)


# ── Event builders ────────────────────────────────────────────────────────────


def make_message_event(
    *,
    content: str = "hello",
    user_id: str = "user-1",
    channel_id: str = "ch-1",
    channel_type: int = 0,
    guild_id: str | None = None,
    platform: str = "mock",
    instance_id: str = "test-inst",
) -> UnifiedEvent:
    guild = Guild(id=guild_id) if guild_id else None
    return UnifiedEvent(
        type="message-created",
        platform=platform,
        self_id=instance_id,
        user=User(id=user_id),
        channel=Channel(id=channel_id, type=channel_type),
        guild=guild,
        message=MessagePayload(id="msg-1", content=content),
    )
