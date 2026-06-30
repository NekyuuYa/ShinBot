"""Tests for shinbot.api.ws_manager.ConnectionManager."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.websockets import WebSocketState

from shinbot.api.ws_manager import ConnectionManager


@pytest.fixture
def manager() -> ConnectionManager:
    return ConnectionManager()


def _make_ws(state: WebSocketState = WebSocketState.CONNECTING) -> MagicMock:
    """Create a mock WebSocket with the given application_state."""
    ws = MagicMock()
    ws.application_state = state
    ws.accept = AsyncMock()
    ws.send_json = AsyncMock()
    return ws


# ── connect() ────────────────────────────────────────────────────────────────


@pytest.mark.unit
async def test_connect_accepts_connecting_socket(manager: ConnectionManager):
    ws = _make_ws(WebSocketState.CONNECTING)
    await manager.connect(ws)

    ws.accept.assert_awaited_once()
    assert ws in manager._connections
    assert manager.count == 1


@pytest.mark.unit
async def test_connect_does_not_accept_already_open_socket(manager: ConnectionManager):
    ws = _make_ws(WebSocketState.CONNECTED)
    await manager.connect(ws)

    ws.accept.assert_not_awaited()
    assert ws in manager._connections
    assert manager.count == 1


@pytest.mark.unit
async def test_connect_multiple_clients(manager: ConnectionManager):
    clients = [_make_ws() for _ in range(5)]
    for ws in clients:
        await manager.connect(ws)

    assert manager.count == 5
    for ws in clients:
        assert ws in manager._connections


# ── disconnect() ─────────────────────────────────────────────────────────────


@pytest.mark.unit
async def test_disconnect_removes_connection(manager: ConnectionManager):
    ws = _make_ws()
    await manager.connect(ws)
    assert manager.count == 1

    manager.disconnect(ws)
    assert manager.count == 0
    assert ws not in manager._connections


@pytest.mark.unit
async def test_disconnect_unknown_socket_is_noop(manager: ConnectionManager):
    ws = _make_ws()
    # Should not raise
    manager.disconnect(ws)
    assert manager.count == 0


@pytest.mark.unit
async def test_disconnect_only_removes_target(manager: ConnectionManager):
    ws_a = _make_ws()
    ws_b = _make_ws()
    await manager.connect(ws_a)
    await manager.connect(ws_b)
    assert manager.count == 2

    manager.disconnect(ws_a)
    assert manager.count == 1
    assert ws_b in manager._connections


# ── broadcast() ──────────────────────────────────────────────────────────────


@pytest.mark.unit
async def test_broadcast_sends_to_all_clients(manager: ConnectionManager):
    clients = [_make_ws() for _ in range(3)]
    for ws in clients:
        await manager.connect(ws)

    payload = {"type": "log", "message": "hello"}
    await manager.broadcast(payload)

    for ws in clients:
        ws.send_json.assert_awaited_once_with(payload)


@pytest.mark.unit
async def test_broadcast_no_connections_does_not_raise(manager: ConnectionManager):
    await manager.broadcast({"anything": True})


@pytest.mark.unit
async def test_broadcast_removes_dead_connection(manager: ConnectionManager):
    good_ws = _make_ws()
    dead_ws = _make_ws()
    dead_ws.send_json = AsyncMock(side_effect=ConnectionError("gone"))

    await manager.connect(good_ws)
    await manager.connect(dead_ws)
    assert manager.count == 2

    await manager.broadcast({"ping": 1})

    assert manager.count == 1
    assert good_ws in manager._connections
    assert dead_ws not in manager._connections


@pytest.mark.unit
async def test_broadcast_sends_concurrently(manager: ConnectionManager):
    """broadcast fans out concurrently -- all sends are scheduled in one gather."""
    order: list[str] = []

    async def track_a(data):
        await asyncio.sleep(0.01)
        order.append("a")

    async def track_b(data):
        order.append("b")

    ws_a = _make_ws()
    ws_b = _make_ws()
    ws_a.send_json = track_a
    ws_b.send_json = track_b

    await manager.connect(ws_a)
    await manager.connect(ws_b)

    await manager.broadcast({"x": 1})

    # b finishes before a because a has a sleep, proving concurrent dispatch
    assert order == ["b", "a"]


# ── count property ───────────────────────────────────────────────────────────


@pytest.mark.unit
async def test_count_starts_at_zero(manager: ConnectionManager):
    assert manager.count == 0


@pytest.mark.unit
async def test_count_tracks_connect_disconnect(manager: ConnectionManager):
    ws_a = _make_ws()
    ws_b = _make_ws()

    await manager.connect(ws_a)
    assert manager.count == 1

    await manager.connect(ws_b)
    assert manager.count == 2

    manager.disconnect(ws_a)
    assert manager.count == 1

    manager.disconnect(ws_b)
    assert manager.count == 0


# ── Concurrent operations ────────────────────────────────────────────────────


@pytest.mark.unit
async def test_concurrent_connect(manager: ConnectionManager):
    clients = [_make_ws() for _ in range(20)]
    await asyncio.gather(*(manager.connect(ws) for ws in clients))

    assert manager.count == 20


@pytest.mark.unit
async def test_concurrent_connect_and_disconnect(manager: ConnectionManager):
    clients = [_make_ws() for _ in range(20)]
    # Pre-add all clients
    for ws in clients:
        await manager.connect(ws)
    assert manager.count == 20

    # Concurrently disconnect all
    await asyncio.gather(*(asyncio.to_thread(manager.disconnect, ws) for ws in clients))
    assert manager.count == 0


@pytest.mark.unit
async def test_concurrent_broadcast_with_failures(manager: ConnectionManager):
    """Concurrent broadcasts while some connections die should not corrupt state."""
    good_clients = [_make_ws() for _ in range(5)]
    dead_clients = [_make_ws() for _ in range(3)]
    for ws in dead_clients:
        ws.send_json = AsyncMock(side_effect=OSError("broken pipe"))

    all_clients = good_clients + dead_clients
    for ws in all_clients:
        await manager.connect(ws)
    assert manager.count == 8

    # Run two broadcasts back to back
    await manager.broadcast({"round": 1})
    await manager.broadcast({"round": 2})

    # Dead clients should have been pruned
    assert manager.count == 5
    for ws in good_clients:
        assert ws in manager._connections
    for ws in dead_clients:
        assert ws not in manager._connections
