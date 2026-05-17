"""Tests for dispatch event bus."""

import pytest

from shinbot.core.dispatch.event_bus import EventBus, StopPropagation


class TestEventBus:
    def setup_method(self):
        self.bus = EventBus()

    @pytest.mark.asyncio
    async def test_basic_emit(self):
        results = []

        async def handler(data):
            results.append(data)

        self.bus.on("test", handler)
        await self.bus.emit("test", "hello")
        assert results == ["hello"]

    @pytest.mark.asyncio
    async def test_priority_order(self):
        order = []

        async def high(data):
            order.append("high")

        async def low(data):
            order.append("low")

        self.bus.on("test", low, priority=200)
        self.bus.on("test", high, priority=10)

        await self.bus.emit("test", None)
        assert order == ["high", "low"]

    @pytest.mark.asyncio
    async def test_wildcard_handler(self):
        results = []

        async def handler(data):
            results.append(data)

        self.bus.on("*", handler)
        await self.bus.emit("any-event", "catch-all")
        assert results == ["catch-all"]

    @pytest.mark.asyncio
    async def test_stop_propagation(self):
        order = []

        async def blocker(data):
            order.append("blocker")
            raise StopPropagation()

        async def after(data):
            order.append("should-not-run")

        self.bus.on("test", blocker, priority=10)
        self.bus.on("test", after, priority=100)

        await self.bus.emit("test", None)
        assert order == ["blocker"]

    @pytest.mark.asyncio
    async def test_handler_error_doesnt_crash(self):
        results = []

        async def broken(data):
            raise RuntimeError("oops")

        async def safe(data):
            results.append("ok")

        self.bus.on("test", broken, priority=10)
        self.bus.on("test", safe, priority=100)

        await self.bus.emit("test", None)
        assert results == ["ok"]

    @pytest.mark.asyncio
    async def test_return_values(self):
        async def handler1(data):
            return "a"

        async def handler2(data):
            return "b"

        async def handler_none(data):
            pass

        self.bus.on("test", handler1)
        self.bus.on("test", handler2)
        self.bus.on("test", handler_none)

        results = await self.bus.emit("test", None)
        assert results == ["a", "b"]

    def test_off(self):
        async def handler(data):
            pass

        self.bus.on("test", handler)
        assert self.bus.handler_count("test") == 1
        self.bus.off("test", handler)
        assert self.bus.handler_count("test") == 0

    def test_off_all_by_owner(self):
        async def h1(d):
            pass

        async def h2(d):
            pass

        self.bus.on("a", h1, owner="plugin-1")
        self.bus.on("b", h2, owner="plugin-1")
        self.bus.on("a", h2, owner="plugin-2")

        removed = self.bus.off_all("plugin-1")
        assert removed == 2
        assert self.bus.handler_count("a") == 1  # plugin-2's handler remains

    def test_handler_count(self):
        async def h(d):
            pass

        self.bus.on("a", h)
        self.bus.on("b", h)
        assert self.bus.handler_count() == 2
        assert self.bus.handler_count("a") == 1

    def test_has_handlers_includes_wildcard(self):
        async def h(d):
            pass

        assert self.bus.has_handlers("a") is False

        self.bus.on("*", h)

        assert self.bus.has_handlers("a") is True
        assert self.bus.has_handlers("a", include_wildcard=False) is False
