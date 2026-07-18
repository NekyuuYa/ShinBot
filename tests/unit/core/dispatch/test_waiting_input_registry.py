"""Unit coverage for interactive-input waiter ownership and cleanup."""

from __future__ import annotations

import asyncio

import pytest

from shinbot.core.dispatch.message_context import (
    MessageContext,
    WaitingInputConflict,
    WaitingInputConsumeDisposition,
    WaitingInputFreezeError,
    WaitingInputFrozen,
    WaitingInputRegistry,
    WaitingInputScope,
)
from shinbot.core.state.session import Session
from shinbot.schema.elements import Message
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


def _context(registry: WaitingInputRegistry) -> MessageContext:
    """Build a context whose wait operation never needs an adapter call."""

    event = UnifiedEvent(
        type="message-created",
        self_id="bot-a",
        platform="test",
        user=User(id="user-a"),
        channel=Channel(id="user-a", type=1),
        message=MessagePayload(id="message-a", content="hello"),
    )
    return MessageContext(
        event=event,
        message=Message.from_text("hello"),
        session=Session(
            id="instance-a:private:user-a",
            instance_id="instance-a",
            session_type="private",
        ),
        adapter=object(),  # type: ignore[arg-type]
        permissions=set(),
        waiting_registry=registry,
    )


def _scope(
    *,
    session_id: str = "session-a",
    bot_id: str = "bot-a",
) -> WaitingInputScope:
    """Build a distinct, fully scoped legacy waiter identity."""

    return WaitingInputScope.from_routing_identity(
        legacy_session_id=session_id,
        bot_id=bot_id,
        bot_session_id=f"{bot_id}:{session_id}",
    )


@pytest.mark.asyncio
async def test_registry_rejects_a_second_live_waiter_for_one_session() -> None:
    """A second handler cannot silently replace the first session waiter."""

    registry = WaitingInputRegistry()
    first = registry.register("session-a")

    with pytest.raises(WaitingInputConflict, match="already waits"):
        registry.register("session-a")

    assert registry.is_waiting("session-a")
    registry.cancel("session-a")
    assert first.cancelled()


@pytest.mark.asyncio
async def test_wait_for_input_timeout_cleans_up_its_waiter() -> None:
    """A timed-out prompt cannot consume a later unrelated user message."""

    registry = WaitingInputRegistry()
    context = _context(registry)

    with pytest.raises(TimeoutError):
        await context.wait_for_input(timeout=0.001)

    assert not registry.is_waiting(context.session_id)


@pytest.mark.asyncio
async def test_wait_for_input_cancellation_cleans_up_its_waiter() -> None:
    """Target-task cancellation does not leave a stale fast-path waiter."""

    registry = WaitingInputRegistry()
    context = _context(registry)
    task = asyncio.create_task(context.wait_for_input(timeout=None))
    await asyncio.sleep(0)
    assert registry.is_waiting(context.session_id)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not registry.is_waiting(context.session_id)


@pytest.mark.asyncio
async def test_wait_for_input_resolution_cleans_up_its_waiter() -> None:
    """A normal input reply leaves the session ready for a later prompt."""

    registry = WaitingInputRegistry()
    context = _context(registry)
    task = asyncio.create_task(context.wait_for_input(timeout=None))
    await asyncio.sleep(0)

    scope = registry.open_scope(context.session_id)
    assert scope is not None
    assert (
        registry.try_consume_open(scope, "answer")
        is WaitingInputConsumeDisposition.CONSUMED
    )
    assert await task == "answer"
    assert not registry.is_waiting(context.session_id)


@pytest.mark.asyncio
async def test_context_waiter_uses_its_bot_scoped_identity() -> None:
    """Ingress can only consume a context waiter through the same scope."""

    registry = WaitingInputRegistry()
    context = _context(registry)
    task = asyncio.create_task(context.wait_for_input(timeout=None))
    await asyncio.sleep(0)

    scope = registry.open_scope(context.session_id)
    assert scope is not None
    assert scope.session_key is not None
    assert (
        registry.try_consume_open(scope, "answer")
        is WaitingInputConsumeDisposition.CONSUMED
    )
    assert await task == "answer"


@pytest.mark.asyncio
async def test_freeze_rejects_new_waiters_and_cancels_the_open_future() -> None:
    """A local freeze closes both future registration and legacy consumption."""

    registry = WaitingInputRegistry()
    scope = _scope()
    lease = registry.acquire(scope, track_owner=False)

    ticket = registry.freeze(scope, cutover_id="cutover-a")

    assert lease.future.cancelled()
    assert not registry.is_waiting(scope.legacy_session_id)
    assert (
        registry.try_consume_open(scope, "answer")
        is WaitingInputConsumeDisposition.FROZEN
    )
    with pytest.raises(WaitingInputFrozen, match="frozen"):
        registry.acquire(scope)
    assert registry.release(lease)
    with pytest.raises(WaitingInputFreezeError, match="cannot thaw"):
        registry.thaw(ticket)


@pytest.mark.asyncio
async def test_freeze_awaits_handler_finally_before_confirming_quiescence() -> None:
    """Cancelling a Future is not proof until its handler releases the lease."""

    registry = WaitingInputRegistry()
    scope = _scope()
    entered_wait = asyncio.Event()
    release_finally = asyncio.Event()

    async def handler() -> None:
        lease = registry.acquire(scope)
        entered_wait.set()
        try:
            await lease.future
        finally:
            await release_finally.wait()
            registry.release(lease)

    handler_task = asyncio.create_task(handler())
    await entered_wait.wait()
    ticket = registry.freeze(scope, cutover_id="cutover-a")
    receipt_task = asyncio.create_task(registry.await_quiescent(ticket, timeout=0.5))
    await asyncio.sleep(0)

    assert not receipt_task.done()
    release_finally.set()
    receipt = await receipt_task

    assert receipt.quiescent
    with pytest.raises(asyncio.CancelledError):
        await handler_task
    assert registry.thaw(ticket)


@pytest.mark.asyncio
async def test_freeze_reports_timeout_when_handler_does_not_leave_finally() -> None:
    """Lifecycle control receives a negative receipt instead of a false proof."""

    registry = WaitingInputRegistry()
    scope = _scope()
    entered_wait = asyncio.Event()
    release_finally = asyncio.Event()

    async def handler() -> None:
        lease = registry.acquire(scope)
        entered_wait.set()
        try:
            await lease.future
        finally:
            await release_finally.wait()
            registry.release(lease)

    handler_task = asyncio.create_task(handler())
    await entered_wait.wait()
    ticket = registry.freeze(scope, cutover_id="cutover-a")

    receipt = await registry.await_quiescent(ticket, timeout=0.001)

    assert not receipt.quiescent
    assert receipt.reason == "lease_release_timeout"
    release_finally.set()
    with pytest.raises(asyncio.CancelledError):
        await handler_task
    assert registry.thaw(ticket)


@pytest.mark.asyncio
async def test_compatibility_waiter_cannot_establish_quiescence_proof() -> None:
    """A bare compatibility Future has no handler ownership evidence."""

    registry = WaitingInputRegistry()
    scope = _scope()
    future = registry.register(scope.legacy_session_id)
    ticket = registry.freeze(
        WaitingInputScope(scope.legacy_session_id),
        cutover_id="cutover-a",
    )

    assert future.cancelled()
    receipt = await registry.await_quiescent(ticket, timeout=0.5)

    assert not receipt.quiescent
    assert receipt.reason == "unmanaged_waiter"
    with pytest.raises(WaitingInputFreezeError, match="cannot thaw"):
        registry.thaw(ticket)


@pytest.mark.asyncio
async def test_callback_waiter_without_owner_task_cannot_establish_quiescence() -> None:
    """A loop callback has no handler task to prove stopped after a freeze."""

    registry = WaitingInputRegistry()
    scope = _scope()
    acquired = asyncio.Event()
    leases = []

    def acquire_from_callback() -> None:
        leases.append(registry.acquire(scope))
        acquired.set()

    asyncio.get_running_loop().call_soon(acquire_from_callback)
    await acquired.wait()
    lease = leases[0]
    assert not lease.managed

    ticket = registry.freeze(scope, cutover_id="cutover-a")
    assert registry.release(lease)
    receipt = await registry.await_quiescent(ticket, timeout=0.5)

    assert not receipt.quiescent
    assert receipt.reason == "unmanaged_waiter"
    with pytest.raises(WaitingInputFreezeError, match="cannot thaw"):
        registry.thaw(ticket)


@pytest.mark.asyncio
async def test_scope_mismatch_does_not_consume_another_bot_waiter() -> None:
    """A shared base session cannot cross-deliver input between bot profiles."""

    registry = WaitingInputRegistry()
    owner_scope = _scope(bot_id="bot-a")
    other_scope = _scope(bot_id="bot-b")
    lease = registry.acquire(owner_scope, track_owner=False)

    disposition = registry.try_consume_open(other_scope, "wrong bot")

    assert disposition is WaitingInputConsumeDisposition.SCOPE_MISMATCH
    assert not lease.future.done()
    assert registry.is_waiting(owner_scope.legacy_session_id)
    assert registry.release(lease)


@pytest.mark.asyncio
async def test_cancel_by_base_session_does_not_cancel_a_scoped_waiter() -> None:
    """Compatibility cleanup cannot bypass a scoped handler's token ownership."""

    registry = WaitingInputRegistry()
    scope = _scope()
    lease = registry.acquire(scope, track_owner=False)

    registry.cancel(scope.legacy_session_id)

    assert not lease.future.done()
    assert registry.is_waiting(scope.legacy_session_id)
    assert registry.release(lease)


@pytest.mark.asyncio
async def test_finished_waiter_future_is_not_consumed_again() -> None:
    """A cancellation before handler cleanup cannot claim a later message."""

    registry = WaitingInputRegistry()
    scope = _scope()
    lease = registry.acquire(scope, track_owner=False)
    lease.future.cancel()

    disposition = registry.try_consume_open(scope, "late answer")

    assert disposition is WaitingInputConsumeDisposition.ABSENT
    assert not registry.is_waiting(scope.legacy_session_id)
    assert registry.release(lease)


@pytest.mark.asyncio
async def test_freeze_is_a_base_session_barrier_across_bot_scopes() -> None:
    """A local legacy drain blocks every scope sharing its session lock."""

    registry = WaitingInputRegistry()
    owner_scope = _scope(bot_id="bot-a")
    other_scope = _scope(bot_id="bot-b")
    ticket = registry.freeze(owner_scope, cutover_id="cutover-a")

    with pytest.raises(WaitingInputFrozen, match="frozen"):
        registry.acquire(other_scope)
    assert (
        registry.try_consume_open(other_scope, "answer")
        is WaitingInputConsumeDisposition.FROZEN
    )
    assert registry.thaw(ticket)


@pytest.mark.asyncio
async def test_owner_task_cannot_wait_for_its_own_quiescence() -> None:
    """Self-drain returns a negative receipt instead of waiting forever."""

    registry = WaitingInputRegistry()
    scope = _scope()
    lease = registry.acquire(scope)
    ticket = registry.freeze(scope, cutover_id="cutover-a")

    receipt = await asyncio.wait_for(registry.await_quiescent(ticket, timeout=None), 0.5)

    assert not receipt.quiescent
    assert receipt.reason == "owner_task_is_current"
    assert registry.release(lease)
    with pytest.raises(WaitingInputFreezeError, match="cannot thaw"):
        registry.thaw(ticket)


@pytest.mark.asyncio
async def test_thaw_rejects_a_released_lease_until_its_handler_has_exited() -> None:
    """Lease release alone does not reopen a slot while handler cleanup runs."""

    registry = WaitingInputRegistry()
    scope = _scope()
    entered_wait = asyncio.Event()
    released_lease = asyncio.Event()
    finish_handler = asyncio.Event()

    async def handler() -> None:
        lease = registry.acquire(scope)
        entered_wait.set()
        try:
            await lease.future
        finally:
            registry.release(lease)
            released_lease.set()
            await finish_handler.wait()

    handler_task = asyncio.create_task(handler())
    await entered_wait.wait()
    ticket = registry.freeze(scope, cutover_id="cutover-a")
    await released_lease.wait()

    with pytest.raises(WaitingInputFreezeError, match="cannot thaw"):
        registry.thaw(ticket)
    finish_handler.set()
    with pytest.raises(asyncio.CancelledError):
        await handler_task
    assert registry.thaw(ticket)


@pytest.mark.asyncio
async def test_quiescence_zero_timeout_polls_completed_handler_state() -> None:
    """A completed drain is observable immediately without a scheduling turn."""

    registry = WaitingInputRegistry()
    scope = _scope()
    entered_wait = asyncio.Event()

    async def handler() -> None:
        lease = registry.acquire(scope)
        entered_wait.set()
        try:
            await lease.future
        finally:
            registry.release(lease)

    handler_task = asyncio.create_task(handler())
    await entered_wait.wait()
    ticket = registry.freeze(scope, cutover_id="cutover-a")
    with pytest.raises(asyncio.CancelledError):
        await handler_task

    receipt = await registry.await_quiescent(ticket, timeout=0)

    assert receipt.quiescent
    assert registry.thaw(ticket)


@pytest.mark.asyncio
async def test_compatibility_waiter_requires_explicit_compatibility_resolve() -> None:
    """An unscoped legacy Future cannot be consumed by scoped ingress work."""

    registry = WaitingInputRegistry()
    scope = _scope()
    future = registry.register(scope.legacy_session_id)

    assert (
        registry.try_consume_open(scope, "wrong delivery")
        is WaitingInputConsumeDisposition.SCOPE_MISMATCH
    )
    assert not future.done()
    assert registry.resolve(scope.legacy_session_id, "legacy answer")
    assert future.result() == "legacy answer"
