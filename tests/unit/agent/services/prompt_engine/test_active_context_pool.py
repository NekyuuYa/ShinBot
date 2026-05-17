from __future__ import annotations

from shinbot.agent.services.context.manager import ActiveContextPool


def test_active_context_pool_incremental_tokens_on_append() -> None:
    """Token estimate should update incrementally when appending new turns."""
    pool = ActiveContextPool(session_id="test", max_messages=10)
    pool.load(
        [
            {"role": "user", "raw_text": "alpha beta gamma", "id": 1, "created_at": 1000},
            {"role": "assistant", "raw_text": "delta epsilon", "id": 2, "created_at": 2000},
        ]
    )
    initial_tokens = pool.token_estimate
    assert initial_tokens > 0
    assert len(pool.messages) == 2

    # Append a new message — token count should increase.
    pool.append({"role": "user", "raw_text": "zeta eta theta", "id": 3, "created_at": 3000})
    assert pool.token_estimate > initial_tokens
    assert len(pool.messages) == 3

    after_append_tokens = pool.token_estimate
    assert after_append_tokens == pool.token_estimate
    assert not hasattr(pool, "trim_turns")


def test_active_context_pool_deduplication() -> None:
    """Appending the same message (by id or content) must be a no-op."""
    pool = ActiveContextPool(session_id="test", max_messages=10)
    pool.append({"role": "user", "raw_text": "hello", "id": 1, "created_at": 1000})
    assert len(pool.messages) == 1

    # Same id -> skip.
    pool.append({"role": "user", "raw_text": "hello", "id": 1, "created_at": 1000})
    assert len(pool.messages) == 1

    # Same content with no id -> skip.
    pool.append({"role": "user", "raw_text": "world", "created_at": 2000})
    pool.append({"role": "user", "raw_text": "world", "created_at": 2000})
    assert len(pool.messages) == 2


def test_active_context_pool_export_strips_internal_keys() -> None:
    """export_turns() must not leak _record_id or _created_at."""
    pool = ActiveContextPool(session_id="test", max_messages=10)
    pool.append(
        {"role": "user", "raw_text": "test", "id": 42, "created_at": 1000, "sender_id": "u1"}
    )
    turns = pool.export_turns()
    assert len(turns) == 1
    assert "_record_id" not in turns[0]
    assert "_created_at" not in turns[0]
    assert turns[0]["sender_id"] == "u1"
    assert turns[0]["content"] == "test"


def test_active_context_pool_maxlen_eviction_updates_tokens() -> None:
    """When deque hits max capacity, auto-eviction must update token estimate."""
    pool = ActiveContextPool(session_id="test", max_messages=3)
    for i in range(5):
        pool.append({"role": "user", "raw_text": f"msg {i}", "id": i, "created_at": i * 1000})

    assert len(pool.messages) == 3
    # Tokens should reflect only the last 3 messages, not all 5.
    turns = pool.export_turns()
    assert all(t["content"].startswith("msg ") for t in turns)
    assert turns[0]["content"] == "msg 2"
