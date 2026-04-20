from __future__ import annotations

import time

import pytest

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.scheduler import AttentionScheduler
from shinbot.core.state.session import Session, SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


@pytest.mark.asyncio
async def test_dispatch_consumes_trigger_attention_before_workflow(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:g1"
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id="inst",
            session_type="group",
            channel_id="g1",
        )
    )
    msg_id = db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="hello",
            sender_id="u1",
            created_at=time.time() * 1000,
        )
    )
    config = AttentionConfig()
    engine = AttentionEngine(config, db.attention)
    state = db.attention.get_or_create_attention(session_id)
    state.attention_value = 12.0
    db.attention.save_attention(state)
    observed: dict[str, float] = {}

    async def dispatcher(_session_id, _batch, _state, _profile):
        refreshed = db.attention.get_or_create_attention(session_id)
        observed["attention_value"] = refreshed.attention_value

    scheduler = AttentionScheduler(engine, db, config, workflow_dispatcher=dispatcher)

    await scheduler._do_dispatch(session_id, "balanced")

    assert observed["attention_value"] < config.base_threshold
    refreshed = db.attention.get_or_create_attention(session_id)
    assert refreshed.last_consumed_msg_log_id == msg_id


def test_incremental_consumption_caps_attention_under_threshold(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:g1"
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id="inst",
            session_type="group",
            channel_id="g1",
        )
    )
    msg_id = db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="new message",
            sender_id="u1",
            created_at=time.time() * 1000,
        )
    )
    state = db.attention.get_or_create_attention(session_id)
    state.attention_value = 9.0
    db.attention.save_attention(state)

    db.attention.update_consumed_cursor_and_cap_attention(session_id, msg_id, 5.0)

    refreshed = db.attention.get_or_create_attention(session_id)
    assert refreshed.last_consumed_msg_log_id == msg_id
    assert refreshed.attention_value < 5.0
