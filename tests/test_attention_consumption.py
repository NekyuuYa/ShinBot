from __future__ import annotations

import asyncio
import time

import pytest

from shinbot.agent.attention.engine import AttentionConfig, AttentionEngine
from shinbot.agent.attention.scheduler import AttentionScheduler, AttentionSchedulerConfig
from shinbot.core.state.session import Session, SessionManager
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


def test_scheduler_uses_dedicated_config_for_response_profiles(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    engine_config = AttentionConfig(base_threshold=3.0)
    scheduler_config = AttentionSchedulerConfig(
        semantic_wait_ms=250.0,
        balanced_base_threshold=7.0,
        passive_base_threshold=9.0,
        passive_min_wait_ms=1200.0,
        immediate_base_threshold=1.5,
    )
    scheduler = AttentionScheduler(
        AttentionEngine(engine_config, db.attention),
        db,
        scheduler_config,
    )

    assert scheduler._resolve_response_profile("balanced") == ("balanced", 7.0, 250.0)
    assert scheduler._resolve_response_profile("passive") == ("passive", 9.0, 1200.0)
    assert scheduler._resolve_response_profile("immediate") == ("immediate", 1.5, 0.0)


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


@pytest.mark.asyncio
async def test_direct_dispatch_runs_workflow_without_attention_update(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:private:u1"
    SessionManager(session_repo=db.sessions).update(
        Session(
            id=session_id,
            instance_id="inst",
            session_type="private",
            channel_id="u1",
        )
    )
    msg_id = db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="hello direct",
            sender_id="u1",
            created_at=time.time() * 1000,
        )
    )

    config = AttentionConfig()
    engine = AttentionEngine(config, db.attention)
    observed: dict[str, object] = {}
    dispatched = asyncio.Event()

    async def dispatcher(_session_id, batch, state, profile):
        observed["session_id"] = _session_id
        observed["batch"] = batch
        observed["attention_value"] = state.attention_value
        observed["profile"] = profile
        dispatched.set()

    scheduler = AttentionScheduler(engine, db, config, workflow_dispatcher=dispatcher)

    await scheduler.dispatch_immediately(session_id, response_profile="disabled")
    await asyncio.wait_for(dispatched.wait(), timeout=1.0)

    assert observed["session_id"] == session_id
    assert observed["profile"] == "disabled"
    assert observed["attention_value"] == 0.0
    assert [item["raw_text"] for item in observed["batch"]] == ["hello direct"]
    refreshed = db.attention.get_or_create_attention(session_id)
    assert refreshed.last_consumed_msg_log_id == msg_id


@pytest.mark.asyncio
async def test_scheduler_persists_self_platform_id_in_attention_metadata(tmp_path):
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
            raw_text="@bot hello",
            sender_id="u1",
            created_at=time.time() * 1000,
        )
    )

    config = AttentionConfig(base_threshold=999.0)
    scheduler = AttentionScheduler(AttentionEngine(config, db.attention), db, config)

    await scheduler.on_message(
        session_id,
        msg_id,
        "u1",
        self_platform_id="bot-42",
    )
    await asyncio.sleep(0)

    refreshed = db.attention.get_or_create_attention(session_id)
    assert refreshed.metadata.get("self_platform_id") == "bot-42"
