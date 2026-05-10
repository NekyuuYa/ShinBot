"""Unit tests for the unified summaries service."""

from __future__ import annotations

import pytest

from shinbot.agent.services.summaries import (
    SummaryService,
    SummaryType,
    SummaryWriteRequest,
)
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


def _insert_message(
    db: DatabaseManager,
    *,
    session_id: str = "bot:group:room",
    msg_id: str = "msg-1",
    created_at: float = 10_000.0,
) -> int:
    return db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id=msg_id,
            sender_id="user-1",
            sender_name="User",
            raw_text="hello",
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


@pytest.fixture
def svc(tmp_path) -> SummaryService:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    return SummaryService(db.agent_summaries)


@pytest.fixture
def db(tmp_path) -> DatabaseManager:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    return db


# -- write & read by session --


def test_save_and_list_by_session(svc: SummaryService) -> None:
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.OVERFLOW_COMPRESSION,
        content="compressed context",
        source_run_id="run-1",
        msg_count=42,
    ))
    records = svc.list_by_session("s1")
    assert len(records) == 1
    assert records[0].content == "compressed context"
    assert records[0].summary_type == SummaryType.OVERFLOW_COMPRESSION
    assert records[0].msg_count == 42


def test_list_by_session_filters_by_type(svc: SummaryService) -> None:
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.OVERFLOW_COMPRESSION,
        content="overflow",
        source_run_id="run-1",
    ))
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="digest",
        source_run_id="run-1",
        block_index=0,
    ))
    assert len(svc.list_by_session("s1")) == 2
    assert len(svc.list_by_session("s1", summary_type=SummaryType.OVERFLOW_COMPRESSION)) == 1
    assert len(svc.list_by_session("s1", summary_type=SummaryType.BLOCK_DIGEST)) == 1


def test_list_by_session_respects_limit(svc: SummaryService) -> None:
    for i in range(5):
        svc.save(SummaryWriteRequest(
            session_id="s1",
            summary_type=SummaryType.BLOCK_DIGEST,
            content=f"block {i}",
            source_run_id="run-1",
            block_index=i,
        ))
    assert len(svc.list_by_session("s1", limit=3)) == 3


# -- query by run_id --


def test_list_by_run_id(svc: SummaryService) -> None:
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.OVERFLOW_COMPRESSION,
        content="overflow",
        source_run_id="run-1",
    ))
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="block-0",
        source_run_id="run-1",
        block_index=0,
    ))
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="block-1",
        source_run_id="run-2",
        block_index=0,
    ))
    records = svc.list_by_run_id("run-1")
    assert len(records) == 2
    assert all(r.source_run_id == "run-1" for r in records)


def test_list_by_run_id_with_type_filter(svc: SummaryService) -> None:
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.OVERFLOW_COMPRESSION,
        content="overflow",
        source_run_id="run-1",
    ))
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="block-0",
        source_run_id="run-1",
        block_index=0,
    ))
    records = svc.list_by_run_id("run-1", summary_type=SummaryType.BLOCK_DIGEST)
    assert len(records) == 1
    assert records[0].block_index == 0


# -- query by run_id + block_index --


def test_get_block_digest(svc: SummaryService) -> None:
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="block-0",
        source_run_id="run-1",
        block_index=0,
    ))
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="block-1",
        source_run_id="run-1",
        block_index=1,
    ))
    record = svc.get_block_digest("run-1", 1)
    assert record is not None
    assert record.content == "block-1"
    assert record.block_index == 1


def test_get_block_digest_not_found(svc: SummaryService) -> None:
    assert svc.get_block_digest("run-999", 0) is None


# -- query by message range --


def test_list_by_message_range(db: DatabaseManager) -> None:
    svc = SummaryService(db.agent_summaries)
    msg1 = _insert_message(db, msg_id="m1", created_at=100.0)
    msg2 = _insert_message(db, msg_id="m2", created_at=200.0)
    msg3 = _insert_message(db, msg_id="m3", created_at=300.0)
    msg4 = _insert_message(db, msg_id="m4", created_at=400.0)

    svc.save(SummaryWriteRequest(
        session_id="bot:group:room",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="covers 1-2",
        source_run_id="run-1",
        block_index=0,
        msg_log_start=msg1,
        msg_log_end=msg2,
        msg_count=2,
    ))
    svc.save(SummaryWriteRequest(
        session_id="bot:group:room",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="covers 3-4",
        source_run_id="run-1",
        block_index=1,
        msg_log_start=msg3,
        msg_log_end=msg4,
        msg_count=2,
    ))

    # Range [1, 2] overlaps only the first block
    records = svc.list_by_message_range(
        "bot:group:room",
        msg_log_start=msg1,
        msg_log_end=msg2,
    )
    assert len(records) == 1
    assert records[0].content == "covers 1-2"

    # Range [2, 3] overlaps both blocks
    records = svc.list_by_message_range(
        "bot:group:room",
        msg_log_start=msg2,
        msg_log_end=msg3,
    )
    assert len(records) == 2

    # Range [4, 5] overlaps only the second block
    records = svc.list_by_message_range(
        "bot:group:room",
        msg_log_start=msg4,
        msg_log_end=msg4 + 100,
    )
    assert len(records) == 1
    assert records[0].content == "covers 3-4"


def test_list_by_message_range_ignores_null_range(svc: SummaryService) -> None:
    """Summaries without msg_log_start/end should not appear in range queries."""
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.ACTIVE_CHAT,
        content="no range",
        source_run_id="run-1",
    ))
    records = svc.list_by_message_range(
        "s1",
        msg_log_start=0,
        msg_log_end=9999,
    )
    assert len(records) == 0


# -- convenience helpers --


def test_save_overflow_compression(svc: SummaryService) -> None:
    rid = svc.save_overflow_compression(
        session_id="s1",
        source_run_id="run-1",
        content="compressed",
        msg_count=100,
    )
    assert rid > 0
    records = svc.list_by_session("s1", summary_type=SummaryType.OVERFLOW_COMPRESSION)
    assert len(records) == 1
    assert records[0].content == "compressed"


def test_save_block_digest(svc: SummaryService) -> None:
    rid = svc.save_block_digest(
        session_id="s1",
        source_run_id="run-1",
        block_index=2,
        content="digest of block 2",
    )
    assert rid > 0
    record = svc.get_block_digest("run-1", 2)
    assert record is not None
    assert record.content == "digest of block 2"


def test_save_active_chat_summary(svc: SummaryService) -> None:
    rid = svc.save_active_chat_summary(
        session_id="s1",
        source_run_id="run-1",
        content="active chat summary",
        metadata={"duration_seconds": 120},
    )
    assert rid > 0
    records = svc.list_by_session("s1", summary_type=SummaryType.ACTIVE_CHAT)
    assert len(records) == 1
    assert records[0].content == "active chat summary"


# -- metadata --


def test_metadata_round_trip(svc: SummaryService) -> None:
    svc.save(SummaryWriteRequest(
        session_id="s1",
        summary_type=SummaryType.BLOCK_DIGEST,
        content="test",
        source_run_id="run-1",
        block_index=0,
        metadata={"key": "value", "count": 42},
    ))
    records = svc.list_by_session("s1")
    assert records[0].block_index == 0
    assert records[0].metadata_json == '{"key": "value", "count": 42, "block_index": 0}'


# -- empty results --


def test_list_by_session_empty(svc: SummaryService) -> None:
    assert svc.list_by_session("nonexistent") == []


def test_list_by_run_id_empty(svc: SummaryService) -> None:
    assert svc.list_by_run_id("nonexistent") == []
