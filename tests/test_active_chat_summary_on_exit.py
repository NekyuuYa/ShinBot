"""Tests for active_chat summary saving on exit paths."""

from __future__ import annotations

import pytest

from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
from shinbot.agent.coordinators.active_chat.models import (
    ActiveChatBatch,
    ActiveChatMessageSignal,
)
from shinbot.agent.coordinators.dispatcher import ActiveReplyDispatcher
from shinbot.agent.scheduler import ActiveChatState


class FakeSummaryService:
    """In-memory summary store for testing."""

    def __init__(self) -> None:
        self.saved: list[dict[str, object]] = []

    def save_active_chat_summary(
        self,
        session_id: str,
        source_run_id: str,
        content: str,
        *,
        msg_log_start: int | None = None,
        msg_log_end: int | None = None,
        msg_count: int = 0,
        metadata: dict[str, object] | None = None,
    ) -> int:
        self.saved.append(
            {
                "session_id": session_id,
                "source_run_id": source_run_id,
                "content": content,
                "msg_log_start": msg_log_start,
                "msg_log_end": msg_log_end,
                "msg_count": msg_count,
                "metadata": metadata or {},
            }
        )
        return len(self.saved)

    def get_latest_by_session(self, session_id: str, *, summary_type=None):
        return None

    def list_by_run_id(self, source_run_id: str, *, summary_type=None):
        return []


def make_active_state(
    *,
    interest_value: float = 30.0,
    active_epoch: int = 0,
) -> ActiveChatState:
    return ActiveChatState(
        session_id="bot:group:room",
        interest_value=interest_value,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=active_epoch,
    )


def make_signal(**kwargs) -> ActiveChatMessageSignal:
    values = {
        "session_id": "bot:group:room",
        "message_log_id": 1,
        "sender_id": "user-1",
        "response_profile": "balanced",
        "self_platform_id": "bot-self",
    }
    values.update(kwargs)
    return ActiveChatMessageSignal(**values)


def make_batch(
    *,
    session_id: str = "bot:group:room",
    message_log_ids: list[int] | None = None,
    active_epoch: int = 0,
    conversation_summary: str = "test summary",
    conversation_messages: list[dict] | None = None,
) -> ActiveChatBatch:
    ids = message_log_ids or [1, 2, 3]
    messages = [
        make_signal(message_log_id=mid, session_id=session_id)
        for mid in ids
    ]
    return ActiveChatBatch(
        session_id=session_id,
        messages=messages,
        active_chat_state=make_active_state(active_epoch=active_epoch),
        response_profile="balanced",
        conversation_summary=conversation_summary,
        conversation_messages=conversation_messages or [{"role": "user", "content": "hi"}],
    )


@pytest.mark.unit
class TestStopActiveChatSavesSummary:
    """stop_active_chat() should save a summary before clearing state."""

    async def test_saves_summary_on_stop(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        # Inject conversation summary via trace compactor
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "compacted summary text"
        coordinator.last_batches["bot:group:room"] = make_batch()

        dispatcher.stop_active_chat("bot:group:room")

        assert len(summary_service.saved) == 1
        record = summary_service.saved[0]
        assert record["session_id"] == "bot:group:room"
        assert record["content"] == "compacted summary text"
        assert record["source_run_id"] == "active_chat:bot:group:room:0"
        assert record["msg_log_start"] == 1
        assert record["msg_log_end"] == 3
        assert record["msg_count"] == 3
        assert record["metadata"]["active_epoch"] == 0

    async def test_no_summary_when_empty_text(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        # conversation_summary defaults to ""
        coordinator.last_batches["bot:group:room"] = make_batch(conversation_summary="")

        dispatcher.stop_active_chat("bot:group:room")

        assert len(summary_service.saved) == 0

    async def test_no_summary_when_no_state(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        # stop without ever starting
        dispatcher.stop_active_chat("bot:group:room")

        assert len(summary_service.saved) == 0

    async def test_no_summary_when_no_summary_service(self) -> None:
        coordinator = ActiveChatCoordinator()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=None,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "should not crash"

        dispatcher.stop_active_chat("bot:group:room")
        # No crash, no summary saved

    async def test_summary_saved_before_state_cleared(self) -> None:
        """Verify summary is saved before coordinator clears state."""
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "will be saved"
        coordinator.last_batches["bot:group:room"] = make_batch()

        dispatcher.stop_active_chat("bot:group:room")

        # Summary was saved
        assert len(summary_service.saved) == 1
        # State is now cleared
        assert coordinator.attention_state_for("bot:group:room") is None


@pytest.mark.unit
class TestFlushActiveChatSummaries:
    """flush_active_chat_summaries() should save summaries for all active sessions."""

    async def test_flush_saves_all_sessions(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        for session_id in ["bot:group:a", "bot:group:b", "bot:group:c"]:
            await coordinator.start_active_chat(
                session_id=session_id,
                active_chat_state=make_active_state(),
            )
            state = coordinator.attention_state_for(session_id)
            assert state is not None
            state.conversation_summary = f"summary for {session_id}"
            coordinator.last_batches[session_id] = make_batch(session_id=session_id)

        dispatcher.flush_active_chat_summaries()

        assert len(summary_service.saved) == 3
        saved_sessions = {r["session_id"] for r in summary_service.saved}
        assert saved_sessions == {"bot:group:a", "bot:group:b", "bot:group:c"}

    async def test_flush_skips_sessions_without_content(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        # Session with summary
        await coordinator.start_active_chat(
            session_id="bot:group:with-summary",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:with-summary")
        assert state is not None
        state.conversation_summary = "has content"
        coordinator.last_batches["bot:group:with-summary"] = make_batch(
            session_id="bot:group:with-summary",
        )

        # Session without summary
        await coordinator.start_active_chat(
            session_id="bot:group:no-summary",
            active_chat_state=make_active_state(),
        )
        coordinator.last_batches["bot:group:no-summary"] = make_batch(
            session_id="bot:group:no-summary",
            conversation_summary="",
        )

        dispatcher.flush_active_chat_summaries()

        assert len(summary_service.saved) == 1
        assert summary_service.saved[0]["session_id"] == "bot:group:with-summary"

    async def test_flush_noop_when_no_coordinator(self) -> None:
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=None,
            summary_service=FakeSummaryService(),
        )
        dispatcher.flush_active_chat_summaries()
        # No crash

    async def test_flush_noop_when_no_summary_service(self) -> None:
        coordinator = ActiveChatCoordinator()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=None,
        )
        dispatcher.flush_active_chat_summaries()
        # No crash


@pytest.mark.unit
class TestSummaryPayload:
    """Test the _build_active_chat_summary_payload helper."""

    async def test_payload_source_run_id_format(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(active_epoch=42),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "some text"
        coordinator.last_batches["bot:group:room"] = make_batch(active_epoch=42)

        dispatcher.stop_active_chat("bot:group:room")

        assert len(summary_service.saved) == 1
        assert summary_service.saved[0]["source_run_id"] == "active_chat:bot:group:room:42"

    async def test_payload_metadata_fields(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "text"
        state.conversation_messages = [{"role": "user", "content": "a"}, {"role": "bot", "content": "b"}]
        coordinator.last_batches["bot:group:room"] = make_batch(message_log_ids=[10, 20, 30])

        dispatcher.stop_active_chat("bot:group:room")

        meta = summary_service.saved[0]["metadata"]
        assert meta["active_epoch"] == 0
        assert meta["conversation_message_count"] == 2
        assert meta["range_source"] == "last_batch"
        assert meta["covered_message_log_ids"] == [10, 20, 30]

        assert summary_service.saved[0]["msg_log_start"] == 10
        assert summary_service.saved[0]["msg_log_end"] == 30
        assert summary_service.saved[0]["msg_count"] == 3

    async def test_payload_no_batch_uses_none_range(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "text but no batch"
        # No entry in last_batches

        dispatcher.stop_active_chat("bot:group:room")

        assert len(summary_service.saved) == 1
        assert summary_service.saved[0]["msg_log_start"] is None
        assert summary_service.saved[0]["msg_log_end"] is None
        assert summary_service.saved[0]["msg_count"] == 0


@pytest.mark.unit
class TestSummaryWriteFailure:
    """Write failures must not block exit."""

    async def test_save_failure_does_not_raise(self) -> None:
        class BrokenSummaryService(FakeSummaryService):
            def save_active_chat_summary(self, **kwargs):
                raise RuntimeError("db down")

        coordinator = ActiveChatCoordinator()
        summary_service = BrokenSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "will fail to save"
        coordinator.last_batches["bot:group:room"] = make_batch()

        # Should not raise
        dispatcher.stop_active_chat("bot:group:room")

        # State is still cleared despite save failure
        assert coordinator.attention_state_for("bot:group:room") is None

    async def test_flush_failure_does_not_block_other_sessions(self) -> None:
        call_count = 0

        class PartiallyBrokenSummaryService(FakeSummaryService):
            def save_active_chat_summary(self, **kwargs):
                nonlocal call_count
                call_count += 1
                if kwargs["session_id"] == "bot:group:broken":
                    raise RuntimeError("db down")
                return super().save_active_chat_summary(**kwargs)

        coordinator = ActiveChatCoordinator()
        summary_service = PartiallyBrokenSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        for sid in ["bot:group:broken", "bot:group:ok"]:
            await coordinator.start_active_chat(
                session_id=sid,
                active_chat_state=make_active_state(),
            )
            state = coordinator.attention_state_for(sid)
            assert state is not None
            state.conversation_summary = f"summary for {sid}"
            coordinator.last_batches[sid] = make_batch(session_id=sid)

        dispatcher.flush_active_chat_summaries()

        # Both were attempted
        assert call_count == 2
        # Only the successful one was recorded
        assert len(summary_service.saved) == 1
        assert summary_service.saved[0]["session_id"] == "bot:group:ok"


@pytest.mark.unit
class TestActiveSessionIds:
    """Test the active_session_ids() method on the coordinator."""

    async def test_returns_active_sessions(self) -> None:
        coordinator = ActiveChatCoordinator()
        assert coordinator.active_session_ids() == []

        await coordinator.start_active_chat(
            session_id="bot:group:a",
            active_chat_state=make_active_state(),
        )
        assert coordinator.active_session_ids() == ["bot:group:a"]

        await coordinator.start_active_chat(
            session_id="bot:group:b",
            active_chat_state=make_active_state(),
        )
        assert set(coordinator.active_session_ids()) == {"bot:group:a", "bot:group:b"}

    async def test_excludes_stopped_sessions(self) -> None:
        coordinator = ActiveChatCoordinator()

        await coordinator.start_active_chat(
            session_id="bot:group:a",
            active_chat_state=make_active_state(),
        )
        await coordinator.start_active_chat(
            session_id="bot:group:b",
            active_chat_state=make_active_state(),
        )

        coordinator.stop_active_chat("bot:group:a")
        assert coordinator.active_session_ids() == ["bot:group:b"]


@pytest.mark.unit
class TestActiveChatSummarySnapshot:
    """Test active_chat summary snapshot boundary."""

    async def test_snapshot_collects_summary_and_last_batch_range(self) -> None:
        coordinator = ActiveChatCoordinator()
        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(active_epoch=77),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "snapshot summary"
        state.conversation_messages = [
            {"role": "assistant", "content": "a"},
            {"role": "tool", "content": "{}"},
        ]
        coordinator.last_batches["bot:group:room"] = make_batch(
            message_log_ids=[30, 10, 20],
            active_epoch=77,
        )

        snapshot = coordinator.summary_snapshot_for("bot:group:room")

        assert snapshot is not None
        assert snapshot.session_id == "bot:group:room"
        assert snapshot.active_epoch == 77
        assert snapshot.conversation_summary == "snapshot summary"
        assert snapshot.conversation_message_count == 2
        assert snapshot.message_log_ids == [30, 10, 20]
        assert snapshot.msg_log_start == 10
        assert snapshot.msg_log_end == 30
        assert snapshot.msg_count == 3
        assert snapshot.range_source == "last_batch"

    async def test_snapshot_returns_none_for_inactive_session(self) -> None:
        coordinator = ActiveChatCoordinator()

        assert coordinator.summary_snapshot_for("bot:group:missing") is None


@pytest.mark.unit
class TestShutdownFlushIntegration:
    """Test that shutdown path triggers summary flush."""

    async def test_shutdown_flushes_before_clearing(self) -> None:
        coordinator = ActiveChatCoordinator()
        summary_service = FakeSummaryService()
        dispatcher = ActiveReplyDispatcher(
            active_chat_workflow=coordinator,
            summary_service=summary_service,
        )

        await coordinator.start_active_chat(
            session_id="bot:group:room",
            active_chat_state=make_active_state(),
        )
        state = coordinator.attention_state_for("bot:group:room")
        assert state is not None
        state.conversation_summary = "shutdown save test"
        coordinator.last_batches["bot:group:room"] = make_batch()

        # Flush before shutdown (same order as AgentRuntime.shutdown)
        dispatcher.flush_active_chat_summaries()
        await coordinator.shutdown()

        assert len(summary_service.saved) == 1
        assert summary_service.saved[0]["content"] == "shutdown save test"
