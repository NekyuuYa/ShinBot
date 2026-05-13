from __future__ import annotations

from shinbot.agent.services.context import ContextManager
from shinbot.agent.services.context.state.state_store import ContextBlockState
from shinbot.agent.services.summaries import (
    MarkdownSummaryStore,
    SummaryService,
    SummaryType,
)
from shinbot.persistence import DatabaseManager


def test_context_manager_persists_compressed_context_summary(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    summary_service = SummaryService(
        db.agent_summaries,
        markdown_store=MarkdownSummaryStore(tmp_path / "summary"),
    )
    context_manager = ContextManager(
        db.message_logs,
        data_dir=tmp_path,
        summary_service=summary_service,
    )
    session_id = "bot:group:room"
    state = context_manager.get_session_state(session_id)
    state.set_short_term_blocks(
        [
            ContextBlockState(block_id="ctx-1", sealed=True),
            ContextBlockState(block_id="ctx-2", sealed=False),
        ]
    )

    result = context_manager.apply_usage_eviction(
        session_id,
        {"input_tokens": 100, "output_tokens": 0},
        max_context_tokens=1,
        evict_ratio=1.0,
        compressed_text="middle summary",
        now_ms=1234567890,
    )

    assert result["compressed_added"] is True
    records = summary_service.list_by_session(
        session_id,
        summary_type=SummaryType.COMPRESSED_CONTEXT,
    )
    assert len(records) == 1
    assert records[0].content == "middle summary"
    assert records[0].source_run_id == "context:bot:group:room:1234567890"
    assert '"source_block_ids": ["ctx-1"]' in records[0].metadata_json

    files = list(
        (tmp_path / "summary" / "sessions" / "bot_group_room" / "compressed_context")
        .glob("*.md")
    )
    assert len(files) == 1
    assert "# Compressed Context" in files[0].read_text(encoding="utf-8")
