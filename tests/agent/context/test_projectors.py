from __future__ import annotations

from shinbot.agent.context import (
    AliasContextProjector,
    CompressedMemoryProjector,
    LongTermMemoryItem,
    LongTermMemoryProjector,
)
from shinbot.agent.context.state.alias_table import AliasEntry
from shinbot.agent.context.state.state_store import (
    CompressedMemoryState,
    ContextBlockState,
    ContextSessionState,
)


def test_alias_context_projector_separates_inactive_context_and_active_constraint() -> None:
    projector = AliasContextProjector()
    state = ContextSessionState(session_id="s-alias")
    state.alias_table.entries = {
        "old-user": AliasEntry(
            platform_id="old-user",
            alias="P0",
            display_name="Old User",
        ),
        "active-user": AliasEntry(
            platform_id="active-user",
            alias="P1",
            display_name="Active User",
        ),
        "agent": AliasEntry(
            platform_id="agent",
            alias="A0",
            display_name="Assistant",
        ),
    }
    blocks = [
        ContextBlockState(
            block_id="ctx-1",
            sealed=True,
            metadata={
                "alias_entries": [
                    {"alias": "P0", "platform_id": "old-user", "display_name": "Old User"},
                    {"alias": "P1", "platform_id": "active-user", "display_name": "Active User"},
                ]
            },
        ),
        ContextBlockState(
            block_id="ctx-2",
            sealed=False,
            metadata={
                "alias_entries": [
                    {"alias": "P1", "platform_id": "active-user", "display_name": "Active User"}
                ]
            },
        ),
    ]

    inactive_message = projector.build_inactive_context_message(
        state=state,
        blocks=blocks,
        unread_records=[],
    )
    active_constraint = projector.build_active_constraint_text(
        alias_table=state.alias_table,
        blocks=blocks,
        unread_records=[],
    )

    assert inactive_message is not None
    assert "P0 = Old User / old-user" in inactive_message["content"][0]["text"]
    assert "P1 = Active User / active-user" not in inactive_message["content"][0]["text"]
    assert "A0 = Assistant / agent" in active_constraint
    assert "P1 = Active User / active-user" in active_constraint
    assert "P0 = Old User / old-user" not in active_constraint
    assert state.inactive_alias_table_frozen is True


def test_compressed_memory_projector_builds_messages_and_source_text() -> None:
    projector = CompressedMemoryProjector()
    state = ContextSessionState(session_id="s-compressed")
    state.alias_table.entries = {
        "user-1": AliasEntry(
            platform_id="user-1",
            alias="P0",
            display_name="Alice",
        )
    }
    memories = [
        CompressedMemoryState(text=""),
        CompressedMemoryState(text="older summary"),
    ]
    blocks = [
        ContextBlockState(
            block_id="ctx-1",
            contents=[
                {
                    "type": "text",
                    "text": "[msgid: 0001]P0: hello [@ P0/user-1]",
                }
            ],
        )
    ]

    messages = projector.build_messages(memories)
    source_text = projector.build_source_text(alias_table=state.alias_table, blocks=blocks)

    assert len(messages) == 1
    assert messages[0]["content"][0]["text"] == "### 压缩记忆\nolder summary"
    assert "P0 = Alice / user-1" in source_text
    assert "[msgid: 0001]Alice: hello [@ Alice/user-1]" in source_text


def test_long_term_memory_projector_omits_empty_memories() -> None:
    assert LongTermMemoryProjector().build_messages([LongTermMemoryItem(text="")]) == []
