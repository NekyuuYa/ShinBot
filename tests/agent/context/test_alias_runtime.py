from __future__ import annotations

from typing import Any

from shinbot.agent.context import ContextAliasRuntime
from shinbot.agent.context.state.alias_table import AliasEntry, SessionAliasTable
from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState


class FakeIdentityStore:
    def __init__(self, identities: dict[str, dict[str, Any]]) -> None:
        self.identities = identities

    def get_identity(self, user_id: str, *, platform: str = "") -> dict[str, Any] | None:
        return self.identities.get(user_id)


def test_alias_runtime_skips_rebuild_when_table_is_fresh() -> None:
    runtime = ContextAliasRuntime()
    table = SessionAliasTable(session_id="s-alias")
    table.entries = {
        "user-1": AliasEntry(
            platform_id="user-1",
            alias="A0",
            display_name="Alice",
        )
    }
    table.mark_rebuilt(1_000)

    rebuilt_table, changed = runtime.rebuild_table(
        table,
        [{"sender_id": "user-1", "sender_name": "Renamed", "role": "user"}],
        now_ms=1_001,
    )

    assert rebuilt_table is table
    assert changed is False
    entry = table.resolve("user-1")
    assert entry is not None
    assert entry.display_name == "Alice"
    assert table.last_rebuild_ms == 1_000


def test_alias_runtime_syncs_active_identity_display_name() -> None:
    runtime = ContextAliasRuntime()
    table = SessionAliasTable(session_id="s-alias")
    table.entries = {
        "user-1": AliasEntry(
            platform_id="user-1",
            alias="A0",
            display_name="Old Name",
            last_seen_ms=1_000,
        )
    }
    identity_store = FakeIdentityStore({"user-1": {"name": "Alice"}})

    changed = runtime.sync_identity_display_name(
        table,
        identity_store,
        user_id="user-1",
        now_ms=1_001,
    )

    assert changed is True
    entry = table.resolve("user-1")
    assert entry is not None
    assert entry.display_name == "Alice"


def test_alias_runtime_reports_inactive_projection_state_change() -> None:
    runtime = ContextAliasRuntime()
    state = ContextSessionState(session_id="s-alias")
    blocks = [
        ContextBlockState(
            block_id="ctx-1",
            sealed=True,
            metadata={
                "alias_entries": [
                    {"alias": "P0", "platform_id": "user-1", "display_name": "Alice"}
                ]
            },
        )
    ]

    message, changed = runtime.build_inactive_context_message(
        state=state,
        blocks=blocks,
    )

    assert message is not None
    assert changed is True
    assert state.inactive_alias_table_frozen is True
