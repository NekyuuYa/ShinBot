from __future__ import annotations

from shinbot.agent.context import ContextSessionRuntime
from shinbot.agent.context.state.state_store import ContextBlockState


def test_session_runtime_loads_state_once_and_saves_by_session_id(tmp_path) -> None:
    runtime = ContextSessionRuntime.from_data_dir(tmp_path)
    state = runtime.get_state("s-session")
    state.set_short_term_blocks([ContextBlockState(block_id="ctx-1", sealed=False)])

    assert runtime.save("s-session") is True

    loaded_runtime = ContextSessionRuntime.from_data_dir(tmp_path)
    loaded = loaded_runtime.get_state("s-session")

    assert loaded.session_id == "s-session"
    assert loaded.alias_table.session_id == "s-session"
    assert [block.block_id for block in loaded.short_term_blocks()] == ["ctx-1"]


def test_session_runtime_save_returns_false_for_unknown_session(tmp_path) -> None:
    runtime = ContextSessionRuntime.from_data_dir(tmp_path)

    assert runtime.save("missing") is False
