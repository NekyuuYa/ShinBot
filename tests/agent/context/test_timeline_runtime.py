from __future__ import annotations

from shinbot.agent.context import ContextTimelineRuntime, TimelineRun
from shinbot.agent.context.state.state_store import ContextBlockState, ContextSessionState


def test_context_timeline_runtime_reuses_cacheable_prefix_and_rebuilds_tail() -> None:
    class FakeBuilder:
        image_registry = object()

        def build_blocks(
            self,
            records,
            *,
            alias_table,
            projection_state,
            self_platform_id="",
            start_block_index=0,
        ):
            return [
                ContextBlockState(
                    block_id=f"ctx-{start_block_index + index + 1}",
                    contents=[{"type": "text", "text": str(record["raw_text"])}],
                    metadata={"record_ids": [record["id"]]},
                )
                for index, record in enumerate(records)
            ]

        def build_assistant_blocks(
            self,
            records,
            *,
            alias_table,
            projection_state,
            self_platform_id="",
            start_block_index=0,
        ):
            return [
                ContextBlockState(
                    block_id=f"assistant-{start_block_index + index + 1}",
                    kind="assistant",
                    contents=[{"type": "text", "text": str(record["raw_text"])}],
                    metadata={"record_ids": [record["id"]]},
                )
                for index, record in enumerate(records)
            ]

    state = ContextSessionState(session_id="s-timeline")
    state.set_short_term_blocks(
        [
            ContextBlockState(
                block_id="ctx-1",
                sealed=True,
                contents=[{"type": "text", "text": "old stable"}],
                metadata={"record_ids": [1]},
            ),
            ContextBlockState(
                block_id="ctx-2",
                sealed=False,
                contents=[{"type": "text", "text": "old tail"}],
                metadata={"record_ids": [2]},
            ),
        ]
    )

    messages = ContextTimelineRuntime(FakeBuilder()).build_prompt_messages(
        [
            {"id": 1, "role": "user", "raw_text": "old stable"},
            {"id": 2, "role": "user", "raw_text": "new tail"},
            {"id": 3, "role": "assistant", "raw_text": "assistant tail"},
        ],
        alias_table=state.alias_table,
        session_state=state,
    )

    assert [block.block_id for block in state.short_term_blocks()] == [
        "ctx-1",
        "ctx-2",
        "assistant-3",
    ]
    assert [block.sealed for block in state.short_term_blocks()] == [True, True, False]
    assert [message["content"][0]["text"] for message in messages] == [
        "old stable",
        "new tail",
        "assistant tail",
    ]


def test_timeline_run_groups_contiguous_records_by_prompt_role() -> None:
    runs = TimelineRun.from_records(
        [
            {"id": 1, "role": "user"},
            {"id": 2, "role": ""},
            {"id": 3, "role": "assistant"},
            {"id": 4, "role": "assistant"},
            {"id": 5, "role": "tool"},
        ]
    )

    assert [(run.role, [record["id"] for record in run.records]) for run in runs] == [
        ("user", [1, 2]),
        ("assistant", [3, 4]),
        ("user", [5]),
    ]
