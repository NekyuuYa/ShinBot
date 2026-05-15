from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from shinbot.agent.services.context import ContextManager, PromptMemoryBundle
from shinbot.agent.services.context.state.alias_table import AliasEntry
from shinbot.agent.services.context.state.state_store import (
    CompressedMemoryState,
    ContextBlockState,
)
from shinbot.agent.services.media import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_LLM_REF,
    MediaService,
)
from shinbot.agent.services.prompt_engine import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptProfile,
    PromptRegistry,
    PromptStage,
)
from shinbot.persistence import DatabaseManager, MediaSemanticRecord, MessageLogRecord
from shinbot.schema.elements import Message, MessageElement


def _write_png(path: Path, color: tuple[int, int, int] = (255, 0, 0)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 8), color).save(path)
    return path


def _extract_message_texts(messages: list[dict[str, object]]) -> list[str]:
    texts: list[str] = []
    for message in messages:
        content = message.get("content", [])
        if isinstance(content, list):
            texts.extend(
                str(block.get("text", ""))
                for block in content
                if isinstance(block, dict) and "text" in block
            )
        else:
            texts.append(str(content))
    return texts


def test_prompt_registry_prefers_active_context_pool(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    db.message_logs.insert(
        MessageLogRecord(
            session_id="s-1",
            role="user",
            raw_text="from pool user",
            created_at=1000,
            is_read=True,
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id="s-1",
            role="assistant",
            raw_text="from pool assistant",
            created_at=2000,
            is_read=True,
        )
    )

    registry = PromptRegistry(context_manager=ContextManager(db.message_logs, data_dir=tmp_path))
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id="s-1",
            context_inputs={"history_turns": [{"role": "user", "content": "stale"}]},
        )
    )

    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    texts = _extract_message_texts(context_stage.messages)
    assert any("from pool user" in text for text in texts)
    assert any("from pool assistant" in text for text in texts)
    assert all("stale" not in text for text in texts)


def test_prompt_registry_marks_last_cacheable_context_message(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    session_id = "s-cache-marker"
    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    state = context_manager.get_session_state(session_id)
    state.alias_table.entries = {
        "stable-user": AliasEntry(
            alias="A0",
            platform_id="stable-user",
            display_name="Stable User",
            message_count=4,
            last_seen_ms=1_000,
        ),
        "tail-user": AliasEntry(
            alias="A1",
            platform_id="tail-user",
            display_name="Tail User",
            message_count=2,
            last_seen_ms=1_100,
        ),
    }
    state.alias_table.rebuilt_since_activity = True
    state.alias_table.last_rebuild_ms = 1_200
    state.compressed_memories = [
        CompressedMemoryState(text="compressed memory", created_at_ms=900)
    ]
    state.set_short_term_blocks(
        [
            ContextBlockState(
                block_id="ctx-0",
                sealed=True,
                contents=[{"type": "text", "text": "stable block"}],
            ),
            ContextBlockState(
                block_id="ctx-1",
                sealed=False,
                contents=[{"type": "text", "text": "dynamic tail"}],
            ),
        ]
    )

    registry = PromptRegistry(context_manager=context_manager)
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    disabled = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id=session_id,
        )
    )
    disabled_context_stage = next(
        stage for stage in disabled.stages if stage.stage == PromptStage.CONTEXT
    )
    assert all(
        "cache_control" not in block
        for message in disabled_context_stage.messages
        for block in message.get("content", [])
        if isinstance(block, dict)
    )

    enabled = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id=session_id,
            metadata={"explicit_prompt_cache_enabled": True},
        )
    )
    context_stage = next(stage for stage in enabled.stages if stage.stage == PromptStage.CONTEXT)

    assert [message["content"][0]["text"] for message in context_stage.messages] == [
        "### 压缩记忆\ncompressed memory",
        "stable block",
        "dynamic tail",
    ]
    assert "cache_control" not in context_stage.messages[0]["content"][0]
    assert context_stage.messages[1]["content"][0]["cache_control"] == {"type": "ephemeral"}
    assert "cache_control" not in context_stage.messages[2]["content"][0]


def test_prompt_registry_consumes_context_memory_bundle() -> None:
    class FakeContextManager:
        def __init__(self) -> None:
            self.requests = []

        def build_prompt_memory_bundle(self, request):
            self.requests.append(request)
            return PromptMemoryBundle(
                context_messages=[
                    {"role": "user", "content": [{"type": "text", "text": "memory"}]}
                ],
                instruction_blocks=[{"type": "text", "text": "unread"}],
                constraint_text="alias constraint",
                cacheable_message_count=1,
                metadata={"message_count": 1},
            )

    context_manager = FakeContextManager()
    registry = PromptRegistry(context_manager=context_manager)
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id="s-bundle",
            context_inputs={
                "unread_records": [{"id": 1, "raw_text": "hello"}],
                "previous_summary": "summary",
                "self_user_id": "bot",
            },
            metadata={"now_ms": 1_234_000_000_000, "explicit_prompt_cache_enabled": True},
        )
    )

    assert len(context_manager.requests) == 1
    bundle_request = context_manager.requests[0]
    assert bundle_request.session_id == "s-bundle"
    assert bundle_request.previous_summary == "summary"
    assert bundle_request.self_platform_id == "bot"
    assert bundle_request.now_ms == 1_234_000_000_000

    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    instruction_stage = next(
        stage for stage in result.stages if stage.stage == PromptStage.INSTRUCTIONS
    )
    constraint_stage = next(
        stage for stage in result.stages if stage.stage == PromptStage.CONSTRAINTS
    )

    assert context_stage.messages[0]["content"][0]["cache_control"] == {"type": "ephemeral"}
    assert instruction_stage.components[0].rendered_content_blocks == [
        {"type": "text", "text": "unread"}
    ]
    assert instruction_stage.components[0].metadata["message_count"] == 1
    assert constraint_stage.rendered_text == "alias constraint"


def test_context_manager_exports_only_read_messages(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    msg_id = db.message_logs.insert(
        MessageLogRecord(
            session_id="s-read-filter",
            role="user",
            raw_text="pending unread",
            sender_id="user-1",
            created_at=1000,
            is_read=False,
        )
    )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    assert context_manager.get_context_inputs("s-read-filter")["history_turns"] == []

    db.message_logs.mark_read(msg_id)
    context_manager.mark_read_until("s-read-filter", msg_id)

    turns = context_manager.get_context_inputs("s-read-filter")["history_turns"]
    assert [turn["content"] for turn in turns] == ["pending unread"]


def test_prompt_registry_includes_media_digest_from_context_pool(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "digest.png", color=(10, 20, 30))
    message = Message.from_elements(MessageElement.img(str(image_path)))
    message_log_id = db.message_logs.insert(
        MessageLogRecord(
            session_id="s-media",
            role="user",
            content_json=json.dumps(
                [element.model_dump(mode="json") for element in message.elements],
                ensure_ascii=False,
            ),
            raw_text="",
            created_at=1000,
            is_read=True,
        )
    )
    items = media_service.ingest_message_media(
        session_id="s-media",
        sender_id="user-1",
        platform_msg_id="msg-media-1",
        elements=message.elements,
        message_log_id=message_log_id,
        seen_at=1_000.0,
    )
    db.media_semantics.upsert(
        MediaSemanticRecord(
            raw_hash=items[0].raw_hash,
            kind="meme_image",
            digest="熊猫头无语",
            verified_by_model=True,
            inspection_agent_ref=BUILTIN_MEDIA_INSPECTION_AGENT_REF,
            inspection_llm_ref=BUILTIN_MEDIA_INSPECTION_LLM_REF,
            metadata={},
            first_seen_at=1_000.0,
            last_seen_at=1_000.0,
            expire_at=1_000.0 + 180 * 24 * 60 * 60,
        )
    )

    registry = PromptRegistry(
        context_manager=ContextManager(db.message_logs, media_service=media_service)
    )
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id="s-media",
        )
    )

    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    texts = _extract_message_texts(context_stage.messages)
    assert any("[表情 id:" in text and "摘要:熊猫头无语" in text for text in texts)


def test_prompt_registry_keeps_active_context_pool_intact_during_assembly(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    for idx in range(3):
        db.message_logs.insert(
            MessageLogRecord(
                session_id="s-2",
                role="user" if idx % 2 == 0 else "assistant",
                raw_text=f"turn {idx} " + "word " * 6,
                created_at=1000 + idx,
                is_read=True,
            )
        )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    registry = PromptRegistry(context_manager=context_manager)
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    result = registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id="s-2",
            model_context_window=12,
        )
    )

    context_stage = next(stage for stage in result.stages if stage.stage == PromptStage.CONTEXT)
    texts = _extract_message_texts(context_stage.messages)
    assert context_stage.components[0].metadata == {"session_id": "s-2"}
    assert "resolver_output" not in context_stage.components[0].metadata
    assert len(context_manager.get_recent_messages("s-2")) == 3
    assert context_stage.messages
    assert any("turn 0" in text for text in texts)
    assert any("turn 1" in text for text in texts)
    assert any("turn 2" in text for text in texts)


def test_prompt_registry_syncs_session_policy_for_track_time_ejection(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    db.message_logs.insert(
        MessageLogRecord(
            session_id="s-3",
            role="user",
            raw_text="seed " + "word " * 6,
            created_at=1000,
            is_read=True,
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id="s-3",
            role="assistant",
            raw_text="seed " + "word " * 6,
            created_at=1001,
            is_read=True,
        )
    )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    registry = PromptRegistry(context_manager=context_manager)
    registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            session_id="s-3",
            model_context_window=12,
        )
    )
    before = len(context_manager.get_recent_messages("s-3"))

    context_manager.track_message_record(
        MessageLogRecord(
            session_id="s-3",
            role="user",
            raw_text="new " + "word " * 6,
            created_at=1002,
        )
    )

    after = len(context_manager.get_recent_messages("s-3"))
    assert after <= before
