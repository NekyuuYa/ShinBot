from __future__ import annotations

import json
import time

import pytest

from shinbot.agent.context import ContextManager, ContextStageBuildConfig, ContextStageBuilder
from shinbot.agent.context.state.alias_table import ALIAS_ACTIVE_WINDOW_MS, ALIAS_REBUILD_IDLE_MS
from shinbot.agent.identity import (
    IdentityStore,
    register_identity_prompt_components,
    register_identity_tools,
)
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.prompt_manager.schema import (
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptProfile,
    PromptStage,
)
from shinbot.agent.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.core.security.permission import PermissionEngine
from shinbot.persistence import DatabaseManager, MessageLogRecord
from shinbot.schema.elements import MessageElement


@pytest.mark.asyncio
async def test_identity_set_nickname_tool_locks_prompt_name(tmp_path):
    identities_path = tmp_path / "identities.json"
    store = IdentityStore(identities_path)
    store.ensure_user(
        user_id="987654321",
        suggested_name="超长平台默认昵称",
        platform="qq",
    )

    registry = ToolRegistry()
    register_identity_tools(registry, store)
    manager = ToolManager(registry, permission_engine=PermissionEngine())

    result = await manager.execute(
        ToolCallRequest(
            tool_name="identity.set_nickname",
            arguments={
                "user_id": "987654321",
                "nickname": "咖啡",
                "aliases": ["咖啡猫"],
                "reason": "默认昵称太长",
            },
            caller="attention.workflow_runner",
            instance_id="inst",
            session_id="inst:group:g1",
        )
    )

    assert result.success is True
    assert result.output["nickname"] == "咖啡"
    assert result.output["locked"] is True

    store.ensure_user(
        user_id="987654321",
        suggested_name="又一个平台默认昵称",
        platform="qq",
    )
    payload = json.loads(identities_path.read_text(encoding="utf-8"))
    assert payload["users"][0]["name"] == "咖啡"
    assert payload["users"][0]["locked"] is True

    prompt_registry = PromptRegistry(identity_store=store)
    register_identity_prompt_components(
        prompt_registry,
        resolver=prompt_registry.resolve_builtin_identity_map_prompt,
    )
    prompt_registry.register_component(
        PromptComponent(
            id="system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system",
        )
    )
    prompt_registry.register_profile(PromptProfile(id="agent.default", base_components=["system"]))

    assembled = prompt_registry.assemble(
        PromptAssemblyRequest(
            profile_id="agent.default",
            context_inputs={
                "platform": "qq",
                "history_turns": [
                    {
                        "role": "user",
                        "content": "叫我短一点",
                        "sender_id": "987654321",
                        "sender_name": "又一个平台默认昵称",
                        "platform": "qq",
                    }
                ],
            },
        )
    )

    final_texts = [str(block.get("text", "")) for block in assembled.messages[-1]["content"]]
    identity_block = next(text for text in final_texts if "参与者身份参考" in text)
    assert "ID: 987654321 -> 昵称: 咖啡" in identity_block
    assert "别名: 咖啡猫" in identity_block


def test_identity_set_nickname_tool_exports_with_attention_tools(tmp_path):
    registry = ToolRegistry()
    register_identity_tools(registry, IdentityStore(tmp_path / "identities.json"))
    manager = ToolManager(registry, permission_engine=PermissionEngine())

    tools = manager.export_model_tools(
        caller="attention.workflow_runner",
        instance_id="inst",
        session_id="inst:group:g1",
        tags={"attention"},
    )

    names = {item["function"]["name"] for item in tools}
    assert "identity.set_nickname" in names


@pytest.mark.asyncio
async def test_identity_set_nickname_updates_hot_alias_entry_immediately(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    store = IdentityStore(tmp_path / "identities.json")
    context_manager = ContextManager(db.message_logs, data_dir=tmp_path, identity_store=store)
    session_id = "inst:group:g1"
    now_ms = int(time.time() * 1000)

    context_manager.track_message_record(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            created_at=now_ms,
            sender_id="987654321",
            sender_name="超长平台默认昵称",
            raw_text="hello",
            is_read=True,
        ),
        platform="qq",
    )

    before_text = context_manager.build_active_alias_constraint_text(session_id, now_ms=now_ms)
    assert "超长平台默认昵称" in before_text

    registry = ToolRegistry()
    register_identity_tools(registry, store, context_manager)
    manager = ToolManager(registry, permission_engine=PermissionEngine())

    result = await manager.execute(
        ToolCallRequest(
            tool_name="identity.set_nickname",
            arguments={
                "user_id": "987654321",
                "nickname": "咖啡",
            },
            caller="attention.workflow_runner",
            instance_id="inst",
            session_id=session_id,
        )
    )

    assert result.success is True
    assert result.output["cache_status"] == "immediate"
    entry = context_manager.get_alias_table(session_id).resolve("987654321")
    assert entry is not None
    assert entry.display_name == "咖啡"

    after_text = context_manager.build_active_alias_constraint_text(session_id, now_ms=now_ms + 1)
    assert "咖啡" in after_text


def test_identity_display_name_updates_cold_alias_entry_on_next_rebuild(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    store = IdentityStore(tmp_path / "identities.json")
    context_manager = ContextManager(db.message_logs, data_dir=tmp_path, identity_store=store)
    session_id = "inst:group:g1"
    created_at = 1_000.0
    created_at_ms = int(created_at * 1000)
    rebuild_ms = created_at_ms + ALIAS_ACTIVE_WINDOW_MS + ALIAS_REBUILD_IDLE_MS + 1

    context_manager.track_message_record(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            created_at=created_at,
            sender_id="cold-user",
            sender_name="旧昵称",
            raw_text="hello",
            is_read=True,
        ),
        platform="qq",
    )

    context_manager.rebuild_alias_table(session_id, now_ms=rebuild_ms, force=True)
    before_entry = context_manager.get_alias_table(session_id).resolve("cold-user")
    assert before_entry is not None
    assert before_entry.display_name == "旧昵称"

    store.set_nickname(
        user_id="cold-user",
        nickname="新昵称",
        platform="qq",
        locked=True,
    )
    changed = context_manager.sync_identity_display_name(
        session_id,
        user_id="cold-user",
        now_ms=rebuild_ms,
    )

    assert changed is False
    entry = context_manager.get_alias_table(session_id).resolve("cold-user")
    assert entry is not None
    assert entry.display_name == "旧昵称"

    context_manager.rebuild_alias_table(session_id, now_ms=rebuild_ms, force=True)
    after_entry = context_manager.get_alias_table(session_id).resolve("cold-user")
    assert after_entry is not None
    assert after_entry.display_name == "新昵称"


def test_inactive_aliases_are_sourced_from_sealed_context_blocks(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:sealed-alias"

    for index in range(12):
        db.message_logs.insert(
            MessageLogRecord(
                session_id=session_id,
                role="user",
                raw_text=f"message-{index} " + ("x" * 24),
                sender_id=f"user-{index}",
                sender_name=f"User {index}",
                created_at=1_000 + index,
                is_read=True,
            )
        )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    context_manager._context_builder = ContextStageBuilder(
        config=ContextStageBuildConfig(min_tokens=1, max_tokens=1)
    )

    context_messages = context_manager.build_context_stage_messages(session_id, now_ms=2_000)
    state = context_manager.get_session_state(session_id)
    alias_table = context_manager.get_alias_table(session_id)

    assert len(state.short_term_blocks()) >= 2
    assert any(block.sealed for block in state.short_term_blocks())
    assert all("会话历史成员映射" not in str(message.get("content")) for message in context_messages)

    first_low_activity_alias = alias_table.resolve("user-0")
    second_low_activity_alias = alias_table.resolve("user-1")
    assert first_low_activity_alias is not None
    assert second_low_activity_alias is not None
    assert first_low_activity_alias.alias.startswith("P")
    assert second_low_activity_alias.alias.startswith("P")

    sealed_alias_entries = state.short_term_blocks()[0].metadata.get("alias_entries", [])
    assert isinstance(sealed_alias_entries, list)
    assert sealed_alias_entries

    inactive_message = context_manager.build_inactive_alias_context_message(session_id, now_ms=2_000)
    active_text = context_manager.build_active_alias_constraint_text(session_id, now_ms=2_000)

    assert inactive_message is not None
    inactive_text = inactive_message["content"][0]["text"]
    assert first_low_activity_alias.alias in inactive_text
    assert second_low_activity_alias.alias in inactive_text
    assert first_low_activity_alias.platform_id in inactive_text
    assert second_low_activity_alias.platform_id in inactive_text
    assert first_low_activity_alias.alias not in active_text
    assert second_low_activity_alias.alias not in active_text


def test_inactive_alias_table_is_frozen_after_first_render(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:inactive-freeze"

    for index in range(12):
        db.message_logs.insert(
            MessageLogRecord(
                session_id=session_id,
                role="user",
                raw_text=f"message-{index} " + ("x" * 24),
                sender_id=f"user-{index}",
                sender_name=f"User {index}",
                created_at=1_000 + index,
                is_read=True,
            )
        )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    context_manager._context_builder = ContextStageBuilder(
        config=ContextStageBuildConfig(min_tokens=1, max_tokens=1)
    )

    context_manager.build_context_stage_messages(session_id, now_ms=2_000)
    first_message = context_manager.build_inactive_alias_context_message(session_id, now_ms=2_000)
    state = context_manager.get_session_state(session_id)

    assert first_message is not None
    assert state.inactive_alias_table_frozen is True
    first_text = first_message["content"][0]["text"]

    frozen_message = context_manager.build_inactive_alias_context_message(
        session_id,
        unread_records=[
            {
                "sender_id": "user-0",
                "sender_name": "User 0",
                "role": "user",
                "raw_text": "I am back",
                "created_at": 3_000,
            }
        ],
        now_ms=3_000,
    )

    assert frozen_message is not None
    assert frozen_message["content"][0]["text"] == first_text
    assert "user-0" in first_text


def test_context_manager_requests_alias_rebuild_after_eviction(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:evict"
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="first",
            sender_id="user-1",
            sender_name="Alpha",
            created_at=1_000,
            is_read=True,
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="second",
            sender_id="user-2",
            sender_name="Beta",
            created_at=2_000,
            is_read=True,
        )
    )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    context_manager.build_context_stage_messages(session_id, now_ms=3_000)
    state = context_manager.get_session_state(session_id)
    state.inactive_alias_entries = [
        {"alias": "P0", "platform_id": "user-2", "display_name": "Beta"}
    ]
    state.inactive_alias_table_frozen = True

    alias_table = context_manager.get_alias_table(session_id)
    assert alias_table.last_rebuild_ms == 3_000
    assert alias_table.should_rebuild(3_001) is False

    result = context_manager.apply_usage_eviction(
        session_id,
        {"input_tokens": 50_000, "output_tokens": 0},
        max_context_tokens=1,
        evict_ratio=1.0,
        now_ms=3_100,
    )

    assert result["triggered"] is True
    assert context_manager.get_alias_table(session_id).pending_rebuild is True
    assert state.inactive_alias_entries == []
    assert state.inactive_alias_table_frozen is False

    rebuilt_text = context_manager.build_active_alias_constraint_text(session_id, now_ms=3_101)

    assert "当前活跃成员映射" in rebuilt_text
    assert context_manager.get_alias_table(session_id).last_rebuild_ms == 3_101
    assert context_manager.get_alias_table(session_id).pending_rebuild is False


def test_context_manager_does_not_allocate_aliases_to_bot_self_messages(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:self-alias"
    self_platform_id = "3575371140"

    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="[@你] hello",
            sender_id="1917419834",
            sender_name="Ginkoro",
            created_at=1_000,
            is_read=True,
            content_json=json.dumps(
                [
                    MessageElement.at(id=self_platform_id).model_dump(mode="json"),
                    MessageElement.text(" hello").model_dump(mode="json"),
                ],
                ensure_ascii=False,
            ),
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="assistant",
            raw_text="普通回复",
            sender_id=self_platform_id,
            sender_name="",
            created_at=1_100,
            is_read=True,
            content_json=json.dumps(
                [MessageElement.text("普通回复").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="assistant",
            raw_text="工具回复",
            sender_id="onebot_v11",
            sender_name="",
            created_at=1_200,
            is_read=True,
            content_json=json.dumps(
                [MessageElement.text("工具回复").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        )
    )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)

    context_messages = context_manager.build_context_stage_messages(
        session_id,
        self_platform_id=self_platform_id,
        now_ms=2_000,
    )
    active_text = context_manager.build_active_alias_constraint_text(
        session_id,
        now_ms=2_000,
    )
    state = context_manager.get_session_state(session_id)

    joined_text = "\n".join(
        str(block.get("text", ""))
        for message in context_messages
        for block in message.get("content", [])
        if isinstance(block, dict) and block.get("type") == "text"
    )
    alias_entries = [
        entry
        for block in state.short_term_blocks()
        for entry in block.metadata.get("alias_entries", [])
        if isinstance(entry, dict)
    ]

    assert "3575371140" not in active_text
    assert "onebot_v11" not in active_text
    assert "Ginkoro" in active_text
    assert "[@ 你] hello" in joined_text
    assert "[msgid:" in joined_text
    assert "普通回复" in joined_text
    assert "工具回复" in joined_text
    assert all(entry.get("platform_id") != self_platform_id for entry in alias_entries)
    assert all(entry.get("platform_id") != "onebot_v11" for entry in alias_entries)


def test_context_manager_mixes_assistant_segments_into_timeline(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "inst:group:assistant-timeline"

    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="用户消息一",
            sender_id="user-1",
            sender_name="Alpha",
            created_at=1_000,
            is_read=True,
            content_json=json.dumps(
                [MessageElement.text("用户消息一").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="assistant",
            raw_text="回复一",
            sender_id="bot-1",
            created_at=1_100,
            is_read=True,
            content_json=json.dumps(
                [MessageElement.text("回复一").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="assistant",
            raw_text="回复二",
            sender_id="bot-1",
            created_at=1_200,
            is_read=True,
            content_json=json.dumps(
                [MessageElement.text("回复二").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        )
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            role="user",
            raw_text="用户消息二",
            sender_id="user-2",
            sender_name="Beta",
            created_at=1_300,
            is_read=True,
            content_json=json.dumps(
                [MessageElement.text("用户消息二").model_dump(mode="json")],
                ensure_ascii=False,
            ),
        )
    )

    context_manager = ContextManager(db.message_logs, data_dir=tmp_path)
    context_messages = context_manager.build_context_stage_messages(
        session_id,
        self_platform_id="bot-1",
        now_ms=2_000,
    )
    state = context_manager.get_session_state(session_id)

    assert [message["role"] for message in context_messages] == ["user", "assistant", "user"]
    assert [block.kind for block in state.short_term_blocks()] == ["context", "assistant", "context"]
    assistant_blocks = context_messages[1]["content"]
    assistant_text = "\n".join(
        str(block.get("text", ""))
        for block in assistant_blocks
        if isinstance(block, dict) and block.get("type") == "text"
    )
    assert "回复一" in assistant_text
    assert "回复二" in assistant_text
    assert all(not block.metadata.get("alias_entries") for block in state.short_term_blocks() if block.kind == "assistant")
