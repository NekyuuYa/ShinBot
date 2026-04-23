"""Tests for media fingerprinting and inspection config resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image

from shinbot.agent.context.message_parts import parse_message_parts
from shinbot.agent.media import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_LLM_REF,
    MediaInspectionRunner,
    MediaService,
    register_media_tools,
    resolve_media_inspection_config,
)
from shinbot.agent.media.config import BUILTIN_MEDIA_INSPECTION_PROMPT_ID
from shinbot.agent.prompt_manager import PromptRegistry
from shinbot.agent.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.agent.workflow.formatting import (
    format_incremental_messages,
    format_message_line,
)
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.pipeline import MessagePipeline
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager
from shinbot.persistence import (
    BotConfigRecord,
    DatabaseManager,
    MediaSemanticRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PromptDefinitionRecord,
)
from shinbot.persistence.records import utc_now_iso
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


class MockAdapter(BaseAdapter):
    def __init__(self, instance_id: str = "test-bot", platform: str = "mock", **kwargs):
        super().__init__(instance_id, platform)
        self.sent: list[tuple[str, list[MessageElement]]] = []

    async def start(self):
        pass

    async def shutdown(self):
        pass

    async def send(self, target_session, elements):
        self.sent.append((target_session, elements))
        return MessageHandle(message_id=f"sent-{len(self.sent)}", adapter_ref=self)

    async def call_api(self, method, params):
        return {"ok": True}

    async def get_capabilities(self):
        return {"elements": ["text", "img"], "actions": [], "limits": {}}


def _make_group_event(
    content: str,
    *,
    user_id: str = "user-1",
    message_id: str = "msg-1",
) -> UnifiedEvent:
    return UnifiedEvent(
        type="message-created",
        self_id="bot-1",
        platform="mock",
        user=User(id=user_id, name="Tester"),
        channel=Channel(id="group:1", type=0),
        message=MessagePayload(id=message_id, content=content),
    )


def _write_png(path: Path, color: tuple[int, int, int] = (255, 0, 0)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 8), color).save(path)
    return path


def _seed_media_runtime(
    db: DatabaseManager,
    *,
    instance_id: str,
    llm_ref: str = "route.media.inspect",
    media_prompt_ref: str | None = None,
    sticker_llm_ref: str | None = None,
    sticker_prompt_ref: str | None = None,
) -> None:
    now = utc_now_iso()
    provider_id = "openai-media"
    model_id = "openai-media/gpt-vision"
    route_id = "route.media.inspect"
    config: dict[str, str] = {"media_inspection_llm": llm_ref}
    if media_prompt_ref is not None:
        config["media_inspection_prompt"] = media_prompt_ref
    if sticker_llm_ref is not None:
        config["sticker_summary_llm"] = sticker_llm_ref
    if sticker_prompt_ref is not None:
        config["sticker_summary_prompt"] = sticker_prompt_ref

    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id=provider_id,
            type="openai",
            display_name="OpenAI Media",
            base_url="https://api.openai.com/v1",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=model_id,
            provider_id=provider_id,
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Vision",
            capabilities=["chat"],
            context_window=64000,
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="media_inspection", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id=model_id,
                priority=10,
                weight=1.0,
            )
        ],
    )
    if sticker_llm_ref is not None and sticker_llm_ref != route_id:
        db.model_registry.upsert_route(
            ModelRouteRecord(id=sticker_llm_ref, purpose="sticker_summary", strategy="priority"),
            members=[
                ModelRouteMemberRecord(
                    route_id=sticker_llm_ref,
                    model_id=model_id,
                    priority=10,
                    weight=1.0,
                )
            ],
        )
    db.bot_configs.upsert(
        BotConfigRecord(
            uuid="bot-config-media",
            instance_id=instance_id,
            main_llm=route_id,
            config=config,
            created_at=now,
            updated_at=now,
        )
    )


def _seed_main_runtime(
    db: DatabaseManager,
    *,
    instance_id: str,
    media_llm_ref: str = "",
) -> str:
    now = utc_now_iso()
    provider_id = "openai-main"
    model_id = "openai-main/gpt-chat"
    route_id = "route.main.chat"
    config: dict[str, str] = {}
    if media_llm_ref:
        config["media_inspection_llm"] = media_llm_ref

    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id=provider_id,
            type="openai",
            display_name="OpenAI Main",
            base_url="https://api.openai.com/v1",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id=model_id,
            provider_id=provider_id,
            litellm_model="openai/gpt-4.1",
            display_name="GPT Chat",
            capabilities=["chat"],
            context_window=128000,
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="chat", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id=model_id,
                priority=10,
                weight=1.0,
            )
        ],
    )
    db.bot_configs.upsert(
        BotConfigRecord(
            uuid="bot-config-main",
            instance_id=instance_id,
            main_llm=route_id,
            config=config,
            created_at=now,
            updated_at=now,
        )
    )
    return route_id


def _seed_custom_media_prompt(db: DatabaseManager, *, prompt_ref: str) -> None:
    now = utc_now_iso()
    prompt_uuid = "prompt-media-custom"
    db.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid=prompt_uuid,
            prompt_id=prompt_ref,
            name="Custom Media Inspector",
            source_type="user_defined",
            source_id="tests",
            stage="system_base",
            type="static_text",
            priority=100,
            enabled=True,
            content="You are a custom media inspector.",
            created_at=now,
            updated_at=now,
        )
    )


class FakeModelRuntime:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.calls = []

    async def generate(self, call):
        self.calls.append(call)
        return type(
            "Result",
            (),
            {
                "text": self.response_text,
                "execution_id": "exec-media",
                "route_id": call.route_id or "",
                "provider_id": "provider",
                "model_id": call.model_id or "",
                "usage": {},
            },
        )()


class FakeInspectionRunner:
    def __init__(self) -> None:
        self.calls = []

    def schedule_items(self, *, instance_id: str, session_id: str, items) -> None:
        self.calls.append(
            {
                "instance_id": instance_id,
                "session_id": session_id,
                "items": items,
            }
        )

    async def answer_question(
        self,
        *,
        instance_id: str,
        session_id: str,
        raw_hash: str,
        question: str,
    ) -> dict[str, str]:
        self.calls.append(
            {
                "instance_id": instance_id,
                "session_id": session_id,
                "raw_hash": raw_hash,
                "question": question,
            }
        )
        return {
            "raw_hash": raw_hash,
            "answer": f"回答: {question}",
            "inspection_agent_ref": BUILTIN_MEDIA_INSPECTION_AGENT_REF,
            "inspection_llm_ref": BUILTIN_MEDIA_INSPECTION_LLM_REF,
        }


def test_media_inspection_config_uses_builtin_fallback():
    resolved = resolve_media_inspection_config(None)

    assert resolved.agent_ref == BUILTIN_MEDIA_INSPECTION_AGENT_REF
    assert resolved.llm_ref == BUILTIN_MEDIA_INSPECTION_LLM_REF
    assert resolved.uses_builtin_agent is True
    assert resolved.uses_builtin_llm is True
    assert resolved.prompt_ref == BUILTIN_MEDIA_INSPECTION_PROMPT_ID
    assert resolved.uses_builtin_prompt is True
    assert "digest no longer than 50" in resolved.builtin_prompt


def test_media_inspection_config_prefers_user_overrides():
    resolved = resolve_media_inspection_config(
        {
            "config": {
                "media_inspection_prompt": "prompt.media.custom",
                "media_inspection_llm": "route.media.fast",
                "sticker_summary_prompt": "prompt.sticker.custom",
                "sticker_summary_llm": "route.sticker.fast",
            }
        }
    )

    assert resolved.agent_ref == BUILTIN_MEDIA_INSPECTION_AGENT_REF
    assert resolved.llm_ref == "route.media.fast"
    assert resolved.prompt_ref == "prompt.media.custom"
    assert resolved.sticker_llm_ref == "route.sticker.fast"
    assert resolved.sticker_prompt_ref == "prompt.sticker.custom"
    assert resolved.uses_builtin_agent is True
    assert resolved.uses_builtin_llm is False
    assert resolved.uses_builtin_prompt is False
    assert resolved.uses_builtin_sticker_llm is False
    assert resolved.uses_builtin_sticker_prompt is False


def test_media_inspection_config_does_not_reuse_image_overrides_for_stickers():
    resolved = resolve_media_inspection_config(
        {
            "config": {
                "media_inspection_prompt": "prompt.media.custom",
                "media_inspection_llm": "route.media.fast",
            }
        }
    )

    assert resolved.llm_ref == "route.media.fast"
    assert resolved.prompt_ref == "prompt.media.custom"
    assert resolved.sticker_llm_ref == "builtin.media_inspection.sticker_default"
    assert resolved.sticker_prompt_ref == "builtin.prompt.sticker_summary"
    assert resolved.uses_builtin_sticker_llm is True
    assert resolved.uses_builtin_sticker_prompt is True


def test_media_service_tracks_repeat_threshold_and_sliding_ttl(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "meme.png")
    message = Message.from_elements(MessageElement.img(str(image_path)))
    late_seen_at = 1_200.0 + 15 * 24 * 60 * 60

    first = service.ingest_message_media(
        session_id="inst:group:1",
        sender_id="user-1",
        platform_msg_id="msg-1",
        elements=message.elements,
        seen_at=1_000.0,
    )
    second = service.ingest_message_media(
        session_id="inst:group:1",
        sender_id="user-2",
        platform_msg_id="msg-2",
        elements=message.elements,
        seen_at=1_100.0,
    )
    third = service.ingest_message_media(
        session_id="inst:group:1",
        sender_id="user-3",
        platform_msg_id="msg-3",
        elements=message.elements,
        seen_at=1_200.0,
    )
    late = service.ingest_message_media(
        session_id="inst:group:1",
        sender_id="user-4",
        platform_msg_id="msg-4",
        elements=message.elements,
        seen_at=late_seen_at,
    )

    assert len(first) == 1
    assert first[0].occurrence_count == 1
    assert first[0].should_request_inspection is False
    assert second[0].occurrence_count == 2
    assert second[0].should_request_inspection is False
    assert third[0].occurrence_count == 3
    assert third[0].should_request_inspection is True
    assert late[0].occurrence_count == 1
    assert late[0].should_request_inspection is False

    asset = db.media_assets.get(third[0].raw_hash)
    occurrence = db.session_media_occurrences.get("inst:group:1", third[0].raw_hash)

    assert asset is not None
    assert asset["storage_path"] == str(image_path.resolve())
    assert asset["file_size"] > 0
    assert asset["strict_dhash"]
    assert asset["expire_at"] == pytest.approx(late_seen_at + 30 * 24 * 60 * 60)

    assert occurrence is not None
    assert occurrence["occurrence_count"] == 1
    assert occurrence["last_sender_id"] == "user-4"
    assert occurrence["last_platform_msg_id"] == "msg-4"
    assert occurrence["expire_at"] == pytest.approx(late_seen_at + 60 * 24 * 60 * 60)
    assert occurrence["recent_timestamps"] == pytest.approx([late_seen_at])


def test_media_service_custom_image_emoji_requests_sticker_summary_immediately(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "sticker.png")
    message = Message.from_elements(MessageElement.img(str(image_path), sub_type="1"))

    first = service.ingest_message_media(
        session_id="inst:group:stickers",
        sender_id="user-1",
        platform_msg_id="sticker-1",
        elements=message.elements,
        seen_at=1_000.0,
    )
    second = service.ingest_message_media(
        session_id="inst:group:stickers",
        sender_id="user-2",
        platform_msg_id="sticker-2",
        elements=message.elements,
        seen_at=1_100.0,
    )
    third = service.ingest_message_media(
        session_id="inst:group:stickers",
        sender_id="user-3",
        platform_msg_id="sticker-3",
        elements=message.elements,
        seen_at=1_200.0,
    )

    assert first[0].is_custom_emoji is True
    assert first[0].occurrence_count == 1
    assert first[0].should_request_inspection is True
    assert second[0].occurrence_count == 2
    assert second[0].should_request_inspection is True
    assert third[0].occurrence_count == 3
    assert third[0].should_request_inspection is True


def test_media_service_qq_image_sub_type_mapping(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    service = MediaService(db)

    normal_path = _write_png(tmp_path / "assets" / "normal.png", color=(0, 0, 255))
    custom_path = _write_png(tmp_path / "assets" / "custom.png", color=(0, 255, 0))
    store_path = _write_png(tmp_path / "assets" / "store.png", color=(255, 0, 0))
    message = Message.from_elements(
        MessageElement.img(str(normal_path), sub_type="0"),
        MessageElement.img(str(custom_path), sub_type="1"),
        MessageElement.img(str(store_path), sub_type="None"),
    )

    items = service.ingest_message_media(
        session_id="inst:group:image-subtypes",
        sender_id="user-1",
        platform_msg_id="subtype-1",
        elements=message.elements,
        seen_at=1_000.0,
    )

    assert len(items) == 3
    assert [item.is_custom_emoji for item in items] == [False, True, True]
    assert [item.should_request_inspection for item in items] == [False, True, True]


def test_context_parts_treat_store_emoji_sub_type_as_custom_emoji(tmp_path):
    image_path = _write_png(tmp_path / "assets" / "store-context.png")
    record = {
        "content_json": json.dumps(
            [
                {
                    "type": "img",
                    "attrs": {"src": str(image_path), "sub_type": "None"},
                }
            ],
            ensure_ascii=False,
        )
    }

    parts = parse_message_parts(record)

    assert len(parts) == 1
    assert parts[0].image is not None
    assert parts[0].image.is_custom_emoji is True


def test_media_service_ignores_native_emoji_elements(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    service = MediaService(db)

    message = Message.from_elements(
        MessageElement.emoji(id="14", name="smile"),
        MessageElement(type="qq:mface", attrs={"emoji_id": "abc", "summary": "smile"}),
    )

    items = service.ingest_message_media(
        session_id="inst:group:emoji",
        sender_id="user-1",
        platform_msg_id="emoji-1",
        elements=message.elements,
        seen_at=1_000.0,
    )

    assert items == []
    with db.connect() as conn:
        asset_count = conn.execute("SELECT COUNT(*) AS cnt FROM media_assets").fetchone()["cnt"]
    assert asset_count == 0


def test_media_service_persists_message_links_and_resolves_by_message_log_id(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "linked.png", color=(5, 15, 25))
    message = Message.from_elements(MessageElement.img(str(image_path)))
    message_log_id = db.message_logs.insert(
        MessageLogRecord(
            session_id="inst-linked:group:1",
            role="user",
            created_at=1_500.0 * 1000,
            platform_msg_id="linked-msg-1",
            sender_id="user-1",
            sender_name="Tester",
            content_json=json.dumps(
                [element.model_dump(mode="json") for element in message.elements],
                ensure_ascii=False,
            ),
            raw_text="[图片]",
            is_read=False,
            is_mentioned=False,
        )
    )

    items = service.ingest_message_media(
        session_id="inst-linked:group:1",
        sender_id="user-1",
        platform_msg_id="linked-msg-1",
        elements=message.elements,
        message_log_id=message_log_id,
        seen_at=1_500.0,
    )

    with db.connect() as conn:
        conn.execute(
            "UPDATE message_logs SET content_json = '[]' WHERE id = ?",
            (message_log_id,),
        )

    links = db.message_media_links.list_by_message_log_id(message_log_id)
    resolved = service.resolve_message_raw_hash(
        session_id="inst-linked:group:1",
        message_log_id=message_log_id,
    )

    assert len(items) == 1
    assert len(links) == 1
    assert links[0]["raw_hash"] == items[0].raw_hash
    assert resolved == items[0].raw_hash


@pytest.mark.asyncio
async def test_media_inspection_runner_persists_verified_semantics(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_media_runtime(db, instance_id="inst-media")
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "inspect.png")
    message = Message.from_elements(MessageElement.img(str(image_path)))
    items = None
    for offset in range(3):
        items = media_service.ingest_message_media(
            session_id="inst-media:group:1",
            sender_id=f"user-{offset}",
            platform_msg_id=f"msg-{offset}",
            elements=message.elements,
            seen_at=1_000.0 + offset,
        )

    assert items is not None
    target = items[0]
    runtime = FakeModelRuntime(
        '{"kind":"meme_image","digest":"熊猫头无语，像在吐槽对方","confidence_band":"high","reason":"同会话重复出现且表达情绪明确"}'
    )
    runner = MediaInspectionRunner(
        db,
        PromptRegistry(),
        runtime,
        media_service,
    )

    result = await runner.inspect_raw_hash(
        instance_id="inst-media",
        session_id="inst-media:group:1",
        raw_hash=target.raw_hash,
    )

    assert result is not None
    assert result["kind"] == "meme_image"
    assert result["verified_by_model"] is True
    assert result["inspection_agent_ref"] == BUILTIN_MEDIA_INSPECTION_AGENT_REF
    assert result["inspection_llm_ref"] == "route.media.inspect"
    assert result["metadata"]["inspection_prompt_ref"] == BUILTIN_MEDIA_INSPECTION_PROMPT_ID
    assert result["digest"] == "熊猫头无语，像在吐槽对方"
    assert len(runtime.calls) == 1
    call = runtime.calls[0]
    assert call.caller == "media.inspection_runner"
    assert call.response_format["type"] == "json_schema"
    assert [message["role"] for message in call.messages] == ["user"]
    user_content = call.messages[-1]["content"]
    assert user_content[0]["type"] == "text"
    assert "You are ShinBot's media inspection agent." in user_content[0]["text"]
    assert "repeat_count_14d=3" in user_content[0]["text"]
    assert user_content[1]["type"] == "image_url"
    assert user_content[1]["image_url"]["url"].startswith("data:image/png;base64,")


@pytest.mark.asyncio
async def test_media_inspection_runner_does_not_fallback_to_main_llm(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    main_route_id = _seed_main_runtime(
        db,
        instance_id="inst-no-media-route",
        media_llm_ref="route.media.missing",
    )
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "no-fallback.png")
    message = Message.from_elements(MessageElement.img(str(image_path)))
    items = None
    for offset in range(3):
        items = media_service.ingest_message_media(
            session_id="inst-no-media-route:group:1",
            sender_id=f"user-{offset}",
            platform_msg_id=f"msg-no-fallback-{offset}",
            elements=message.elements,
            seen_at=1_000.0 + offset,
        )

    runtime = FakeModelRuntime(
        '{"kind":"meme_image","digest":"不应该调用主模型","confidence_band":"low","reason":"missing media route"}'
    )
    runner = MediaInspectionRunner(db, PromptRegistry(), runtime, media_service)

    result = await runner.inspect_raw_hash(
        instance_id="inst-no-media-route",
        session_id="inst-no-media-route:group:1",
        raw_hash=items[0].raw_hash,
    )

    assert main_route_id == "route.main.chat"
    assert result is None
    assert runtime.calls == []
    assert db.media_semantics.get(items[0].raw_hash) is None


@pytest.mark.asyncio
async def test_sticker_summary_uses_separate_runtime_caller(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_media_runtime(
        db,
        instance_id="inst-sticker",
        sticker_llm_ref="route.sticker.summary",
    )
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "sticker-summary.png")
    message = Message.from_elements(MessageElement.img(str(image_path), sub_type="1"))
    items = None
    for offset in range(3):
        items = media_service.ingest_message_media(
            session_id="inst-sticker:group:1",
            sender_id=f"user-{offset}",
            platform_msg_id=f"msg-sticker-{offset}",
            elements=message.elements,
            seen_at=1_000.0 + offset,
        )

    runtime = FakeModelRuntime(
        '{"kind":"emoji_native","digest":"微笑表情，表达开心","confidence_band":"high","reason":"custom sticker"}'
    )
    runner = MediaInspectionRunner(db, PromptRegistry(), runtime, media_service)

    result = await runner.inspect_raw_hash(
        instance_id="inst-sticker",
        session_id="inst-sticker:group:1",
        raw_hash=items[0].raw_hash,
        prefer_sticker_model=True,
    )

    assert result is not None
    assert len(runtime.calls) == 1
    call = runtime.calls[0]
    assert call.caller == "media.sticker_summary_runner"
    assert call.purpose == "sticker_summary"
    assert call.route_id == "route.sticker.summary"
    assert call.metadata["inspection_llm_ref"] == "route.sticker.summary"
    assert call.metadata["summary_mode"] == "sticker"
    assert [message["role"] for message in call.messages] == ["user"]
    assert "You are ShinBot's sticker summary agent." in call.messages[0]["content"][0]["text"]


@pytest.mark.asyncio
async def test_media_inspection_runner_supports_custom_prompt_id(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_custom_media_prompt(db, prompt_ref="prompt.media.custom")
    _seed_media_runtime(
        db,
        instance_id="inst-media-custom",
        media_prompt_ref="prompt.media.custom",
    )
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "inspect-custom.png", color=(0, 0, 255))
    message = Message.from_elements(MessageElement.img(str(image_path)))
    items = None
    for offset in range(3):
        items = media_service.ingest_message_media(
            session_id="inst-media-custom:group:1",
            sender_id=f"user-{offset}",
            platform_msg_id=f"msg-{offset}",
            elements=message.elements,
            seen_at=2_000.0 + offset,
        )

    runtime = FakeModelRuntime(
        '{"kind":"generic_image","digest":"蓝色方块示例图","confidence_band":"medium","reason":"重复出现但画面更像普通示例图"}'
    )
    runner = MediaInspectionRunner(
        db,
        PromptRegistry(),
        runtime,
        media_service,
    )

    await runner.inspect_raw_hash(
        instance_id="inst-media-custom",
        session_id="inst-media-custom:group:1",
        raw_hash=items[0].raw_hash,
    )

    assert len(runtime.calls) == 1
    assert runtime.calls[0].metadata["inspection_prompt_ref"] == "prompt.media.custom"
    rendered_text = json.dumps(runtime.calls[0].messages, ensure_ascii=False)
    assert "You are a custom media inspector." in rendered_text
    assert [message["role"] for message in runtime.calls[0].messages] == ["user"]


@pytest.mark.asyncio
async def test_pipeline_ingests_local_image_media(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    adapter = MockAdapter()
    pipeline = MessagePipeline(
        adapter_manager=AdapterManager(),
        session_manager=SessionManager(data_dir=tmp_path, session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        command_registry=CommandRegistry(),
        event_bus=EventBus(),
        database=db,
        media_service=MediaService(db),
    )

    image_path = _write_png(tmp_path / "assets" / "pipeline.png", color=(0, 255, 0))
    content = Message.from_elements(MessageElement.img(str(image_path))).to_xml()
    await pipeline.process_event(_make_group_event(content), adapter)

    session_id = "test-bot:group:group:1"
    rows = db.message_logs.get_recent(session_id, limit=5)
    assert len(rows) == 1

    with db.connect() as conn:
        asset_count = conn.execute("SELECT COUNT(*) AS cnt FROM media_assets").fetchone()["cnt"]
        link_count = conn.execute("SELECT COUNT(*) AS cnt FROM message_media_links").fetchone()[
            "cnt"
        ]
        occ_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM session_media_occurrences"
        ).fetchone()["cnt"]
        raw_hash = conn.execute("SELECT raw_hash FROM media_assets").fetchone()["raw_hash"]

    assert asset_count == 1
    assert link_count == 1
    assert occ_count == 1
    occurrence = db.session_media_occurrences.get(session_id, raw_hash)
    links = db.message_media_links.list_by_message_log_id(rows[0]["id"])
    assert occurrence is not None
    assert len(links) == 1
    assert links[0]["raw_hash"] == raw_hash
    assert occurrence["occurrence_count"] == 1


@pytest.mark.asyncio
async def test_pipeline_schedules_media_inspection_on_third_repeat(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)
    inspection_runner = FakeInspectionRunner()

    adapter = MockAdapter()
    pipeline = MessagePipeline(
        adapter_manager=AdapterManager(),
        session_manager=SessionManager(data_dir=tmp_path, session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        command_registry=CommandRegistry(),
        event_bus=EventBus(),
        database=db,
        media_service=media_service,
        media_inspection_runner=inspection_runner,
    )

    image_path = _write_png(tmp_path / "assets" / "repeat.png", color=(64, 64, 64))
    content = Message.from_elements(MessageElement.img(str(image_path))).to_xml()
    for index in range(3):
        await pipeline.process_event(
            _make_group_event(
                content,
                user_id=f"user-{index}",
                message_id=f"msg-repeat-{index}",
            ),
            adapter,
        )

    assert len(inspection_runner.calls) == 1
    scheduled = inspection_runner.calls[0]
    assert scheduled["instance_id"] == "test-bot"
    assert scheduled["session_id"] == "test-bot:group:group:1"
    assert len(scheduled["items"]) == 1
    assert scheduled["items"][0].occurrence_count == 3
    assert scheduled["items"][0].should_request_inspection is True


@pytest.mark.asyncio
async def test_pipeline_schedules_sticker_summary_for_custom_image_emoji(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)
    inspection_runner = FakeInspectionRunner()

    adapter = MockAdapter()
    pipeline = MessagePipeline(
        adapter_manager=AdapterManager(),
        session_manager=SessionManager(data_dir=tmp_path, session_repo=db.sessions),
        permission_engine=PermissionEngine(),
        command_registry=CommandRegistry(),
        event_bus=EventBus(),
        database=db,
        media_service=media_service,
        media_inspection_runner=inspection_runner,
    )

    image_path = _write_png(tmp_path / "assets" / "pipeline-sticker.png", color=(255, 128, 0))
    content = Message.from_elements(MessageElement.img(str(image_path), sub_type="1")).to_xml()

    await pipeline.process_event(_make_group_event(content, message_id="msg-sticker-1"), adapter)

    assert len(inspection_runner.calls) == 1
    scheduled = inspection_runner.calls[0]
    assert scheduled["instance_id"] == "test-bot"
    assert scheduled["session_id"] == "test-bot:group:group:1"
    assert len(scheduled["items"]) == 1
    assert scheduled["items"][0].is_custom_emoji is True
    assert scheduled["items"][0].occurrence_count == 1
    assert scheduled["items"][0].should_request_inspection is True


@pytest.mark.asyncio
async def test_media_tool_inspect_original_uses_latest_session_image(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)
    inspection_runner = FakeInspectionRunner()
    registry = ToolRegistry()
    manager = ToolManager(registry, permission_engine=PermissionEngine())
    register_media_tools(registry, media_service, inspection_runner)

    image_path = _write_png(tmp_path / "assets" / "tool.png", color=(128, 0, 128))
    message = Message.from_elements(MessageElement.img(str(image_path)))
    items = media_service.ingest_message_media(
        session_id="inst-tool:group:1",
        sender_id="user-1",
        platform_msg_id="tool-msg-1",
        elements=message.elements,
        seen_at=3_000.0,
    )
    db.message_logs.insert(
        MessageLogRecord(
            session_id="inst-tool:group:1",
            role="user",
            created_at=3_000.0 * 1000,
            platform_msg_id="tool-msg-1",
            sender_id="user-1",
            sender_name="Tester",
            content_json=json.dumps(
                [element.model_dump(mode="json") for element in message.elements],
                ensure_ascii=False,
            ),
            raw_text="这是谁",
            is_read=False,
            is_mentioned=False,
        )
    )

    result = await manager.execute(
        ToolCallRequest(
            tool_name="media.inspect_original",
            arguments={"question": "这张图里是谁？"},
            caller="attention.workflow_runner",
            instance_id="inst-tool",
            session_id="inst-tool:group:1",
        )
    )

    assert result.success is True
    assert result.output["answer"] == "回答: 这张图里是谁？"
    assert inspection_runner.calls[0]["raw_hash"] == items[0].raw_hash


def test_workflow_runner_formats_media_digest_in_batch_context(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "digest.png", color=(10, 20, 30))
    message = Message.from_elements(MessageElement.img(str(image_path)))
    items = None
    for offset in range(3):
        items = media_service.ingest_message_media(
            session_id="inst-workflow:group:1",
            sender_id=f"user-{offset}",
            platform_msg_id=f"wf-msg-{offset}",
            elements=message.elements,
            seen_at=4_000.0 + offset,
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
            first_seen_at=4_010.0,
            last_seen_at=4_010.0,
            expire_at=4_010.0 + 180 * 24 * 60 * 60,
        )
    )
    batch = [
        {
            "id": 1,
            "session_id": "inst-workflow:group:1",
            "platform_msg_id": "wf-msg-0",
            "sender_id": "user-1",
            "sender_name": "Tester",
            "raw_text": "哈哈",
            "is_mentioned": 0,
            "content_json": json.dumps(
                [element.model_dump(mode="json") for element in message.elements],
                ensure_ascii=False,
            ),
        }
    ]
    text = format_message_line(batch[0], media_service, include_message_reference=True)

    assert "[表情: 熊猫头无语]" in text
    assert "[媒体引用:" in text
    assert "message_log_id=1" in text
    assert "platform_msg_id=wf-msg-0" in text


def test_incremental_workflow_context_does_not_include_message_ids():
    text = format_incremental_messages(
        [
            {
                "id": 2,
                "session_id": "inst-workflow:group:1",
                "platform_msg_id": "wf-msg-2",
                "sender_id": "user-2",
                "sender_name": "Tester",
                "raw_text": "补充消息",
            }
        ]
    )

    assert "Tester: 补充消息" in text
    assert "message_log_id=2" not in text
    assert "platform_msg_id=wf-msg-2" not in text
