from __future__ import annotations

from media_support import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_LLM_REF,
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    DatabaseManager,
    MediaService,
    Message,
    MessageElement,
    MessageLogRecord,
    _write_png,
    json,
    parse_message_parts,
    pytest,
    resolve_media_inspection_config,
)


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
