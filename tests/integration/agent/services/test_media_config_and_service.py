from __future__ import annotations

from media_support import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_LLM_REF,
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    DatabaseManager,
    MediaSemanticRecord,
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
    assert "不超过 50 个中文字符" in resolved.builtin_prompt


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
    assert first[0].should_request_inspection is True  # threshold=1, first occurrence triggers
    assert second[0].occurrence_count == 2
    assert second[0].should_request_inspection is True
    assert third[0].occurrence_count == 3
    assert third[0].should_request_inspection is True
    assert late[0].occurrence_count == 1
    assert late[0].should_request_inspection is True  # new window, count=1 >= threshold

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
    message = Message.from_elements(MessageElement.img(str(image_path), sub_type="3"))

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
    flash_path = _write_png(tmp_path / "assets" / "flash.png", color=(0, 255, 0))
    sticker_path = _write_png(tmp_path / "assets" / "sticker.png", color=(255, 0, 0))
    message = Message.from_elements(
        MessageElement.img(str(normal_path), sub_type="0"),
        MessageElement.img(str(flash_path), sub_type="1"),
        MessageElement.img(str(sticker_path), sub_type="3"),
    )

    items = service.ingest_message_media(
        session_id="inst:group:image-subtypes",
        sender_id="user-1",
        platform_msg_id="subtype-1",
        elements=message.elements,
        seen_at=1_000.0,
    )

    assert len(items) == 3
    assert [item.is_custom_emoji for item in items] == [False, False, True]
    # With threshold=1, all images trigger inspection on first occurrence
    assert [item.should_request_inspection for item in items] == [True, True, True]


def test_context_parts_treat_sticker_sub_type_as_custom_emoji(tmp_path):
    image_path = _write_png(tmp_path / "assets" / "sticker-context.png")
    record = {
        "content_json": json.dumps(
            [
                {
                    "type": "img",
                    "attrs": {"src": str(image_path), "sub_type": "3"},
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


def test_media_service_reuses_semantics_by_strict_dhash(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    service = MediaService(db)

    source_path = _write_png(tmp_path / "assets" / "source.png", color=(20, 30, 40))
    mirror_path = _write_png(tmp_path / "assets" / "mirror.png", color=(20, 30, 40))
    mirror_bytes = mirror_path.read_bytes() + b"mirror"
    mirror_path.write_bytes(mirror_bytes)

    source = Message.from_elements(MessageElement.img(str(source_path)))
    mirror = Message.from_elements(MessageElement.img(str(mirror_path)))

    source_items = service.ingest_message_media(
        session_id="inst:group:match",
        sender_id="user-1",
        platform_msg_id="match-1",
        elements=source.elements,
        seen_at=1_000.0,
    )
    db.media_semantics.upsert(
        MediaSemanticRecord(
            raw_hash=source_items[0].raw_hash,
            strict_dhash=source_items[0].strict_dhash,
            kind="meme_image",
            digest="相同表情",
            verified_by_model=True,
            inspection_agent_ref="builtin.media_inspection.agent",
            inspection_llm_ref="builtin.media_inspection.default",
            metadata={},
            first_seen_at=1_000.0,
            last_seen_at=1_000.0,
            expire_at=1_000.0 + 180 * 24 * 60 * 60,
        )
    )

    mirror_items = service.ingest_message_media(
        session_id="inst:group:match",
        sender_id="user-2",
        platform_msg_id="match-2",
        elements=mirror.elements,
        seen_at=1_010.0,
    )

    semantic = service.get_media_semantic(
        mirror_items[0].raw_hash,
        strict_dhash=mirror_items[0].strict_dhash,
    )

    assert mirror_items[0].should_request_inspection is False
    assert semantic is not None
    assert semantic["digest"] == "相同表情"


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
