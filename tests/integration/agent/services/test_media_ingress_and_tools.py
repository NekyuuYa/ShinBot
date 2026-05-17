from __future__ import annotations

from media_support import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_LLM_REF,
    DatabaseManager,
    FakeInspectionRunner,
    MediaSemanticRecord,
    MediaService,
    Message,
    MessageElement,
    MessageLogRecord,
    MockAdapter,
    PermissionEngine,
    ToolCallRequest,
    ToolManager,
    ToolRegistry,
    _make_group_event,
    _make_media_ingress,
    _write_png,
    format_incremental_messages,
    format_message_line,
    json,
    pytest,
    register_media_tools,
)


@pytest.mark.asyncio
async def test_ingress_ingests_local_image_media(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()

    adapter = MockAdapter()
    ingress = _make_media_ingress(tmp_path, db, media_service=MediaService(db))

    image_path = _write_png(tmp_path / "assets" / "ingress.png", color=(0, 255, 0))
    content = Message.from_elements(MessageElement.img(str(image_path))).to_xml()
    await ingress.process_event(_make_group_event(content), adapter)

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
async def test_ingress_schedules_media_inspection_on_third_repeat(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)
    inspection_runner = FakeInspectionRunner()

    adapter = MockAdapter()
    ingress = _make_media_ingress(
        tmp_path,
        db,
        media_service=media_service,
        media_inspection_runner=inspection_runner,
    )

    image_path = _write_png(tmp_path / "assets" / "repeat.png", color=(64, 64, 64))
    content = Message.from_elements(MessageElement.img(str(image_path))).to_xml()
    for index in range(3):
        await ingress.process_event(
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
async def test_ingress_schedules_sticker_summary_for_custom_image_emoji(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    media_service = MediaService(db)
    inspection_runner = FakeInspectionRunner()

    adapter = MockAdapter()
    ingress = _make_media_ingress(
        tmp_path,
        db,
        media_service=media_service,
        media_inspection_runner=inspection_runner,
    )

    image_path = _write_png(tmp_path / "assets" / "ingress-sticker.png", color=(255, 128, 0))
    content = Message.from_elements(MessageElement.img(str(image_path), sub_type="1")).to_xml()

    await ingress.process_event(_make_group_event(content, message_id="msg-sticker-1"), adapter)

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
