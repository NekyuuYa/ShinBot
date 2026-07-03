from __future__ import annotations

from media_support import (
    BUILTIN_MEDIA_INSPECTION_AGENT_REF,
    BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
    DatabaseManager,
    FakeModelRuntime,
    MediaInspectionRunner,
    MediaService,
    Message,
    MessageElement,
    PromptRegistry,
    _seed_custom_media_prompt,
    _seed_main_runtime,
    _seed_media_runtime,
    _write_png,
    build_media_reanalysis_messages,
    json,
    pytest,
)

from shinbot.agent.runtime.task_manager import AgentTaskManager


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
    assert [message["role"] for message in call.messages] == ["system", "user"]
    system_content = call.messages[0]["content"]
    assert system_content[0]["type"] == "text"
    assert "你是 ShinBot 的媒体检查代理" in system_content[0]["text"]
    user_content = call.messages[-1]["content"]
    assert user_content[0]["type"] == "text"
    assert "repeat_count_14d=3" in user_content[0]["text"]
    assert user_content[1]["type"] == "image_url"
    assert user_content[1]["image_url"]["url"].startswith("data:image/png;base64,")
    assert "media.media_inspection.instruction" in call.metadata["prompt_component_ids"]


@pytest.mark.asyncio
async def test_media_inspection_runner_registers_tasks_in_scope(tmp_path):
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    _seed_media_runtime(db, instance_id="inst-media-scope")
    media_service = MediaService(db)

    image_path = _write_png(tmp_path / "assets" / "inspect-scope.png")
    message = Message.from_elements(MessageElement.img(str(image_path)))
    items = None
    for offset in range(3):
        items = media_service.ingest_message_media(
            session_id="inst-media-scope:group:1",
            sender_id=f"user-{offset}",
            platform_msg_id=f"msg-scope-{offset}",
            elements=message.elements,
            seen_at=1_000.0 + offset,
        )

    runtime = FakeModelRuntime(
        '{"kind":"meme_image","digest":"scope","confidence_band":"high","reason":"scope"}'
    )
    runner = MediaInspectionRunner(db, PromptRegistry(), runtime, media_service)
    manager = AgentTaskManager()
    runner.bind_task_scope(manager.scope("agent:test:media_inspection"))
    await runner.inspect_raw_hash(
        instance_id="inst-media-scope",
        session_id="inst-media-scope:group:1",
        raw_hash=items[0].raw_hash,
    )

    assert runtime.calls
    assert manager.tasks(prefix="agent:test:media_inspection") == []


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
    assert "media.sticker_summary.instruction" in call.metadata["prompt_component_ids"]
    assert [message["role"] for message in call.messages] == ["system", "user"]
    assert "你是 ShinBot 的表情包摘要代理" in call.messages[0]["content"][0]["text"]


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
    assert "media.media_inspection.instruction" in runtime.calls[0].metadata["prompt_component_ids"]
    rendered_text = json.dumps(runtime.calls[0].messages, ensure_ascii=False)
    assert "You are a custom media inspector." in rendered_text
    assert [message["role"] for message in runtime.calls[0].messages] == ["system", "user"]


def test_media_reanalysis_messages_use_prompt_registry(tmp_path):
    image_path = _write_png(tmp_path / "assets" / "reanalysis.png", color=(80, 90, 100))

    messages = build_media_reanalysis_messages(
        prompt_registry=PromptRegistry(),
        instance_id="inst-media",
        session_id="inst-media:group:1",
        raw_hash="raw-hash",
        asset={"storage_path": str(image_path), "mime_type": "image/png", "width": 8, "height": 8},
        question="这张图是什么？",
        model_context_window=64000,
    )

    assert [message["role"] for message in messages] == ["system", "user"]
    assert "媒体重新分析代理" in messages[0]["content"][0]["text"]
    assert "这张图是什么？" in messages[1]["content"][0]["text"]
    assert messages[1]["content"][1]["type"] == "image_url"
