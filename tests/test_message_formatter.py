"""Unit tests for the message formatter service."""

from __future__ import annotations

import json

from shinbot.agent.services.message_formatter import (
    EmojiMode,
    ImageMode,
    MessageFormatConfig,
    MessageFormatterService,
    PackMode,
    format_messages,
)


def _make_record(
    *,
    sender_id: str = "user-1",
    sender_name: str = "Alice",
    raw_text: str = "hello",
    content_json: str = "",
    role: str = "user",
    created_at: float = 1000.0,
    record_id: int = 1,
) -> dict:
    return {
        "id": record_id,
        "sender_id": sender_id,
        "sender_name": sender_name,
        "raw_text": raw_text,
        "content_json": content_json,
        "role": role,
        "created_at": created_at,
    }


# -- basic formatting --


def test_format_plain_text_messages() -> None:
    records = [
        _make_record(raw_text="hello", sender_name="Alice"),
        _make_record(raw_text="world", sender_id="user-2", sender_name="Bob"),
    ]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, inject_sender=True)
    result = format_messages(records, config)

    assert result.message_count == 2
    assert "Alice: hello" in result.packed_text
    assert "Bob: world" in result.packed_text


def test_format_empty_records() -> None:
    result = format_messages([], MessageFormatConfig())
    assert result.message_count == 0
    assert result.packed_text == ""


def test_format_skips_empty_messages() -> None:
    records = [_make_record(raw_text="", content_json="")]
    result = format_messages(records, MessageFormatConfig())
    assert result.message_count == 0


# -- content_json parsing --


def test_format_content_json_text_element() -> None:
    content = json.dumps([{"type": "text", "attrs": {"content": "hi there"}}])
    records = [_make_record(content_json=content)]
    result = format_messages(records, MessageFormatConfig(pack_mode=PackMode.PACK))
    assert "hi there" in result.packed_text


def test_format_content_json_mention() -> None:
    content = json.dumps([{"type": "at", "attrs": {"id": "bot-1", "name": "Bot"}}])
    records = [_make_record(content_json=content, raw_text="")]
    result = format_messages(records, MessageFormatConfig(pack_mode=PackMode.PACK, self_platform_id="bot-1"))
    assert "[@ 你]" in result.packed_text


def test_format_content_json_mention_other() -> None:
    content = json.dumps([{"type": "at", "attrs": {"id": "user-2", "name": "Bob"}}])
    records = [_make_record(content_json=content, raw_text="")]
    result = format_messages(records, MessageFormatConfig(pack_mode=PackMode.PACK))
    assert "[@ Bob]" in result.packed_text


def test_format_content_json_quote() -> None:
    content = json.dumps([{"type": "quote", "attrs": {"id": "42"}}])
    records = [_make_record(content_json=content, raw_text="")]
    result = format_messages(records, MessageFormatConfig(pack_mode=PackMode.PACK))
    assert "[引用消息 id:42]" in result.packed_text


def test_format_content_json_image_description_mode(monkeypatch, tmp_path) -> None:
    """When raw_hash is available and a description exists, use it."""
    img_file = tmp_path / "test.jpg"
    img_file.write_bytes(b"\xff\xd8\xff\xe0fake-jpg")
    content = json.dumps([{"type": "img", "attrs": {"src": str(img_file)}}])
    records = [_make_record(content_json=content, raw_text="")]
    config = MessageFormatConfig(image_mode=ImageMode.DESCRIPTION, pack_mode=PackMode.PACK)

    class FakeFingerprint:
        raw_hash = "abc123"
        strict_dhash = "dhash"
        storage_path = str(img_file)

    import shinbot.agent.services.media.fingerprint as fp_mod

    monkeypatch.setattr(fp_mod, "fingerprint_image_file", lambda _: FakeFingerprint())
    result = format_messages(records, config, image_descriptions={"abc123": "a cat photo"})
    assert "[图片: a cat photo]" in result.packed_text


def test_format_content_json_image_no_description() -> None:
    content = json.dumps([{"type": "img", "attrs": {"src": "hash123"}}])
    records = [_make_record(content_json=content, raw_text="")]
    config = MessageFormatConfig(image_mode=ImageMode.DESCRIPTION, pack_mode=PackMode.PACK)
    result = format_messages(records, config)
    assert "[图片]" in result.packed_text


def test_format_content_json_emoji_semantic_mode(monkeypatch, tmp_path) -> None:
    img_file = tmp_path / "emoji.png"
    img_file.write_bytes(b"\x89PNGfake")
    content = json.dumps([{"type": "img", "attrs": {"src": str(img_file), "sub_type": 1}}])
    records = [_make_record(content_json=content, raw_text="")]
    config = MessageFormatConfig(emoji_mode=EmojiMode.SEMANTIC, pack_mode=PackMode.PACK)

    class FakeFingerprint:
        raw_hash = "emoji_abc"
        strict_dhash = "dhash"
        storage_path = str(img_file)

    import shinbot.agent.services.media.fingerprint as fp_mod

    monkeypatch.setattr(fp_mod, "fingerprint_image_file", lambda _: FakeFingerprint())
    result = format_messages(records, config, image_descriptions={"emoji_abc": "smile"})
    assert "[表情: smile]" in result.packed_text


def test_format_content_json_poke() -> None:
    content = json.dumps([{"type": "sb:poke", "attrs": {"target": "user-1"}}])
    records = [_make_record(content_json=content, raw_text="")]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, self_platform_id="user-1")
    result = format_messages(records, config)
    assert "[戳一戳" in result.packed_text


# -- pack modes --


def test_pack_mode_individual() -> None:
    records = [
        _make_record(raw_text="a", sender_name="Alice"),
        _make_record(raw_text="b", sender_name="Bob"),
    ]
    config = MessageFormatConfig(pack_mode=PackMode.INDIVIDUAL)
    result = format_messages(records, config)

    assert result.packed_text == ""
    assert len(result.messages) == 2
    assert result.messages[0].text == "a"
    assert result.messages[1].text == "b"


def test_pack_mode_pack_without_sender() -> None:
    records = [
        _make_record(raw_text="hello"),
        _make_record(raw_text="world"),
    ]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, inject_sender=False)
    result = format_messages(records, config)
    assert "hello" in result.packed_text
    assert "world" in result.packed_text
    assert "user-1" not in result.packed_text


# -- timestamp --


def test_sparse_timestamp_insertion() -> None:
    records = [
        _make_record(created_at=1000.0, raw_text="a"),
        _make_record(created_at=1000.5, raw_text="b"),
        _make_record(created_at=1000.0 + 5 * 60, raw_text="c"),
    ]
    config = MessageFormatConfig(
        pack_mode=PackMode.PACK,
        timestamp_mode="sparse",
        inject_sender=False,
    )
    result = format_messages(records, config)
    lines = result.packed_text.split("\n")
    # Should have a timestamp line before "c" due to 5-minute gap
    assert len(lines) >= 3


# -- sender label resolution --


def test_assistant_role_gets_you_label() -> None:
    records = [_make_record(role="assistant", raw_text="response")]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, inject_sender=True)
    result = format_messages(records, config)
    assert "你: response" in result.packed_text


def test_self_platform_id_gets_you_label() -> None:
    records = [_make_record(sender_id="bot-1", raw_text="msg")]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, inject_sender=True, self_platform_id="bot-1")
    result = format_messages(records, config)
    assert "你: msg" in result.packed_text


def test_display_name_override() -> None:
    records = [_make_record(sender_id="user-1", sender_name="Alice")]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, inject_sender=True)
    result = format_messages(records, config, display_names={"user-1": "Alicia"})
    assert "Alicia: hello" in result.packed_text


# -- service --


def test_service_format_text() -> None:
    svc = MessageFormatterService()
    records = [_make_record(raw_text="test")]
    text = svc.format_text(records)
    assert "test" in text


def test_service_format_with_config() -> None:
    svc = MessageFormatterService()
    records = [_make_record(raw_text="hello", sender_name="Alice")]
    result = svc.format(records, MessageFormatConfig(pack_mode=PackMode.INDIVIDUAL))
    assert len(result.messages) == 1
    assert result.messages[0].sender_label == "Alice"


def test_service_resolves_identity_store_name() -> None:
    class FakeIdentityStore:
        def get_identity(self, user_id: str, *, platform: str = "") -> dict | None:
            assert user_id == "user-1"
            return {"name": "Alicia"}

    svc = MessageFormatterService(identity_store=FakeIdentityStore())
    records = [_make_record(sender_id="user-1", sender_name="Alice", raw_text="hello")]
    result = svc.format(records, MessageFormatConfig(pack_mode=PackMode.PACK))
    assert "Alicia: hello" in result.packed_text


def test_service_resolves_media_semantics_by_fingerprint(monkeypatch, tmp_path) -> None:
    img_file = tmp_path / "semantic.jpg"
    img_file.write_bytes(b"\xff\xd8semantic")
    content = json.dumps([{"type": "img", "attrs": {"src": str(img_file)}}])

    class FakeFingerprint:
        raw_hash = "raw-semantic"
        strict_dhash = "dhash"
        storage_path = str(img_file)

    class FakeMediaService:
        def get_media_semantic(self, raw_hash: str) -> dict | None:
            assert raw_hash == "raw-semantic"
            return {"digest": "a semantic image"}

    import shinbot.agent.services.media.fingerprint as fp_mod

    monkeypatch.setattr(fp_mod, "fingerprint_image_file", lambda _: FakeFingerprint())
    svc = MessageFormatterService(media_service=FakeMediaService())
    records = [_make_record(content_json=content, raw_text="")]
    result = svc.format(records, MessageFormatConfig(pack_mode=PackMode.PACK))
    assert "[图片: a semantic image]" in result.packed_text


# -- mixed content --


def test_format_mixed_text_and_mention() -> None:
    content = json.dumps([
        {"type": "text", "attrs": {"content": "hey "}},
        {"type": "at", "attrs": {"id": "bot-1", "name": "Bot"}},
        {"type": "text", "attrs": {"content": " check this"}},
    ])
    records = [_make_record(content_json=content, raw_text="")]
    config = MessageFormatConfig(pack_mode=PackMode.PACK, self_platform_id="bot-1")
    result = format_messages(records, config)
    assert "hey" in result.packed_text
    assert "[@ 你]" in result.packed_text
    assert "check this" in result.packed_text
