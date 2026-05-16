from __future__ import annotations

import base64
import hashlib
import json

from shinbot.agent.services.model_runtime.extraction import (
    extract_injected_context,
    sanitize_messages_for_audit,
)


def test_extract_injected_context_redacts_data_url_payloads() -> None:
    raw = b"tiny image bytes"
    encoded = base64.b64encode(raw).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()

    payload = extract_injected_context(
        [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "inspect this"},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{encoded}",
                            "detail": "low",
                        },
                    },
                ],
            }
        ]
    )

    assert encoded not in payload
    blocks = json.loads(payload)
    image_block = blocks[1]
    assert image_block["image_url"]["url"] == f"media:sha256:{digest}"
    assert image_block["image_url"]["raw_hash"] == digest
    assert image_block["image_url"]["mime_type"] == "image/png"
    assert image_block["image_url"]["byte_size"] == len(raw)
    assert image_block["image_url"]["encoded_chars"] == len(encoded)
    assert image_block["image_url"]["redacted"] is True
    assert image_block["image_url"]["detail"] == "low"


def test_extract_injected_context_keeps_external_image_url() -> None:
    payload = extract_injected_context(
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.test/image.png"},
                    }
                ],
            }
        ]
    )

    assert json.loads(payload) == [
        {
            "type": "image_url",
            "image_url": {"url": "https://example.test/image.png"},
        }
    ]


def test_extract_injected_context_redacts_invalid_data_url() -> None:
    payload = extract_injected_context(
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/gif;base64,not-valid-*"},
                    }
                ],
            }
        ]
    )

    assert "not-valid-*" not in payload
    image_url = json.loads(payload)[0]["image_url"]
    assert image_url["url"] == "data-url:redacted"
    assert image_url["decode_error"] == "invalid_base64"
    assert image_url["redacted"] is True


def test_extract_injected_context_string_user_message() -> None:
    payload = extract_injected_context([{"role": "user", "content": "hello"}])

    assert json.loads(payload) == [{"type": "text", "text": "hello"}]


def test_sanitize_messages_for_audit_redacts_all_message_data_urls() -> None:
    raw = b"snapshot image"
    encoded = base64.b64encode(raw).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()

    messages = sanitize_messages_for_audit(
        [
            {
                "role": "system",
                "content": [{"type": "text", "text": "rules"}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{encoded}"},
                    }
                ],
            },
        ]
    )

    serialized = json.dumps(messages, ensure_ascii=False)
    assert encoded not in serialized
    assert messages[1]["content"][0]["image_url"]["url"] == f"media:sha256:{digest}"
