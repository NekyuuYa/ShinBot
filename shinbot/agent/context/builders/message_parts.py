"""Normalize stored message AST payloads into formatter-friendly parts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class NormalizedImagePart:
    raw_hash: str = ""
    strict_dhash: str = ""
    source_path: str = ""
    is_custom_emoji: bool = False


@dataclass(slots=True)
class NormalizedMessagePart:
    kind: str
    text: str = ""
    platform_id: str = ""
    display_name: str = ""
    quote_id: str = ""
    image: NormalizedImagePart | None = None


def parse_message_parts(
    record: dict[str, Any],
    *,
    self_platform_id: str = "",
) -> list[NormalizedMessagePart]:
    content_json = str(record.get("content_json", "") or "").strip()
    if not content_json:
        text = str(record.get("raw_text", "") or "").strip()
        return [NormalizedMessagePart(kind="text", text=text)] if text else []

    try:
        payload = json.loads(content_json)
    except json.JSONDecodeError:
        text = str(record.get("raw_text", "") or "").strip()
        return [NormalizedMessagePart(kind="text", text=text)] if text else []
    if not isinstance(payload, list):
        return []

    parts: list[NormalizedMessagePart] = []
    _walk_payload(payload, parts, self_platform_id=self_platform_id)
    return parts


def _walk_payload(
    payload: list[object],
    parts: list[NormalizedMessagePart],
    *,
    self_platform_id: str,
) -> None:
    for item in payload:
        if not isinstance(item, dict):
            continue
        element_type = str(item.get("type", "") or "").strip()
        attrs = item.get("attrs") if isinstance(item.get("attrs"), dict) else {}

        if element_type == "text":
            _append_text(parts, str(attrs.get("content", "") or ""))
        elif element_type == "br":
            _append_text(parts, "\n")
        elif element_type == "emoji":
            label = str(attrs.get("name", "") or attrs.get("id", "") or "emoji").strip()
            _append_text(parts, f"[表情:{label}]")
        elif element_type == "at":
            target_id = str(attrs.get("id", "") or "").strip()
            target_name = str(attrs.get("name", "") or "").strip()
            if target_id and self_platform_id and target_id == self_platform_id:
                target_name = target_name or "你"
            parts.append(
                NormalizedMessagePart(
                    kind="mention",
                    platform_id=target_id,
                    display_name=target_name,
                )
            )
        elif element_type == "sb:poke":
            target_id = str(attrs.get("target", "") or "").strip()
            parts.append(
                NormalizedMessagePart(
                    kind="poke",
                    platform_id=target_id,
                    display_name="你" if target_id and target_id == self_platform_id else "",
                )
            )
        elif element_type == "quote":
            parts.append(
                NormalizedMessagePart(
                    kind="quote",
                    quote_id=str(attrs.get("id", "") or "").strip(),
                )
            )
        elif element_type == "img":
            parts.append(
                NormalizedMessagePart(
                    kind="image",
                    image=_resolve_image_part(attrs),
                )
            )

        children = item.get("children")
        if isinstance(children, list):
            _walk_payload(children, parts, self_platform_id=self_platform_id)


def _resolve_image_part(attrs: dict[str, Any]) -> NormalizedImagePart:
    from shinbot.agent.media.classification import is_emoji_image_sub_type
    from shinbot.agent.media.fingerprint import fingerprint_image_file

    src = str(attrs.get("src", "") or "").strip()
    is_custom_emoji = is_emoji_image_sub_type(
        attrs.get("sub_type"),
        has_sub_type="sub_type" in attrs,
    )
    if not src:
        return NormalizedImagePart(is_custom_emoji=is_custom_emoji)

    candidate = Path(src).expanduser()
    if not candidate.is_file():
        return NormalizedImagePart(source_path=str(candidate), is_custom_emoji=is_custom_emoji)

    fingerprint = fingerprint_image_file(candidate)
    if fingerprint is None:
        return NormalizedImagePart(source_path=str(candidate), is_custom_emoji=is_custom_emoji)
    return NormalizedImagePart(
        raw_hash=fingerprint.raw_hash,
        strict_dhash=fingerprint.strict_dhash,
        source_path=fingerprint.storage_path,
        is_custom_emoji=is_custom_emoji,
    )


def _append_text(parts: list[NormalizedMessagePart], text: str) -> None:
    if not text:
        return
    if parts and parts[-1].kind == "text":
        parts[-1].text += text
        return
    parts.append(NormalizedMessagePart(kind="text", text=text))
