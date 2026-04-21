"""Runtime media ingestion and config helpers."""

from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.agent.media.config import (
    ResolvedMediaInspectionConfig,
    resolve_media_inspection_config,
)
from shinbot.agent.media.fingerprint import MediaFingerprint, fingerprint_image_file
from shinbot.persistence.records import MediaAssetRecord, SessionMediaOccurrenceRecord
from shinbot.schema.elements import MessageElement

if TYPE_CHECKING:
    from shinbot.persistence.engine import DatabaseManager

RAW_MEDIA_TTL_SECONDS = 30 * 24 * 60 * 60
SESSION_OCCURRENCE_TTL_SECONDS = 60 * 24 * 60 * 60
SEMANTIC_TTL_SECONDS = 180 * 24 * 60 * 60
SESSION_REPEAT_WINDOW_SECONDS = 14 * 24 * 60 * 60
MEME_VERIFICATION_THRESHOLD = 3


@dataclass(slots=True)
class IngestedMediaItem:
    raw_hash: str
    strict_dhash: str
    storage_path: str
    occurrence_count: int
    should_request_inspection: bool
    known_kind: str = ""
    known_digest: str = ""
    is_custom_emoji: bool = False


class MediaService:
    """Stores media fingerprints and session-local repeat state."""

    def __init__(self, database: DatabaseManager) -> None:
        self._database = database

    def resolve_inspection_config(self, instance_id: str) -> ResolvedMediaInspectionConfig:
        bot_config = self._database.bot_configs.get_by_instance_id(instance_id)
        return resolve_media_inspection_config(bot_config)

    def ingest_message_media(
        self,
        *,
        session_id: str,
        sender_id: str,
        platform_msg_id: str,
        elements: list[MessageElement],
        message_log_id: int | None = None,
        seen_at: float | None = None,
    ) -> list[IngestedMediaItem]:
        observed_at = seen_at if seen_at is not None else time.time()
        results: list[IngestedMediaItem] = []
        linked_raw_hashes: list[str] = []

        for source_path, is_custom_emoji in self._iter_local_image_paths(elements):
            fingerprint = fingerprint_image_file(source_path)
            if fingerprint is None:
                continue
            linked_raw_hashes.append(fingerprint.raw_hash)

            self._database.media_assets.upsert(
                MediaAssetRecord(
                    raw_hash=fingerprint.raw_hash,
                    element_type="img",
                    storage_path=fingerprint.storage_path,
                    mime_type=fingerprint.mime_type,
                    file_size=fingerprint.file_size,
                    strict_dhash=fingerprint.strict_dhash,
                    width=fingerprint.width,
                    height=fingerprint.height,
                    metadata={},
                    first_seen_at=observed_at,
                    last_seen_at=observed_at,
                    expire_at=observed_at + RAW_MEDIA_TTL_SECONDS,
                )
            )

            occurrence = self._record_session_occurrence(
                session_id=session_id,
                sender_id=sender_id,
                platform_msg_id=platform_msg_id,
                fingerprint=fingerprint,
                seen_at=observed_at,
            )

            semantics = self._database.media_semantics.get(fingerprint.raw_hash)
            known_kind = ""
            known_digest = ""
            if semantics is not None:
                self._database.media_semantics.touch(
                    fingerprint.raw_hash,
                    last_seen_at=observed_at,
                    expire_at=observed_at + SEMANTIC_TTL_SECONDS,
                )
                known_kind = str(semantics.get("kind") or "")
                known_digest = str(semantics.get("digest") or "")

            already_verified = bool(semantics and semantics.get("verified_by_model"))
            if is_custom_emoji:
                # Custom stickers/emoji: inspect on first appearance; skip if already verified.
                should_request_inspection = not already_verified
            else:
                # Normal images: only inspect after repeated occurrences.
                should_request_inspection = (
                    occurrence["occurrence_count"] >= MEME_VERIFICATION_THRESHOLD
                    and not already_verified
                )

            results.append(
                IngestedMediaItem(
                    raw_hash=fingerprint.raw_hash,
                    strict_dhash=fingerprint.strict_dhash,
                    storage_path=fingerprint.storage_path,
                    occurrence_count=int(occurrence["occurrence_count"]),
                    should_request_inspection=should_request_inspection,
                    known_kind=known_kind,
                    known_digest=known_digest,
                    is_custom_emoji=is_custom_emoji,
                )
            )

        if message_log_id is not None and linked_raw_hashes:
            self._database.message_media_links.replace_for_message(
                message_log_id=message_log_id,
                session_id=session_id,
                platform_msg_id=platform_msg_id,
                raw_hashes=linked_raw_hashes,
                created_at=observed_at,
            )

        return results

    def cleanup_expired(self, *, now: float | None = None) -> dict[str, int]:
        cutoff = now if now is not None else time.time()
        deleted_files = 0
        expired_assets = self._database.media_assets.list_expired(cutoff)
        for asset in expired_assets:
            storage_path = str(asset.get("storage_path") or "").strip()
            if storage_path:
                try:
                    os.remove(storage_path)
                    deleted_files += 1
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
            self._database.media_assets.delete(str(asset["raw_hash"]))

        deleted_occurrences = self._database.session_media_occurrences.delete_expired(cutoff)
        deleted_semantics = self._database.media_semantics.delete_expired(cutoff)
        return {
            "deleted_assets": len(expired_assets),
            "deleted_files": deleted_files,
            "deleted_occurrences": deleted_occurrences,
            "deleted_semantics": deleted_semantics,
        }

    def resolve_message_raw_hash(
        self,
        *,
        session_id: str,
        raw_hash: str = "",
        message_log_id: int | None = None,
        platform_msg_id: str = "",
        fallback_to_latest: bool = True,
    ) -> str | None:
        if raw_hash:
            asset = self._database.media_assets.get(raw_hash)
            if asset is not None:
                return raw_hash

        if message_log_id is not None:
            links = self._database.message_media_links.list_by_message_log_id(message_log_id)
            if links:
                return str(links[0]["raw_hash"])
            record = self._database.message_logs.get(message_log_id)
            return self._extract_first_raw_hash_from_record(record)

        if platform_msg_id:
            links = self._database.message_media_links.list_by_platform_msg_id(
                session_id,
                platform_msg_id,
            )
            if links:
                return str(links[0]["raw_hash"])
            record = self._database.message_logs.get_by_platform_msg_id(session_id, platform_msg_id)
            return self._extract_first_raw_hash_from_record(record)

        if not fallback_to_latest:
            return None

        latest_link = self._database.message_media_links.get_latest_by_session(session_id)
        if latest_link is not None:
            return str(latest_link["raw_hash"])

        for record in reversed(self._database.message_logs.get_recent(session_id, limit=20)):
            raw_hash_candidate = self._extract_first_raw_hash_from_record(record)
            if raw_hash_candidate:
                return raw_hash_candidate
        return None

    def summarize_message_media(
        self,
        record: dict[str, object] | None,
    ) -> list[str]:
        raw_hashes = self._resolve_record_raw_hashes(record)
        parts: list[str] = []
        for raw_hash in raw_hashes:
            semantics = self._database.media_semantics.get(raw_hash)
            if semantics is not None:
                kind = str(semantics.get("kind") or "").strip()
                digest = str(semantics.get("digest") or "").strip()
                if digest:
                    label = "图片"
                    if kind == "meme_image":
                        label = "表情"
                    elif kind == "emoji_native":
                        label = "表情符号"
                    parts.append(f"[{label}: {digest}]")
                    continue
            asset = self._database.media_assets.get(raw_hash)
            if asset is not None:
                parts.append("[图片]")
        return parts

    def get_message_image_data_urls(
        self,
        record: dict[str, object] | None,
    ) -> list[dict[str, Any]]:
        """Return image_url content blocks for all image assets linked to a message."""
        raw_hashes = self._resolve_record_raw_hashes(record)
        blocks: list[dict[str, Any]] = []
        for raw_hash in raw_hashes:
            asset = self._database.media_assets.get(raw_hash)
            if asset is None:
                continue
            storage_path = str(asset.get("storage_path") or "").strip()
            if not storage_path:
                continue
            try:
                with open(storage_path, "rb") as fh:
                    data = fh.read()
                mime_type = str(asset.get("mime_type") or "").strip() or "image/jpeg"
                encoded = base64.b64encode(data).decode("ascii")
                url = f"data:{mime_type};base64,{encoded}"
                blocks.append({"type": "image_url", "image_url": {"url": url}})
            except OSError:
                pass
        return blocks

    def _record_session_occurrence(
        self,
        *,
        session_id: str,
        sender_id: str,
        platform_msg_id: str,
        fingerprint: MediaFingerprint,
        seen_at: float,
    ) -> dict[str, object]:
        existing = self._database.session_media_occurrences.get(session_id, fingerprint.raw_hash)
        recent_timestamps = list(existing.get("recent_timestamps", [])) if existing else []
        cutoff = seen_at - SESSION_REPEAT_WINDOW_SECONDS
        pruned = [float(timestamp) for timestamp in recent_timestamps if float(timestamp) >= cutoff]
        pruned.append(seen_at)

        self._database.session_media_occurrences.upsert(
            SessionMediaOccurrenceRecord(
                session_id=session_id,
                raw_hash=fingerprint.raw_hash,
                strict_dhash=fingerprint.strict_dhash,
                last_sender_id=sender_id,
                last_platform_msg_id=platform_msg_id,
                recent_timestamps=pruned,
                occurrence_count=len(pruned),
                first_seen_at=float(existing.get("first_seen_at", seen_at))
                if existing
                else seen_at,
                last_seen_at=seen_at,
                expire_at=seen_at + SESSION_OCCURRENCE_TTL_SECONDS,
            )
        )
        return {
            "occurrence_count": len(pruned),
            "recent_timestamps": pruned,
        }

    def _extract_first_raw_hash_from_record(
        self,
        record: dict[str, object] | None,
    ) -> str | None:
        raw_hashes = self._resolve_record_raw_hashes(record)
        return raw_hashes[0] if raw_hashes else None

    def _resolve_record_raw_hashes(
        self,
        record: dict[str, object] | None,
    ) -> list[str]:
        if not record:
            return []

        record_id = record.get("id")
        if isinstance(record_id, int):
            links = self._database.message_media_links.list_by_message_log_id(record_id)
            if links:
                return [str(link["raw_hash"]) for link in links]

        session_id = str(record.get("session_id") or "")
        platform_msg_id = str(record.get("platform_msg_id") or "")
        if session_id and platform_msg_id:
            links = self._database.message_media_links.list_by_platform_msg_id(
                session_id,
                platform_msg_id,
            )
            if links:
                return [str(link["raw_hash"]) for link in links]

        return self._extract_raw_hashes_from_record(record)

    def _extract_raw_hashes_from_record(
        self,
        record: dict[str, object] | None,
    ) -> list[str]:
        if not record:
            return []
        content_json = str(record.get("content_json") or "")
        if not content_json:
            return []
        try:
            payload = json.loads(content_json)
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []

        raw_hashes: list[str] = []
        for source_path in self._iter_local_image_paths_from_payload(payload):
            fingerprint = fingerprint_image_file(source_path)
            if fingerprint is None:
                continue
            raw_hashes.append(fingerprint.raw_hash)
        return raw_hashes

    def _iter_local_image_paths(self, elements: list[MessageElement]) -> list[tuple[Path, bool]]:
        """Return (local_path, is_custom_emoji) pairs for every img element."""
        paths: list[tuple[Path, bool]] = []
        for element in elements:
            if element.type == "img":
                src = str(element.attrs.get("src") or "").strip()
                if src:
                    candidate = Path(src).expanduser()
                    if candidate.is_file():
                        sub_type = str(element.attrs.get("sub_type") or "").strip()
                        paths.append((candidate, sub_type == "1"))
            if element.children:
                paths.extend(self._iter_local_image_paths(element.children))
        return paths

    def _iter_local_image_paths_from_payload(self, payload: list[object]) -> list[Path]:
        paths: list[Path] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "") == "img":
                attrs = item.get("attrs")
                if isinstance(attrs, dict):
                    src = str(attrs.get("src") or "").strip()
                    if src:
                        candidate = Path(src).expanduser()
                        if candidate.is_file():
                            paths.append(candidate)
            children = item.get("children")
            if isinstance(children, list):
                paths.extend(self._iter_local_image_paths_from_payload(children))
        return paths
