"""Media asset and media relationship repositories."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from shinbot.persistence.records import (
    MediaAssetRecord,
    MediaSemanticRecord,
    SessionMediaOccurrenceRecord,
)

from .base import Repository


class MediaAssetRepository(Repository):
    """Persistence adapter for raw media asset cache entries."""

    _JSON_FIELDS = {"metadata": ("metadata_json", {})}

    def get(self, raw_hash: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_assets WHERE raw_hash = ?",
                (raw_hash,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: MediaAssetRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT first_seen_at FROM media_assets WHERE raw_hash = ?",
                (payload["raw_hash"],),
            ).fetchone()
            first_seen_at = row["first_seen_at"] if row is not None else payload["first_seen_at"]
            conn.execute(
                """
                INSERT INTO media_assets (
                    raw_hash, element_type, storage_path, mime_type, file_size,
                    strict_dhash, width, height, metadata_json, first_seen_at,
                    last_seen_at, expire_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(raw_hash) DO UPDATE SET
                    element_type = excluded.element_type,
                    storage_path = excluded.storage_path,
                    mime_type = excluded.mime_type,
                    file_size = excluded.file_size,
                    strict_dhash = excluded.strict_dhash,
                    width = excluded.width,
                    height = excluded.height,
                    metadata_json = excluded.metadata_json,
                    last_seen_at = excluded.last_seen_at,
                    expire_at = excluded.expire_at
                """,
                (
                    payload["raw_hash"],
                    payload["element_type"],
                    payload["storage_path"],
                    payload["mime_type"],
                    payload["file_size"],
                    payload["strict_dhash"],
                    payload["width"],
                    payload["height"],
                    self.json_dumps(payload["metadata"]),
                    first_seen_at,
                    payload["last_seen_at"],
                    payload["expire_at"],
                ),
            )

    def list_expired(self, now: float) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM media_assets
                WHERE expire_at < ?
                ORDER BY expire_at ASC, raw_hash ASC
                """,
                (now,),
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def delete(self, raw_hash: str) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM media_assets WHERE raw_hash = ?", (raw_hash,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row, json_fields=self._JSON_FIELDS)


class MessageMediaLinkRepository(Repository):
    """Persistence adapter for message-log to media-asset relations."""

    def replace_for_message(
        self,
        *,
        message_log_id: int,
        session_id: str,
        platform_msg_id: str,
        raw_hashes: list[str],
        created_at: float,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM message_media_links WHERE message_log_id = ?",
                (message_log_id,),
            )
            for media_index, raw_hash in enumerate(raw_hashes):
                conn.execute(
                    """
                    INSERT INTO message_media_links (
                        message_log_id, session_id, platform_msg_id, raw_hash, media_index, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        message_log_id,
                        session_id,
                        platform_msg_id,
                        raw_hash,
                        media_index,
                        created_at,
                    ),
                )

    def list_by_message_log_id(self, message_log_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_media_links
                WHERE message_log_id = ?
                ORDER BY media_index ASC, id ASC
                """,
                (message_log_id,),
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def list_by_platform_msg_id(
        self,
        session_id: str,
        platform_msg_id: str,
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM message_media_links
                WHERE session_id = ? AND platform_msg_id = ?
                ORDER BY message_log_id DESC, media_index ASC, id ASC
                """,
                (session_id, platform_msg_id),
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def get_latest_by_session(self, session_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM message_media_links
                WHERE session_id = ?
                ORDER BY message_log_id DESC, media_index ASC, id ASC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row)


class SessionMediaOccurrenceRepository(Repository):
    """Persistence adapter for per-session image repeat tracking."""

    _JSON_FIELDS = {"recent_timestamps": ("recent_timestamps_json", [])}

    def get(self, session_id: str, raw_hash: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM session_media_occurrences
                WHERE session_id = ? AND raw_hash = ?
                """,
                (session_id, raw_hash),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: SessionMediaOccurrenceRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT first_seen_at FROM session_media_occurrences
                WHERE session_id = ? AND raw_hash = ?
                """,
                (payload["session_id"], payload["raw_hash"]),
            ).fetchone()
            first_seen_at = row["first_seen_at"] if row is not None else payload["first_seen_at"]
            conn.execute(
                """
                INSERT INTO session_media_occurrences (
                    session_id, raw_hash, strict_dhash, last_sender_id,
                    last_platform_msg_id, recent_timestamps_json, occurrence_count,
                    first_seen_at, last_seen_at, expire_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, raw_hash) DO UPDATE SET
                    strict_dhash = excluded.strict_dhash,
                    last_sender_id = excluded.last_sender_id,
                    last_platform_msg_id = excluded.last_platform_msg_id,
                    recent_timestamps_json = excluded.recent_timestamps_json,
                    occurrence_count = excluded.occurrence_count,
                    last_seen_at = excluded.last_seen_at,
                    expire_at = excluded.expire_at
                """,
                (
                    payload["session_id"],
                    payload["raw_hash"],
                    payload["strict_dhash"],
                    payload["last_sender_id"],
                    payload["last_platform_msg_id"],
                    self.json_dumps(payload["recent_timestamps"]),
                    payload["occurrence_count"],
                    first_seen_at,
                    payload["last_seen_at"],
                    payload["expire_at"],
                ),
            )

    def delete_expired(self, now: float) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM session_media_occurrences WHERE expire_at < ?",
                (now,),
            )
            return int(cursor.rowcount or 0)

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row, json_fields=self._JSON_FIELDS)


class MediaSemanticRepository(Repository):
    """Persistence adapter for verified media semantics cache."""

    _JSON_FIELDS = {"metadata": ("metadata_json", {})}

    def get(self, raw_hash: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_semantics WHERE raw_hash = ?",
                (raw_hash,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: MediaSemanticRecord) -> None:
        payload = asdict(record)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT first_seen_at FROM media_semantics WHERE raw_hash = ?",
                (payload["raw_hash"],),
            ).fetchone()
            first_seen_at = row["first_seen_at"] if row is not None else payload["first_seen_at"]
            conn.execute(
                """
                INSERT INTO media_semantics (
                    raw_hash, kind, digest, verified_by_model, inspection_agent_ref,
                    inspection_llm_ref, metadata_json, first_seen_at, last_seen_at,
                    expire_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(raw_hash) DO UPDATE SET
                    kind = excluded.kind,
                    digest = excluded.digest,
                    verified_by_model = excluded.verified_by_model,
                    inspection_agent_ref = excluded.inspection_agent_ref,
                    inspection_llm_ref = excluded.inspection_llm_ref,
                    metadata_json = excluded.metadata_json,
                    last_seen_at = excluded.last_seen_at,
                    expire_at = excluded.expire_at
                """,
                (
                    payload["raw_hash"],
                    payload["kind"],
                    payload["digest"],
                    1 if payload["verified_by_model"] else 0,
                    payload["inspection_agent_ref"],
                    payload["inspection_llm_ref"],
                    self.json_dumps(payload["metadata"]),
                    first_seen_at,
                    payload["last_seen_at"],
                    payload["expire_at"],
                ),
            )

    def touch(self, raw_hash: str, *, last_seen_at: float, expire_at: float) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE media_semantics
                SET last_seen_at = ?, expire_at = ?
                WHERE raw_hash = ?
                """,
                (last_seen_at, expire_at, raw_hash),
            )

    def delete_expired(self, now: float) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM media_semantics WHERE expire_at < ?",
                (now,),
            )
            return int(cursor.rowcount or 0)

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(
            row,
            bool_fields=("verified_by_model",),
            json_fields=self._JSON_FIELDS,
        )
