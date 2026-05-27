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
    """Persistence adapter for raw media asset cache entries.

    Stores media files (images, audio, etc.) keyed by their content hash
    for deduplication across the system. Each asset tracks storage location,
    dimensions, MIME type, and arbitrary metadata.
    """

    _JSON_FIELDS = {"metadata": ("metadata_json", {})}

    def get(self, raw_hash: str) -> dict[str, Any] | None:
        """Return the asset entry for *raw_hash*, or ``None`` if absent."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_assets WHERE raw_hash = ?",
                (raw_hash,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: MediaAssetRecord) -> None:
        """Insert or update a media asset record.

        When an entry with the same ``raw_hash`` already exists the metadata
        dictionaries are merged (new values win) and timestamps are updated.
        """
        payload = asdict(record)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT first_seen_at, metadata_json FROM media_assets WHERE raw_hash = ?",
                (payload["raw_hash"],),
            ).fetchone()
            first_seen_at = row["first_seen_at"] if row is not None else payload["first_seen_at"]
            metadata = dict(payload["metadata"])
            if row is not None:
                metadata = {
                    **self.json_loads(row["metadata_json"], {}),
                    **metadata,
                }
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
                    self.json_dumps(metadata),
                    first_seen_at,
                    payload["last_seen_at"],
                    payload["expire_at"],
                ),
            )

    def update_metadata(self, raw_hash: str, metadata: dict[str, Any]) -> None:
        """Replace the JSON metadata blob for the asset identified by *raw_hash*."""
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE media_assets
                SET metadata_json = ?
                WHERE raw_hash = ?
                """,
                (self.json_dumps(metadata), raw_hash),
            )

    def list_expired(self, now: float) -> list[dict[str, Any]]:
        """Return all assets whose ``expire_at`` timestamp is before *now*.

        Results are ordered oldest-first by expiry time.
        """
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
        """Delete the asset entry identified by *raw_hash*."""
        with self.connect() as conn:
            conn.execute("DELETE FROM media_assets WHERE raw_hash = ?", (raw_hash,))

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row, json_fields=self._JSON_FIELDS)


class MessageMediaLinkRepository(Repository):
    """Persistence adapter for message-log to media-asset relations.

    Maps individual media assets (by hash) to the message-log entries they
    belong to, preserving the original ordering and platform identifiers.
    """

    def replace_for_message(
        self,
        *,
        message_log_id: int,
        session_id: str,
        platform_msg_id: str,
        raw_hashes: list[str],
        created_at: float,
    ) -> None:
        """Replace all media links for a message-log entry.

        Removes every existing link for *message_log_id* and inserts fresh
        entries for each *raw_hashes* value in order.
        """
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
        """Return all media links associated with *message_log_id*.

        Results are ordered by ``media_index`` ascending.
        """
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
        """Return all media links for a platform message within a session.

        Results are ordered by ``message_log_id`` descending (newest first),
        then by ``media_index`` ascending.
        """
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
        """Return the most-recent media link in *session_id*, or ``None``."""
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
    """Persistence adapter for per-session image repeat tracking.

    Tracks how many times a particular media asset (by hash) has appeared
    within a chat session, including sender identity and recent timestamps.
    """

    _JSON_FIELDS = {"recent_timestamps": ("recent_timestamps_json", [])}

    def get(self, session_id: str, raw_hash: str) -> dict[str, Any] | None:
        """Return the occurrence record for a session/asset pair, or ``None``."""
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
        """Insert or update an occurrence record.

        When an entry already exists for the (session_id, raw_hash) pair the
        occurrence count and recent timestamps are overwritten; the original
        ``first_seen_at`` is preserved.
        """
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
        """Delete all occurrence records whose ``expire_at`` is before *now*.

        Returns the number of rows removed.
        """
        with self.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM session_media_occurrences WHERE expire_at < ?",
                (now,),
            )
            return int(cursor.rowcount or 0)

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(row, json_fields=self._JSON_FIELDS)


class MediaSemanticRepository(Repository):
    """Persistence adapter for verified media semantics cache.

    Stores the result of LLM-based or agent-based inspection of media
    assets (e.g. image content classification). Entries are keyed by
    content hash and also indexable by perceptual hash (strict dhash).
    """

    _JSON_FIELDS = {"metadata": ("metadata_json", {})}

    def get(self, raw_hash: str) -> dict[str, Any] | None:
        """Return the semantic record for *raw_hash*, or ``None``."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_semantics WHERE raw_hash = ?",
                (raw_hash,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def get_by_strict_dhash(self, strict_dhash: str) -> dict[str, Any] | None:
        """Return the first semantic record matching a perceptual hash.

        *strict_dhash* is stripped before lookup; returns ``None`` when
        empty or when no match is found.
        """
        normalized = strict_dhash.strip()
        if not normalized:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM media_semantics WHERE strict_dhash = ? LIMIT 1",
                (normalized,),
            ).fetchone()
        return self._row_to_payload(row) if row is not None else None

    def upsert(self, record: MediaSemanticRecord) -> None:
        """Insert or update a semantic record.

        On conflict the original ``first_seen_at`` is preserved while all
        other fields are replaced with the values from *record*.
        """
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
                    raw_hash, strict_dhash, kind, digest, verified_by_model, inspection_agent_ref,
                    inspection_llm_ref, metadata_json, first_seen_at, last_seen_at, expire_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(raw_hash) DO UPDATE SET
                    strict_dhash = excluded.strict_dhash,
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
                    payload["strict_dhash"],
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
        """Refresh the timestamp fields of an existing semantic record.

        Updates ``last_seen_at`` and ``expire_at`` without touching any
        other columns.
        """
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
        """Delete all semantic records whose ``expire_at`` is before *now*.

        Returns the number of rows removed.
        """
        with self.connect() as conn:
            cursor = conn.execute(
                "DELETE FROM media_semantics WHERE expire_at < ?",
                (now,),
            )
            return int(cursor.rowcount or 0)

    def list_recent(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Return up to *limit* semantic records ordered by most recently seen."""
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM media_semantics
                ORDER BY last_seen_at DESC, raw_hash DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_payload(row) for row in rows]

    def _row_to_payload(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(
            row,
            bool_fields=("verified_by_model",),
            json_fields=self._JSON_FIELDS,
        )
