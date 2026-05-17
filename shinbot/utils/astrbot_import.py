"""Import AstrBot ``platform_message_history`` rows into ShinBot."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from shinbot.persistence.config import DatabaseConfig
from shinbot.persistence.schema import apply_schema
from shinbot.schema.elements import MessageElement

_CST = ZoneInfo("Asia/Shanghai")
_SUPPORTED_PLATFORM_ID = "Shinku"
_SUPPORTED_INSTANCE_ID = "onebot_v11"
_SUPPORTED_PLATFORM = "qq"
_SHINKU_BOT_SENDER_ID = "3575371140"


@dataclass(slots=True)
class ImportStats:
    rows_seen: int = 0
    rows_filtered: int = 0
    sessions_upserted: int = 0
    messages_inserted: int = 0
    messages_skipped: int = 0
    duplicate_messages: int = 0


@dataclass(slots=True)
class SessionTarget:
    session_id: str
    instance_id: str
    session_type: str
    platform: str
    channel_id: str
    display_name: str
    created_at: float
    last_active: float
    source_platform_id: str
    source_user_id: str


@dataclass(slots=True)
class ImportedMessage:
    platform_msg_id: str
    role: str
    sender_id: str
    sender_name: str
    content_json: str
    raw_text: str
    created_at_ms: float
    is_mentioned: bool


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def stream_top_level_array(
    path: Path,
    key: str,
    *,
    chunk_size: int = 65536,
):
    """Yield one object at a time from a top-level JSON array field."""

    decoder = json.JSONDecoder()
    marker = f'"{key}"'

    with path.open("r", encoding="utf-8") as handle:
        buffer = ""
        pos = 0
        found = False

        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            if not found:
                idx = buffer.find(marker)
                if idx == -1:
                    if len(buffer) > len(marker):
                        buffer = buffer[-len(marker) :]
                    continue
                pos = idx + len(marker)
                found = True
                break

        if not found:
            return

        while True:
            while pos < len(buffer) and buffer[pos] in " \r\n\t:":
                pos += 1
            if pos < len(buffer):
                break
            chunk = handle.read(chunk_size)
            if not chunk:
                raise ValueError(f"Unexpected EOF before {key!r} array start")
            buffer += chunk

        if buffer[pos] != "[":
            raise ValueError(f"Expected array for {key!r}")
        pos += 1

        while True:
            while True:
                while pos < len(buffer) and buffer[pos] in " \r\n\t,":
                    pos += 1
                if pos < len(buffer):
                    break
                chunk = handle.read(chunk_size)
                if not chunk:
                    raise ValueError(f"Unexpected EOF inside {key!r} array")
                buffer += chunk

            if buffer[pos] == "]":
                return

            while True:
                try:
                    obj, end = decoder.raw_decode(buffer, pos)
                    break
                except json.JSONDecodeError:
                    chunk = handle.read(chunk_size)
                    if not chunk:
                        raise
                    buffer += chunk

            yield obj
            pos = end

            if pos > chunk_size:
                buffer = buffer[pos:]
                pos = 0


def _parse_iso_seconds(value: str | None, fallback: float) -> float:
    if not value:
        return fallback
    try:
        return datetime.fromisoformat(value).replace(tzinfo=_CST).timestamp()
    except ValueError:
        return fallback


def _map_session_target(row: dict[str, Any]) -> SessionTarget | None:
    platform_id = str(row.get("platform_id") or "").strip()
    if platform_id != _SUPPORTED_PLATFORM_ID:
        return None

    user_id = str(row.get("user_id") or "").strip()
    if not user_id:
        return None

    now = time.time()
    created_at = _parse_iso_seconds(str(row.get("created_at") or ""), now)
    updated_at = _parse_iso_seconds(str(row.get("updated_at") or ""), created_at)
    session_id = f"{_SUPPORTED_INSTANCE_ID}:group:{user_id}:{user_id}"

    return SessionTarget(
        session_id=session_id,
        instance_id=_SUPPORTED_INSTANCE_ID,
        session_type="group",
        platform=_SUPPORTED_PLATFORM,
        channel_id=user_id,
        display_name="",
        created_at=created_at,
        last_active=updated_at,
        source_platform_id=platform_id,
        source_user_id=user_id,
    )


def _classify_role(row: dict[str, Any]) -> str:
    sender_id = str(row.get("sender_id") or "").strip()
    sender_name = str(row.get("sender_name") or "").strip()
    if sender_id == _SHINKU_BOT_SENDER_ID or sender_name == "AstrBot":
        return "assistant"
    return "user"


def _build_elements_from_shinku_content(
    content: Any,
) -> tuple[list[MessageElement], str, bool]:
    if not isinstance(content, list):
        return [], "", False

    elements: list[MessageElement] = []
    raw_text_parts: list[str] = []
    is_mentioned = False

    for item in content:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type") or "").strip().lower()
        data = item.get("data")
        if not isinstance(data, dict):
            data = {}

        if item_type == "text":
            text = str(data.get("text") or "")
            elements.append(MessageElement.text(text))
            raw_text_parts.append(text)
        elif item_type == "at":
            target_id = str(data.get("qq") or "").strip()
            if not target_id:
                continue
            elements.append(MessageElement.at(id=target_id))
            if target_id == _SHINKU_BOT_SENDER_ID:
                is_mentioned = True
        elif item_type == "reply":
            quote_id = str(data.get("id") or "").strip()
            if not quote_id:
                continue
            elements.append(MessageElement.quote(quote_id))
        elif item_type == "poke":
            attrs = {"type": str(data.get("type") or "poke")}
            target_id = str(data.get("id") or "").strip()
            if target_id:
                attrs["target"] = target_id
            elements.append(MessageElement(type="sb:poke", attrs=attrs))
        elif item_type in {"image", "file"}:
            continue

    return elements, "".join(raw_text_parts), is_mentioned


def _normalize_platform_message_history_row(
    row: dict[str, Any],
) -> ImportedMessage | None:
    target = _map_session_target(row)
    if target is None:
        return None

    elements, raw_text, is_mentioned = _build_elements_from_shinku_content(row.get("content"))
    if not elements:
        return None

    sender_id = str(row.get("sender_id") or "").strip()
    sender_name = str(row.get("sender_name") or "").strip()
    created_at_ms = _parse_iso_seconds(str(row.get("created_at") or ""), time.time()) * 1000

    return ImportedMessage(
        platform_msg_id=f"astrbot:platform_message_history:{row.get('id')}",
        role=_classify_role(row),
        sender_id=sender_id,
        sender_name=sender_name,
        content_json=_json_dumps([element.model_dump(mode="json") for element in elements]),
        raw_text=raw_text,
        created_at_ms=created_at_ms,
        is_mentioned=is_mentioned,
    )


def _upsert_session(conn: sqlite3.Connection, target: SessionTarget) -> None:
    plugin_data = {
        "astrbot_import": {
            "source": "platform_message_history",
            "source_platform_id": target.source_platform_id,
            "source_user_id": target.source_user_id,
        }
    }

    conn.execute(
        """
        INSERT INTO sessions (
            id, instance_id, session_type, platform, guild_id, channel_id, display_name,
            permission_group, created_at, last_active, state_json, plugin_data_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            last_active = CASE
                WHEN excluded.last_active > sessions.last_active THEN excluded.last_active
                ELSE sessions.last_active
            END,
            plugin_data_json = excluded.plugin_data_json
        """,
        (
            target.session_id,
            target.instance_id,
            target.session_type,
            target.platform,
            None,
            target.channel_id,
            target.display_name,
            "default",
            target.created_at,
            target.last_active,
            _json_dumps({}),
            _json_dumps(plugin_data),
        ),
    )
    conn.execute(
        """
        INSERT INTO session_configs (
            session_id, prefixes_json, llm_enabled, is_muted, audit_enabled, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(session_id) DO UPDATE SET
            updated_at = excluded.updated_at
        """,
        (
            target.session_id,
            _json_dumps(["/"]),
            1,
            0,
            0,
            time.time(),
        ),
    )


def _load_existing_platform_msg_ids(conn: sqlite3.Connection, session_id: str) -> set[str]:
    rows = conn.execute(
        "SELECT platform_msg_id FROM message_logs WHERE session_id = ?",
        (session_id,),
    ).fetchall()
    return {str(row[0]) for row in rows if row[0]}


def _insert_message(
    conn: sqlite3.Connection,
    target: SessionTarget,
    message: ImportedMessage,
) -> None:
    conn.execute(
        """
        INSERT INTO message_logs (
            session_id, platform_msg_id, sender_id, sender_name,
            content_json, raw_text, role, is_read, is_mentioned, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            target.session_id,
            message.platform_msg_id,
            message.sender_id,
            message.sender_name,
            message.content_json,
            message.raw_text,
            message.role,
            1,
            1 if message.is_mentioned else 0,
            message.created_at_ms,
        ),
    )


def import_astrbot_export(
    *,
    json_path: Path,
    data_dir: Path,
    dry_run: bool = False,
) -> ImportStats:
    """Import AstrBot ``platform_message_history`` rows for the Shinku platform."""

    config = DatabaseConfig.from_bootstrap(data_dir=data_dir)
    stats = ImportStats()

    config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(config.sqlite_path, check_same_thread=False) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        apply_schema(conn)

        existing_ids_by_session: dict[str, set[str]] = {}
        seen_sessions: set[str] = set()

        for row in stream_top_level_array(json_path, "platform_message_history"):
            stats.rows_seen += 1
            target = _map_session_target(row)
            if target is None:
                stats.rows_filtered += 1
                continue

            if target.session_id not in seen_sessions:
                _upsert_session(conn, target)
                seen_sessions.add(target.session_id)
                stats.sessions_upserted += 1

            existing_ids = existing_ids_by_session.get(target.session_id)
            if existing_ids is None:
                existing_ids = _load_existing_platform_msg_ids(conn, target.session_id)
                existing_ids_by_session[target.session_id] = existing_ids

            message = _normalize_platform_message_history_row(row)
            if message is None:
                stats.messages_skipped += 1
                continue

            if message.platform_msg_id in existing_ids:
                stats.duplicate_messages += 1
                continue

            if not dry_run:
                _insert_message(conn, target, message)
            existing_ids.add(message.platform_msg_id)
            stats.messages_inserted += 1

    return stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", required=True, type=Path, help="AstrBot 导出的 JSON 文件")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="ShinBot data 目录，默认 data",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只统计将导入的数据，不实际写入数据库",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    stats = import_astrbot_export(
        json_path=args.json,
        data_dir=args.data_dir,
        dry_run=args.dry_run,
    )
    print(
        json.dumps(
            {
                "json": str(args.json),
                "data_dir": str(args.data_dir),
                "dry_run": args.dry_run,
                "rows_seen": stats.rows_seen,
                "rows_filtered": stats.rows_filtered,
                "sessions_upserted": stats.sessions_upserted,
                "messages_inserted": stats.messages_inserted,
                "messages_skipped": stats.messages_skipped,
                "duplicate_messages": stats.duplicate_messages,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
