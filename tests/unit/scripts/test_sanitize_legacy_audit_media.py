from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
from pathlib import Path

from scripts.sanitize_legacy_audit_media import sanitize_legacy_audit_media

RAW_IMAGE = b"legacy image" * 128


def test_dry_run_reports_savings_without_mutating_db(tmp_path: Path) -> None:
    db_path = tmp_path / "shinbot.sqlite3"
    encoded = _seed_legacy_audit_db(db_path)

    summary = sanitize_legacy_audit_media(db_path)

    assert summary.applied is False
    assert summary.bytes_saved_in_payloads > 0
    assert summary.columns[0].rows_matched == 1
    assert summary.columns[0].rows_updated == 1
    assert summary.columns[1].rows_matched == 1
    assert summary.columns[1].rows_updated == 1
    assert encoded in _read_payloads(db_path)


def test_apply_rewrites_inline_image_payloads_to_hash_references(tmp_path: Path) -> None:
    db_path = tmp_path / "shinbot.sqlite3"
    encoded = _seed_legacy_audit_db(db_path)
    digest = hashlib.sha256(RAW_IMAGE).hexdigest()

    summary = sanitize_legacy_audit_media(db_path, apply=True)

    assert summary.applied is True
    assert summary.bytes_saved_in_payloads > 0
    payloads = _read_payloads(db_path)
    assert encoded not in payloads
    assert f"media:sha256:{digest}" in payloads
    assert '"redacted": true' in payloads


def test_invalid_legacy_json_is_reported_and_left_untouched(tmp_path: Path) -> None:
    db_path = tmp_path / "shinbot.sqlite3"
    _create_legacy_audit_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO ai_interactions (id, injected_context_json) VALUES (?, ?)",
            (1, '[{"type":"image_url","image_url":{"url":"data:image/png;base64,'),
        )
        conn.commit()
    finally:
        conn.close()

    summary = sanitize_legacy_audit_media(db_path, apply=True)

    assert summary.columns[0].rows_failed == 1
    assert summary.warnings
    assert "data:image/png;base64" in _read_payloads(db_path)


def _seed_legacy_audit_db(db_path: Path) -> str:
    _create_legacy_audit_schema(db_path)
    encoded = base64.b64encode(RAW_IMAGE).decode("ascii")
    image_block = {
        "type": "image_url",
        "image_url": {"url": f"data:image/png;base64,{encoded}", "detail": "low"},
    }
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO ai_interactions (id, injected_context_json) VALUES (?, ?)",
            (
                1,
                json.dumps([{"type": "text", "text": "inspect"}, image_block]),
            ),
        )
        conn.execute(
            "INSERT INTO prompt_snapshots (id, messages_json) VALUES (?, ?)",
            (
                "snap-1",
                json.dumps(
                    [
                        {
                            "role": "user",
                            "content": [{"type": "text", "text": "inspect"}, image_block],
                        }
                    ]
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return encoded


def _create_legacy_audit_schema(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE ai_interactions (
                id INTEGER PRIMARY KEY,
                injected_context_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE prompt_snapshots (
                id TEXT PRIMARY KEY,
                messages_json TEXT NOT NULL DEFAULT '[]'
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _read_payloads(db_path: Path) -> str:
    conn = sqlite3.connect(db_path)
    try:
        ai_payload = conn.execute(
            "SELECT injected_context_json FROM ai_interactions ORDER BY id"
        ).fetchall()
        snapshot_payload = conn.execute(
            "SELECT messages_json FROM prompt_snapshots ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return "\n".join(row[0] for row in [*ai_payload, *snapshot_payload])
