#!/usr/bin/env python3
"""One-shot sanitizer for legacy audit rows with inline media payloads."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shinbot.agent.services.model_runtime.extraction import (
    extract_injected_context,
    sanitize_messages_for_audit,
)


@dataclass(frozen=True, slots=True)
class SanitizationTarget:
    table: str
    pk_column: str
    payload_column: str
    sanitizer: Callable[[str], str]


@dataclass(slots=True)
class SanitizedColumnSummary:
    table: str
    column: str
    rows_matched: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    bytes_before: int = 0
    bytes_after: int = 0

    @property
    def bytes_saved(self) -> int:
        return max(0, self.bytes_before - self.bytes_after)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "column": self.column,
            "rowsMatched": self.rows_matched,
            "rowsUpdated": self.rows_updated,
            "rowsFailed": self.rows_failed,
            "bytesBefore": self.bytes_before,
            "bytesAfter": self.bytes_after,
            "bytesSaved": self.bytes_saved,
        }


@dataclass(slots=True)
class SanitizationSummary:
    db_path: Path
    applied: bool
    vacuumed: bool = False
    db_bytes_before: int = 0
    db_bytes_after: int = 0
    columns: list[SanitizedColumnSummary] = field(default_factory=list)
    dropped_tables: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def bytes_saved_in_payloads(self) -> int:
        return sum(item.bytes_saved for item in self.columns)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dbPath": str(self.db_path),
            "applied": self.applied,
            "vacuumed": self.vacuumed,
            "dbBytesBefore": self.db_bytes_before,
            "dbBytesAfter": self.db_bytes_after,
            "payloadBytesSaved": self.bytes_saved_in_payloads,
            "columns": [item.to_dict() for item in self.columns],
            "droppedTables": list(self.dropped_tables),
            "warnings": list(self.warnings),
        }


TARGETS = (
    SanitizationTarget(
        table="ai_interactions",
        pk_column="id",
        payload_column="injected_context_json",
        sanitizer=lambda value: extract_injected_context(
            [{"role": "user", "content": _load_json_list_or_string(value)}]
        ),
    ),
    SanitizationTarget(
        table="prompt_snapshots",
        pk_column="id",
        payload_column="messages_json",
        sanitizer=lambda value: json.dumps(
            sanitize_messages_for_audit(_load_json_list(value)),
            ensure_ascii=False,
        ),
    ),
)
MIGRATED_CONFIG_TABLES = (
    "model_route_members",
    "model_definitions",
    "model_routes",
    "model_providers",
    "agents",
    "context_strategies",
    "personas",
    "prompt_definitions",
    "bot_configs",
)


class LegacyAuditMediaSanitizationError(RuntimeError):
    """Raised when a legacy audit media sanitization cannot run safely."""


def sanitize_legacy_audit_media(
    db_path: Path | str,
    *,
    apply: bool = False,
    vacuum: bool = False,
    drop_migrated_config_tables: bool = False,
) -> SanitizationSummary:
    """Replace inline data URLs in legacy audit rows with hash references.

    The routine is intentionally narrow: it only rewrites historical audit/snapshot
    columns that could contain provider-call media payloads.  It can also remove
    legacy configuration tables after those records have been exported to files.
    It does not delete message, media, cost, or runtime history.
    """

    sqlite_path = Path(db_path)
    if not sqlite_path.is_file():
        raise LegacyAuditMediaSanitizationError(f"Database was not found: {sqlite_path}")

    summary = SanitizationSummary(
        db_path=sqlite_path,
        applied=apply,
        db_bytes_before=sqlite_path.stat().st_size,
        db_bytes_after=sqlite_path.stat().st_size,
    )
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        for target in TARGETS:
            if not _has_table_column(conn, target.table, target.payload_column):
                summary.warnings.append(
                    f"Skipped {target.table}.{target.payload_column}: column was not found"
                )
                continue
            summary.columns.append(_sanitize_target(conn, target, apply=apply, summary=summary))
        if drop_migrated_config_tables:
            summary.dropped_tables = _drop_migrated_config_tables(conn, apply=apply)
        if apply:
            conn.commit()
            if vacuum:
                conn.execute("VACUUM")
                summary.vacuumed = True
            summary.db_bytes_after = sqlite_path.stat().st_size
        else:
            conn.rollback()
    finally:
        conn.close()
    if not apply and vacuum:
        summary.warnings.append("Ignored --vacuum because --apply was not set")
    return summary


def _sanitize_target(
    conn: sqlite3.Connection,
    target: SanitizationTarget,
    *,
    apply: bool,
    summary: SanitizationSummary,
) -> SanitizedColumnSummary:
    table = _quote_identifier(target.table)
    pk_column = _quote_identifier(target.pk_column)
    payload_column = _quote_identifier(target.payload_column)
    column_summary = SanitizedColumnSummary(
        table=target.table,
        column=target.payload_column,
    )
    rows = conn.execute(
        f"""
        SELECT {pk_column} AS pk, {payload_column} AS payload
        FROM {table}
        WHERE {payload_column} LIKE '%data:image%'
           OR {payload_column} LIKE '%data:%;base64%'
        """
    )
    for row in rows:
        original = str(row["payload"] or "")
        column_summary.rows_matched += 1
        column_summary.bytes_before += _byte_len(original)
        try:
            sanitized = target.sanitizer(original)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            column_summary.rows_failed += 1
            column_summary.bytes_after += _byte_len(original)
            summary.warnings.append(
                f"Skipped {target.table}.{target.payload_column} row {row['pk']!r}: {exc}"
            )
            continue

        column_summary.bytes_after += _byte_len(sanitized)
        if sanitized == original:
            continue
        column_summary.rows_updated += 1
        if apply:
            conn.execute(
                f"UPDATE {table} SET {payload_column} = ? WHERE {pk_column} = ?",
                (sanitized, row["pk"]),
            )
    return column_summary


def _load_json_list(value: str) -> list[dict[str, Any]]:
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        raise ValueError("expected a JSON list")
    return [item for item in parsed if isinstance(item, dict)]


def _load_json_list_or_string(value: str) -> list[dict[str, Any]] | str:
    parsed = json.loads(value)
    if isinstance(parsed, str):
        return parsed
    if not isinstance(parsed, list):
        raise ValueError("expected a JSON list or string")
    return [item for item in parsed if isinstance(item, dict)]


def _has_table_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if row is None:
        return False
    return any(str(item[1]) == column_name for item in conn.execute(f"PRAGMA table_info({table_name})"))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _byte_len(value: str) -> int:
    return len(value.encode("utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Replace inline data:image/base64 payloads in legacy audit SQLite rows "
            "with media:sha256 references."
        )
    )
    parser.add_argument(
        "--db",
        required=True,
        help="SQLite database to sanitize. Run against a copied database during migration.",
    )
    parser.add_argument("--apply", action="store_true", help="Rewrite matching rows.")
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Compact the database after applying updates.",
    )
    parser.add_argument(
        "--drop-migrated-config-tables",
        action="store_true",
        help=(
            "Drop legacy config tables that should already be exported to "
            "config.toml/models.json/persona/prompt/agent files."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary.")
    args = parser.parse_args(argv)

    summary = sanitize_legacy_audit_media(
        Path(args.db),
        apply=bool(args.apply),
        vacuum=bool(args.vacuum),
        drop_migrated_config_tables=bool(args.drop_migrated_config_tables),
    )
    payload = summary.to_dict()
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_summary(payload)
    return 0


def _print_summary(summary: dict[str, Any]) -> None:
    print(f"Legacy DB: {summary['dbPath']}")
    print(f"Applied: {summary['applied']}")
    print(f"Vacuumed: {summary['vacuumed']}")
    print(f"DB bytes: {summary['dbBytesBefore']} -> {summary['dbBytesAfter']}")
    print(f"Payload bytes saved: {summary['payloadBytesSaved']}")
    for column in summary["columns"]:
        print(
            f"- {column['table']}.{column['column']}: "
            f"{column['rowsMatched']} matched, {column['rowsUpdated']} updated, "
            f"{column['rowsFailed']} failed"
        )
    if summary["droppedTables"]:
        action = "Dropped" if summary["applied"] else "Would drop"
        print(f"{action} migrated config tables: {', '.join(summary['droppedTables'])}")
    if summary["warnings"]:
        print("Warnings:")
        for warning in summary["warnings"]:
            print(f"- {warning}")


def _drop_migrated_config_tables(conn: sqlite3.Connection, *, apply: bool) -> list[str]:
    existing = set(_existing_tables(conn))
    tables = [table for table in MIGRATED_CONFIG_TABLES if table in existing]
    if apply:
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {_quote_identifier(table)}")
    return tables


def _existing_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return [str(row[0]) for row in rows]


if __name__ == "__main__":
    raise SystemExit(main())
