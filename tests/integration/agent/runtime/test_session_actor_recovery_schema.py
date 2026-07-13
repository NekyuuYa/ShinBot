from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from shinbot.persistence import DatabaseManager

_CASE_DIGEST = "c" * 64
_CASE_ID = f"recovery-case:v1:{_CASE_DIGEST}"
_RECOVERY_CASE_COLUMNS = {
    "case_id",
    "profile_id",
    "session_id",
    "ownership_generation",
    "certificate_version",
    "policy_version",
    "work_graph_digest",
    "latest_certificate_digest",
    "status",
    "next_delivery_cycle",
    "delivery_count",
    "last_event_id",
    "last_error",
    "created_at",
    "updated_at",
}
_CANONICAL_RECOVERY_TRIGGER_NAMES = {
    "trg_agent_recovery_case_current_authority_update",
    "trg_agent_recovery_case_generation_insert",
    "trg_agent_recovery_case_generation_update",
    "trg_agent_recovery_case_identity_immutable",
    "trg_agent_recovery_case_insert_guard",
    "trg_agent_recovery_case_progress_evidence",
    "trg_agent_recovery_case_progress_monotonic",
    "trg_agent_recovery_case_status_transition",
    "trg_agent_recovery_case_terminal_immutable",
    "trg_agent_recovery_case_time_monotonic",
}


def _make_database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    database.initialize()
    return database


def _insert_aggregate(
    database: DatabaseManager,
    *,
    ownership_generation: int = 3,
    profile_id: str = "profile-a",
    session_id: str = "bot:group:room",
) -> None:
    with database.connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_session_aggregates (
                profile_id, session_id, ownership_generation,
                created_at, updated_at
            ) VALUES (?, ?, ?, 100, 100)
            """,
            (profile_id, session_id, ownership_generation),
        )


def _case_values(**overrides: Any) -> dict[str, object]:
    values: dict[str, object] = {
        "case_id": _CASE_ID,
        "profile_id": "profile-a",
        "session_id": "bot:group:room",
        "ownership_generation": 3,
        "certificate_version": 1,
        "policy_version": 1,
        "work_graph_digest": "a" * 64,
        "latest_certificate_digest": "b" * 64,
        "status": "open",
        "next_delivery_cycle": 0,
        "delivery_count": 0,
        "last_event_id": "",
        "last_error": "",
        "created_at": 100,
        "updated_at": 100,
    }
    values.update(overrides)
    return values


def _execute_case_insert(
    conn: sqlite3.Connection,
    values: dict[str, object],
    *,
    replace: bool = False,
) -> None:
    verb = "INSERT OR REPLACE" if replace else "INSERT"
    conn.execute(
        f"""
        {verb} INTO agent_session_recovery_cases (
            case_id, profile_id, session_id, ownership_generation,
            certificate_version, policy_version, work_graph_digest,
            latest_certificate_digest, status, next_delivery_cycle,
            delivery_count, last_event_id, last_error,
            created_at, updated_at
        ) VALUES (
            :case_id, :profile_id, :session_id, :ownership_generation,
            :certificate_version, :policy_version, :work_graph_digest,
            :latest_certificate_digest, :status, :next_delivery_cycle,
            :delivery_count, :last_event_id, :last_error,
            :created_at, :updated_at
        )
        """,
        values,
    )


def _insert_case(database: DatabaseManager, **overrides: Any) -> None:
    with database.connect() as conn:
        _execute_case_insert(conn, _case_values(**overrides))


def _recovery_event_id(case_id: str, delivery_cycle: int) -> str:
    case_digest = case_id.rsplit(":", maxsplit=1)[-1]
    return f"recovery-requested:v1:{case_digest}:{delivery_cycle}"


def _insert_recovery_request(
    conn: sqlite3.Connection,
    *,
    case_id: str = _CASE_ID,
    delivery_cycle: int,
    certificate_digest: str = "b" * 64,
    work_graph_digest: str = "a" * 64,
    policy_version: int = 1,
    occurred_at: float,
    profile_id: str = "profile-a",
    session_id: str = "bot:group:room",
    ownership_generation: int = 3,
    kind: str = "RecoveryRequested",
    source: str = "durable_session_recovery_scanner",
) -> str:
    event_id = _recovery_event_id(case_id, delivery_cycle)
    payload_json = json.dumps(
        {
            "case_id": case_id,
            "certificate": {
                "case_id": case_id,
                "certificate_digest": certificate_digest,
                "policy_version": policy_version,
                "schema": "shinbot.agent.session.recovery-certificate",
                "subject": {
                    "ownership_generation": ownership_generation,
                    "profile_id": profile_id,
                    "session_id": session_id,
                },
                "version": 1,
                "work_graph_digest": work_graph_digest,
            },
            "delivery_cycle": delivery_cycle,
            "schema": "shinbot.agent.session.recovery-delivery",
            "version": 1,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    conn.execute(
        """
        INSERT INTO agent_session_mailbox (
            event_id, profile_id, session_id, ownership_generation,
            kind, source, occurred_at, payload_json, causation_id,
            available_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            profile_id,
            session_id,
            ownership_generation,
            kind,
            source,
            occurred_at,
            payload_json,
            case_id,
            occurred_at,
            occurred_at,
        ),
    )
    return event_id


def _advance_case_delivery(
    conn: sqlite3.Connection,
    *,
    case_id: str = _CASE_ID,
    delivery_cycle: int,
    certificate_digest: str = "b" * 64,
    work_graph_digest: str = "a" * 64,
    policy_version: int = 1,
    updated_at: float,
) -> str:
    event_id = _insert_recovery_request(
        conn,
        case_id=case_id,
        delivery_cycle=delivery_cycle,
        certificate_digest=certificate_digest,
        work_graph_digest=work_graph_digest,
        policy_version=policy_version,
        occurred_at=updated_at,
    )
    next_delivery_count = delivery_cycle + 1
    conn.execute(
        """
        UPDATE agent_session_recovery_cases
        SET next_delivery_cycle = ?,
            delivery_count = ?,
            last_event_id = ?,
            latest_certificate_digest = ?,
            updated_at = ?
        WHERE case_id = ?
        """,
        (
            next_delivery_count,
            next_delivery_count,
            event_id,
            certificate_digest,
            updated_at,
            case_id,
        ),
    )
    return event_id


def _replace_with_weak_recovery_table(
    conn: sqlite3.Connection,
    *,
    untyped_numeric_authority: bool = False,
) -> None:
    generation_type = "" if untyped_numeric_authority else "INTEGER"
    timestamp_type = "" if untyped_numeric_authority else "REAL"
    conn.execute("DROP TABLE agent_session_recovery_cases")
    conn.execute(
        f"""
        CREATE TABLE agent_session_recovery_cases (
            case_id TEXT PRIMARY KEY,
            profile_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            ownership_generation {generation_type} NOT NULL,
            certificate_version INTEGER NOT NULL,
            policy_version INTEGER NOT NULL,
            work_graph_digest TEXT NOT NULL,
            latest_certificate_digest TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            next_delivery_cycle INTEGER NOT NULL DEFAULT 0,
            delivery_count INTEGER NOT NULL DEFAULT 0,
            last_event_id TEXT NOT NULL DEFAULT '',
            last_error TEXT NOT NULL DEFAULT '',
            created_at {timestamp_type} NOT NULL,
            updated_at {timestamp_type} NOT NULL
        )
        """
    )


def _replace_with_incomplete_recovery_table(
    conn: sqlite3.Connection,
    *,
    seed_row: bool,
) -> None:
    conn.execute("DROP TABLE agent_session_recovery_cases")
    conn.execute(
        """
        CREATE TABLE agent_session_recovery_cases (
            case_id TEXT PRIMARY KEY
        )
        """
    )
    if seed_row:
        conn.execute(
            """
            INSERT INTO agent_session_recovery_cases (case_id)
            VALUES (?)
            """,
            (_CASE_ID,),
        )


def test_recovery_case_schema_is_created_idempotently(tmp_path: Path) -> None:
    database = _make_database(tmp_path)
    database.initialize()

    with database.connect() as conn:
        table = conn.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_session_recovery_cases'
            """
        ).fetchone()
        indexes = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'index'
                  AND tbl_name = 'agent_session_recovery_cases'
                """
            )
        }
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(agent_session_recovery_cases)")
        }
        triggers = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger'
                  AND tbl_name = 'agent_session_recovery_cases'
                """
            )
        }

    assert table is not None
    assert columns == _RECOVERY_CASE_COLUMNS
    assert {
        "idx_agent_session_recovery_cases_status",
        "idx_agent_session_recovery_cases_session",
    } <= indexes
    assert triggers == _CANONICAL_RECOVERY_TRIGGER_NAMES


def test_recovery_case_schema_is_added_to_preexisting_actor_database(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    with database.connect() as conn:
        conn.execute("DROP TABLE agent_session_recovery_cases")

    database.initialize()

    with database.connect() as conn:
        table = conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_session_recovery_cases'
            """
        ).fetchone()
        triggers = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger'
                  AND tbl_name = 'agent_session_recovery_cases'
                """
            )
        }

    assert table is not None
    assert triggers == _CANONICAL_RECOVERY_TRIGGER_NAMES


def test_recovery_case_triggers_are_added_idempotently(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    with database.connect() as conn:
        for trigger_name in _CANONICAL_RECOVERY_TRIGGER_NAMES:
            conn.execute(f"DROP TRIGGER {trigger_name}")

    database.initialize()
    database.initialize()

    with database.connect() as conn:
        restored = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger'
                  AND tbl_name = 'agent_session_recovery_cases'
                """
            )
        }

    assert restored == _CANONICAL_RECOVERY_TRIGGER_NAMES


def test_recovery_case_schema_accepts_one_valid_case_and_cascades(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        row = conn.execute("SELECT * FROM agent_session_recovery_cases").fetchone()
        assert row is not None
        assert row["status"] == "open"
        assert row["next_delivery_cycle"] == 0
        conn.execute(
            """
            DELETE FROM agent_session_aggregates
            WHERE profile_id = 'profile-a' AND session_id = 'bot:group:room'
            """
        )
        remaining = conn.execute("SELECT COUNT(*) FROM agent_session_recovery_cases").fetchone()[0]

    assert remaining == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "unknown"},
        {"status": "applied"},
        {"status": "delivery_exhausted"},
        {"case_id": "arbitrary"},
        {"case_id": f"recovery-case:v1:{'G' * 64}"},
        {"ownership_generation": 0},
        {"ownership_generation": 4},
        {"certificate_version": 0},
        {"certificate_version": 2},
        {"policy_version": 0},
        {"work_graph_digest": "a" * 63},
        {"work_graph_digest": "A" * 64},
        {"latest_certificate_digest": "z" * 64},
        {"next_delivery_cycle": -1},
        {"next_delivery_cycle": 1},
        {"delivery_count": -1},
        {"delivery_count": 1, "last_event_id": ""},
        {
            "next_delivery_cycle": 1,
            "delivery_count": 1,
            "last_event_id": "not-a-mailbox-event",
        },
        {"updated_at": 99},
    ],
)
def test_recovery_case_schema_rejects_invalid_records(
    tmp_path: Path,
    overrides: dict[str, object],
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)

    with pytest.raises(sqlite3.IntegrityError):
        _insert_case(database, **overrides)


def test_recovery_case_schema_enforces_semantic_case_uniqueness(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with pytest.raises(sqlite3.IntegrityError, match="already exists"):
        _insert_case(
            database,
            case_id=f"recovery-case:v1:{'d' * 64}",
            latest_certificate_digest="e" * 64,
        )


def test_insert_or_replace_cannot_reset_terminal_recovery_case(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = 'superseded', updated_at = 101
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        )

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="already exists"):
            _execute_case_insert(
                conn,
                _case_values(status="open", updated_at=102),
                replace=True,
            )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == ("superseded", 101.0)


def test_insert_or_replace_cannot_replace_semantic_recovery_identity(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)
    replacement_id = f"recovery-case:v1:{'d' * 64}"

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="already exists"):
            _execute_case_insert(
                conn,
                _case_values(
                    case_id=replacement_id,
                    latest_certificate_digest="e" * 64,
                ),
                replace=True,
            )

    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT case_id, latest_certificate_digest
            FROM agent_session_recovery_cases
            """
        ).fetchall()

    assert [tuple(row) for row in rows] == [(_CASE_ID, "b" * 64)]


@pytest.mark.parametrize(
    "terminal_status",
    ["applied", "superseded", "delivery_exhausted"],
)
def test_fresh_recovery_case_cannot_start_terminal(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)

    with pytest.raises(sqlite3.IntegrityError, match="initial state"):
        _insert_case(database, status=terminal_status)


def test_fresh_recovery_case_cannot_start_with_delivery_progress(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    event_id = _recovery_event_id(_CASE_ID, 0)
    with database.connect() as conn:
        _insert_recovery_request(
            conn,
            delivery_cycle=0,
            occurred_at=100,
        )

    with pytest.raises(sqlite3.IntegrityError, match="initial state"):
        _insert_case(
            database,
            next_delivery_cycle=1,
            delivery_count=1,
            last_event_id=event_id,
        )


def test_fresh_recovery_case_requires_one_creation_timestamp(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)

    with pytest.raises(sqlite3.IntegrityError, match="initial state"):
        _insert_case(database, updated_at=101)


def test_fresh_scanner_blocked_case_requires_a_reason(tmp_path: Path) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)

    with pytest.raises(sqlite3.IntegrityError, match="initial state"):
        _insert_case(database, status="scanner_blocked")

    _insert_case(
        database,
        status="scanner_blocked",
        last_error="authority conflict",
    )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, delivery_count, last_event_id, last_error
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == ("scanner_blocked", 0, "", "authority conflict")


@pytest.mark.parametrize(
    "last_error",
    [pytest.param("\t", id="tab"), pytest.param("\n", id="newline")],
)
def test_scanner_blocked_rejects_ascii_whitespace_only_reason(
    tmp_path: Path,
    last_error: str,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)

    with pytest.raises(sqlite3.IntegrityError):
        _insert_case(
            database,
            status="scanner_blocked",
            last_error=last_error,
        )


@pytest.mark.parametrize("field_name", ["profile_id", "session_id"])
@pytest.mark.parametrize(
    "boundary_whitespace",
    [
        pytest.param(" ", id="space"),
        pytest.param("\t", id="tab"),
        pytest.param("\n", id="line-feed"),
        pytest.param("\v", id="vertical-tab"),
        pytest.param("\f", id="form-feed"),
        pytest.param("\r", id="carriage-return"),
    ],
)
def test_recovery_case_rejects_ascii_boundary_whitespace_in_subject(
    tmp_path: Path,
    field_name: str,
    boundary_whitespace: str,
) -> None:
    subject = {
        "profile_id": "profile-a",
        "session_id": "bot:group:room",
    }
    subject[field_name] = f"{boundary_whitespace}{subject[field_name]}{boundary_whitespace}"
    database = _make_database(tmp_path)
    _insert_aggregate(database, **subject)

    with pytest.raises(sqlite3.IntegrityError):
        _insert_case(database, **subject)


@pytest.mark.parametrize(
    ("field_name", "blob_value"),
    [
        ("work_graph_digest", b"a" * 64),
        ("latest_certificate_digest", b"b" * 64),
        ("last_error", b"binary authority error"),
    ],
)
def test_recovery_case_rejects_blob_textual_authority(
    tmp_path: Path,
    field_name: str,
    blob_value: bytes,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    overrides: dict[str, object] = {field_name: sqlite3.Binary(blob_value)}
    if field_name == "work_graph_digest":
        _insert_case(database)
        overrides.update(
            {
                "case_id": f"recovery-case:v1:{'d' * 64}",
                "latest_certificate_digest": "e" * 64,
            }
        )

    with pytest.raises(sqlite3.IntegrityError):
        _insert_case(database, **overrides)


def test_recovery_case_progress_requires_matching_mailbox_evidence(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="matching RecoveryRequested mailbox",
        ):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 1,
                    delivery_count = 1,
                    last_event_id = ?,
                    updated_at = 101
                WHERE case_id = ?
                """,
                (_recovery_event_id(_CASE_ID, 0), _CASE_ID),
            )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT next_delivery_cycle, delivery_count,
                   last_event_id, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == (0, 0, "", 100.0)


def test_legacy_recovery_source_cannot_authorize_delivery_progress(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)
    with database.connect() as conn:
        event_id = _insert_recovery_request(
            conn,
            delivery_cycle=0,
            occurred_at=101,
            source="session_actor_recovery",
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="matching RecoveryRequested mailbox",
        ):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 1,
                    delivery_count = 1,
                    last_event_id = ?,
                    updated_at = 101
                WHERE case_id = ?
                """,
                (event_id, _CASE_ID),
            )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT next_delivery_cycle, delivery_count, last_event_id
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == (0, 0, "")


def test_recovery_case_accepts_sequential_mailbox_backed_delivery_progress(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        first_event_id = _advance_case_delivery(
            conn,
            delivery_cycle=0,
            updated_at=101,
        )
        second_event_id = _advance_case_delivery(
            conn,
            delivery_cycle=1,
            updated_at=102,
        )
        row = conn.execute(
            """
            SELECT next_delivery_cycle, delivery_count, last_event_id
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()
        mailbox = conn.execute(
            """
            SELECT event_id, kind, source
            FROM agent_session_mailbox
            WHERE causation_id = ?
            ORDER BY mailbox_id
            """,
            (_CASE_ID,),
        ).fetchall()

    assert row is not None
    assert tuple(row) == (2, 2, second_event_id)
    assert [tuple(item) for item in mailbox] == [
        (
            first_event_id,
            "RecoveryRequested",
            "durable_session_recovery_scanner",
        ),
        (
            second_event_id,
            "RecoveryRequested",
            "durable_session_recovery_scanner",
        ),
    ]


def test_recovery_case_generation_is_exact_at_insert_but_history_remains_mutable(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET ownership_generation = 4, updated_at = 101
            WHERE profile_id = 'profile-a' AND session_id = 'bot:group:room'
            """
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = 'superseded', updated_at = 101
            WHERE case_id = ?
            """,
            (f"recovery-case:v1:{'c' * 64}",),
        )

    with pytest.raises(sqlite3.IntegrityError, match="ownership generation"):
        _insert_case(
            database,
            case_id=f"recovery-case:v1:{'d' * 64}",
            work_graph_digest="d" * 64,
            latest_certificate_digest="e" * 64,
        )

    _insert_case(
        database,
        case_id=f"recovery-case:v1:{'e' * 64}",
        ownership_generation=4,
        work_graph_digest="e" * 64,
        latest_certificate_digest="f" * 64,
        created_at=101,
        updated_at=101,
    )

    with database.connect() as conn:
        historical = conn.execute(
            """
            SELECT status FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (f"recovery-case:v1:{'c' * 64}",),
        ).fetchone()

    assert historical is not None
    assert str(historical["status"]) == "superseded"


def test_recovery_case_identity_cannot_be_rewritten(tmp_path: Path) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="identity is immutable"):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET case_id = ?
                WHERE case_id = ?
                """,
                (
                    f"recovery-case:v1:{'d' * 64}",
                    f"recovery-case:v1:{'c' * 64}",
                ),
            )


def test_recovery_case_updates_require_monotonic_time_and_delivery_progress(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        _advance_case_delivery(
            conn,
            delivery_cycle=0,
            updated_at=199,
        )
        _advance_case_delivery(
            conn,
            delivery_cycle=1,
            updated_at=200,
        )
        _insert_recovery_request(
            conn,
            delivery_cycle=3,
            occurred_at=201,
        )
        with pytest.raises(sqlite3.IntegrityError, match="advance updated_at"):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET updated_at = 150
                WHERE case_id = ?
                """,
                (_CASE_ID,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="delivery progress"):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 1,
                    delivery_count = 1,
                    last_event_id = ?,
                    updated_at = 201
                WHERE case_id = ?
                """,
                (_recovery_event_id(_CASE_ID, 0), _CASE_ID),
            )
        with pytest.raises(sqlite3.IntegrityError, match="delivery progress"):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 4,
                    delivery_count = 4,
                    last_event_id = ?,
                    updated_at = 201
                WHERE case_id = ?
                """,
                (_recovery_event_id(_CASE_ID, 3), _CASE_ID),
            )
        _advance_case_delivery(
            conn,
            delivery_cycle=2,
            certificate_digest="d" * 64,
            updated_at=201,
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET latest_certificate_digest = ?
                WHERE case_id = ?
                """,
                ("e" * 64, _CASE_ID),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="matching RecoveryRequested mailbox",
        ):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET latest_certificate_digest = ?, updated_at = 202
                WHERE case_id = ?
                """,
                ("e" * 64, _CASE_ID),
            )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT delivery_count, next_delivery_cycle,
                   latest_certificate_digest, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert int(row["delivery_count"]) == 3
    assert int(row["next_delivery_cycle"]) == 3
    assert str(row["latest_certificate_digest"]) == "d" * 64
    assert float(row["updated_at"]) == 201


def test_recovery_case_scanner_blocked_can_reopen_or_reach_terminal(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    case_id = f"recovery-case:v1:{'c' * 64}"
    _insert_case(database)

    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = 'scanner_blocked',
                last_error = 'authority conflict',
                updated_at = 101
            WHERE case_id = ?
            """,
            (case_id,),
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = 'open', last_error = '', updated_at = 102
            WHERE case_id = ?
            """,
            (case_id,),
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = 'superseded', updated_at = 103
            WHERE case_id = ?
            """,
            (case_id,),
        )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, last_error, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (case_id,),
        ).fetchone()

    assert row is not None
    assert str(row["status"]) == "superseded"
    assert str(row["last_error"]) == ""
    assert float(row["updated_at"]) == 103


def test_recovery_case_cannot_enter_scanner_blocked_without_a_reason(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET status = 'scanner_blocked',
                    last_error = '   ',
                    updated_at = 101
                WHERE case_id = ?
                """,
                (_CASE_ID,),
            )

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT status, last_error, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == ("open", "", 100.0)


@pytest.mark.parametrize(
    "terminal_status",
    ["applied", "superseded", "delivery_exhausted"],
)
def test_terminal_recovery_case_cannot_reopen_or_mutate(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    needs_delivery = terminal_status in {"applied", "delivery_exhausted"}
    _insert_case(database)

    with database.connect() as conn:
        if needs_delivery:
            _advance_case_delivery(
                conn,
                delivery_cycle=0,
                updated_at=100.5,
            )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = ?, updated_at = 101
            WHERE case_id = ?
            """,
            (terminal_status, _CASE_ID),
        )

    with database.connect() as conn:
        with pytest.raises(sqlite3.IntegrityError, match="terminal recovery case"):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET status = 'open', updated_at = 102
                WHERE case_id = ?
                """,
                (_CASE_ID,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="terminal recovery case"):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET latest_certificate_digest = ?, updated_at = 102
                WHERE case_id = ?
                """,
                ("d" * 64, _CASE_ID),
            )


def test_recovery_case_migration_rebuilds_empty_incomplete_table(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    with database.connect() as conn:
        _replace_with_incomplete_recovery_table(conn, seed_row=False)

    database.initialize()

    with database.connect() as conn:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(agent_session_recovery_cases)")
        }
        triggers = {
            str(row["name"])
            for row in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger'
                  AND tbl_name = 'agent_session_recovery_cases'
                """
            )
        }
        row_count = conn.execute("SELECT COUNT(*) FROM agent_session_recovery_cases").fetchone()[0]

    assert columns == _RECOVERY_CASE_COLUMNS
    assert triggers == _CANONICAL_RECOVERY_TRIGGER_NAMES
    assert int(row_count) == 0


def test_recovery_case_migration_rejects_nonempty_incomplete_table(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    with database.connect() as conn:
        _replace_with_incomplete_recovery_table(conn, seed_row=True)

    with pytest.raises(
        sqlite3.IntegrityError,
        match="invalid recovery case authority",
    ):
        database.initialize()

    with database.connect() as conn:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(agent_session_recovery_cases)")
        }
        rows = conn.execute("SELECT case_id FROM agent_session_recovery_cases").fetchall()
        migration_artifacts = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name LIKE 'agent_session_recovery_cases%legacy%'
            """
        ).fetchall()

    assert columns == {"case_id"}
    assert [tuple(row) for row in rows] == [(_CASE_ID,)]
    assert migration_artifacts == []


def test_recovery_case_migration_rebuilds_weak_schema_and_preserves_history(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    with database.connect() as conn:
        _replace_with_weak_recovery_table(conn)
        event_id = _insert_recovery_request(
            conn,
            delivery_cycle=0,
            occurred_at=101,
        )
        _execute_case_insert(
            conn,
            _case_values(
                next_delivery_cycle=1,
                delivery_count=1,
                last_event_id=event_id,
                updated_at=101,
            ),
        )

    database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT case_id, profile_id, session_id, ownership_generation,
                   certificate_version, policy_version, work_graph_digest,
                   latest_certificate_digest, status, next_delivery_cycle,
                   delivery_count, last_event_id, last_error,
                   created_at, updated_at,
                   typeof(work_graph_digest),
                   typeof(latest_certificate_digest)
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        table_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'agent_session_recovery_cases'
                """
            ).fetchone()["sql"]
        )
        triggers = {
            str(trigger["name"])
            for trigger in conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger'
                  AND tbl_name = 'agent_session_recovery_cases'
                """
            )
        }
        migration_artifacts = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name LIKE 'agent_session_recovery_cases%legacy%'
            """
        ).fetchall()

    assert row is not None
    assert tuple(row) == (
        _CASE_ID,
        "profile-a",
        "bot:group:room",
        3,
        1,
        1,
        "a" * 64,
        "b" * 64,
        "open",
        1,
        1,
        event_id,
        "",
        100.0,
        101.0,
        "text",
        "text",
    )
    assert "typeof(work_graph_digest) = 'text'" in table_sql
    assert "typeof(latest_certificate_digest) = 'text'" in table_sql
    assert triggers == _CANONICAL_RECOVERY_TRIGGER_NAMES
    assert migration_artifacts == []

    with database.connect() as conn:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="matching RecoveryRequested mailbox",
        ):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 2,
                    delivery_count = 2,
                    last_event_id = ?,
                    updated_at = 102
                WHERE case_id = ?
                """,
                (_recovery_event_id(_CASE_ID, 1), _CASE_ID),
            )


def test_recovery_case_schema_verifier_preserves_literal_case(tmp_path: Path) -> None:
    database = _make_database(tmp_path)
    with database.connect() as conn:
        canonical_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'agent_session_recovery_cases'
                """
            ).fetchone()["sql"]
        )
        conn.execute("DROP TABLE agent_session_recovery_cases")
        conn.execute(canonical_sql.replace("DEFAULT 'open'", "DEFAULT 'OPEN'", 1))

    database.initialize()

    with database.connect() as conn:
        rebuilt_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'agent_session_recovery_cases'
                """
            ).fetchone()["sql"]
        )

    assert "DEFAULT 'OPEN'" not in rebuilt_sql
    assert "DEFAULT 'open'" in rebuilt_sql


def test_recovery_case_migration_preserves_prior_generation_history(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database, ownership_generation=4)
    with database.connect() as conn:
        _replace_with_weak_recovery_table(conn)
        _execute_case_insert(
            conn,
            _case_values(
                ownership_generation=3,
                status="superseded",
                updated_at=101,
            ),
        )

    database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT recovery.ownership_generation, recovery.status,
                   recovery.created_at, recovery.updated_at,
                   aggregate.ownership_generation
            FROM agent_session_recovery_cases AS recovery
            JOIN agent_session_aggregates AS aggregate
              ON aggregate.profile_id = recovery.profile_id
             AND aggregate.session_id = recovery.session_id
            WHERE recovery.case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == (3, "superseded", 100.0, 101.0, 4)


@pytest.mark.parametrize(
    (
        "operation",
        "allowed",
        "expected_status",
        "expected_delivery_count",
        "expected_certificate_digest",
        "expected_last_error",
    ),
    [
        ("progress", False, "open", 0, "b" * 64, ""),
        ("rewrite_digest", False, "open", 1, "b" * 64, ""),
        (
            "reopen_scanner_blocked",
            False,
            "scanner_blocked",
            0,
            "b" * 64,
            "authority conflict",
        ),
        ("apply", False, "open", 1, "b" * 64, ""),
        ("supersede", True, "superseded", 0, "b" * 64, ""),
        (
            "exhaust_delivery",
            True,
            "delivery_exhausted",
            1,
            "b" * 64,
            "",
        ),
    ],
)
def test_prior_generation_recovery_case_only_allows_terminal_disposition(
    tmp_path: Path,
    operation: str,
    allowed: bool,
    expected_status: str,
    expected_delivery_count: int,
    expected_certificate_digest: str,
    expected_last_error: str,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)
    with database.connect() as conn:
        if operation in {"rewrite_digest", "apply", "exhaust_delivery"}:
            _advance_case_delivery(
                conn,
                delivery_cycle=0,
                updated_at=101,
            )
        elif operation == "reopen_scanner_blocked":
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET status = 'scanner_blocked',
                    last_error = 'authority conflict',
                    updated_at = 101
                WHERE case_id = ?
                """,
                (_CASE_ID,),
            )
        elif operation == "progress":
            _insert_recovery_request(
                conn,
                delivery_cycle=0,
                occurred_at=101,
            )

        conn.execute(
            """
            UPDATE agent_session_aggregates
            SET ownership_generation = 4, updated_at = 102
            WHERE profile_id = 'profile-a'
              AND session_id = 'bot:group:room'
            """
        )

        if operation == "progress":
            statement = """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 1,
                    delivery_count = 1,
                    last_event_id = :event_id,
                    updated_at = 103
                WHERE case_id = :case_id
            """
            parameters = {
                "case_id": _CASE_ID,
                "event_id": _recovery_event_id(_CASE_ID, 0),
            }
        elif operation == "rewrite_digest":
            statement = """
                UPDATE agent_session_recovery_cases
                SET latest_certificate_digest = :certificate_digest,
                    updated_at = 103
                WHERE case_id = :case_id
            """
            parameters = {
                "case_id": _CASE_ID,
                "certificate_digest": "d" * 64,
            }
        elif operation == "reopen_scanner_blocked":
            statement = """
                UPDATE agent_session_recovery_cases
                SET status = 'open', last_error = '', updated_at = 103
                WHERE case_id = :case_id
            """
            parameters = {"case_id": _CASE_ID}
        elif operation == "apply":
            statement = """
                UPDATE agent_session_recovery_cases
                SET status = 'applied', updated_at = 103
                WHERE case_id = :case_id
            """
            parameters = {"case_id": _CASE_ID}
        elif operation == "supersede":
            statement = """
                UPDATE agent_session_recovery_cases
                SET status = 'superseded', updated_at = 103
                WHERE case_id = :case_id
            """
            parameters = {"case_id": _CASE_ID}
        else:
            statement = """
                UPDATE agent_session_recovery_cases
                SET status = 'delivery_exhausted', updated_at = 103
                WHERE case_id = :case_id
            """
            parameters = {"case_id": _CASE_ID}

        if allowed:
            conn.execute(statement, parameters)
        else:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(statement, parameters)

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT recovery.ownership_generation, recovery.status,
                   recovery.delivery_count,
                   recovery.latest_certificate_digest,
                   recovery.last_error,
                   aggregate.ownership_generation
            FROM agent_session_recovery_cases AS recovery
            JOIN agent_session_aggregates AS aggregate
              ON aggregate.profile_id = recovery.profile_id
             AND aggregate.session_id = recovery.session_id
            WHERE recovery.case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()

    assert row is not None
    assert tuple(row) == (
        3,
        expected_status,
        expected_delivery_count,
        expected_certificate_digest,
        expected_last_error,
        4,
    )


@pytest.mark.parametrize(
    (
        "field_name",
        "malformed_value",
        "untyped_numeric_authority",
        "storage_class",
        "persisted_value",
    ),
    [
        (
            "work_graph_digest",
            sqlite3.Binary(b"a" * 64),
            False,
            "blob",
            b"a" * 64,
        ),
        ("ownership_generation", "3", True, "text", "3"),
        ("created_at", "100", True, "text", "100"),
    ],
)
def test_recovery_case_migration_fails_closed_for_invalid_weak_authority(
    tmp_path: Path,
    field_name: str,
    malformed_value: object,
    untyped_numeric_authority: bool,
    storage_class: str,
    persisted_value: object,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    with database.connect() as conn:
        _replace_with_weak_recovery_table(
            conn,
            untyped_numeric_authority=untyped_numeric_authority,
        )
        _execute_case_insert(
            conn,
            _case_values(**{field_name: malformed_value}),
        )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="invalid recovery case authority",
    ):
        database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            f"""
            SELECT case_id, typeof({field_name}), {field_name}
            FROM agent_session_recovery_cases
            """
        ).fetchone()
        table_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'agent_session_recovery_cases'
                """
            ).fetchone()["sql"]
        )
        migration_artifacts = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name LIKE 'agent_session_recovery_cases%legacy%'
            """
        ).fetchall()

    assert row is not None
    assert tuple(row) == (_CASE_ID, storage_class, persisted_value)
    assert "typeof(work_graph_digest)" not in table_sql
    assert migration_artifacts == []


def test_reinitialize_replaces_same_name_inert_recovery_triggers(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    insert_trigger = "trg_agent_recovery_case_insert_guard"
    evidence_trigger = "trg_agent_recovery_case_progress_evidence"
    with database.connect() as conn:
        conn.execute(f"DROP TRIGGER IF EXISTS {insert_trigger}")
        conn.execute(f"DROP TRIGGER IF EXISTS {evidence_trigger}")
        conn.execute(
            f"""
            CREATE TRIGGER {insert_trigger}
            BEFORE INSERT ON agent_session_recovery_cases
            BEGIN
                SELECT 1;
            END
            """
        )
        conn.execute(
            f"""
            CREATE TRIGGER {evidence_trigger}
            BEFORE UPDATE OF
                status, next_delivery_cycle, delivery_count,
                latest_certificate_digest
            ON agent_session_recovery_cases
            BEGIN
                SELECT 1;
            END
            """
        )
        inert_sql = {
            str(row["name"]): str(row["sql"])
            for row in conn.execute(
                """
                SELECT name, sql FROM sqlite_master
                WHERE type = 'trigger' AND name IN (?, ?)
                """,
                (insert_trigger, evidence_trigger),
            )
        }

    database.initialize()
    database.initialize()

    with database.connect() as conn:
        canonical_sql = {
            str(row["name"]): str(row["sql"])
            for row in conn.execute(
                """
                SELECT name, sql FROM sqlite_master
                WHERE type = 'trigger' AND name IN (?, ?)
                """,
                (insert_trigger, evidence_trigger),
            )
        }

    assert canonical_sql.keys() == inert_sql.keys()
    assert canonical_sql[insert_trigger] != inert_sql[insert_trigger]
    assert canonical_sql[evidence_trigger] != inert_sql[evidence_trigger]
    assert "already exists" in canonical_sql[insert_trigger]
    assert "initial state" in canonical_sql[insert_trigger]
    assert "matching RecoveryRequested mailbox" in canonical_sql[evidence_trigger]

    _insert_aggregate(database)
    _insert_case(database)
    second_case_id = f"recovery-case:v1:{'d' * 64}"
    _insert_case(
        database,
        case_id=second_case_id,
        work_graph_digest="d" * 64,
        latest_certificate_digest="e" * 64,
    )
    with database.connect() as conn:
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET status = 'superseded', updated_at = 101
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        )
        with pytest.raises(sqlite3.IntegrityError, match="already exists"):
            _execute_case_insert(
                conn,
                _case_values(updated_at=102),
                replace=True,
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="matching RecoveryRequested mailbox",
        ):
            conn.execute(
                """
                UPDATE agent_session_recovery_cases
                SET next_delivery_cycle = 1,
                    delivery_count = 1,
                    last_event_id = ?,
                    updated_at = 101
                WHERE case_id = ?
                """,
                (_recovery_event_id(second_case_id, 0), second_case_id),
            )


def test_reinitialize_fails_closed_when_inert_trigger_admitted_bad_authority(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)
    evidence_trigger = "trg_agent_recovery_case_progress_evidence"
    with database.connect() as conn:
        conn.execute(f"DROP TRIGGER {evidence_trigger}")
        conn.execute(
            f"""
            CREATE TRIGGER {evidence_trigger}
            BEFORE UPDATE OF
                status, next_delivery_cycle, delivery_count,
                latest_certificate_digest
            ON agent_session_recovery_cases
            BEGIN
                SELECT 1;
            END
            """
        )
        inert_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = ?
                """,
                (evidence_trigger,),
            ).fetchone()["sql"]
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = 1,
                delivery_count = 1,
                last_event_id = ?,
                updated_at = 101
            WHERE case_id = ?
            """,
            (_recovery_event_id(_CASE_ID, 0), _CASE_ID),
        )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="invalid recovery case authority",
    ):
        database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT next_delivery_cycle, delivery_count,
                   last_event_id, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()
        trigger_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = ?
                """,
                (evidence_trigger,),
            ).fetchone()["sql"]
        )
        migration_artifacts = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name LIKE 'agent_session_recovery_cases%legacy%'
            """
        ).fetchall()

    assert row is not None
    assert tuple(row) == (
        1,
        1,
        _recovery_event_id(_CASE_ID, 0),
        101.0,
    )
    assert trigger_sql == inert_sql
    assert migration_artifacts == []


def test_reinitialize_requires_mailbox_evidence_for_every_delivery_cycle(
    tmp_path: Path,
) -> None:
    database = _make_database(tmp_path)
    _insert_aggregate(database)
    _insert_case(database)
    evidence_trigger = "trg_agent_recovery_case_progress_evidence"
    with database.connect() as conn:
        conn.execute(f"DROP TRIGGER {evidence_trigger}")
        conn.execute(
            f"""
            CREATE TRIGGER {evidence_trigger}
            BEFORE UPDATE OF
                status, next_delivery_cycle, delivery_count,
                latest_certificate_digest
            ON agent_session_recovery_cases
            BEGIN
                SELECT 1;
            END
            """
        )
        inert_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = ?
                """,
                (evidence_trigger,),
            ).fetchone()["sql"]
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = 1,
                delivery_count = 1,
                last_event_id = ?,
                updated_at = 101
            WHERE case_id = ?
            """,
            (_recovery_event_id(_CASE_ID, 0), _CASE_ID),
        )
        last_event_id = _insert_recovery_request(
            conn,
            delivery_cycle=1,
            occurred_at=102,
        )
        conn.execute(
            """
            UPDATE agent_session_recovery_cases
            SET next_delivery_cycle = 2,
                delivery_count = 2,
                last_event_id = ?,
                updated_at = 102
            WHERE case_id = ?
            """,
            (last_event_id, _CASE_ID),
        )

    with pytest.raises(
        sqlite3.IntegrityError,
        match="invalid recovery case authority",
    ):
        database.initialize()

    with database.connect() as conn:
        row = conn.execute(
            """
            SELECT next_delivery_cycle, delivery_count,
                   last_event_id, updated_at
            FROM agent_session_recovery_cases
            WHERE case_id = ?
            """,
            (_CASE_ID,),
        ).fetchone()
        mailbox_event_ids = [
            str(mailbox["event_id"])
            for mailbox in conn.execute(
                """
                SELECT event_id FROM agent_session_mailbox
                WHERE causation_id = ?
                ORDER BY mailbox_id
                """,
                (_CASE_ID,),
            )
        ]
        trigger_sql = str(
            conn.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = ?
                """,
                (evidence_trigger,),
            ).fetchone()["sql"]
        )
        migration_artifacts = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name LIKE 'agent_session_recovery_cases%legacy%'
            """
        ).fetchall()

    assert row is not None
    assert tuple(row) == (2, 2, last_event_id, 102.0)
    assert mailbox_event_ids == [last_event_id]
    assert trigger_sql == inert_sql
    assert migration_artifacts == []
