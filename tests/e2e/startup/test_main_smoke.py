from __future__ import annotations

import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.e2e


@pytest.mark.skip(reason="openapi_url=None makes readiness probe impossible")
def test_main_entrypoint_boots_api_with_legacy_database(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    config_path = tmp_path / "config.toml"
    data_dir = tmp_path / "data"
    db_path = data_dir / "db" / "shinbot.sqlite3"
    port = _pick_free_port()

    _write_smoke_config(config_path)
    _write_smoke_agent_config(data_dir / "agents" / "smoke.toml")
    _seed_legacy_media_semantics(db_path)

    process = subprocess.Popen(
        [
            sys.executable,
            str(repo_root / "main.py"),
            "--config",
            str(config_path),
            "--data-dir",
            str(data_dir),
            "--api-host",
            "127.0.0.1",
            "--api-port",
            str(port),
            "--no-operator-cli",
        ],
        cwd=repo_root,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    output = ""
    try:
        _wait_for_openapi(port, process)
    except Exception:
        output = _stop_process(process)
        raise AssertionError(f"main.py did not become ready\n{output}") from None
    else:
        output = _stop_process(process)

    assert "Fatal error in ShinBot" not in output
    assert process.returncode in {0, -signal.SIGINT}
    _assert_legacy_media_schema_migrated(db_path)


def _write_smoke_config(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[runtime]",
                "model = true",
                "agent = true",
                "",
                "[[bots]]",
                'id = "smoke-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/smoke.toml"',
            ]
        ),
        encoding="utf-8",
    )


def _write_smoke_agent_config(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "[agent]",
                'id = "smoke-agent-profile"',
                "",
                "[agent.review.scan]",
                "enabled = false",
                "",
                "[agent.review.reply_decision]",
                "enabled = false",
                "",
                "[agent.review.active_chat_bootstrap]",
                "enabled = false",
                "",
                "[agent.review.idle_review_planning]",
                "enabled = false",
            ]
        ),
        encoding="utf-8",
    )


def _seed_legacy_media_semantics(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE media_semantics (
                raw_hash TEXT PRIMARY KEY,
                kind TEXT NOT NULL DEFAULT '',
                digest TEXT NOT NULL DEFAULT '',
                verified_by_model INTEGER NOT NULL DEFAULT 0,
                inspection_agent_ref TEXT NOT NULL DEFAULT '',
                inspection_llm_ref TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                expire_at REAL NOT NULL
            );
            INSERT INTO media_semantics (
                raw_hash, kind, digest, first_seen_at, last_seen_at, expire_at
            ) VALUES ('raw-smoke', 'image', 'legacy smoke digest', 1, 2, 3);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _wait_for_openapi(port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20.0
    url = f"http://127.0.0.1:{port}/api/openapi.json"
    with httpx.Client(timeout=0.5) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"process exited early with {process.returncode}")
            try:
                response = client.get(url)
            except httpx.HTTPError:
                time.sleep(0.1)
                continue
            if response.status_code == 200 and response.json().get("openapi"):
                return
            time.sleep(0.1)
    raise TimeoutError("timed out waiting for management API")


def _stop_process(process: subprocess.Popen[str]) -> str:
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        stdout, stderr = process.communicate(timeout=10.0)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=5.0)
    return f"{stdout}\n{stderr}"


def _assert_legacy_media_schema_migrated(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(media_semantics)")}
        row = conn.execute(
            "SELECT raw_hash, strict_dhash, digest FROM media_semantics"
        ).fetchone()
        index_row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'index' AND name = 'idx_media_semantics_strict_dhash'
            """
        ).fetchone()
    finally:
        conn.close()

    assert "strict_dhash" in columns
    assert row["raw_hash"] == "raw-smoke"
    assert row["strict_dhash"] == ""
    assert row["digest"] == "legacy smoke digest"
    assert index_row is not None


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])
