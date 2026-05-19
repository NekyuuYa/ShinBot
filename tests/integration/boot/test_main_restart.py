from __future__ import annotations

import sys

import pytest

import main
from shinbot.core.application.runtime_control import ProcessExitCode


def test_main_reexecs_process_for_restart_exit_code(monkeypatch) -> None:
    calls: dict[str, object] = {}
    argv = ["main.py", "--operator-cli"]

    def fake_asyncio_run(coro) -> int:
        coro.close()
        return int(ProcessExitCode.RESTART_MANUAL)

    monkeypatch.setattr(main.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(sys, "argv", argv)

    def fake_execv(executable: str, args: list[str]) -> None:
        calls["executable"] = executable
        calls["args"] = args
        raise SystemExit(0)

    monkeypatch.setattr(main.os, "execv", fake_execv)

    with pytest.raises(SystemExit) as exc_info:
        main.main()

    assert exc_info.value.code == 0

    assert calls == {
        "executable": sys.executable,
        "args": [sys.executable, *argv],
    }
