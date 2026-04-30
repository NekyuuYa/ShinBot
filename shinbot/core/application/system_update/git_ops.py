"""Async git command helpers for update orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from .common import DEFAULT_GIT_TIMEOUT_SECONDS, MAX_OUTPUT_CHARS, SystemUpdateError


@dataclass(slots=True)
class GitCommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def output(self) -> str:
        text = "\n".join(part for part in (self.stdout.strip(), self.stderr.strip()) if part).strip()
        if len(text) > MAX_OUTPUT_CHARS:
            return f"{text[: MAX_OUTPUT_CHARS - 3]}..."
        return text


def default_git_executable() -> str | None:
    return shutil.which("git")


async def run_git(
    *,
    repo_root: Path | None,
    git_executable: str | None,
    unavailable_message: str,
    args: tuple[str, ...],
    timeout: float = DEFAULT_GIT_TIMEOUT_SECONDS,
) -> GitCommandResult:
    if repo_root is None:
        return GitCommandResult(returncode=1, stderr=unavailable_message)
    if not git_executable:
        return GitCommandResult(returncode=1, stderr="git is not installed")

    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_SSH_COMMAND"] = "ssh -oBatchMode=yes"

    process = await asyncio.create_subprocess_exec(
        git_executable,
        *args,
        cwd=str(repo_root),
        env=env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError as exc:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        with contextlib.suppress(Exception):
            await process.communicate()
        raise SystemUpdateError(
            code="UPDATE_FAILED",
            message=f"git {' '.join(args)} timed out after {int(timeout)} seconds",
            status_code=504,
        ) from exc

    return GitCommandResult(
        returncode=process.returncode or 0,
        stdout=stdout.decode("utf-8", errors="replace"),
        stderr=stderr.decode("utf-8", errors="replace"),
    )


def find_git_root(start: Path) -> Path | None:
    current = start
    if current.is_file():
        current = current.parent

    for path in (current, *current.parents):
        if (path / ".git").exists():
            return path
    return None


def parse_ahead_behind(raw: str) -> tuple[int, int]:
    parts = raw.strip().split()
    if len(parts) < 2:
        return 0, 0
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return 0, 0

