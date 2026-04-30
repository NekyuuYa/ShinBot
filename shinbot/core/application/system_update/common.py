"""Shared types and constants for system update services."""

from __future__ import annotations

from typing import Any

DEFAULT_ALLOWED_BRANCHES = ("main", "master")
DEFAULT_PULL_TIMEOUT_SECONDS = 120.0
DEFAULT_GIT_TIMEOUT_SECONDS = 15.0
DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS = 20.0
MAX_OUTPUT_CHARS = 4000
DASHBOARD_DIST_MANIFEST = ".shinbot-dashboard-dist.json"
DEFAULT_DASHBOARD_DIST_PACKAGE_MAX_BYTES = 100 * 1024 * 1024


class SystemUpdateError(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        status_code: int,
        output: str = "",
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.output = output


def normalize_allowed_branches(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, (list, tuple, set)):
        branches = tuple(str(item).strip() for item in raw if str(item).strip())
        if branches:
            return branches
    return DEFAULT_ALLOWED_BRANCHES

