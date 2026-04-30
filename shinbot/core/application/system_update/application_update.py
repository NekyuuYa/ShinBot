"""Main application git update service."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from shinbot.core.application.runtime_control import RestartReason, RuntimeControl

from .common import (
    DEFAULT_ALLOWED_BRANCHES,
    DEFAULT_GIT_TIMEOUT_SECONDS,
    DEFAULT_PULL_TIMEOUT_SECONDS,
    DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS,
    SystemUpdateError,
    normalize_allowed_branches,
)
from .git_ops import (
    GitCommandResult,
    default_git_executable,
    find_git_root,
    parse_ahead_behind,
    run_git,
)


class SystemUpdateService:
    """Inspect and update a fixed local git checkout with conservative safeguards."""

    def __init__(
        self,
        *,
        config: dict[str, Any],
        config_path: Path | str | None = None,
    ) -> None:
        admin_cfg = config.get("admin", {})
        configured_repo = admin_cfg.get("update_repo")
        configured_branches = admin_cfg.get("update_allowed_branches", DEFAULT_ALLOWED_BRANCHES)

        self._config_path = Path(config_path).resolve() if config_path is not None else None
        self._allowed_branches = normalize_allowed_branches(configured_branches)
        self._git_executable = default_git_executable()
        self._lock = asyncio.Lock()
        self._repo_root = self._detect_repo_root(configured_repo)

    @property
    def update_in_progress(self) -> bool:
        return self._lock.locked()

    async def inspect(self) -> dict[str, Any]:
        return await self._inspect(ignore_lock=False)

    async def pull_and_request_restart(
        self,
        *,
        runtime_control: RuntimeControl,
        requested_by: str = "",
    ) -> dict[str, Any]:
        if self._lock.locked():
            raise SystemUpdateError(
                code="UPDATE_ALREADY_RUNNING",
                message="Another update is already running",
                status_code=409,
            )

        async with self._lock:
            status = await self._inspect(ignore_lock=True)
            if status["blockCode"] == "already_up_to_date":
                current_commit = str(status["currentCommit"])
                return {
                    "accepted": True,
                    "updated": False,
                    "alreadyUpToDate": True,
                    "restartRequested": False,
                    "restartRequest": None,
                    "repoPath": str(self._repo_root) if self._repo_root is not None else "",
                    "branch": status["branch"],
                    "upstream": status["upstream"],
                    "beforeCommit": current_commit,
                    "beforeCommitShort": current_commit[:12],
                    "afterCommit": current_commit,
                    "afterCommitShort": current_commit[:12],
                    "output": "Already up to date.",
                }

            if not status["canUpdate"]:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=status["blockMessage"] or "Update is not allowed in the current repo state",
                    status_code=self._status_code_for_block(status["blockCode"]),
                )

            if runtime_control.restart_requested:
                raise SystemUpdateError(
                    code="RESTART_ALREADY_REQUESTED",
                    message="A restart request is already pending",
                    status_code=409,
                )

            before_commit = str(status["currentCommit"])
            pull_result = await self._run_git(
                "pull",
                "--ff-only",
                timeout=DEFAULT_PULL_TIMEOUT_SECONDS,
            )
            if pull_result.returncode != 0:
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="git pull --ff-only failed",
                    status_code=500,
                    output=pull_result.output,
                )

            after_commit_result = await self._run_git("rev-parse", "HEAD")
            if after_commit_result.returncode != 0:
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Unable to resolve HEAD after git pull",
                    status_code=500,
                    output=after_commit_result.output,
                )

            after_commit = after_commit_result.stdout.strip()
            updated = before_commit != after_commit
            restart_request = None

            if updated:
                if runtime_control.restart_requested:
                    restart_request = runtime_control.snapshot()
                else:
                    request = runtime_control.request_restart(
                        reason=RestartReason.UPDATE,
                        requested_by=requested_by.strip(),
                        source="api.system.update",
                    )
                    restart_request = request.to_payload()

            return {
                "accepted": True,
                "updated": updated,
                "alreadyUpToDate": not updated,
                "restartRequested": restart_request is not None,
                "restartRequest": restart_request,
                "repoPath": str(self._repo_root) if self._repo_root is not None else "",
                "branch": status["branch"],
                "upstream": status["upstream"],
                "beforeCommit": before_commit,
                "beforeCommitShort": before_commit[:12],
                "afterCommit": after_commit,
                "afterCommitShort": after_commit[:12],
                "output": pull_result.output,
            }

    async def _inspect(self, *, ignore_lock: bool) -> dict[str, Any]:
        payload = {
            "repoDetected": self._repo_root is not None,
            "repoPath": str(self._repo_root) if self._repo_root is not None else "",
            "branch": "",
            "upstream": "",
            "upstreamRef": "",
            "upstreamTrackingCommit": "",
            "upstreamTrackingCommitShort": "",
            "remoteName": "",
            "remoteUrl": "",
            "remoteHeadCommit": "",
            "remoteHeadCommitShort": "",
            "remoteCheckOk": False,
            "updateAvailable": False,
            "aheadCount": 0,
            "behindCount": 0,
            "currentCommit": "",
            "currentCommitShort": "",
            "dirty": False,
            "dirtyCount": 0,
            "dirtyEntries": [],
            "allowedBranches": list(self._allowed_branches),
            "canUpdate": False,
            "blockCode": None,
            "blockMessage": None,
            "updateInProgress": self.update_in_progress,
        }

        if self._repo_root is None:
            payload["blockCode"] = "repo_unavailable"
            payload["blockMessage"] = "The local application repository could not be resolved"
            return payload

        if not self._git_executable:
            payload["blockCode"] = "git_unavailable"
            payload["blockMessage"] = "git is not available on the server"
            return payload

        branch_result = await self._run_git("branch", "--show-current")
        commit_result = await self._run_git("rev-parse", "HEAD")
        status_result = await self._run_git("status", "--porcelain")
        upstream_result = await self._run_git(
            "rev-parse",
            "--abbrev-ref",
            "--symbolic-full-name",
            "@{upstream}",
        )

        branch = branch_result.stdout.strip()
        current_commit = commit_result.stdout.strip()
        upstream = upstream_result.stdout.strip() if upstream_result.returncode == 0 else ""
        dirty_entries = [line for line in status_result.stdout.splitlines() if line.strip()]

        payload.update(
            {
                "branch": branch,
                "upstream": upstream,
                "currentCommit": current_commit,
                "currentCommitShort": current_commit[:12],
                "dirty": bool(dirty_entries),
                "dirtyCount": len(dirty_entries),
                "dirtyEntries": dirty_entries[:10],
            }
        )

        if branch_result.returncode != 0 or not branch:
            payload["blockCode"] = "branch_unavailable"
            payload["blockMessage"] = "Unable to determine the current git branch"
            return payload

        if commit_result.returncode != 0 or not current_commit:
            payload["blockCode"] = "commit_unavailable"
            payload["blockMessage"] = "Unable to determine the current git commit"
            return payload

        if status_result.returncode != 0:
            payload["blockCode"] = "status_unavailable"
            payload["blockMessage"] = "Unable to inspect the current git working tree state"
            return payload

        if branch not in self._allowed_branches:
            payload["blockCode"] = "branch_not_allowed"
            payload["blockMessage"] = (
                f"Updates are only allowed on protected branches: {', '.join(self._allowed_branches)}"
            )
            return payload

        if dirty_entries:
            payload["blockCode"] = "working_tree_dirty"
            payload["blockMessage"] = "The working tree has uncommitted changes; update is blocked"
            return payload

        if upstream_result.returncode != 0 or not upstream:
            payload["blockCode"] = "missing_upstream"
            payload["blockMessage"] = "The current branch does not track an upstream remote branch"
            return payload

        remote_name_result = await self._run_git("config", "--get", f"branch.{branch}.remote")
        upstream_ref_result = await self._run_git("config", "--get", f"branch.{branch}.merge")
        remote_name = remote_name_result.stdout.strip()
        upstream_ref = upstream_ref_result.stdout.strip()
        if remote_name_result.returncode != 0 or upstream_ref_result.returncode != 0:
            payload["blockCode"] = "missing_upstream"
            payload["blockMessage"] = "Unable to resolve the configured upstream remote branch"
            return payload

        remote_result = await self._run_git("config", "--get", f"remote.{remote_name}.url")
        payload.update(
            {
                "upstreamRef": upstream_ref,
                "remoteName": remote_name,
                "remoteUrl": remote_result.stdout.strip() if remote_result.returncode == 0 else "",
            }
        )

        fetch_result = await self._run_git(
            "fetch",
            "--prune",
            remote_name,
            timeout=DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS,
        )
        if fetch_result.returncode != 0:
            payload["blockCode"] = "remote_upstream_unavailable"
            payload["blockMessage"] = "Unable to fetch the remote upstream branch"
            return payload

        upstream_commit_result = await self._run_git("rev-parse", "@{upstream}")
        counts_result = await self._run_git("rev-list", "--left-right", "--count", "HEAD...@{upstream}")
        upstream_tracking_commit = upstream_commit_result.stdout.strip()
        ahead_count, behind_count = parse_ahead_behind(counts_result.stdout)

        if upstream_commit_result.returncode != 0 or not upstream_tracking_commit:
            payload["blockCode"] = "upstream_unavailable"
            payload["blockMessage"] = "Unable to resolve the local upstream tracking commit"
            return payload

        if counts_result.returncode != 0:
            payload["blockCode"] = "upstream_compare_failed"
            payload["blockMessage"] = "Unable to compare the current branch with its upstream tracking branch"
            return payload

        payload.update(
            {
                "upstreamTrackingCommit": upstream_tracking_commit,
                "upstreamTrackingCommitShort": upstream_tracking_commit[:12],
                "remoteHeadCommit": upstream_tracking_commit,
                "remoteHeadCommitShort": upstream_tracking_commit[:12],
                "remoteCheckOk": True,
                "updateAvailable": upstream_tracking_commit != current_commit,
                "aheadCount": ahead_count,
                "behindCount": behind_count,
            }
        )

        if upstream_tracking_commit == current_commit:
            payload["blockCode"] = "already_up_to_date"
            payload["blockMessage"] = "The current branch already matches its remote upstream HEAD"
            return payload

        if ahead_count > 0:
            payload["blockCode"] = "local_ahead"
            payload["blockMessage"] = (
                "The local branch has commits not present in its upstream tracking branch; update is blocked"
            )
            return payload

        if self._lock.locked() and not ignore_lock:
            payload["blockCode"] = "update_in_progress"
            payload["blockMessage"] = "Another update is already running"
            return payload

        payload["canUpdate"] = True
        return payload

    async def _run_git(
        self,
        *args: str,
        timeout: float = DEFAULT_GIT_TIMEOUT_SECONDS,
    ) -> GitCommandResult:
        return await run_git(
            repo_root=self._repo_root,
            git_executable=self._git_executable,
            unavailable_message="Repository root is unavailable",
            args=args,
            timeout=timeout,
        )

    def _detect_repo_root(self, configured_repo: Any) -> Path | None:
        candidates: list[Path] = []

        if isinstance(configured_repo, str) and configured_repo.strip():
            candidate = Path(configured_repo.strip())
            if not candidate.is_absolute() and self._config_path is not None:
                candidate = self._config_path.parent / candidate
            candidates.append(candidate)

        if self._config_path is not None:
            candidates.append(self._config_path.parent)

        candidates.extend(
            [
                Path.cwd(),
                Path(__file__).resolve().parents[4],
            ]
        )

        seen: set[Path] = set()
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            repo_root = find_git_root(resolved)
            if repo_root is not None:
                return repo_root
        return None

    @staticmethod
    def _status_code_for_block(block_code: Any) -> int:
        if block_code in {
            "repo_unavailable",
            "git_unavailable",
            "branch_unavailable",
            "commit_unavailable",
            "status_unavailable",
            "upstream_unavailable",
            "upstream_compare_failed",
            "remote_upstream_unavailable",
        }:
            return 503
        return 409
