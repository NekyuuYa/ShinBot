"""Safe local git update orchestration for the management API."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import shutil
import stat
import tempfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shinbot.core.application.runtime_control import RestartReason, RuntimeControl

DEFAULT_ALLOWED_BRANCHES = ("main", "master")
DEFAULT_PULL_TIMEOUT_SECONDS = 120.0
DEFAULT_GIT_TIMEOUT_SECONDS = 15.0
DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS = 20.0
MAX_OUTPUT_CHARS = 4000
DASHBOARD_DIST_MANIFEST = ".shinbot-dashboard-dist.json"
DEFAULT_DASHBOARD_DIST_PACKAGE_MAX_BYTES = 100 * 1024 * 1024


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
        self._allowed_branches = self._normalize_allowed_branches(configured_branches)
        self._git_executable = shutil.which("git")
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
        upstream_commit_result = await self._run_git("rev-parse", "@{upstream}")

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
        counts_result = await self._run_git("rev-list", "--left-right", "--count", "HEAD...@{upstream}")
        ahead_count, behind_count = self._parse_ahead_behind(counts_result.stdout)
        remote_head_result = await self._run_git(
            "ls-remote",
            remote_name,
            upstream_ref,
            timeout=DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS,
        )
        remote_head_commit = self._parse_ls_remote_head(remote_head_result.stdout)
        upstream_tracking_commit = upstream_commit_result.stdout.strip()

        payload.update(
            {
                "upstreamRef": upstream_ref,
                "upstreamTrackingCommit": upstream_tracking_commit,
                "upstreamTrackingCommitShort": upstream_tracking_commit[:12],
                "remoteName": remote_name,
                "remoteUrl": remote_result.stdout.strip() if remote_result.returncode == 0 else "",
                "aheadCount": ahead_count,
                "behindCount": behind_count,
            }
        )

        if upstream_commit_result.returncode != 0 or not upstream_tracking_commit:
            payload["blockCode"] = "upstream_unavailable"
            payload["blockMessage"] = "Unable to resolve the local upstream tracking commit"
            return payload

        if counts_result.returncode != 0:
            payload["blockCode"] = "upstream_compare_failed"
            payload["blockMessage"] = "Unable to compare the current branch with its upstream tracking branch"
            return payload

        if remote_head_result.returncode != 0 or not remote_head_commit:
            payload["blockCode"] = "remote_upstream_unavailable"
            payload["blockMessage"] = "Unable to inspect the remote upstream branch"
            return payload

        payload.update(
            {
                "remoteHeadCommit": remote_head_commit,
                "remoteHeadCommitShort": remote_head_commit[:12],
                "remoteCheckOk": True,
                "updateAvailable": remote_head_commit != current_commit,
            }
        )

        if remote_head_commit == current_commit:
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
        if self._repo_root is None:
            return GitCommandResult(returncode=1, stderr="Repository root is unavailable")
        if not self._git_executable:
            return GitCommandResult(returncode=1, stderr="git is not installed")

        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_SSH_COMMAND"] = "ssh -oBatchMode=yes"

        process = await asyncio.create_subprocess_exec(
            self._git_executable,
            *args,
            cwd=str(self._repo_root),
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except asyncio.TimeoutError as exc:
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
                Path(__file__).resolve().parents[3],
            ]
        )

        seen: set[Path] = set()
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            repo_root = self._find_git_root(resolved)
            if repo_root is not None:
                return repo_root
        return None

    def _find_git_root(self, start: Path) -> Path | None:
        current = start
        if current.is_file():
            current = current.parent

        for path in (current, *current.parents):
            if (path / ".git").exists():
                return path
        return None

    def _normalize_allowed_branches(self, raw: Any) -> tuple[str, ...]:
        if isinstance(raw, (list, tuple, set)):
            branches = tuple(str(item).strip() for item in raw if str(item).strip())
            if branches:
                return branches
        return DEFAULT_ALLOWED_BRANCHES

    def _parse_ahead_behind(self, raw: str) -> tuple[int, int]:
        parts = raw.strip().split()
        if len(parts) < 2:
            return 0, 0
        try:
            return int(parts[0]), int(parts[1])
        except ValueError:
            return 0, 0

    def _parse_ls_remote_head(self, raw: str) -> str:
        for line in raw.splitlines():
            parts = line.strip().split()
            if len(parts) >= 2:
                return parts[0]
        return ""

    def _status_code_for_block(self, block_code: Any) -> int:
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


class DashboardDistUpdateService:
    """Update the served WebUI by copying a prebuilt dist checkout."""

    def __init__(
        self,
        *,
        config: dict[str, Any],
        config_path: Path | str | None = None,
        target_dist_dir: Path | str | None = None,
    ) -> None:
        admin_cfg = config.get("admin", {})
        source_repo = admin_cfg.get("dashboard_dist_update_repo")
        source_subdir = admin_cfg.get("dashboard_dist_update_subdir", ".")
        package_source = admin_cfg.get("dashboard_dist_update_zip") or admin_cfg.get(
            "dashboard_dist_update_package"
        )
        configured_branches = admin_cfg.get(
            "dashboard_dist_update_allowed_branches",
            DEFAULT_ALLOWED_BRANCHES,
        )

        self._config_path = Path(config_path).resolve() if config_path is not None else None
        self._package_source = str(package_source).strip() if isinstance(package_source, str) else ""
        self._package_expected_sha256 = str(
            admin_cfg.get("dashboard_dist_update_zip_sha256")
            or admin_cfg.get("dashboard_dist_update_package_sha256")
            or ""
        ).strip()
        self._package_expected_sha256_source = str(
            admin_cfg.get("dashboard_dist_update_zip_sha256_url")
            or admin_cfg.get("dashboard_dist_update_package_sha256_url")
            or ""
        ).strip()
        self._package_max_bytes = int(
            admin_cfg.get(
                "dashboard_dist_update_package_max_bytes",
                DEFAULT_DASHBOARD_DIST_PACKAGE_MAX_BYTES,
            )
        )
        self._allow_insecure_http = bool(
            admin_cfg.get("dashboard_dist_update_allow_insecure_http", False)
        )
        self._mode = "zip" if self._package_source else "git"
        self._enabled = bool(self._package_source) or (
            isinstance(source_repo, str) and bool(source_repo.strip())
        )
        self._source_repo = (
            self._resolve_path(source_repo)
            if isinstance(source_repo, str) and bool(source_repo.strip())
            else None
        )
        self._source_subdir = str(source_subdir or ".").strip() or "."
        self._allowed_branches = self._normalize_allowed_branches(configured_branches)
        self._target_dist_dir = self._resolve_target_dist(config, target_dist_dir)
        self._git_executable = shutil.which("git")
        self._lock = asyncio.Lock()

    @property
    def update_in_progress(self) -> bool:
        return self._lock.locked()

    async def inspect(self) -> dict[str, Any]:
        if self._mode == "zip":
            return await self._inspect_zip(ignore_lock=False)
        return await self._inspect(ignore_lock=False)

    async def update_dist(self) -> dict[str, Any]:
        if self._lock.locked():
            raise SystemUpdateError(
                code="UPDATE_ALREADY_RUNNING",
                message="Another WebUI dist update is already running",
                status_code=409,
            )

        async with self._lock:
            if self._mode == "zip":
                return await self._update_dist_from_zip()

            status = await self._inspect(ignore_lock=True)
            if status["blockCode"] == "already_up_to_date":
                return {
                    "accepted": True,
                    "updated": False,
                    "copied": False,
                    "restartRequired": False,
                    "sourceCommit": status["currentCommit"],
                    "sourceCommitShort": status["currentCommitShort"],
                    "targetDistPath": status["targetDistPath"],
                    "output": "WebUI dist is already up to date.",
                }

            if not status["canUpdate"]:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=status["blockMessage"] or "WebUI dist update is not allowed",
                    status_code=self._status_code_for_block(status["blockCode"]),
                )

            output = ""
            if status["remoteHeadCommit"] != status["currentCommit"]:
                pull_result = await self._run_git(
                    "pull",
                    "--ff-only",
                    timeout=DEFAULT_PULL_TIMEOUT_SECONDS,
                )
                output = pull_result.output
                if pull_result.returncode != 0:
                    raise SystemUpdateError(
                        code="UPDATE_FAILED",
                        message="git pull --ff-only failed for WebUI dist repo",
                        status_code=500,
                        output=output,
                    )

            refreshed = await self._inspect(ignore_lock=True)
            if refreshed["blockCode"] not in {"already_up_to_date", None} and not refreshed[
                "canUpdate"
            ]:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=refreshed["blockMessage"] or "WebUI dist source became invalid",
                    status_code=self._status_code_for_block(refreshed["blockCode"]),
                )

            self._replace_dist(
                source_dist=Path(str(refreshed["sourceDistPath"])),
                target_dist=Path(str(refreshed["targetDistPath"])),
                source_commit=str(refreshed["currentCommit"]),
            )

            return {
                "accepted": True,
                "updated": status["remoteHeadCommit"] != status["currentCommit"],
                "copied": True,
                "restartRequired": False,
                "sourceCommit": refreshed["currentCommit"],
                "sourceCommitShort": refreshed["currentCommitShort"],
                "targetDistPath": refreshed["targetDistPath"],
                "output": output or "WebUI dist replaced from prebuilt source.",
            }

    async def _inspect_zip(self, *, ignore_lock: bool) -> dict[str, Any]:
        payload = {
            "enabled": self._enabled,
            "sourceType": "zip",
            "packageSource": self._package_source,
            "packageSha256": "",
            "expectedPackageSha256": self._package_expected_sha256,
            "expectedPackageSha256Url": self._package_expected_sha256_source,
            "deployedPackageSha256": self._read_deployed_package_sha256(self._target_dist_dir),
            "targetDistPath": str(self._target_dist_dir),
            "replaceRequired": True,
            "canUpdate": False,
            "blockCode": None,
            "blockMessage": None,
            "updateInProgress": self.update_in_progress,
        }

        if not self._enabled or not self._package_source:
            payload["blockCode"] = "not_configured"
            payload["blockMessage"] = "WebUI dist zip package source is not configured"
            return payload

        expected_sha = ""
        try:
            expected_sha = self._resolve_expected_package_sha256()
        except SystemUpdateError as exc:
            payload["blockCode"] = exc.code.lower()
            payload["blockMessage"] = exc.message
            return payload
        payload["expectedPackageSha256"] = expected_sha

        if self._lock.locked() and not ignore_lock:
            payload["blockCode"] = "update_in_progress"
            payload["blockMessage"] = "Another WebUI dist update is already running"
            return payload

        if self._is_url(self._package_source):
            source_url = urllib.parse.urlparse(self._package_source)
            if source_url.scheme == "http" and not self._allow_insecure_http:
                payload["blockCode"] = "insecure_package_url"
                payload["blockMessage"] = "WebUI dist zip package URL must use HTTPS"
                return payload

            # Avoid network work during status refresh. Download and validation happen only on POST.
            if expected_sha and expected_sha == payload["deployedPackageSha256"]:
                payload["blockCode"] = "already_up_to_date"
                payload["blockMessage"] = "The served WebUI dist already matches the configured zip package"
                return payload
            payload["canUpdate"] = True
            payload["blockMessage"] = "Zip package will be downloaded and verified when replacement starts"
            return payload

        package_path = self._resolve_path(self._package_source)
        payload["packageSource"] = str(package_path)
        if not package_path.is_file():
            payload["blockCode"] = "package_unavailable"
            payload["blockMessage"] = "Configured WebUI dist zip package does not exist"
            return payload

        package_size = package_path.stat().st_size
        if package_size > self._package_max_bytes:
            payload["blockCode"] = "package_too_large"
            payload["blockMessage"] = "Configured WebUI dist zip package exceeds the size limit"
            return payload

        package_sha = self._sha256_file(package_path)
        payload["packageSha256"] = package_sha
        payload["replaceRequired"] = package_sha != payload["deployedPackageSha256"]

        if expected_sha and package_sha != expected_sha:
            payload["blockCode"] = "package_hash_mismatch"
            payload["blockMessage"] = "Configured WebUI dist zip package does not match expected SHA256"
            return payload

        try:
            self._validate_zip_package(package_path)
        except SystemUpdateError as exc:
            payload["blockCode"] = exc.code.lower()
            payload["blockMessage"] = exc.message
            return payload

        if package_sha == payload["deployedPackageSha256"]:
            payload["blockCode"] = "already_up_to_date"
            payload["blockMessage"] = "The served WebUI dist already matches the configured zip package"
            return payload

        payload["canUpdate"] = True
        return payload

    async def _update_dist_from_zip(self) -> dict[str, Any]:
        status = await self._inspect_zip(ignore_lock=True)
        if status["blockCode"] == "already_up_to_date":
            return {
                "accepted": True,
                "updated": False,
                "copied": False,
                "restartRequired": False,
                "sourceCommit": status["packageSha256"],
                "sourceCommitShort": str(status["packageSha256"])[:12],
                "packageSha256": status["packageSha256"],
                "packageSha256Short": str(status["packageSha256"])[:12],
                "targetDistPath": status["targetDistPath"],
                "output": "WebUI dist is already up to date.",
            }

        if not status["canUpdate"]:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message=status["blockMessage"] or "WebUI dist zip package update is not allowed",
                status_code=self._status_code_for_block(status["blockCode"]),
            )

        target_parent = self._target_dist_dir.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        staged_zip = self._stage_package_zip(target_parent)
        extract_root = Path(tempfile.mkdtemp(prefix=".webui-dist-extract-", dir=str(target_parent)))

        try:
            package_sha = self._sha256_file(staged_zip)
            expected_sha = self._resolve_expected_package_sha256()
            if expected_sha and package_sha != expected_sha:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Downloaded WebUI dist zip package does not match expected SHA256",
                    status_code=409,
                )

            self._extract_zip_package(staged_zip, extract_root)
            source_dist = self._resolve_extracted_dist(extract_root)
            self._replace_dist(
                source_dist=source_dist,
                target_dist=self._target_dist_dir,
                source_commit=package_sha,
                package_sha256=package_sha,
            )
        finally:
            if staged_zip.exists():
                staged_zip.unlink()
            if extract_root.exists():
                shutil.rmtree(extract_root)

        return {
            "accepted": True,
            "updated": True,
            "copied": True,
            "restartRequired": False,
            "sourceCommit": package_sha,
            "sourceCommitShort": package_sha[:12],
            "packageSha256": package_sha,
            "packageSha256Short": package_sha[:12],
            "targetDistPath": str(self._target_dist_dir),
            "output": "WebUI dist replaced from zip package.",
        }

    async def _inspect(self, *, ignore_lock: bool) -> dict[str, Any]:
        payload = {
            "enabled": self._enabled,
            "sourceRepoPath": str(self._source_repo) if self._source_repo is not None else "",
            "sourceSubdir": self._source_subdir,
            "sourceDistPath": "",
            "targetDistPath": str(self._target_dist_dir),
            "branch": "",
            "upstream": "",
            "upstreamRef": "",
            "remoteName": "",
            "remoteUrl": "",
            "currentCommit": "",
            "currentCommitShort": "",
            "remoteHeadCommit": "",
            "remoteHeadCommitShort": "",
            "remoteCheckOk": False,
            "updateAvailable": False,
            "replaceRequired": False,
            "deployedSourceCommit": "",
            "deployedSourceCommitShort": "",
            "dirty": False,
            "dirtyCount": 0,
            "dirtyEntries": [],
            "allowedBranches": list(self._allowed_branches),
            "canUpdate": False,
            "blockCode": None,
            "blockMessage": None,
            "updateInProgress": self.update_in_progress,
        }

        if not self._enabled:
            payload["blockCode"] = "not_configured"
            payload["blockMessage"] = "WebUI dist update source is not configured"
            return payload

        if self._source_repo is None or not (self._source_repo / ".git").exists():
            payload["blockCode"] = "repo_unavailable"
            payload["blockMessage"] = "The configured WebUI dist source repo is unavailable"
            return payload

        if not self._git_executable:
            payload["blockCode"] = "git_unavailable"
            payload["blockMessage"] = "git is not available on the server"
            return payload

        source_dist = (self._source_repo / self._source_subdir).resolve()
        payload["sourceDistPath"] = str(source_dist)

        if self._same_path(source_dist, self._target_dist_dir):
            payload["blockCode"] = "same_source_and_target"
            payload["blockMessage"] = "WebUI dist source and target cannot be the same directory"
            return payload

        source_error = self._validate_dist(source_dist)
        if source_error:
            payload["blockCode"] = "invalid_dist_source"
            payload["blockMessage"] = source_error
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
        deployed_commit = self._read_deployed_source_commit(self._target_dist_dir)

        payload.update(
            {
                "branch": branch,
                "upstream": upstream,
                "currentCommit": current_commit,
                "currentCommitShort": current_commit[:12],
                "dirty": bool(dirty_entries),
                "dirtyCount": len(dirty_entries),
                "dirtyEntries": dirty_entries[:10],
                "deployedSourceCommit": deployed_commit,
                "deployedSourceCommitShort": deployed_commit[:12],
                "replaceRequired": deployed_commit != current_commit,
            }
        )

        if branch_result.returncode != 0 or not branch:
            payload["blockCode"] = "branch_unavailable"
            payload["blockMessage"] = "Unable to determine the WebUI dist source branch"
            return payload

        if commit_result.returncode != 0 or not current_commit:
            payload["blockCode"] = "commit_unavailable"
            payload["blockMessage"] = "Unable to determine the WebUI dist source commit"
            return payload

        if status_result.returncode != 0:
            payload["blockCode"] = "status_unavailable"
            payload["blockMessage"] = "Unable to inspect the WebUI dist source working tree"
            return payload

        if branch not in self._allowed_branches:
            payload["blockCode"] = "branch_not_allowed"
            payload["blockMessage"] = (
                f"WebUI dist updates are only allowed on branches: {', '.join(self._allowed_branches)}"
            )
            return payload

        if dirty_entries:
            payload["blockCode"] = "working_tree_dirty"
            payload["blockMessage"] = "The WebUI dist source repo has uncommitted changes"
            return payload

        if upstream_result.returncode != 0 or not upstream:
            payload["blockCode"] = "missing_upstream"
            payload["blockMessage"] = "The WebUI dist source branch does not track an upstream"
            return payload

        remote_name_result = await self._run_git("config", "--get", f"branch.{branch}.remote")
        upstream_ref_result = await self._run_git("config", "--get", f"branch.{branch}.merge")
        remote_name = remote_name_result.stdout.strip()
        upstream_ref = upstream_ref_result.stdout.strip()
        if remote_name_result.returncode != 0 or upstream_ref_result.returncode != 0:
            payload["blockCode"] = "missing_upstream"
            payload["blockMessage"] = "Unable to resolve the WebUI dist upstream remote branch"
            return payload

        remote_result = await self._run_git("config", "--get", f"remote.{remote_name}.url")
        remote_head_result = await self._run_git(
            "ls-remote",
            remote_name,
            upstream_ref,
            timeout=DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS,
        )
        remote_head_commit = self._parse_ls_remote_head(remote_head_result.stdout)

        payload.update(
            {
                "remoteName": remote_name,
                "upstreamRef": upstream_ref,
                "remoteUrl": remote_result.stdout.strip() if remote_result.returncode == 0 else "",
            }
        )

        if remote_head_result.returncode != 0 or not remote_head_commit:
            payload["blockCode"] = "remote_upstream_unavailable"
            payload["blockMessage"] = "Unable to inspect the WebUI dist remote upstream branch"
            return payload

        payload.update(
            {
                "remoteHeadCommit": remote_head_commit,
                "remoteHeadCommitShort": remote_head_commit[:12],
                "remoteCheckOk": True,
                "updateAvailable": remote_head_commit != current_commit,
            }
        )

        if self._lock.locked() and not ignore_lock:
            payload["blockCode"] = "update_in_progress"
            payload["blockMessage"] = "Another WebUI dist update is already running"
            return payload

        if remote_head_commit == current_commit and not payload["replaceRequired"]:
            payload["blockCode"] = "already_up_to_date"
            payload["blockMessage"] = "The served WebUI dist already matches the source repo HEAD"
            return payload

        payload["canUpdate"] = True
        return payload

    async def _run_git(
        self,
        *args: str,
        timeout: float = DEFAULT_GIT_TIMEOUT_SECONDS,
    ) -> GitCommandResult:
        if self._source_repo is None:
            return GitCommandResult(returncode=1, stderr="WebUI dist source repo is unavailable")
        if not self._git_executable:
            return GitCommandResult(returncode=1, stderr="git is not installed")

        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_SSH_COMMAND"] = "ssh -oBatchMode=yes"
        process = await asyncio.create_subprocess_exec(
            self._git_executable,
            *args,
            cwd=str(self._source_repo),
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        except asyncio.TimeoutError as exc:
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

    def _replace_dist(
        self,
        *,
        source_dist: Path,
        target_dist: Path,
        source_commit: str,
        package_sha256: str = "",
    ) -> None:
        source_error = self._validate_dist(source_dist)
        if source_error:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message=source_error,
                status_code=409,
            )

        target_parent = target_dist.parent
        target_parent.mkdir(parents=True, exist_ok=True)
        tmp_dir = target_parent / f".{target_dist.name}.tmp-{os.getpid()}"
        backup_dir = target_parent / f".{target_dist.name}.backup-{os.getpid()}"

        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        if backup_dir.exists():
            shutil.rmtree(backup_dir)

        try:
            shutil.copytree(source_dist, tmp_dir)
            self._write_manifest(tmp_dir, source_commit, package_sha256=package_sha256)
            tmp_error = self._validate_dist(tmp_dir)
            if tmp_error:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=tmp_error,
                    status_code=409,
                )

            if target_dist.exists():
                target_dist.rename(backup_dir)
            tmp_dir.rename(target_dist)
            if backup_dir.exists():
                shutil.rmtree(backup_dir)
        except Exception:
            if target_dist.exists() and backup_dir.exists():
                shutil.rmtree(target_dist)
            if backup_dir.exists():
                backup_dir.rename(target_dist)
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            raise

    def _validate_dist(self, dist_dir: Path) -> str:
        if not dist_dir.is_dir():
            return "WebUI dist source directory does not exist"
        if not (dist_dir / "index.html").is_file():
            return "WebUI dist source must contain index.html"
        for path in dist_dir.rglob("*"):
            if path.is_symlink():
                return "WebUI dist source must not contain symlinks"
        return ""

    def _read_deployed_source_commit(self, target_dist: Path) -> str:
        manifest_path = target_dist / DASHBOARD_DIST_MANIFEST
        if not manifest_path.is_file():
            return ""
        try:
            payload = json.loads(manifest_path.read_text("utf-8"))
        except Exception:
            return ""
        commit = payload.get("sourceCommit")
        return commit if isinstance(commit, str) else ""

    def _read_deployed_package_sha256(self, target_dist: Path) -> str:
        manifest_path = target_dist / DASHBOARD_DIST_MANIFEST
        if not manifest_path.is_file():
            return ""
        try:
            payload = json.loads(manifest_path.read_text("utf-8"))
        except Exception:
            return ""
        package_sha = payload.get("packageSha256")
        return package_sha if isinstance(package_sha, str) else ""

    def _write_manifest(self, dist_dir: Path, source_commit: str, *, package_sha256: str = "") -> None:
        payload = {"sourceCommit": source_commit}
        if package_sha256:
            payload["packageSha256"] = package_sha256
        (dist_dir / DASHBOARD_DIST_MANIFEST).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            "utf-8",
        )

    def _resolve_expected_package_sha256(self) -> str:
        if self._package_expected_sha256:
            return self._normalize_sha256(self._package_expected_sha256)
        if not self._package_expected_sha256_source:
            return ""

        if self._is_url(self._package_expected_sha256_source):
            parsed = urllib.parse.urlparse(self._package_expected_sha256_source)
            if parsed.scheme == "http" and not self._allow_insecure_http:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="WebUI dist SHA256 URL must use HTTPS",
                    status_code=409,
                )
            try:
                with urllib.request.urlopen(self._package_expected_sha256_source, timeout=10) as response:
                    raw = response.read(4096).decode("utf-8", errors="replace")
            except Exception as exc:
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message=f"Failed to read WebUI dist SHA256: {exc}",
                    status_code=502,
                ) from exc
            return self._normalize_sha256(raw)

        sha_path = self._resolve_path(self._package_expected_sha256_source)
        if not sha_path.is_file():
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="Configured WebUI dist SHA256 file does not exist",
                status_code=409,
            )
        return self._normalize_sha256(sha_path.read_text("utf-8"))

    def _normalize_sha256(self, raw: str) -> str:
        token = raw.strip().split()[0] if raw.strip() else ""
        if len(token) != 64 or any(ch not in "0123456789abcdefABCDEF" for ch in token):
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist SHA256 must be a 64-character hex digest",
                status_code=409,
            )
        return token.lower()

    def _stage_package_zip(self, target_parent: Path) -> Path:
        fd, tmp_name = tempfile.mkstemp(prefix=".webui-dist-package-", suffix=".zip", dir=target_parent)
        os.close(fd)
        tmp_zip = Path(tmp_name)

        if self._is_url(self._package_source):
            self._download_package(self._package_source, tmp_zip)
        else:
            package_path = self._resolve_path(self._package_source)
            if not package_path.is_file():
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Configured WebUI dist zip package does not exist",
                    status_code=409,
                )
            if package_path.stat().st_size > self._package_max_bytes:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Configured WebUI dist zip package exceeds the size limit",
                    status_code=409,
                )
            shutil.copyfile(package_path, tmp_zip)

        self._validate_zip_package(tmp_zip)
        return tmp_zip

    def _download_package(self, url: str, target: Path) -> None:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist zip package URL must use HTTP or HTTPS",
                status_code=409,
            )
        if parsed.scheme == "http" and not self._allow_insecure_http:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist zip package URL must use HTTPS",
                status_code=409,
            )

        request = urllib.request.Request(url, headers={"User-Agent": "ShinBot-WebUI-Updater"})
        downloaded = 0
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                with target.open("wb") as out:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        downloaded += len(chunk)
                        if downloaded > self._package_max_bytes:
                            raise SystemUpdateError(
                                code="UPDATE_NOT_ALLOWED",
                                message="Downloaded WebUI dist zip package exceeds the size limit",
                                status_code=409,
                            )
                        out.write(chunk)
        except SystemUpdateError:
            raise
        except Exception as exc:
            raise SystemUpdateError(
                code="UPDATE_FAILED",
                message=f"Failed to download WebUI dist zip package: {exc}",
                status_code=502,
            ) from exc

    def _validate_zip_package(self, package_path: Path) -> None:
        try:
            with zipfile.ZipFile(package_path) as archive:
                entries = archive.infolist()
                if not entries:
                    raise SystemUpdateError(
                        code="UPDATE_NOT_ALLOWED",
                        message="WebUI dist zip package is empty",
                        status_code=409,
                    )
                for info in entries:
                    self._validate_zip_entry(info)
                if not self._zip_contains_dist_index(entries):
                    raise SystemUpdateError(
                        code="UPDATE_NOT_ALLOWED",
                        message=(
                            "WebUI dist zip package must contain index.html at root "
                            "or in one top-level directory"
                        ),
                        status_code=409,
                    )
        except zipfile.BadZipFile as exc:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist package is not a valid zip file",
                status_code=409,
            ) from exc

    def _zip_contains_dist_index(self, entries: list[zipfile.ZipInfo]) -> bool:
        names = [info.filename.strip("/") for info in entries if not info.is_dir()]
        names = [name for name in names if name and not name.startswith("__MACOSX/")]
        if "index.html" in names:
            return True

        top_levels = {name.split("/", 1)[0] for name in names if "/" in name}
        return len(top_levels) == 1 and f"{next(iter(top_levels))}/index.html" in names

    def _extract_zip_package(self, package_path: Path, extract_root: Path) -> None:
        with zipfile.ZipFile(package_path) as archive:
            for info in archive.infolist():
                self._validate_zip_entry(info)
                destination = (extract_root / info.filename).resolve()
                if not destination.is_relative_to(extract_root):
                    raise SystemUpdateError(
                        code="UPDATE_NOT_ALLOWED",
                        message="WebUI dist zip package contains unsafe paths",
                        status_code=409,
                    )
            archive.extractall(extract_root)

    def _validate_zip_entry(self, info: zipfile.ZipInfo) -> None:
        name = info.filename
        if not name or name.startswith(("/", "\\")):
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist zip package contains absolute paths",
                status_code=409,
            )
        normalized = Path(name)
        if ".." in normalized.parts:
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist zip package contains parent-directory paths",
                status_code=409,
            )

        file_type = (info.external_attr >> 16) & 0o170000
        if stat.S_ISLNK(file_type):
            raise SystemUpdateError(
                code="UPDATE_NOT_ALLOWED",
                message="WebUI dist zip package must not contain symlinks",
                status_code=409,
            )

    def _resolve_extracted_dist(self, extract_root: Path) -> Path:
        if (extract_root / "index.html").is_file():
            self._ensure_no_symlinks(extract_root)
            return extract_root

        children = [path for path in extract_root.iterdir() if path.name != "__MACOSX"]
        dirs = [path for path in children if path.is_dir()]
        if len(dirs) == 1 and (dirs[0] / "index.html").is_file():
            self._ensure_no_symlinks(dirs[0])
            return dirs[0]

        raise SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="WebUI dist zip package must contain index.html at root or in one top-level directory",
            status_code=409,
        )

    def _ensure_no_symlinks(self, root: Path) -> None:
        for path in root.rglob("*"):
            if path.is_symlink():
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="WebUI dist package must not contain symlinks",
                    status_code=409,
                )

    def _sha256_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _resolve_target_dist(self, config: dict[str, Any], target_dist_dir: Path | str | None) -> Path:
        if target_dist_dir is not None:
            return Path(target_dist_dir).resolve()

        configured_dist = config.get("admin", {}).get("dashboard_dist")
        if isinstance(configured_dist, str) and configured_dist.strip():
            return self._resolve_path(configured_dist)

        if self._config_path is not None:
            return (self._config_path.parent / "dashboard" / "dist").resolve()
        return (Path.cwd() / "dashboard" / "dist").resolve()

    def _resolve_path(self, raw: Any) -> Path:
        path = Path(str(raw).strip())
        if not path.is_absolute() and self._config_path is not None:
            path = self._config_path.parent / path
        return path.resolve()

    def _is_url(self, raw: str) -> bool:
        return urllib.parse.urlparse(raw).scheme in {"http", "https"}

    def _normalize_allowed_branches(self, raw: Any) -> tuple[str, ...]:
        if isinstance(raw, (list, tuple, set)):
            branches = tuple(str(item).strip() for item in raw if str(item).strip())
            if branches:
                return branches
        return DEFAULT_ALLOWED_BRANCHES

    def _parse_ls_remote_head(self, raw: str) -> str:
        for line in raw.splitlines():
            parts = line.strip().split()
            if len(parts) >= 2:
                return parts[0]
        return ""

    def _same_path(self, left: Path, right: Path) -> bool:
        try:
            return left.resolve() == right.resolve()
        except OSError:
            return False

    def _status_code_for_block(self, block_code: Any) -> int:
        if block_code in {
            "repo_unavailable",
            "git_unavailable",
            "branch_unavailable",
            "commit_unavailable",
            "status_unavailable",
            "remote_upstream_unavailable",
            "package_unavailable",
        }:
            return 503
        return 409
