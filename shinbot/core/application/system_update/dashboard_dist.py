"""Dashboard dist update service."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import urllib.parse
from pathlib import Path
from typing import Any

from .common import (
    DEFAULT_ALLOWED_BRANCHES,
    DEFAULT_DASHBOARD_DIST_PACKAGE_MAX_BYTES,
    DEFAULT_GIT_TIMEOUT_SECONDS,
    DEFAULT_PULL_TIMEOUT_SECONDS,
    DEFAULT_REMOTE_CHECK_TIMEOUT_SECONDS,
    SystemUpdateError,
    normalize_allowed_branches,
)
from .dist_files import (
    read_deployed_package_sha256,
    read_deployed_source_commit,
    replace_dist,
    same_path,
    sha256_file,
    validate_dist,
)
from .dist_package import (
    extract_zip_package,
    is_url,
    resolve_expected_package_sha256,
    resolve_extracted_dist,
    stage_package_zip,
    validate_zip_package,
)
from .git_ops import (
    GitCommandResult,
    default_git_executable,
    parse_ahead_behind,
    run_git,
)


class DashboardDistUpdateService:
    """Update the served WebUI by copying a prebuilt dist checkout or zip package."""

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
        self._allowed_branches = normalize_allowed_branches(configured_branches)
        self._target_dist_dir = self._resolve_target_dist(config, target_dist_dir)
        self._git_executable = default_git_executable()
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

            replace_dist(
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
            "deployedPackageSha256": read_deployed_package_sha256(self._target_dist_dir),
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

        if is_url(self._package_source):
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

        package_sha = sha256_file(package_path)
        payload["packageSha256"] = package_sha
        payload["replaceRequired"] = package_sha != payload["deployedPackageSha256"]

        if expected_sha and package_sha != expected_sha:
            payload["blockCode"] = "package_hash_mismatch"
            payload["blockMessage"] = "Configured WebUI dist zip package does not match expected SHA256"
            return payload

        try:
            validate_zip_package(package_path)
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
            package_sha = sha256_file(staged_zip)
            expected_sha = self._resolve_expected_package_sha256()
            if expected_sha and package_sha != expected_sha:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Downloaded WebUI dist zip package does not match expected SHA256",
                    status_code=409,
                )

            extract_zip_package(staged_zip, extract_root)
            source_dist = resolve_extracted_dist(extract_root)
            replace_dist(
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
            "aheadCount": 0,
            "behindCount": 0,
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

        if same_path(source_dist, self._target_dist_dir):
            payload["blockCode"] = "same_source_and_target"
            payload["blockMessage"] = "WebUI dist source and target cannot be the same directory"
            return payload

        source_error = validate_dist(source_dist)
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
        deployed_commit = read_deployed_source_commit(self._target_dist_dir)

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
        payload.update(
            {
                "remoteName": remote_name,
                "upstreamRef": upstream_ref,
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
            payload["blockMessage"] = "Unable to fetch the WebUI dist remote upstream branch"
            return payload

        remote_head_result = await self._run_git("rev-parse", "@{upstream}")
        counts_result = await self._run_git("rev-list", "--left-right", "--count", "HEAD...@{upstream}")
        remote_head_commit = remote_head_result.stdout.strip()
        ahead_count, behind_count = parse_ahead_behind(counts_result.stdout)

        if remote_head_result.returncode != 0 or not remote_head_commit:
            payload["blockCode"] = "remote_upstream_unavailable"
            payload["blockMessage"] = "Unable to inspect the WebUI dist remote upstream branch"
            return payload

        if counts_result.returncode != 0:
            payload["blockCode"] = "upstream_compare_failed"
            payload["blockMessage"] = "Unable to compare the WebUI dist source with its upstream"
            return payload

        payload.update(
            {
                "remoteHeadCommit": remote_head_commit,
                "remoteHeadCommitShort": remote_head_commit[:12],
                "remoteCheckOk": True,
                "updateAvailable": remote_head_commit != current_commit,
                "aheadCount": ahead_count,
                "behindCount": behind_count,
            }
        )

        if ahead_count > 0:
            payload["blockCode"] = "local_ahead"
            payload["blockMessage"] = (
                "The WebUI dist source branch has commits not present in its upstream; update is blocked"
            )
            return payload

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
        return await run_git(
            repo_root=self._source_repo,
            git_executable=self._git_executable,
            unavailable_message="WebUI dist source repo is unavailable",
            args=args,
            timeout=timeout,
        )

    def _resolve_expected_package_sha256(self) -> str:
        return resolve_expected_package_sha256(
            expected_sha256=self._package_expected_sha256,
            expected_sha256_source=self._package_expected_sha256_source,
            allow_insecure_http=self._allow_insecure_http,
            resolve_path=self._resolve_path,
        )

    def _stage_package_zip(self, target_parent: Path) -> Path:
        return stage_package_zip(
            package_source=self._package_source,
            target_parent=target_parent,
            package_max_bytes=self._package_max_bytes,
            allow_insecure_http=self._allow_insecure_http,
            resolve_path=self._resolve_path,
        )

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

    @staticmethod
    def _status_code_for_block(block_code: Any) -> int:
        if block_code in {
            "repo_unavailable",
            "git_unavailable",
            "branch_unavailable",
            "commit_unavailable",
            "status_unavailable",
            "remote_upstream_unavailable",
            "upstream_compare_failed",
            "package_unavailable",
        }:
            return 503
        return 409
