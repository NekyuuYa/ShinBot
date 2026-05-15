from __future__ import annotations

import zipfile
from pathlib import Path

import pytest
from fastapi import HTTPException

from shinbot.api.routers import system as system_router
from shinbot.core.application.runtime_control import RestartReason, RuntimeControl
from shinbot.core.application.system_update import (
    DASHBOARD_DIST_MANIFEST,
    DashboardDistUpdateService,
    GitCommandResult,
    SystemUpdateError,
    SystemUpdateService,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class _FakeAuthConfig:
    def __init__(self, *, username: str = "admin", default_credentials: bool = True) -> None:
        self.username = username
        self._default_credentials = default_credentials

    def is_using_default_credentials(self) -> bool:
        return self._default_credentials


class _FakeSystemUpdateService:
    def __init__(
        self,
        *,
        status: dict[str, object] | None = None,
        result: dict[str, object] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.status = status or {}
        self.result = result or {}
        self.error = error
        self.inspect_called = False
        self.pull_called = False
        self.requested_by = ""

    async def inspect(self) -> dict[str, object]:
        self.inspect_called = True
        if self.error is not None:
            raise self.error
        return self.status

    async def pull_and_request_restart(
        self,
        *,
        runtime_control: RuntimeControl,
        requested_by: str = "",
    ) -> dict[str, object]:
        self.pull_called = True
        self.requested_by = requested_by
        if self.error is not None:
            raise self.error
        return self.result


def _base_update_status() -> dict[str, object]:
    return {
        "repoDetected": True,
        "repoPath": "/repo",
        "branch": "master",
        "upstream": "origin/master",
        "upstreamRef": "refs/heads/master",
        "upstreamTrackingCommit": "0123456789abcdef0123456789abcdef01234567",
        "upstreamTrackingCommitShort": "0123456789ab",
        "remoteName": "origin",
        "remoteUrl": "git@example.com:shinbot.git",
        "remoteHeadCommit": "fedcba9876543210fedcba9876543210fedcba98",
        "remoteHeadCommitShort": "fedcba987654",
        "remoteCheckOk": True,
        "updateAvailable": True,
        "aheadCount": 0,
        "behindCount": 1,
        "currentCommit": "0123456789abcdef0123456789abcdef01234567",
        "currentCommitShort": "0123456789ab",
        "dirty": False,
        "dirtyCount": 0,
        "dirtyEntries": [],
        "allowedBranches": ["main", "master"],
        "canUpdate": True,
        "blockCode": None,
        "blockMessage": None,
        "updateInProgress": False,
    }


@pytest.mark.asyncio
async def test_system_update_state_blocks_default_credentials():
    fake_service = _FakeSystemUpdateService(status=_base_update_status())
    response = await system_router.get_update_state(
        auth_config=_FakeAuthConfig(default_credentials=True),
        runtime_control=RuntimeControl(),
        system_update=fake_service,
    )

    payload = response["data"]
    assert fake_service.inspect_called is True
    assert payload["credentialsChangeRequired"] is True
    assert payload["canUpdate"] is False
    assert payload["blockCode"] == "default_credentials"


@pytest.mark.asyncio
async def test_system_update_route_rejects_default_credentials_without_calling_service():
    fake_service = _FakeSystemUpdateService()

    with pytest.raises(HTTPException) as exc_info:
        await system_router.pull_update_and_restart(
            auth_config=_FakeAuthConfig(default_credentials=True),
            runtime_control=RuntimeControl(),
            system_update=fake_service,
        )

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail["code"] == "UPDATE_NOT_ALLOWED"
    assert fake_service.pull_called is False


@pytest.mark.asyncio
async def test_system_update_route_returns_success_payload():
    fake_service = _FakeSystemUpdateService(
        result={
            "accepted": True,
            "updated": True,
            "alreadyUpToDate": False,
            "restartRequested": True,
            "restartRequest": {
                "reason": "update",
                "requested_at": 1234567890,
                "requested_by": "owner",
                "source": "api.system.update",
            },
            "repoPath": "/repo",
            "branch": "master",
            "upstream": "origin/master",
            "beforeCommit": "0123456789abcdef0123456789abcdef01234567",
            "beforeCommitShort": "0123456789ab",
            "afterCommit": "fedcba9876543210fedcba9876543210fedcba98",
            "afterCommitShort": "fedcba987654",
            "output": "Updating 0123456..fedcba9",
        }
    )
    response = await system_router.pull_update_and_restart(
        auth_config=_FakeAuthConfig(username="owner", default_credentials=False),
        runtime_control=RuntimeControl(),
        system_update=fake_service,
    )

    payload = response["data"]
    assert fake_service.pull_called is True
    assert fake_service.requested_by == "owner"
    assert payload["updated"] is True
    assert payload["restartRequested"] is True
    assert payload["afterCommitShort"] == "fedcba987654"


@pytest.mark.asyncio
async def test_system_update_route_maps_service_errors():
    fake_service = _FakeSystemUpdateService(
        error=SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="The working tree has uncommitted changes; update is blocked",
            status_code=409,
        )
    )

    with pytest.raises(HTTPException) as exc_info:
        await system_router.pull_update_and_restart(
            auth_config=_FakeAuthConfig(username="owner", default_credentials=False),
            runtime_control=RuntimeControl(),
            system_update=fake_service,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "UPDATE_NOT_ALLOWED"
    assert "working tree" in exc_info.value.detail["message"]


@pytest.mark.asyncio
async def test_system_update_service_blocks_dirty_working_tree(tmp_path: Path, monkeypatch):
    (tmp_path / ".git").mkdir()
    service = SystemUpdateService(
        config={"admin": {"update_repo": str(tmp_path)}},
        config_path=tmp_path / "config.toml",
    )
    service._git_executable = "git"

    async def fake_run_git(*args, **kwargs):
        responses = {
            ("branch", "--show-current"): GitCommandResult(returncode=0, stdout="master\n"),
            ("rev-parse", "HEAD"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("status", "--porcelain"): GitCommandResult(returncode=0, stdout=" M README.md\n"),
            ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="origin/master\n",
            ),
            ("rev-parse", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("config", "--get", "remote.origin.url"): GitCommandResult(
                returncode=0,
                stdout="git@example.com:shinbot.git\n",
            ),
        }
        return responses[args]

    monkeypatch.setattr(service, "_run_git", fake_run_git)

    status = await service.inspect()

    assert status["canUpdate"] is False
    assert status["dirty"] is True
    assert status["blockCode"] == "working_tree_dirty"


@pytest.mark.asyncio
async def test_system_update_service_requests_restart_when_pull_advances_head(
    tmp_path: Path,
    monkeypatch,
):
    (tmp_path / ".git").mkdir()
    service = SystemUpdateService(
        config={"admin": {"update_repo": str(tmp_path)}},
        config_path=tmp_path / "config.toml",
    )
    service._git_executable = "git"
    runtime_control = RuntimeControl()

    head_results = iter(
        [
            GitCommandResult(returncode=0, stdout="0123456789abcdef0123456789abcdef01234567\n"),
            GitCommandResult(returncode=0, stdout="fedcba9876543210fedcba9876543210fedcba98\n"),
        ]
    )

    async def fake_run_git(*args, **kwargs):
        if args == ("rev-parse", "HEAD"):
            return next(head_results)
        if args == ("pull", "--ff-only"):
            return GitCommandResult(returncode=0, stdout="Updating 0123456..fedcba9\n")
        responses = {
            ("branch", "--show-current"): GitCommandResult(returncode=0, stdout="master\n"),
            ("status", "--porcelain"): GitCommandResult(returncode=0, stdout=""),
            ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="origin/master\n",
            ),
            ("rev-parse", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="fedcba9876543210fedcba9876543210fedcba98\n",
            ),
            ("config", "--get", "branch.master.remote"): GitCommandResult(
                returncode=0,
                stdout="origin\n",
            ),
            ("config", "--get", "branch.master.merge"): GitCommandResult(
                returncode=0,
                stdout="refs/heads/master\n",
            ),
            ("config", "--get", "remote.origin.url"): GitCommandResult(
                returncode=0,
                stdout="git@example.com:shinbot.git\n",
            ),
            ("fetch", "--prune", "origin"): GitCommandResult(returncode=0),
            ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0\t1\n",
            ),
        }
        return responses[args]

    monkeypatch.setattr(service, "_run_git", fake_run_git)

    result = await service.pull_and_request_restart(
        runtime_control=runtime_control,
        requested_by="owner",
    )

    assert result["updated"] is True
    assert result["restartRequested"] is True
    assert runtime_control.restart_requested is True
    assert runtime_control.restart_request is not None
    assert runtime_control.restart_request.reason == RestartReason.UPDATE
    assert runtime_control.restart_request.requested_by == "owner"


@pytest.mark.asyncio
async def test_system_update_service_blocks_when_remote_upstream_cannot_be_checked(
    tmp_path: Path,
    monkeypatch,
):
    (tmp_path / ".git").mkdir()
    service = SystemUpdateService(
        config={"admin": {"update_repo": str(tmp_path)}},
        config_path=tmp_path / "config.toml",
    )
    service._git_executable = "git"

    async def fake_run_git(*args, **kwargs):
        responses = {
            ("branch", "--show-current"): GitCommandResult(returncode=0, stdout="master\n"),
            ("rev-parse", "HEAD"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("status", "--porcelain"): GitCommandResult(returncode=0, stdout=""),
            ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="origin/master\n",
            ),
            ("rev-parse", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("config", "--get", "branch.master.remote"): GitCommandResult(
                returncode=0,
                stdout="origin\n",
            ),
            ("config", "--get", "branch.master.merge"): GitCommandResult(
                returncode=0,
                stdout="refs/heads/master\n",
            ),
            ("config", "--get", "remote.origin.url"): GitCommandResult(
                returncode=0,
                stdout="git@example.com:shinbot.git\n",
            ),
            ("fetch", "--prune", "origin"): GitCommandResult(
                returncode=128,
                stderr="Could not read from remote repository.",
            ),
        }
        return responses[args]

    monkeypatch.setattr(service, "_run_git", fake_run_git)

    status = await service.inspect()

    assert status["canUpdate"] is False
    assert status["remoteCheckOk"] is False
    assert status["blockCode"] == "remote_upstream_unavailable"


@pytest.mark.asyncio
async def test_system_update_service_skips_restart_when_already_up_to_date(
    tmp_path: Path,
    monkeypatch,
):
    (tmp_path / ".git").mkdir()
    service = SystemUpdateService(
        config={"admin": {"update_repo": str(tmp_path)}},
        config_path=tmp_path / "config.toml",
    )
    service._git_executable = "git"
    runtime_control = RuntimeControl()

    async def fake_run_git(*args, **kwargs):
        if args == ("rev-parse", "HEAD"):
            return GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            )
        if args == ("pull", "--ff-only"):
            raise AssertionError("git pull should not run when remote HEAD already matches local HEAD")
        responses = {
            ("branch", "--show-current"): GitCommandResult(returncode=0, stdout="master\n"),
            ("status", "--porcelain"): GitCommandResult(returncode=0, stdout=""),
            ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="origin/master\n",
            ),
            ("rev-parse", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("config", "--get", "branch.master.remote"): GitCommandResult(
                returncode=0,
                stdout="origin\n",
            ),
            ("config", "--get", "branch.master.merge"): GitCommandResult(
                returncode=0,
                stdout="refs/heads/master\n",
            ),
            ("config", "--get", "remote.origin.url"): GitCommandResult(
                returncode=0,
                stdout="git@example.com:shinbot.git\n",
            ),
            ("fetch", "--prune", "origin"): GitCommandResult(returncode=0),
            ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0\t0\n",
            ),
        }
        return responses[args]

    monkeypatch.setattr(service, "_run_git", fake_run_git)

    result = await service.pull_and_request_restart(
        runtime_control=runtime_control,
        requested_by="owner",
    )

    assert result["updated"] is False
    assert result["alreadyUpToDate"] is True
    assert result["restartRequested"] is False
    assert runtime_control.restart_requested is False


@pytest.mark.asyncio
async def test_dashboard_dist_update_service_disabled_without_config(tmp_path: Path):
    service = DashboardDistUpdateService(
        config={"admin": {}},
        config_path=tmp_path / "config.toml",
        target_dist_dir=tmp_path / "dashboard" / "dist",
    )

    status = await service.inspect()

    assert status["enabled"] is False
    assert status["canUpdate"] is False
    assert status["blockCode"] == "not_configured"


@pytest.mark.asyncio
async def test_dashboard_dist_update_service_replaces_target_from_zip_package(tmp_path: Path):
    package_path = tmp_path / "webui-dist.zip"
    target_dist = tmp_path / "served" / "dist"
    target_dist.mkdir(parents=True)
    (target_dist / "index.html").write_text("<html>old</html>", "utf-8")

    with zipfile.ZipFile(package_path, "w") as archive:
        archive.writestr("index.html", "<html>new</html>")
        archive.writestr("assets/index.js", "console.log('new')")

    service = DashboardDistUpdateService(
        config={"admin": {"dashboard_dist_update_zip": str(package_path)}},
        config_path=tmp_path / "config.toml",
        target_dist_dir=target_dist,
    )

    status = await service.inspect()
    result = await service.update_dist()

    assert status["enabled"] is True
    assert status["sourceType"] == "zip"
    assert status["canUpdate"] is True
    assert len(status["packageSha256"]) == 64
    assert result["copied"] is True
    assert result["restartRequired"] is False
    assert result["packageSha256"] == status["packageSha256"]
    assert (target_dist / "index.html").read_text("utf-8") == "<html>new</html>"
    assert (target_dist / "assets" / "index.js").is_file()
    assert (target_dist / DASHBOARD_DIST_MANIFEST).is_file()


@pytest.mark.asyncio
async def test_dashboard_dist_update_service_replaces_target_from_prebuilt_source(
    tmp_path: Path,
    monkeypatch,
):
    source_repo = tmp_path / "dist-source"
    source_dist = source_repo / "dist"
    target_dist = tmp_path / "served" / "dist"
    (source_repo / ".git").mkdir(parents=True)
    source_dist.mkdir()
    target_dist.mkdir(parents=True)
    (source_dist / "index.html").write_text("<html>new</html>", "utf-8")
    (source_dist / "assets").mkdir()
    (source_dist / "assets" / "index.js").write_text("console.log('new')", "utf-8")
    (target_dist / "index.html").write_text("<html>old</html>", "utf-8")

    service = DashboardDistUpdateService(
        config={
            "admin": {
                "dashboard_dist_update_repo": str(source_repo),
                "dashboard_dist_update_subdir": "dist",
            }
        },
        config_path=tmp_path / "config.toml",
        target_dist_dir=target_dist,
    )
    service._git_executable = "git"

    async def fake_run_git(*args, **kwargs):
        responses = {
            ("branch", "--show-current"): GitCommandResult(returncode=0, stdout="master\n"),
            ("rev-parse", "HEAD"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("status", "--porcelain"): GitCommandResult(returncode=0, stdout=""),
            ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="origin/master\n",
            ),
            ("config", "--get", "branch.master.remote"): GitCommandResult(
                returncode=0,
                stdout="origin\n",
            ),
            ("config", "--get", "branch.master.merge"): GitCommandResult(
                returncode=0,
                stdout="refs/heads/master\n",
            ),
            ("config", "--get", "remote.origin.url"): GitCommandResult(
                returncode=0,
                stdout="git@example.com:shinbot-dashboard-dist.git\n",
            ),
            ("fetch", "--prune", "origin"): GitCommandResult(returncode=0),
            ("rev-parse", "@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0123456789abcdef0123456789abcdef01234567\n",
            ),
            ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): GitCommandResult(
                returncode=0,
                stdout="0\t0\n",
            ),
        }
        return responses[args]

    monkeypatch.setattr(service, "_run_git", fake_run_git)

    status = await service.inspect()
    result = await service.update_dist()

    assert status["canUpdate"] is True
    assert status["replaceRequired"] is True
    assert result["copied"] is True
    assert result["restartRequired"] is False
    assert (target_dist / "index.html").read_text("utf-8") == "<html>new</html>"
    assert (target_dist / "assets" / "index.js").is_file()
    assert (target_dist / DASHBOARD_DIST_MANIFEST).is_file()
