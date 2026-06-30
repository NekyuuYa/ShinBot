from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import HTTPException

from shinbot.api.routers import system as system_router
from shinbot.core.application.runtime_control import RestartReason, RuntimeControl
from shinbot.core.application.system_update import (
    DashboardBuildService,
    FrameworkUpdateCommandService,
    GitCommandResult,
    SystemUpdateError,
)
from shinbot.core.application.system_update.application_update import SystemUpdateService

pytestmark = [pytest.mark.integration, pytest.mark.slow]


class _FakeAuthConfig:
    def __init__(self, *, username: str = "admin", default_credentials: bool = True) -> None:
        self.username = username
        self._default_credentials = default_credentials

    def is_using_default_credentials(self) -> bool:
        return self._default_credentials


class _FakeFrameworkUpdateService:
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
        self.run_called = False
        self.requested_by = ""

    async def inspect(self) -> dict[str, object]:
        self.inspect_called = True
        if self.error is not None:
            raise self.error
        return self.status

    async def run_and_request_restart(
        self,
        *,
        runtime_control: RuntimeControl,
        requested_by: str = "",
    ) -> dict[str, object]:
        self.run_called = True
        self.requested_by = requested_by
        if self.error is not None:
            raise self.error
        return self.result


def _base_update_status() -> dict[str, object]:
    return {
        "enabled": True,
        "workdir": "/repo",
        "command": "deploy/update.sh",
        "restartAfterSuccess": True,
        "canUpdate": True,
        "blockCode": None,
        "blockMessage": None,
        "updateInProgress": False,
    }


@pytest.mark.asyncio
async def test_system_update_state_blocks_default_credentials():
    fake_service = _FakeFrameworkUpdateService(status=_base_update_status())
    response = await system_router.get_update_state(
        auth_config=_FakeAuthConfig(default_credentials=True),
        runtime_control=RuntimeControl(),
        framework_update=fake_service,
    )

    payload = response["data"]
    assert fake_service.inspect_called is True
    assert payload["credentialsChangeRequired"] is True
    assert payload["canUpdate"] is False
    assert payload["blockCode"] == "default_credentials"


@pytest.mark.asyncio
async def test_system_update_route_rejects_default_credentials_without_calling_service():
    fake_service = _FakeFrameworkUpdateService()

    with pytest.raises(HTTPException) as exc_info:
        await system_router.pull_update_and_restart(
            auth_config=_FakeAuthConfig(default_credentials=True),
            runtime_control=RuntimeControl(),
            framework_update=fake_service,
        )

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail["code"] == "UPDATE_NOT_ALLOWED"
    assert fake_service.run_called is False


@pytest.mark.asyncio
async def test_system_update_route_returns_success_payload():
    fake_service = _FakeFrameworkUpdateService(
        result={
            "accepted": True,
            "updated": True,
            "restartRequested": True,
            "restartRequest": {
                "reason": "update",
                "requested_at": 1234567890,
                "requested_by": "owner",
                "source": "operator.framework_update",
            },
            "workdir": "/repo",
            "command": "deploy/update.sh",
            "output": "updated",
        }
    )
    response = await system_router.pull_update_and_restart(
        auth_config=_FakeAuthConfig(username="owner", default_credentials=False),
        runtime_control=RuntimeControl(),
        framework_update=fake_service,
    )

    payload = response["data"]
    assert fake_service.run_called is True
    assert fake_service.requested_by == "owner"
    assert payload["updated"] is True
    assert payload["restartRequested"] is True
    assert payload["command"] == "deploy/update.sh"


@pytest.mark.asyncio
async def test_system_update_route_maps_service_errors():
    fake_service = _FakeFrameworkUpdateService(
        error=SystemUpdateError(
            code="UPDATE_NOT_ALLOWED",
            message="Framework update command is not configured",
            status_code=409,
        )
    )

    with pytest.raises(HTTPException) as exc_info:
        await system_router.pull_update_and_restart(
            auth_config=_FakeAuthConfig(username="owner", default_credentials=False),
            runtime_control=RuntimeControl(),
            framework_update=fake_service,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "UPDATE_NOT_ALLOWED"
    assert "not configured" in exc_info.value.detail["message"]


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
async def test_dashboard_build_service_runs_configured_command(tmp_path: Path, monkeypatch):
    dashboard_dir = tmp_path / "dashboard"
    dashboard_dir.mkdir()
    (dashboard_dir / "package.json").write_text("{}", "utf-8")

    service = DashboardBuildService(
        config={"admin": {"dashboard_build_command": "pnpm build"}},
        config_path=tmp_path / "config.toml",
        dashboard_dir=dashboard_dir,
    )
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/pnpm" if name == "pnpm" else None)

    class _Process:
        returncode = 0

        async def communicate(self):
            return b"built\n", None

    async def fake_create_subprocess_exec(*args, **kwargs):
        assert args == ("pnpm", "build")
        assert kwargs["cwd"] == dashboard_dir
        return _Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    status = await service.inspect()
    result = await service.build()

    assert status["canBuild"] is True
    assert result["built"] is True
    assert result["output"] == "built\n"


@pytest.mark.asyncio
async def test_framework_update_command_service_runs_command_and_requests_restart(
    tmp_path: Path,
    monkeypatch,
):
    runtime_control = RuntimeControl()
    service = FrameworkUpdateCommandService(
        config={"admin": {"framework_update_command": "echo updated"}},
        config_path=tmp_path / "config.toml",
    )

    class _Process:
        returncode = 0

        async def communicate(self):
            return b"updated\n", None

    async def fake_create_subprocess_exec(*args, **kwargs):
        assert args == ("echo", "updated")
        assert kwargs["cwd"] == tmp_path
        return _Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    status = await service.inspect()
    result = await service.run_and_request_restart(
        runtime_control=runtime_control,
        requested_by="operator",
    )

    assert status["canUpdate"] is True
    assert result["restartRequested"] is True
    assert runtime_control.restart_requested is True
    assert result["output"] == "updated\n"
