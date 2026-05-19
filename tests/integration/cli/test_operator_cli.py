from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from shinbot.core.application.runtime_control import RestartReason, RuntimeControl
from shinbot.core.cli.commands import OperatorCommandRouter
from shinbot.core.cli.console import OperatorCliSession
from shinbot.utils import logger as logger_utils


class _AdapterManager:
    all_instances: list[object] = []

    def is_running(self, _instance_id: str) -> bool:
        return False


class _PluginManager:
    all_plugins: list[object] = []


class _CommandRegistry:
    all_commands: list[object] = []


class _FrameworkUpdateService:
    def __init__(self) -> None:
        self.called = False

    async def inspect(self) -> dict[str, object]:
        return {"canUpdate": True, "updateInProgress": False, "blockCode": None}

    async def run_and_request_restart(
        self,
        *,
        runtime_control: RuntimeControl,
        requested_by: str = "",
    ) -> dict[str, object]:
        self.called = True
        request = runtime_control.request_restart(
            reason=RestartReason.UPDATE,
            requested_by=requested_by,
            source="test",
        )
        return {
            "repoPath": "/repo",
            "branch": "master",
            "beforeCommitShort": "111111111111",
            "afterCommitShort": "222222222222",
            "updated": True,
            "restartRequested": True,
            "restartRequest": request.to_payload(),
            "output": "Updated.",
        }


class _DashboardBuildService:
    def __init__(self) -> None:
        self.called = False

    async def inspect(self) -> dict[str, object]:
        return {"canBuild": True, "buildInProgress": False, "blockCode": None}

    async def build(self) -> dict[str, object]:
        self.called = True
        return {
            "dashboardPath": "/repo/dashboard",
            "distPath": "/repo/dashboard/dist",
            "command": "pnpm build",
            "output": "built",
        }


class _Bot:
    def __init__(self) -> None:
        self.adapter_manager = _AdapterManager()
        self.plugin_manager = _PluginManager()
        self.command_registry = _CommandRegistry()
        self.bot_service_configs = (object(),)
        self.database = None
        self.model_runtime = object()
        self.agent_runtime = None
        self.runtime_control = RuntimeControl()


class _Boot:
    def __init__(self) -> None:
        self.state = SimpleNamespace(value="RUNNING")
        self.bot = _Bot()
        self.data_dir = Path("data")
        self.config_path = Path("config.toml")
        self.dashboard_build_service = _DashboardBuildService()
        self.framework_update_service = _FrameworkUpdateService()


@pytest.fixture
def restore_logging_runtime():
    original_policy = logger_utils.runtime_log_manager.third_party_noise_policy()
    try:
        yield
    finally:
        logger_utils.runtime_log_manager.set_third_party_noise_policy(original_policy)


@pytest.mark.asyncio
async def test_operator_command_router_accepts_slash_commands() -> None:
    router = OperatorCommandRouter(boot=_Boot(), api_host="127.0.0.1", api_port=3945)

    outcome = await router.execute("/status")

    assert outcome.message is not None
    assert "boot_state      RUNNING" in outcome.message
    assert "model_runtime   yes" in outcome.message
    assert "/status" in router.command_words


@pytest.mark.asyncio
async def test_operator_command_router_requests_restart() -> None:
    boot = _Boot()
    router = OperatorCommandRouter(boot=boot, api_host="127.0.0.1", api_port=3945)

    outcome = await router.execute("restart config updated")

    assert outcome.message is not None
    assert "Restart requested" in outcome.message
    assert outcome.exit_requested is True
    assert boot.bot.runtime_control.restart_requested is True


@pytest.mark.asyncio
async def test_operator_command_router_updates_framework_and_exits_for_restart() -> None:
    boot = _Boot()
    router = OperatorCommandRouter(boot=boot, api_host="127.0.0.1", api_port=3945)

    outcome = await router.execute("update framework")

    assert outcome.message is not None
    assert "Framework update accepted" in outcome.message
    assert outcome.exit_requested is True
    assert boot.framework_update_service.called is True
    assert boot.bot.runtime_control.restart_requested is True


@pytest.mark.asyncio
async def test_operator_command_router_builds_dashboard() -> None:
    boot = _Boot()
    router = OperatorCommandRouter(boot=boot, api_host="127.0.0.1", api_port=3945)

    outcome = await router.execute("build dashboard")

    assert outcome.message is not None
    assert "Dashboard build complete" in outcome.message
    assert outcome.exit_requested is False
    assert boot.dashboard_build_service.called is True


@pytest.mark.asyncio
async def test_operator_command_router_reports_update_status() -> None:
    router = OperatorCommandRouter(boot=_Boot(), api_host="127.0.0.1", api_port=3945)

    outcome = await router.execute("update status")

    assert outcome.message is not None
    assert "framework" in outcome.message
    assert "dashboard-build" in outcome.message


@pytest.mark.asyncio
async def test_operator_command_router_controls_logging_runtime(restore_logging_runtime) -> None:
    router = OperatorCommandRouter(boot=_Boot(), api_host="127.0.0.1", api_port=3945)

    status = await router.execute("log")
    assert status.message is not None
    assert "third_party_noise" in status.message

    updated = await router.execute("log third-party off")
    assert updated.message == "Third-party noise policy set to off"

    sources = await router.execute("log sources")
    assert sources.message is not None
    assert "SOURCE" in sources.message or sources.message == "(empty)"


def test_operator_cli_session_can_be_constructed(tmp_path: Path) -> None:
    boot = _Boot()
    boot.data_dir = tmp_path
    server = SimpleNamespace(should_exit=False)

    session = OperatorCliSession(
        boot=boot,
        api_host="127.0.0.1",
        api_port=3945,
        server=server,
    )

    assert session is not None
