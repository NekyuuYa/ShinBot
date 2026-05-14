from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from shinbot.core.application.runtime_control import RuntimeControl
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
    assert boot.bot.runtime_control.restart_requested is True


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
