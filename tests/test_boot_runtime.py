from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.agent.runtime.config import AgentRuntimeConfigError
from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot import BootController
from tests.conftest import MockAdapter


def _write_config(path: Path, *, runtime: str = "") -> None:
    path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                runtime,
            ]
        ),
        encoding="utf-8",
    )


def test_setup_instances_reads_normalized_adapter_instances(tmp_path: Path):
    boot = BootController(config_path=tmp_path / "config.toml", data_dir=tmp_path / "data")
    boot.config = {
        "adapter_instances": [
            {
                "id": "mock-main",
                "adapter": "mock",
                "enabled": True,
                "config": {},
            },
            {
                "id": "mock-disabled",
                "adapter": "mock",
                "enabled": False,
                "config": {},
            },
        ]
    }
    bot = ShinBot(data_dir=tmp_path / "data")
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    boot.bot = bot

    boot._setup_instances()

    assert bot.adapter_manager.get_instance("mock-main") is not None
    assert bot.adapter_manager.get_instance("mock-disabled") is None


@pytest.mark.asyncio
async def test_boot_mounts_model_and_agent_by_default(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path)
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is not None
        assert bot.agent_runtime is not None
        assert bot.agent_runtime.model_runtime is bot.model_runtime
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_loads_agent_config_for_full_bot(tmp_path: Path):
    data_dir = tmp_path / "data"
    agent_config = data_dir / "agents" / "full-agent.toml"
    agent_config.parent.mkdir(parents=True)
    agent_config.write_text(
        "\n".join(
            [
                "[agent]",
                'id = "full-agent-profile"',
                "",
                "[agent.review]",
                "scan_batch_size = 9",
                "",
                "[agent.active_chat]",
                "initial_interest = 42",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[[bots]]",
                'id = "full-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/full-agent.toml"',
            ]
        ),
        encoding="utf-8",
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)

    try:
        bot = await boot.boot()
        assert bot.agent_runtime is not None
        profile = bot.agent_runtime.agent_profile_for_bot("full-agent")
        assert profile.profile_id == "full-agent-profile"
        assert profile.config.review_workflow_config.review_scan_batch_size == 9
        assert profile.config.active_chat_policy_config.initial_interest_value == 42
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_fails_when_full_bot_agent_config_is_missing(tmp_path: Path):
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[[bots]]",
                'id = "full-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/missing.toml"',
            ]
        ),
        encoding="utf-8",
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)

    with pytest.raises(AgentRuntimeConfigError, match="full-agent"):
        await boot.boot()


@pytest.mark.asyncio
async def test_boot_can_mount_model_without_agent(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path, runtime="[runtime]\nagent = false")
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is not None
        assert bot.agent_runtime is None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_can_disable_model_when_agent_is_disabled(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path, runtime="[runtime]\nmodel = false\nagent = false")
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is None
        assert bot.agent_runtime is None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_shutdown_closes_agent_runtime(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path)
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")
    bot = await boot.boot()
    assert bot.agent_runtime is not None

    closed = False
    original_shutdown = bot.agent_runtime.shutdown

    async def shutdown_probe() -> None:
        nonlocal closed
        closed = True
        await original_shutdown()

    bot.agent_runtime.shutdown = shutdown_probe
    await boot.shutdown()

    assert closed is True
