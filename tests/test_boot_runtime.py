from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.core.application.boot import BootController


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
