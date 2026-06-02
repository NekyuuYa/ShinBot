from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from shinbot.core.application.app import ShinBot
from shinbot.core.plugins.types import PluginState
from tests.conftest import MockAdapter, make_message_event


def _repo_path(*parts: str) -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent.joinpath(*parts)
    raise RuntimeError("Could not locate repository root")


@pytest.mark.asyncio
async def test_builtin_commands_plugin_registers_default_commands(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    metadata_path = _repo_path(
        "shinbot",
        "builtin_plugins",
        "shinbot_plugin_builtin_commands",
        "metadata.json",
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    meta = await bot.plugin_manager.load_plugin_async(
        "shinbot_plugin_builtin_commands",
        "shinbot.builtin_plugins.shinbot_plugin_builtin_commands",
        declared_metadata=metadata,
    )

    assert meta.state == PluginState.ACTIVE
    assert sorted(meta.commands) == ["about", "help", "ping"]
    assert bot.command_registry.get("help") is not None
    assert bot.command_registry.get("commands") is not None
    assert bot.command_registry.get("version") is not None


@pytest.mark.asyncio
async def test_builtin_commands_plugin_ping_command_replies(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    adapter = bot.add_adapter("inst1", "mock")

    metadata_path = _repo_path(
        "shinbot",
        "builtin_plugins",
        "shinbot_plugin_builtin_commands",
        "metadata.json",
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    await bot.plugin_manager.load_plugin_async(
        "shinbot_plugin_builtin_commands",
        "shinbot.builtin_plugins.shinbot_plugin_builtin_commands",
        declared_metadata=metadata,
    )

    event = make_message_event(content="/ping", instance_id="inst1")
    await bot.on_event(event, adapter)
    await asyncio.sleep(0)

    assert len(adapter.sent) == 1
    assert adapter.sent[0][1][0].text_content == "pong"
