from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from shinbot.agent.runtime import install_agent_runtime
from shinbot.core.application.app import ShinBot
from shinbot.core.plugins.types import PluginState
from shinbot.core.state.session import get_agent_pause_until
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
    assert sorted(meta.commands) == ["about", "help", "mute", "ping", "whoami"]
    assert bot.command_registry.get("help") is not None
    assert bot.command_registry.get("commands") is not None
    assert bot.command_registry.get("mute") is not None
    assert bot.command_registry.get("whoami") is not None
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


@pytest.mark.asyncio
async def test_builtin_commands_plugin_whoami_reports_binding_keys(tmp_path: Path) -> None:
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

    event = make_message_event(content="/whoami", instance_id="inst1")
    await bot.on_event(event, adapter)
    await asyncio.sleep(0)

    assert len(adapter.sent) == 1
    reply = adapter.sent[0][1][0].text_content
    assert "你的唯一用户标识（user_id）：user-1" in reply
    assert "全局权限绑定 key：inst1:user-1" in reply
    assert "当前会话权限绑定 key：inst1:group:ch-1.user-1" in reply


@pytest.mark.asyncio
async def test_builtin_commands_plugin_mute_pauses_agent_but_keeps_commands_available(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    adapter = bot.add_adapter("inst1", "mock")
    runtime = install_agent_runtime(bot)

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

    await bot.on_event(make_message_event(content="/mute 15m", instance_id="inst1"), adapter)
    await asyncio.sleep(0)

    session = bot.session_manager.get("inst1:group:ch-1")
    assert session is not None
    pause_until = get_agent_pause_until(session)
    assert pause_until is not None
    assert runtime.session_pause_until(session.id) == pause_until
    assert "已暂停当前 session 的 agent 活动 15m" in adapter.sent[0][1][0].text_content

    await bot.on_event(make_message_event(content="/ping", instance_id="inst1"), adapter)
    await asyncio.sleep(0)
    assert adapter.sent[1][1][0].text_content == "pong"

    await bot.on_event(make_message_event(content="/mute off", instance_id="inst1"), adapter)
    await asyncio.sleep(0)
    assert runtime.session_pause_until(session.id) is None
    assert adapter.sent[2][1][0].text_content == "已恢复当前 session 的 agent 活动"
