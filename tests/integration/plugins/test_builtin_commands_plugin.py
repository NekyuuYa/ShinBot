from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from shinbot.agent.runtime import install_agent_runtime
from shinbot.core.application.app import ShinBot
from shinbot.core.application.bots_config import (
    BotBindingConfig,
    BotCommandsConfig,
    BotPluginsConfig,
    BotServiceConfig,
)
from shinbot.core.message_routes.command import CommandDef
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
    assert sorted(meta.commands) == ["about", "help", "mute", "ping", "unmute", "whoami"]
    assert bot.command_registry.get("help") is not None
    assert bot.command_registry.get("commands") is not None
    assert bot.command_registry.get("mute") is not None
    assert bot.command_registry.get("unmute") is not None
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
    assert "平台权限标识：inst1:user-1" in reply
    assert "全局权限绑定 key：inst1:user-1" in reply
    assert "当前会话权限绑定 key：inst1:group:ch-1.inst1:user-1" in reply


@pytest.mark.asyncio
async def test_builtin_commands_plugin_help_only_lists_allowed_commands(tmp_path: Path) -> None:
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

    await bot.on_event(make_message_event(content="/help", instance_id="inst1"), adapter)
    await asyncio.sleep(0)

    assert len(adapter.sent) == 1
    reply = adapter.sent[0][1][0].text_content
    assert "/help / /commands - 列出当前可用的基础指令" in reply
    assert "/ping - 检查消息命令链路是否可用" in reply
    assert "/mute - 暂停当前 session 的 agent 活动一段时间" not in reply
    assert "/unmute / /resume - 恢复当前 session 的 agent 活动" not in reply


@pytest.mark.asyncio
async def test_builtin_commands_plugin_help_uses_matched_command_prefix(tmp_path: Path) -> None:
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
    bot.configure_bot_service_configs(
        (
            BotServiceConfig(
                id="inst1",
                display_name="Test Bot",
                commands=BotCommandsConfig(prefixes=("#",)),
                plugins=BotPluginsConfig(
                    enabled=True,
                    enabled_plugins=("shinbot_plugin_builtin_commands",),
                ),
                bindings=(
                    BotBindingConfig(
                        id="default",
                        adapter_instance_id="inst1",
                        session_patterns=("group:*",),
                    ),
                ),
            ),
        )
    )

    await bot.on_event(make_message_event(content="#help", instance_id="inst1"), adapter)
    await asyncio.sleep(0)

    assert len(adapter.sent) == 1
    reply = adapter.sent[0][1][0].text_content
    assert "#help / #commands - 列出当前可用的基础指令" in reply
    assert "/help / /commands - 列出当前可用的基础指令" not in reply


@pytest.mark.asyncio
async def test_builtin_commands_plugin_help_respects_current_command_permissions(
    tmp_path: Path,
) -> None:
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
    bot.command_registry.set_permission_override("ping", "cmd.support.ping")

    await bot.on_event(make_message_event(content="/help", instance_id="inst1"), adapter)
    await asyncio.sleep(0)

    reply = adapter.sent[0][1][0].text_content
    assert "/help / /commands - 列出当前可用的基础指令" in reply
    assert "/ping - 检查消息命令链路是否可用" not in reply

    bot.permission_engine.add_group(
        bot.permission_engine.get_group("default").model_copy(
            update={
                "id": "support",
                "permissions": {"cmd.help", "cmd.support.ping"},
            }
        )
    )
    bot.permission_engine.bind("inst1:user-1", "support")

    await bot.on_event(make_message_event(content="/help", instance_id="inst1"), adapter)
    await asyncio.sleep(0)

    reply = adapter.sent[1][1][0].text_content
    assert "/ping - 检查消息命令链路是否可用" in reply


@pytest.mark.asyncio
async def test_builtin_commands_plugin_help_hides_context_disabled_plugin_commands(
    tmp_path: Path,
) -> None:
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

    async def blocked_command(_ctx, _args):
        return None

    bot.command_registry.register(
        CommandDef(
            name="blocked",
            handler=blocked_command,
            description="策略禁用插件指令",
            permission="cmd.help",
            owner="blocked-plugin",
        )
    )
    bot.configure_bot_service_configs(
        (
            BotServiceConfig(
                id="inst1",
                display_name="Test Bot",
                plugins=BotPluginsConfig(
                    enabled=True,
                    enabled_plugins=("shinbot_plugin_builtin_commands",),
                ),
                bindings=(
                    BotBindingConfig(
                        id="default",
                        adapter_instance_id="inst1",
                        session_patterns=("group:*",),
                    ),
                ),
            ),
        )
    )

    await bot.on_event(make_message_event(content="/help", instance_id="inst1"), adapter)
    await asyncio.sleep(0)

    assert len(adapter.sent) == 1
    reply = adapter.sent[0][1][0].text_content
    assert "/help / /commands - 列出当前可用的基础指令" in reply
    assert "/blocked - 策略禁用插件指令" not in reply


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

    bot.permission_engine.bind("inst1:user-1", "owner")

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


@pytest.mark.asyncio
async def test_builtin_commands_plugin_mute_requires_admin_by_default(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    adapter = bot.add_adapter("inst1", "mock")
    install_agent_runtime(bot)

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

    assert len(adapter.sent) == 1
    assert adapter.sent[0][1][0].text_content == "权限不足：需要 cmd.mute"


@pytest.mark.asyncio
async def test_builtin_commands_plugin_unmute_uses_cmd_mute_permission(
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

    session = bot.session_manager.get_or_create("inst1", make_message_event(instance_id="inst1"))
    session.permission_group = "restricted"
    bot.permission_engine.add_group(
        bot.permission_engine.get_group("default").model_copy(
            update={"id": "restricted", "permissions": {"cmd.help", "cmd.ping", "cmd.about"}}
        )
    )

    await runtime.pause_session_until(session.id, pause_until=9999999999.0)

    await bot.on_event(make_message_event(content="/unmute", instance_id="inst1"), adapter)
    await asyncio.sleep(0)
    assert adapter.sent[0][1][0].text_content == "权限不足：需要 cmd.mute"
    assert runtime.session_pause_until(session.id) == 9999999999.0
