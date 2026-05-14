from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from shinbot.core.application.boot import BootController
from shinbot.core.application.boot_preflight import BootPreflightError
from shinbot.core.application.bots_config import (
    load_bot_service_configs,
    validate_bot_service_configs,
)


def _base_config() -> dict:
    return {
        "adapter_instances": [
            {
                "id": "qq-main",
                "adapter": "onebot_v11",
                "enabled": True,
                "config": {},
            }
        ],
        "plugins": [
            {
                "id": "debug-message",
                "module": "shinbot.builtin_plugins.shinbot_debug_message",
                "enabled": False,
            }
        ],
        "bots": [
            {
                "id": "command-bot",
                "display_name": "Command Bot",
                "enabled": True,
                "commands": {"enabled": True, "prefixes": ["/", "!"]},
                "plugins": {
                    "enabled": True,
                    "enabled_plugins": ["debug-message"],
                    "disabled_plugins": [],
                },
                "agent": {"mode": "none"},
                "bindings": [
                    {
                        "id": "command-bot-admin-group",
                        "adapter_instance_id": "qq-main",
                        "session_patterns": ["group:10001", "private:*"],
                        "enabled": True,
                        "priority": 100,
                    }
                ],
            }
        ],
    }


def test_load_bot_service_configs_accepts_session_patterns_array(tmp_path: Path) -> None:
    config = _base_config()

    bots = load_bot_service_configs(config, data_dir=tmp_path / "data")

    assert len(bots) == 1
    assert bots[0].id == "command-bot"
    assert bots[0].commands.prefixes == ("/", "!")
    assert bots[0].plugins.enabled_plugins == ("debug-message",)
    assert bots[0].bindings[0].session_patterns == ("group:10001", "private:*")
    assert bots[0].bindings[0].priority == 100


def test_config_example_matches_bot_service_config_shape(tmp_path: Path) -> None:
    config = tomllib.loads(Path("config.example.toml").read_text(encoding="utf-8"))

    issues = validate_bot_service_configs(config, data_dir=tmp_path / "data")

    assert issues == []


def test_validate_rejects_singular_session_pattern_key() -> None:
    config = _base_config()
    binding = config["bots"][0]["bindings"][0]
    binding.pop("session_patterns")
    binding["session_pattern"] = "group:10001"

    issues = validate_bot_service_configs(config)

    assert ("bots[0].bindings[0].session_pattern", "deprecated") in {
        (issue.path, issue.code) for issue in issues
    }
    assert ("bots[0].bindings[0].session_patterns", "required") in {
        (issue.path, issue.code) for issue in issues
    }


def test_validate_requires_session_patterns_to_be_array() -> None:
    config = _base_config()
    config["bots"][0]["bindings"][0]["session_patterns"] = "group:10001"

    issues = validate_bot_service_configs(config)

    assert [issue.path for issue in issues] == ["bots[0].bindings[0].session_patterns"]
    assert issues[0].code == "type"


def test_validate_rejects_unknown_adapter_and_plugin_refs() -> None:
    config = _base_config()
    config["bots"][0]["plugins"]["enabled_plugins"] = ["missing-plugin"]
    config["bots"][0]["bindings"][0]["adapter_instance_id"] = "missing-adapter"

    issues = validate_bot_service_configs(config)

    assert ("bots[0].plugins.enabled_plugins[0]", "unknown_ref") in {
        (issue.path, issue.code) for issue in issues
    }
    assert ("bots[0].bindings[0].adapter_instance_id", "unknown_ref") in {
        (issue.path, issue.code) for issue in issues
    }


def test_validate_rejects_duplicate_bot_and_binding_ids() -> None:
    config = _base_config()
    duplicate_bot = dict(config["bots"][0])
    duplicate_bot["bindings"] = [dict(config["bots"][0]["bindings"][0])]
    config["bots"].append(duplicate_bot)

    issues = validate_bot_service_configs(config)

    assert ("bots[1].id", "duplicate") in {(issue.path, issue.code) for issue in issues}
    assert ("bots[1].bindings[0].id", "duplicate") in {
        (issue.path, issue.code) for issue in issues
    }


def test_validate_full_agent_config_must_stay_inside_data_dir(tmp_path: Path) -> None:
    config = _base_config()
    config["bots"][0]["agent"] = {"mode": "full", "config": "../agent.toml"}

    issues = validate_bot_service_configs(config, data_dir=tmp_path / "data")

    assert [issue.path for issue in issues] == ["bots[0].agent.config"]
    assert issues[0].code == "path"


def test_boot_phase1_rejects_invalid_bot_service_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[[adapter_instances]]",
                'id = "qq-main"',
                'adapter = "onebot_v11"',
                "enabled = true",
                "",
                "[[bots]]",
                'id = "command-bot"',
                "enabled = true",
                "",
                "[[bots.bindings]]",
                'id = "bad-binding"',
                'adapter_instance_id = "qq-main"',
                'session_patterns = "group:10001"',
            ]
        ),
        encoding="utf-8",
    )
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    with pytest.raises(BootPreflightError):
        boot._phase1_environment()
