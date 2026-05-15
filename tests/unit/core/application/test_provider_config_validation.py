from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot import BootController
from shinbot.core.application.provider_config_validation import (
    ProviderConfigValidationError,
    validate_adapter_instance_configs,
    validate_plugin_configs,
)
from shinbot.core.config_provider import ConfigProviderRegistry, load_provider_schema
from tests.conftest import MockAdapter


def _schema_path(relative: str) -> Path:
    return _repo_root() / relative


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not locate repository root")


def test_validate_adapter_instance_configs_reports_provider_issues() -> None:
    registry = ConfigProviderRegistry()
    registry.register(
        load_provider_schema(
            _schema_path("shinbot/builtin_plugins/shinbot_adapter_onebot_v11/config.schema.toml")
        )
    )

    issues = validate_adapter_instance_configs(
        {
            "adapter_instances": [
                {
                    "id": "main",
                    "adapter": "onebot_v11",
                    "config": {"mode": "bad", "reverse_port": 70000},
                }
            ]
        },
        registry,
    )

    assert [(issue.path, issue.code) for issue in issues] == [
        ("adapter_instances[0].config.mode", "choices"),
        ("adapter_instances[0].config.reverse_port", "max"),
    ]


def test_validate_plugin_configs_reports_provider_issues() -> None:
    registry = ConfigProviderRegistry()
    registry.register(
        load_provider_schema(
            _schema_path("shinbot/builtin_plugins/shinbot_plugin_search/config.schema.toml")
        )
    )

    issues = validate_plugin_configs(
        {
            "plugins": [
                {
                    "id": "shinbot_plugin_search",
                    "config": {
                        "timeout_seconds": 0.5,
                        "default_max_results": 20,
                        "default_search_depth": "deep",
                    },
                }
            ]
        },
        registry,
    )

    assert [(issue.path, issue.code) for issue in issues] == [
        ("plugins[0].config.timeout_seconds", "min"),
        ("plugins[0].config.default_max_results", "max"),
        ("plugins[0].config.default_search_depth", "choices"),
    ]


def test_validate_plugin_configs_skips_plugins_without_provider_schema() -> None:
    issues = validate_plugin_configs(
        {"plugins": [{"id": "no_schema_plugin", "config": {"anything": ["goes"]}}]},
        ConfigProviderRegistry(),
    )

    assert issues == []


def test_sleepy_provider_schema_loads_defaults() -> None:
    registry = ConfigProviderRegistry()
    registry.register(
        load_provider_schema(
            _schema_path("shinbot/builtin_plugins/shinbot_plugin_sleepy/config.schema.toml"),
            example_path=_schema_path(
                "shinbot/builtin_plugins/shinbot_plugin_sleepy/config.example.toml"
            ),
        )
    )

    defaults = registry.default_config("plugin", "shinbot_plugin_sleepy")

    assert defaults["enabled"] is True
    assert defaults["schedules"][0]["name"] == "Sleep"
    assert registry.get("plugin", "shinbot_plugin_sleepy").example_toml.startswith("enabled")


def test_setup_instances_rejects_invalid_adapter_config(tmp_path: Path) -> None:
    boot = BootController(config_path=tmp_path / "config.toml", data_dir=tmp_path / "data")
    boot.config = {
        "adapter_instances": [
            {
                "id": "main",
                "adapter": "onebot_v11",
                "enabled": True,
                "config": {"mode": "bad"},
            }
        ]
    }
    bot = ShinBot(data_dir=tmp_path / "data")
    bot.adapter_manager.register_adapter("onebot_v11", MockAdapter)
    bot.config_provider_registry.register(
        load_provider_schema(
            _schema_path("shinbot/builtin_plugins/shinbot_adapter_onebot_v11/config.schema.toml")
        )
    )
    boot.bot = bot

    with pytest.raises(ProviderConfigValidationError) as exc_info:
        boot._setup_instances()

    assert exc_info.value.issues[0].path == "adapter_instances[0].config.mode"
    assert bot.adapter_manager.get_instance("main") is None


@pytest.mark.asyncio
async def test_boot_rejects_invalid_plugin_config_after_schema_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builtin_root = tmp_path / "empty_builtin_plugins"
    builtin_root.mkdir()
    monkeypatch.setattr("shinbot.core.plugins.manager._BUILTIN_PLUGINS_DIR", builtin_root)

    data_dir = tmp_path / "data"
    plugins_dir = data_dir / "plugins"
    plugin_id = "demo_schema_plugin"
    plugin_dir = plugins_dir / plugin_id
    plugin_dir.mkdir(parents=True)
    sys.modules.pop("plugins.demo_schema_plugin", None)
    sys.modules.pop("plugins", None)
    (plugins_dir / "__init__.py").write_text("", encoding="utf-8")
    (plugin_dir / "__init__.py").write_text("def setup(plg):\n    pass\n", encoding="utf-8")
    (plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": plugin_id,
                "name": "Demo Schema Plugin",
                "version": "1.0.0",
                "entry": "__init__.py",
                "role": "logic",
            }
        ),
        encoding="utf-8",
    )
    (plugin_dir / "config.schema.toml").write_text(
        """
[provider]
kind = "plugin"
id = "demo_schema_plugin"

[[fields]]
path = "max_items"
type = "integer"
default = 1
max = 3
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[admin]
username = "admin"
password = "admin"
jwt_expire_hours = 24

[runtime]
model = false
agent = false

[[plugins]]
id = "demo_schema_plugin"
module = "plugins.demo_schema_plugin"
enabled = true

[plugins.config]
max_items = 5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    boot = BootController(config_path=config_path, data_dir=data_dir)
    try:
        with pytest.raises(ProviderConfigValidationError) as exc_info:
            await boot.boot()
    finally:
        await boot.shutdown()
        sys.modules.pop("plugins.demo_schema_plugin", None)
        sys.modules.pop("plugins", None)

    assert exc_info.value.issues[0].path == "plugins[0].config.max_items"
    assert exc_info.value.issues[0].code == "max"
