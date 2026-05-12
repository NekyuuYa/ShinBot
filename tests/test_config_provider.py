from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.core.config_provider import (
    ConfigProviderKind,
    ConfigProviderLoadError,
    ConfigProviderRegistry,
    load_provider_schema,
    load_provider_schema_from_module,
)


def write_provider_files(tmp_path: Path) -> Path:
    schema_path = tmp_path / "config.schema.toml"
    schema_path.write_text(
        """
[provider]
kind = "adapter"
id = "onebot_v11"
display_name = "OneBot v11"
description = "OneBot v11 adapter."
config_version = "1.0.0"
owner = "builtin"

[[fields]]
path = "mode"
type = "enum"
required = true
default = "reverse"
choices = ["forward", "reverse"]
description = "Connection mode."

[[fields]]
path = "reverse.port"
type = "integer"
required = true
default = 8082
min = 1
max = 65535
visible_when = "mode == 'reverse'"

[[fields]]
path = "access_token"
type = "string"
default = ""
secret = true

[[fields]]
path = "advanced.reconnect_delay"
type = "float"
default = 5.0
min = 0.0
advanced = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "config.example.toml").write_text(
        'mode = "reverse"\naccess_token = ""\n',
        encoding="utf-8",
    )
    return schema_path


def test_load_provider_schema_from_toml(tmp_path: Path) -> None:
    schema_path = write_provider_files(tmp_path)

    provider = load_provider_schema(
        schema_path,
        example_path=tmp_path / "config.example.toml",
        owner_module="demo.adapter",
    )

    assert provider.kind == ConfigProviderKind.ADAPTER
    assert provider.id == "onebot_v11"
    assert provider.display_name == "OneBot v11"
    assert provider.owner_module == "demo.adapter"
    assert provider.metadata["owner"] == "builtin"
    assert provider.example_toml == 'mode = "reverse"\naccess_token = ""\n'
    assert provider.fields[0].path == "mode"
    assert provider.fields[0].choices == ("forward", "reverse")
    assert provider.fields[2].secret is True
    assert provider.fields[3].advanced is True


def test_provider_registry_defaults_and_validation(tmp_path: Path) -> None:
    provider = load_provider_schema(write_provider_files(tmp_path))
    registry = ConfigProviderRegistry()
    registry.register(provider)

    assert registry.default_config("adapter", "onebot_v11") == {
        "mode": "reverse",
        "reverse": {"port": 8082},
        "access_token": "",
        "advanced": {"reconnect_delay": 5.0},
    }

    assert registry.validate(
        "adapter",
        "onebot_v11",
        {
            "mode": "forward",
            "reverse": {"port": 3001},
            "access_token": "secret",
            "advanced": {"reconnect_delay": 0.5},
        },
        path_prefix="adapter_instances[0].config",
    ) == []

    issues = registry.validate(
        "adapter",
        "onebot_v11",
        {
            "mode": "bad",
            "reverse": {"port": 70000},
            "access_token": 123,
            "advanced": {"reconnect_delay": -1},
        },
        path_prefix="adapter_instances[0].config",
    )

    assert [issue.code for issue in issues] == ["choices", "max", "type", "min"]
    assert issues[0].path == "adapter_instances[0].config.mode"
    assert issues[1].path == "adapter_instances[0].config.reverse.port"


def test_provider_registry_reports_required_missing_without_default(tmp_path: Path) -> None:
    schema_path = tmp_path / "config.schema.toml"
    schema_path.write_text(
        """
[provider]
kind = "plugin"
id = "demo"

[[fields]]
path = "api_key"
type = "string"
required = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    registry = ConfigProviderRegistry()
    registry.register(load_provider_schema(schema_path))

    issues = registry.validate("plugin", "demo", {})

    assert len(issues) == 1
    assert issues[0].path == "api_key"
    assert issues[0].code == "required"


def test_provider_registry_rejects_duplicate_registration(tmp_path: Path) -> None:
    provider = load_provider_schema(write_provider_files(tmp_path))
    registry = ConfigProviderRegistry()
    registry.register(provider)

    with pytest.raises(ValueError, match="registered"):
        registry.register(provider)


def test_load_provider_schema_rejects_duplicate_field_paths(tmp_path: Path) -> None:
    schema_path = tmp_path / "config.schema.toml"
    schema_path.write_text(
        """
[provider]
kind = "adapter"
id = "bad"

[[fields]]
path = "mode"
type = "string"

[[fields]]
path = "mode"
type = "string"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigProviderLoadError, match="duplicate"):
        load_provider_schema(schema_path)


def test_load_provider_schema_from_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package_dir = tmp_path / "demo_provider"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    write_provider_files(package_dir)
    monkeypatch.syspath_prepend(str(tmp_path))

    provider = load_provider_schema_from_module("demo_provider")

    assert provider.id == "onebot_v11"
    assert provider.owner_module == "demo_provider"
    assert provider.example_toml.startswith("mode")


def test_registry_catalog_is_serializable(tmp_path: Path) -> None:
    registry = ConfigProviderRegistry()
    registry.register(load_provider_schema(write_provider_files(tmp_path)))

    catalog = registry.catalog()

    assert catalog[0]["kind"] == "adapter"
    assert catalog[0]["id"] == "onebot_v11"
    assert catalog[0]["fields"][0]["type"] == "enum"
    assert catalog[0]["fields"][2]["secret"] is True
