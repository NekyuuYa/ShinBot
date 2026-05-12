"""Validation for provider-owned config blocks in the main runtime config."""

from __future__ import annotations

from typing import Any

from shinbot.core.application.config_sections import normalize_adapter_instance_record
from shinbot.core.config_provider import (
    ConfigProviderKind,
    ConfigProviderRegistry,
    ConfigValidationIssue,
)


class ProviderConfigValidationError(ValueError):
    """Raised when adapter/plugin config blocks fail provider schema validation."""

    def __init__(self, issues: list[ConfigValidationIssue]) -> None:
        self.issues = list(issues)
        super().__init__(_format_issues(self.issues))


def validate_provider_configs(
    config: dict[str, Any],
    registry: ConfigProviderRegistry,
) -> list[ConfigValidationIssue]:
    """Validate all provider-owned config blocks with registered schemas."""

    return [
        *validate_plugin_configs(config, registry),
        *validate_adapter_instance_configs(config, registry),
    ]


def validate_plugin_configs(
    config: dict[str, Any],
    registry: ConfigProviderRegistry,
) -> list[ConfigValidationIssue]:
    """Validate ``[[plugins]].config`` blocks for plugins that expose a schema."""

    issues: list[ConfigValidationIssue] = []
    for index, item in _section_items(config, "plugins", issues):
        plugin_id = _string_field(item, "id")
        config_block = _config_block(item, f"plugins[{index}].config", issues)
        if config_block is None or not plugin_id:
            continue

        if registry.get(ConfigProviderKind.PLUGIN, plugin_id) is None:
            continue

        issues.extend(
            registry.validate(
                ConfigProviderKind.PLUGIN,
                plugin_id,
                config_block,
                path_prefix=f"plugins[{index}].config",
            )
        )
    return issues


def validate_adapter_instance_configs(
    config: dict[str, Any],
    registry: ConfigProviderRegistry,
) -> list[ConfigValidationIssue]:
    """Validate ``[[adapter_instances]].config`` blocks with adapter schemas."""

    issues: list[ConfigValidationIssue] = []
    for index, item in _section_items(config, "adapter_instances", issues):
        normalized = normalize_adapter_instance_record(item)
        adapter_id = str(normalized["adapter"]).strip()
        config_block = _config_block(item, f"adapter_instances[{index}].config", issues)
        if config_block is None or not adapter_id:
            continue

        if registry.get(ConfigProviderKind.ADAPTER, adapter_id) is None:
            continue

        issues.extend(
            registry.validate(
                ConfigProviderKind.ADAPTER,
                adapter_id,
                config_block,
                path_prefix=f"adapter_instances[{index}].config",
            )
        )
    return issues


def _section_items(
    config: dict[str, Any],
    section: str,
    issues: list[ConfigValidationIssue],
) -> list[tuple[int, dict[str, Any]]]:
    raw_section = config.get(section, [])
    if raw_section is None:
        return []
    if not isinstance(raw_section, list):
        issues.append(
            ConfigValidationIssue(
                path=section,
                message="must be a list of tables",
                code="type",
            )
        )
        return []

    result: list[tuple[int, dict[str, Any]]] = []
    for index, item in enumerate(raw_section):
        if not isinstance(item, dict):
            issues.append(
                ConfigValidationIssue(
                    path=f"{section}[{index}]",
                    message="must be a table",
                    code="type",
                )
            )
            continue
        result.append((index, item))
    return result


def _config_block(
    item: dict[str, Any],
    path: str,
    issues: list[ConfigValidationIssue],
) -> dict[str, Any] | None:
    if "config" not in item or item["config"] is None:
        return {}
    raw_config = item["config"]
    if isinstance(raw_config, dict):
        return dict(raw_config)

    issues.append(
        ConfigValidationIssue(
            path=path,
            message="must be a table",
            code="type",
        )
    )
    return None


def _string_field(item: dict[str, Any], key: str) -> str:
    value = item.get(key)
    return value.strip() if isinstance(value, str) else ""


def _format_issues(issues: list[ConfigValidationIssue]) -> str:
    if not issues:
        return "provider config is invalid"
    lines = ["provider config is invalid:"]
    lines.extend(f"- {issue.path}: {issue.message} ({issue.code})" for issue in issues)
    return "\n".join(lines)
