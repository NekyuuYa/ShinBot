"""Static boot-time configuration preflight checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shinbot.core.application.bots_config import (
    BotServiceConfig,
    parse_bot_service_configs,
)
from shinbot.core.config_provider import ConfigValidationIssue
from shinbot.persistence.config import resolve_sqlite_path


@dataclass(slots=True, frozen=True)
class BootPreflightResult:
    """Parsed config artifacts produced by boot preflight."""

    bot_service_configs: tuple[BotServiceConfig, ...]
    issues: tuple[ConfigValidationIssue, ...] = ()


class BootPreflightError(ValueError):
    """Raised when static boot config validation fails."""

    def __init__(self, issues: list[ConfigValidationIssue]) -> None:
        self.issues = list(issues)
        super().__init__(_format_issues(self.issues))


def run_boot_preflight(
    config: dict[str, Any],
    *,
    data_dir: Path | str,
    raise_on_error: bool = True,
) -> BootPreflightResult:
    """Validate boot config before runtime objects are created."""

    root_data_dir = Path(data_dir)
    bots, bot_issues = parse_bot_service_configs(config, data_dir=root_data_dir)
    issues: list[ConfigValidationIssue] = [
        *bot_issues,
        *_validate_top_level_tables(config),
        *_validate_runtime_section(config),
        *_validate_database_section(config),
        *_validate_adapter_instances(config),
        *_validate_plugins(config),
        *_validate_agent_config_files(
            bots,
            data_dir=root_data_dir,
            agent_runtime_enabled=_runtime_feature_enabled(config, "agent", default=True)
            and _runtime_feature_enabled(config, "model", default=True),
        ),
    ]

    if issues and raise_on_error:
        raise BootPreflightError(issues)
    return BootPreflightResult(bot_service_configs=bots, issues=tuple(issues))


def _validate_top_level_tables(config: dict[str, Any]) -> list[ConfigValidationIssue]:
    issues: list[ConfigValidationIssue] = []
    for section in ("admin", "database", "logging", "permissions", "runtime"):
        value = config.get(section)
        if value is not None and not isinstance(value, dict):
            issues.append(
                ConfigValidationIssue(
                    path=section,
                    message="must be a table",
                    code="type",
                )
            )
    return issues


def _validate_runtime_section(config: dict[str, Any]) -> list[ConfigValidationIssue]:
    runtime = config.get("runtime")
    if runtime is None:
        return []
    if not isinstance(runtime, dict):
        return []

    issues: list[ConfigValidationIssue] = []
    for key in ("model", "agent"):
        if key in runtime and not isinstance(runtime[key], bool):
            issues.append(
                ConfigValidationIssue(
                    path=f"runtime.{key}",
                    message="must be a boolean",
                    code="type",
                )
            )
    backend_config = runtime.get("model_backend")
    if backend_config is not None and not isinstance(backend_config, dict):
        issues.append(
            ConfigValidationIssue(
                path="runtime.model_backend",
                message="must be a table",
                code="type",
            )
        )
    elif isinstance(backend_config, dict):
        backend_type = backend_config.get("type")
        if backend_type is not None and not isinstance(backend_type, str):
            issues.append(
                ConfigValidationIssue(
                    path="runtime.model_backend.type",
                    message="must be a string",
                    code="type",
                )
            )
        elif isinstance(backend_type, str):
            from shinbot.core.runtime.model_backend import SUPPORTED_MODEL_BACKENDS

            if backend_type.strip() not in SUPPORTED_MODEL_BACKENDS:
                issues.append(
                    ConfigValidationIssue(
                        path="runtime.model_backend.type",
                        message=(
                            "must be one of "
                            f"{', '.join(sorted(SUPPORTED_MODEL_BACKENDS))}"
                        ),
                        code="choices",
                    )
                )
    return issues


def _validate_database_section(config: dict[str, Any]) -> list[ConfigValidationIssue]:
    database = config.get("database")
    if database is None or not isinstance(database, dict):
        return []

    issues: list[ConfigValidationIssue] = []
    if "url" in database and database["url"] is not None:
        url = database["url"]
        if not isinstance(url, str):
            issues.append(
                ConfigValidationIssue(
                    path="database.url",
                    message="must be a string",
                    code="type",
                )
            )
        elif url.strip():
            try:
                resolve_sqlite_path(url.strip())
            except ValueError as exc:
                issues.append(
                    ConfigValidationIssue(
                        path="database.url",
                        message=str(exc),
                        code="database_url",
                    )
                )

    if "snapshot_ttl" in database and database["snapshot_ttl"] is not None:
        snapshot_ttl = database["snapshot_ttl"]
        if not isinstance(snapshot_ttl, int) or isinstance(snapshot_ttl, bool):
            issues.append(
                ConfigValidationIssue(
                    path="database.snapshot_ttl",
                    message="must be an integer",
                    code="type",
                )
            )
        elif snapshot_ttl < 0:
            issues.append(
                ConfigValidationIssue(
                    path="database.snapshot_ttl",
                    message="must be greater than or equal to 0",
                    code="min",
                )
            )
    return issues


def _validate_adapter_instances(config: dict[str, Any]) -> list[ConfigValidationIssue]:
    issues: list[ConfigValidationIssue] = []
    for index, item in _section_tables(config, "adapter_instances"):
        path = f"adapter_instances[{index}]"
        _validate_optional_bool(item, "enabled", path, issues)
        _validate_required_string(item, "adapter", path, issues)
        _validate_optional_table(item, "config", path, issues)
    return issues


def _validate_plugins(config: dict[str, Any]) -> list[ConfigValidationIssue]:
    issues: list[ConfigValidationIssue] = []
    for index, item in _section_tables(config, "plugins"):
        path = f"plugins[{index}]"
        _validate_optional_bool(item, "enabled", path, issues)
        _validate_optional_string(item, "module", path, issues)
        _validate_optional_table(item, "config", path, issues)
    return issues


def _validate_agent_config_files(
    bots: tuple[BotServiceConfig, ...],
    *,
    data_dir: Path,
    agent_runtime_enabled: bool,
) -> list[ConfigValidationIssue]:
    if not agent_runtime_enabled:
        return []

    issues: list[ConfigValidationIssue] = []
    for bot_index, bot in enumerate(bots):
        if not bot.enabled or bot.agent.mode == "none" or not bot.agent.config:
            continue
        config_path = data_dir / bot.agent.config
        if not config_path.is_file():
            issues.append(
                ConfigValidationIssue(
                    path=f"bots[{bot_index}].agent.config",
                    message=f"agent config file not found: {bot.agent.config}",
                    code="not_found",
                )
            )
    return issues


def _section_tables(
    config: dict[str, Any],
    section: str,
) -> list[tuple[int, dict[str, Any]]]:
    raw_section = config.get(section, [])
    if not isinstance(raw_section, list):
        return []
    return [
        (index, item)
        for index, item in enumerate(raw_section)
        if isinstance(item, dict)
    ]


def _validate_required_string(
    item: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
) -> None:
    if key not in item:
        issues.append(
            ConfigValidationIssue(
                path=f"{parent_path}.{key}",
                message="field is required",
                code="required",
            )
        )
        return
    _validate_optional_string(item, key, parent_path, issues, required=True)


def _validate_optional_string(
    item: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
    *,
    required: bool = False,
) -> None:
    if key not in item:
        return
    value = item[key]
    if isinstance(value, str) and (value.strip() or not required):
        return
    issues.append(
        ConfigValidationIssue(
            path=f"{parent_path}.{key}",
            message="must be a non-empty string" if required else "must be a string",
            code="required" if required else "type",
        )
    )


def _validate_optional_bool(
    item: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
) -> None:
    if key in item and not isinstance(item[key], bool):
        issues.append(
            ConfigValidationIssue(
                path=f"{parent_path}.{key}",
                message="must be a boolean",
                code="type",
            )
        )


def _validate_optional_table(
    item: dict[str, Any],
    key: str,
    parent_path: str,
    issues: list[ConfigValidationIssue],
) -> None:
    if key in item and item[key] is not None and not isinstance(item[key], dict):
        issues.append(
            ConfigValidationIssue(
                path=f"{parent_path}.{key}",
                message="must be a table",
                code="type",
            )
        )


def _runtime_feature_enabled(
    config: dict[str, Any],
    name: str,
    *,
    default: bool,
) -> bool:
    section = config.get("runtime", {})
    if not isinstance(section, dict):
        return default
    value = section.get(name, default)
    return value if isinstance(value, bool) else default


def _format_issues(issues: list[ConfigValidationIssue]) -> str:
    if not issues:
        return "boot config preflight failed"
    lines = ["boot config preflight failed:"]
    lines.extend(f"- {issue.path}: {issue.message} ({issue.code})" for issue in issues)
    return "\n".join(lines)


__all__ = [
    "BootPreflightError",
    "BootPreflightResult",
    "run_boot_preflight",
]
