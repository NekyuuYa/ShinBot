#!/usr/bin/env python3
"""One-shot export of legacy SQLite config tables into the file-backed layout."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sqlite3
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import tomli_w

from shinbot.admin.persona_files import render_persona_markdown
from shinbot.admin.prompt_definition_admin import (
    PromptDefinitionAdminError,
    PromptDefinitionDraft,
    render_prompt_definition_markdown,
)
from shinbot.persistence.records import utc_now_iso
from shinbot.persistence.repositories.admin_instance_configs import (
    INSTANCE_CONFIGS_FILE_VERSION,
)
from shinbot.persistence.repositories.model_registry import MODEL_REGISTRY_FILE_VERSION

SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9_.:-]+")
PERSONA_SAFE_STEM_RE = re.compile(r"[^A-Za-z0-9_.-]+")
TAB_TO_CAPABILITY = {"chat": "completion", "embedding": "embedding", "other": "rerank"}
EMPTY_MODELS_PAYLOAD = {
    "version": MODEL_REGISTRY_FILE_VERSION,
    "providers": [],
    "models": [],
    "routes": [],
}
EMPTY_INSTANCE_CONFIGS_PAYLOAD = {
    "version": INSTANCE_CONFIGS_FILE_VERSION,
    "configs": [],
}
EMPTY_MAIN_CONFIG = {
    "logging": {"level": "INFO", "third_party_noise": "debug"},
    "database": {"url": "sqlite:///data/db/shinbot.sqlite3", "snapshot_ttl": 10800},
    "admin": {},
    "adapter_instances": [],
    "plugins": [],
    "bots": [],
    "permissions": {},
}
MAIN_AGENT_ID = "full-agent"


@dataclass(slots=True)
class LegacyConfigMigrationPlan:
    """Dry-run representation of config files to write."""

    data_dir: Path
    db_path: Path
    models_payload: dict[str, Any] = field(
        default_factory=lambda: copy.deepcopy(EMPTY_MODELS_PAYLOAD)
    )
    instance_configs_payload: dict[str, Any] = field(
        default_factory=lambda: copy.deepcopy(EMPTY_INSTANCE_CONFIGS_PAYLOAD)
    )
    main_config_payload: dict[str, Any] = field(
        default_factory=lambda: copy.deepcopy(EMPTY_MAIN_CONFIG)
    )
    agent_files: dict[Path, str] = field(default_factory=dict)
    persona_files: dict[Path, str] = field(default_factory=dict)
    prompt_definition_files: dict[Path, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def models_path(self) -> Path:
        return self.data_dir / "models.json"

    @property
    def instance_configs_path(self) -> Path:
        return self.data_dir / "instance-configs.json"

    @property
    def main_config_path(self) -> Path:
        return self.data_dir / "config.toml"

    def summary(self) -> dict[str, Any]:
        return {
            "dbPath": str(self.db_path),
            "modelsPath": str(self.models_path),
            "providers": len(self.models_payload["providers"]),
            "models": len(self.models_payload["models"]),
            "routes": len(self.models_payload["routes"]),
            "instanceConfigsPath": str(self.instance_configs_path),
            "instanceConfigs": len(self.instance_configs_payload["configs"]),
            "mainConfigPath": str(self.main_config_path),
            "adapterInstances": len(self.main_config_payload["adapter_instances"]),
            "plugins": len(self.main_config_payload["plugins"]),
            "bots": len(self.main_config_payload["bots"]),
            "agentFiles": [str(path) for path in sorted(self.agent_files)],
            "personaFiles": [str(path) for path in sorted(self.persona_files)],
            "promptDefinitionFiles": [
                str(path) for path in sorted(self.prompt_definition_files)
            ],
            "warnings": list(self.warnings),
        }


class LegacyConfigMigrationError(RuntimeError):
    """Raised when a migration cannot be safely applied."""


def build_migration_plan(
    *,
    data_dir: Path | str,
    db_path: Path | str | None = None,
) -> LegacyConfigMigrationPlan:
    data_root = Path(data_dir)
    sqlite_path = Path(db_path) if db_path is not None else data_root / "db" / "shinbot.sqlite3"
    plan = LegacyConfigMigrationPlan(data_dir=data_root, db_path=sqlite_path)
    if not sqlite_path.is_file():
        plan.warnings.append(f"Legacy database was not found: {sqlite_path}")
        return plan

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        plan.models_payload = _build_models_payload(conn, plan.warnings)
        plan.instance_configs_payload = _build_instance_configs_payload(conn)
        plan.main_config_payload, plan.agent_files = _build_main_config_payload(
            conn,
            data_root,
            plan.warnings,
        )
        plan.persona_files = _build_persona_files(conn, data_root, plan.warnings)
        plan.prompt_definition_files = _build_prompt_definition_files(
            conn,
            data_root,
            persona_prompt_definition_ids=_persona_prompt_definition_ids(conn),
            warnings=plan.warnings,
        )
    finally:
        conn.close()
    return plan


def apply_migration(plan: LegacyConfigMigrationPlan, *, overwrite: bool = False) -> None:
    """Write a migration plan to disk without mutating the legacy database."""

    plan.data_dir.mkdir(parents=True, exist_ok=True)
    _write_json_if_safe(
        plan.models_path,
        plan.models_payload,
        empty_payload=EMPTY_MODELS_PAYLOAD,
        overwrite=overwrite,
    )
    _write_json_if_safe(
        plan.instance_configs_path,
        plan.instance_configs_payload,
        empty_payload=EMPTY_INSTANCE_CONFIGS_PAYLOAD,
        overwrite=overwrite,
    )
    _write_toml_if_safe(
        plan.main_config_path,
        plan.main_config_payload,
        empty_payload=EMPTY_MAIN_CONFIG,
        overwrite=overwrite,
    )
    for path, content in plan.agent_files.items():
        _write_text_if_safe(path, content, overwrite=overwrite)
    for path, content in {
        **plan.persona_files,
        **plan.prompt_definition_files,
    }.items():
        _write_text_if_safe(path, content, overwrite=overwrite)


def _build_models_payload(
    conn: sqlite3.Connection,
    warnings: list[str],
) -> dict[str, Any]:
    providers = _legacy_providers(conn)
    provider_id_by_uuid = {
        str(provider.get("provider_uuid")): str(provider["id"])
        for provider in providers
        if provider.get("provider_uuid")
    }
    models = _legacy_models(conn, provider_id_by_uuid=provider_id_by_uuid, warnings=warnings)
    routes = _legacy_routes(conn)
    members_by_route = _legacy_route_members(conn)
    for route in routes:
        route["members"] = members_by_route.get(str(route["id"]), [])
    return {
        "version": MODEL_REGISTRY_FILE_VERSION,
        "providers": [_drop_empty_private_keys(item, private_keys={"provider_uuid"}) for item in providers],
        "models": models,
        "routes": routes,
    }


def _legacy_providers(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "model_providers"):
        return []
    providers: list[dict[str, Any]] = []
    for row in _select_all(conn, "model_providers"):
        default_params = _json_object(row.get("default_params_json"), {})
        tab = default_params.pop("_tab", None)
        capability_type = (
            TAB_TO_CAPABILITY.get(str(tab), "completion")
            if tab
            else _string(row.get("capability_type"), "completion")
        )
        providers.append(
            {
                "provider_uuid": _string(row.get("provider_uuid")),
                "id": _string(row.get("id")),
                "type": _string(row.get("type")),
                "display_name": _string(row.get("display_name"), _string(row.get("id"))),
                "capability_type": capability_type,
                "base_url": _string(row.get("base_url")),
                "auth": _json_object(row.get("auth_json"), {}),
                "default_params": default_params,
                "enabled": _bool(row.get("enabled"), True),
                "created_at": _string(row.get("created_at"), utc_now_iso()),
                "updated_at": _string(row.get("updated_at"), utc_now_iso()),
            }
        )
    return sorted(providers, key=lambda item: str(item["id"]))


def _legacy_models(
    conn: sqlite3.Connection,
    *,
    provider_id_by_uuid: dict[str, str],
    warnings: list[str],
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "model_definitions"):
        return []
    models: list[dict[str, Any]] = []
    for row in _select_all(conn, "model_definitions"):
        provider_id = _string(row.get("provider_id"))
        if not provider_id and row.get("provider_uuid") is not None:
            provider_id = provider_id_by_uuid.get(_string(row.get("provider_uuid")), "")
        if not provider_id:
            warnings.append(f"Skipped model {_string(row.get('id'))!r}: provider was not found")
            continue
        models.append(
            {
                "id": _string(row.get("id")),
                "provider_id": provider_id,
                "litellm_model": _string(row.get("litellm_model")),
                "display_name": _string(row.get("display_name"), _string(row.get("id"))),
                "capabilities": _json_list(row.get("capabilities_json"), []),
                "context_window": _int_or_none(row.get("context_window")),
                "default_params": _json_object(row.get("default_params_json"), {}),
                "cost_metadata": _json_object(row.get("cost_metadata_json"), {}),
                "enabled": _bool(row.get("enabled"), True),
                "created_at": _string(row.get("created_at"), utc_now_iso()),
                "updated_at": _string(row.get("updated_at"), utc_now_iso()),
            }
        )
    return sorted(models, key=lambda item: (str(item["provider_id"]), str(item["id"])))


def _legacy_routes(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "model_routes"):
        return []
    routes: list[dict[str, Any]] = []
    for row in _select_all(conn, "model_routes"):
        routes.append(
            {
                "id": _string(row.get("id")),
                "purpose": _string(row.get("purpose")),
                "strategy": _string(row.get("strategy"), "priority"),
                "enabled": _bool(row.get("enabled"), True),
                "sticky_sessions": _bool(row.get("sticky_sessions"), False),
                "metadata": _json_object(row.get("metadata_json"), {}),
                "members": [],
                "created_at": _string(row.get("created_at"), utc_now_iso()),
                "updated_at": _string(row.get("updated_at"), utc_now_iso()),
            }
        )
    return sorted(routes, key=lambda item: str(item["id"]))


def _legacy_route_members(conn: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    if not _table_exists(conn, "model_route_members"):
        return {}
    members_by_route: dict[str, list[dict[str, Any]]] = {}
    rows = sorted(
        _select_all(conn, "model_route_members"),
        key=lambda item: (str(item.get("route_id")), int(item.get("priority") or 0), int(item.get("id") or 0)),
    )
    for row in rows:
        route_id = _string(row.get("route_id"))
        members_by_route.setdefault(route_id, []).append(
            {
                "model_id": _string(row.get("model_id")),
                "priority": int(row.get("priority") or 0),
                "weight": float(row.get("weight") or 1.0),
                "conditions": _json_object(row.get("conditions_json"), {}),
                "timeout_override": _float_or_none(row.get("timeout_override")),
                "enabled": _bool(row.get("enabled"), True),
            }
        )
    return members_by_route


def _build_instance_configs_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    configs: list[dict[str, Any]] = []
    if _table_exists(conn, "bot_configs"):
        for row in _select_all(conn, "bot_configs"):
            configs.append(
                {
                    "uuid": _string(row.get("uuid")),
                    "instance_id": _string(row.get("instance_id")),
                    "main_llm": _string(row.get("main_llm")),
                    "config": _json_object(row.get("config_json"), {}),
                    "tags": _json_list(row.get("tags_json"), []),
                    "created_at": _string(row.get("created_at"), utc_now_iso()),
                    "updated_at": _string(row.get("updated_at"), utc_now_iso()),
                }
            )
    return {
        "version": INSTANCE_CONFIGS_FILE_VERSION,
        "configs": sorted(configs, key=lambda item: (str(item["instance_id"]), str(item["uuid"]))),
    }


def _build_main_config_payload(
    conn: sqlite3.Connection,
    data_dir: Path,
    warnings: list[str],
) -> tuple[dict[str, Any], dict[Path, str]]:
    legacy_config_path = data_dir.parent / "config.toml"
    if not legacy_config_path.is_file():
        warnings.append(f"Legacy main config was not found: {legacy_config_path}")
        return copy.deepcopy(EMPTY_MAIN_CONFIG), {}

    try:
        legacy_payload = tomllib.loads(legacy_config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        warnings.append(f"Legacy main config could not be parsed: {exc}")
        return copy.deepcopy(EMPTY_MAIN_CONFIG), {}

    admin_cfg = _legacy_admin_config(legacy_payload)
    plugins, plugin_config_files = _legacy_plugins_config(legacy_payload, data_dir, warnings)
    adapter_instances = _legacy_adapter_instances(legacy_payload, warnings)
    bots, agent_files = _legacy_bots_config(conn, legacy_payload, data_dir, warnings)

    return (
        {
            "logging": _legacy_logging_config(legacy_payload),
            "database": _legacy_database_config(),
            "admin": admin_cfg,
            "adapter_instances": adapter_instances,
            "plugins": plugins,
            "bots": bots,
            "permissions": _legacy_permissions_config(legacy_payload),
        },
        {**plugin_config_files, **agent_files},
    )


def _legacy_logging_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    logging_cfg = payload.get("logging")
    level = "INFO"
    third_party_noise = "debug"
    if isinstance(logging_cfg, Mapping):
        level = _string(logging_cfg.get("level"), "INFO") or "INFO"
        if "third_party_noise" in logging_cfg:
            third_party_noise = _string(logging_cfg.get("third_party_noise"), "debug") or "debug"
    return {"level": level, "third_party_noise": third_party_noise}


def _legacy_database_config() -> dict[str, Any]:
    return {"url": "sqlite:///data/db/shinbot.sqlite3", "snapshot_ttl": 10800}


def _legacy_permissions_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    permissions = payload.get("permissions")
    return dict(permissions) if isinstance(permissions, Mapping) else {}


def _legacy_admin_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    admin = payload.get("admin")
    if not isinstance(admin, Mapping):
        return {}
    result = {
        "username": _string(admin.get("username")),
        "password": _string(admin.get("password")),
        "jwt_expire_hours": _int_or_none(admin.get("jwt_expire_hours")) or 24,
    }
    dashboard_dist = _string(admin.get("dashboard_dist"))
    if dashboard_dist:
        result["dashboard_dist"] = dashboard_dist
    if _string(admin.get("jwt_secret")):
        result["jwt_secret"] = _string(admin.get("jwt_secret"))
    return {key: value for key, value in result.items() if value not in ("", None)}


def _legacy_adapter_instances(payload: Mapping[str, Any], warnings: list[str]) -> list[dict[str, Any]]:
    instances = payload.get("instances")
    if not isinstance(instances, list):
        return []

    adapter_instances: list[dict[str, Any]] = []
    for index, item in enumerate(instances):
        if not isinstance(item, Mapping):
            warnings.append(f"Skipped legacy instance at index {index}: not a table")
            continue

        instance_id = _string(item.get("id"))
        adapter = _string(item.get("adapterType"), _string(item.get("platform")))
        if not instance_id or not adapter:
            warnings.append(f"Skipped legacy instance at index {index}: missing id or adapter")
            continue

        config = _legacy_adapter_config(adapter, _json_object(item.get("config"), {}))
        record = {
            "id": instance_id,
            "name": _string(item.get("name"), instance_id),
            "adapter": adapter,
            "enabled": True,
            "config": config,
        }
        created_at = _int_or_none(item.get("createdAt"))
        last_modified = _int_or_none(item.get("lastModified"))
        if created_at is not None:
            record["createdAt"] = created_at
        if last_modified is not None:
            record["lastModified"] = last_modified
        adapter_instances.append(record)

    return adapter_instances


def _legacy_adapter_config(adapter: str, raw_config: dict[str, Any]) -> dict[str, Any]:
    config = dict(raw_config)
    if adapter == "onebot_v11":
        config.setdefault("mode", "reverse")
        if "url" in config:
            config["url"] = str(config["url"])
        if "reverse_port" in config and isinstance(config.get("reverse_port"), str):
            try:
                config["reverse_port"] = int(str(config["reverse_port"]).strip())
            except ValueError:
                pass
        if "download_resources" in config:
            download_resources = config.pop("download_resources")
            if "auto_download_media" not in config:
                config["auto_download_media"] = _bool(download_resources, True)
        config.setdefault("auto_download_media", True)
        config.setdefault("download_file_resources", False)
        config.setdefault("resource_cache_dir", "data/temp/resources")
        config.setdefault("reconnect_delay", 5)
        config.setdefault("max_reconnects", -1)
        config.setdefault("request_timeout", 20)
        config.setdefault("forward_max_depth", 3)
        config.setdefault("silent_reconnect", True)
        config.setdefault("reconnect_log_interval", 30)
    elif adapter == "satori":
        config.setdefault("host", "localhost:5140")
        config.setdefault("path", "/v1/events")
        config.setdefault("auto_download_media", True)
        config.setdefault("download_file_resources", False)
        config.setdefault("resource_cache_dir", "data/temp/resources")
        config.setdefault("reconnect_delay", 5)
        config.setdefault("max_reconnects", -1)
        config.setdefault("silent_reconnect", True)
        config.setdefault("reconnect_log_interval", 30)
    return config


def _legacy_plugins_config(
    payload: Mapping[str, Any],
    data_dir: Path,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], dict[Path, str]]:
    plugin_configs = payload.get("plugin_configs")
    plugin_states = payload.get("plugin_states")
    if not isinstance(plugin_configs, Mapping) and not isinstance(plugin_states, Mapping):
        return [], {}

    plugin_entries: dict[str, dict[str, Any]] = {}
    plugin_files: dict[Path, str] = {}

    if isinstance(plugin_configs, Mapping):
        for plugin_id, raw_config in plugin_configs.items():
            if not isinstance(plugin_id, str):
                continue
            plugin_entries.setdefault(plugin_id, {"id": plugin_id, "enabled": True, "config": {}})
            plugin_entries[plugin_id]["config"] = (
                _coerce_plugin_config(raw_config) if isinstance(raw_config, Mapping) else {}
            )

    if isinstance(plugin_states, Mapping):
        for plugin_id, raw_state in plugin_states.items():
            if not isinstance(plugin_id, str):
                continue
            entry = plugin_entries.setdefault(
                plugin_id,
                {"id": plugin_id, "enabled": True, "config": {}},
            )
            if isinstance(raw_state, Mapping) and "enabled" in raw_state:
                entry["enabled"] = _bool(raw_state.get("enabled"), True)

    plugins = [plugin_entries[key] for key in sorted(plugin_entries)]
    return plugins, plugin_files


def _coerce_plugin_config(raw_config: Mapping[str, Any]) -> dict[str, Any]:
    return dict(raw_config)


def _render_plugin_config(plugin_id: str, raw_config: Mapping[str, Any]) -> str:
    payload = dict(raw_config)
    return _toml_dumps(payload)


def _legacy_bots_config(
    conn: sqlite3.Connection,
    payload: Mapping[str, Any],
    data_dir: Path,
    warnings: list[str],
) -> tuple[list[dict[str, Any]], dict[Path, str]]:
    legacy_instances = payload.get("instances")
    if not isinstance(legacy_instances, list):
        return [], {}

    agent_refs_by_instance = _legacy_agent_refs_by_instance(conn)
    bots: list[dict[str, Any]] = []
    agent_files: dict[Path, str] = {}
    active_binding_index = 0

    for item in legacy_instances:
        if not isinstance(item, Mapping):
            continue
        instance_id = _string(item.get("id"))
        if not instance_id:
            continue
        agent_ref = agent_refs_by_instance.get(instance_id, {})
        agent_path, agent_payload = _build_agent_file(instance_id, agent_ref, data_dir, warnings)
        if agent_path is not None and agent_payload is not None:
            agent_files[agent_path] = agent_payload
        binding = _build_bot_binding(instance_id, index=active_binding_index)
        active_binding_index += 1
        bots.append(
            {
                "id": instance_id,
                "display_name": _string(item.get("name"), instance_id),
                "enabled": True,
                "commands": {
                    "enabled": True,
                    "prefixes": ["/", "!"],
                },
                "plugins": {
                    "enabled": True,
                    "enabled_plugins": ["*"],
                    "disabled_plugins": [],
                },
                "agent": {
                    "mode": "full" if agent_path is not None else "none",
                    "config": str(agent_path.relative_to(data_dir)) if agent_path is not None else "",
                },
                "bindings": [binding],
            }
        )

    return bots, agent_files


def _build_bot_binding(instance_id: str, *, index: int) -> dict[str, Any]:
    return {
        "id": f"{instance_id}-binding",
        "adapter_instance_id": instance_id,
        "session_patterns": ["group:*", "private:*"],
        "enabled": True,
        "priority": index,
    }


def _build_agent_file(
    instance_id: str,
    agent_ref: Mapping[str, Any],
    data_dir: Path,
    warnings: list[str],
) -> tuple[Path | None, str | None]:
    persona_id = _string(agent_ref.get("persona_id"))
    if not persona_id:
        warnings.append(f"Skipped agent file for {instance_id!r}: persona was not found")
        return None, None

    agent_id = _safe_agent_stem(
        _string(agent_ref.get("agent_id"), MAIN_AGENT_ID),
        fallback=MAIN_AGENT_ID,
    )
    if not agent_id:
        agent_id = MAIN_AGENT_ID
    agent_path = data_dir / "agents" / f"{agent_id}.toml"
    content = _render_agent_toml(
        agent_id,
        persona_id,
        llm_ref=_string(agent_ref.get("llm_ref")),
    )
    return agent_path, content


def _legacy_agent_refs_by_instance(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not _table_exists(conn, "bot_configs") or not _table_exists(conn, "agents"):
        return {}

    bot_columns = _table_columns(conn, "bot_configs")
    if "default_agent_uuid" not in bot_columns:
        return {}

    agents_by_uuid = {
        _string(row.get("uuid")): row
        for row in _select_all(conn, "agents")
        if _string(row.get("uuid"))
    }
    model_index = _legacy_model_ref_index(conn)
    refs: dict[str, dict[str, Any]] = {}
    for row in _select_all(conn, "bot_configs"):
        instance_id = _string(row.get("instance_id"))
        agent_uuid = _string(row.get("default_agent_uuid"))
        agent = agents_by_uuid.get(agent_uuid)
        if not instance_id or agent is None:
            continue
        refs[instance_id] = {
            "agent_id": _string(agent.get("agent_id"), instance_id),
            "name": _string(agent.get("name")),
            "persona_id": _string(agent.get("persona_uuid")),
            "llm_ref": _legacy_llm_ref(_string(row.get("main_llm")), model_index),
        }
    return refs


def _legacy_model_ref_index(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        "routes": {
            _string(row.get("id"))
            for row in _select_all(conn, "model_routes")
            if _string(row.get("id"))
        },
        "models": {
            _string(row.get("id"))
            for row in _select_all(conn, "model_definitions")
            if _string(row.get("id"))
        },
    }


def _legacy_llm_ref(value: str, model_index: dict[str, set[str]]) -> str:
    if not value:
        return ""
    if value in model_index.get("routes", set()):
        return f"[route]{value}"
    if value in model_index.get("models", set()):
        return f"[model]{value}"
    return value


def _render_agent_toml(agent_id: str, persona_id: str, *, llm_ref: str) -> str:
    default_llm = llm_ref or ""
    return "\n".join(
        [
            "[agent]",
            f"id = {_toml_string(agent_id)}",
            'mode = "full"',
            f"persona_id = {_toml_string(persona_id)}",
            "",
            "[agent.prompt_files]",
            'locale = "zh-CN"',
            'fallback_locales = ["en-US"]',
            "sync_to_data = true",
            'data_root = "prompts"',
            "",
            "[agent.defaults]",
            f"llm = {_toml_string(default_llm)}",
            "max_model_retries = 1",
            "retry_backoff_seconds = 0.25",
            "",
            "[agent.defaults.message_format]",
            "use_thumbnail = true",
            "include_sender = true",
            "include_time = true",
            "include_message_id = true",
            "",
            "[agent.review]",
            "scan_batch_size = 500",
            "overflow_threshold_messages = 3000",
            "overflow_compression_batch_size = 500",
            "reply_context_before_messages = 30",
            "reply_context_after_messages = 10",
            "block_digest_concurrency = 4",
            "bootstrap_timeout_seconds = 20",
            "",
            "[agent.review.overflow_compression]",
            "enabled = true",
            f"llm = {_toml_string(default_llm)}",
            "max_model_retries = 1",
            "retry_backoff_seconds = 0.25",
            "",
            "[agent.review.overflow_compression.prompts]",
            'system = "review.overflow_compression.system"',
            'task = "review.overflow_compression.task"',
            'constraints = "review.overflow_compression.constraints"',
            "",
            "[agent.review.scan]",
            "enabled = true",
            f"llm = {_toml_string(default_llm)}",
            "max_model_retries = 1",
            "retry_backoff_seconds = 0.25",
            "",
            "[agent.review.scan.prompts]",
            'system = "review.review_scan.system"',
            'task = "review.review_scan.task"',
            'constraints = "review.review_scan.constraints"',
            "",
            "[agent.review.block_digest]",
            "enabled = true",
            f"llm = {_toml_string(default_llm)}",
            "max_model_retries = 1",
            "retry_backoff_seconds = 0.25",
            "",
            "[agent.review.block_digest.prompts]",
            'system = "review.block_digest.system"',
            'task = "review.block_digest.task"',
            'constraints = "review.block_digest.constraints"',
            "",
            "[agent.review.reply_decision]",
            "enabled = true",
            f"llm = {_toml_string(default_llm)}",
            "max_model_retries = 1",
            "retry_backoff_seconds = 0.25",
            "",
            "[agent.review.reply_decision.prompts]",
            'system = "review.reply_decision.system"',
            'task = "review.reply_decision.task"',
            'constraints = "review.reply_decision.constraints"',
            'repair = "review.reply_decision.repair"',
            "",
            "[agent.review.active_chat_bootstrap]",
            "enabled = true",
            f"llm = {_toml_string(default_llm)}",
            "max_model_retries = 1",
            "retry_backoff_seconds = 0.25",
            "",
            "[agent.review.active_chat_bootstrap.prompts]",
            'system = "review.active_chat_bootstrap.system"',
            'task = "review.active_chat_bootstrap.task"',
            'constraints = "review.active_chat_bootstrap.constraints"',
            "",
            "[agent.active_chat]",
            "initial_interest = 15",
            "half_life_seconds = 20",
            "tick_interval_seconds = 5",
            "idle_interest_threshold = 5",
            "max_interest = 100",
            "post_round_attention_multiplier = 0.25",
            "",
            "[agent.active_chat.interest_delta]",
            "normal_message = 1",
            "mention_self = 8",
            "reply_to_self = 5",
            "poke = 0",
            "mention_other = 0",
            "send_reply = 8",
            "send_reply_low = 3",
            "no_reply = -5",
            "no_reply_strong = -10",
            "send_poke = 3",
            "",
            "[agent.active_chat.attention]",
            "base_contribution = 1.0",
            "mention_self_contribution = 4.0",
            "mention_other_contribution = 0.5",
            "reply_to_self_contribution = 3.0",
            "poke_self_contribution = 0.8",
            "poke_other_contribution = 0.2",
            "threshold = 5.0",
            "",
            "[agent.active_chat.fast_mode]",
            f"llm = {_toml_string(default_llm)}",
            "",
            "[agent.active_chat.fast_mode.prompts]",
            'system = "active_chat.fast_mode.system"',
            'constraints = "active_chat.fast_mode.constraints"',
            'repair = "active_chat.fast_mode.repair"',
            'conversation_summary = "active_chat.fast_mode.conversation_summary"',
            'handoff_overflow = "active_chat.handoff.overflow"',
            'handoff_digest = "active_chat.handoff.digest"',
            'handoff_legacy = "active_chat.handoff.legacy"',
            "",
            "[agent.summaries]",
            "active_chat_summary_max_age_seconds = 1800",
            "",
            "[agent.summaries.markdown]",
            "enabled = true",
            'dir = "summary"',
            "",
        ]
    )


def _build_persona_files(
    conn: sqlite3.Connection,
    data_dir: Path,
    warnings: list[str],
) -> dict[Path, str]:
    if not _table_exists(conn, "personas"):
        return {}
    definitions = {
        _string(row.get("uuid")): row
        for row in _select_all(conn, "prompt_definitions")
    } if _table_exists(conn, "prompt_definitions") else {}
    files: dict[Path, str] = {}
    for row in _select_all(conn, "personas"):
        persona_id = _safe_persona_stem(_string(row.get("uuid")), fallback="persona")
        definition = definitions.get(_string(row.get("prompt_definition_uuid")))
        prompt_text = _string(definition.get("content")) if definition is not None else ""
        if not prompt_text.strip():
            warnings.append(f"Skipped persona {persona_id!r}: prompt content was empty")
            continue
        files[data_dir / "personas" / f"{persona_id}.md"] = render_persona_markdown(
            persona_id=persona_id,
            name=_string(row.get("name"), persona_id),
            prompt_text=prompt_text,
            tags=[str(item) for item in _json_list(row.get("tags_json"), [])],
            enabled=_bool(row.get("enabled"), True),
            created_at=_string(row.get("created_at"), utc_now_iso()),
            updated_at=_string(row.get("updated_at"), utc_now_iso()),
            version=_string(definition.get("version"), "1.0.0") if definition else "1.0.0",
            description=_string(definition.get("description")) if definition else "",
        )
    return files


def _build_prompt_definition_files(
    conn: sqlite3.Connection,
    data_dir: Path,
    *,
    persona_prompt_definition_ids: set[str],
    warnings: list[str],
) -> dict[Path, str]:
    if not _table_exists(conn, "prompt_definitions"):
        return {}
    columns = _table_columns(conn, "prompt_definitions")
    if {"prompt_id", "stage", "type"} - columns:
        return {}

    files: dict[Path, str] = {}
    for row in _select_all(conn, "prompt_definitions"):
        if _string(row.get("uuid")) in persona_prompt_definition_ids:
            continue
        prompt_id = _safe_prompt_stem(_string(row.get("prompt_id"), _string(row.get("uuid"))))
        try:
            draft = PromptDefinitionDraft(
                uuid=prompt_id,
                prompt_id=prompt_id,
                name=_string(row.get("name"), prompt_id),
                source_type=_legacy_prompt_source_type(row.get("source_type")),
                source_id=_string(row.get("source_id")),
                owner_plugin_id=_string(row.get("owner_plugin_id")),
                owner_module=_string(row.get("owner_module")),
                module_path=_string(row.get("module_path")),
                stage=_string(row.get("stage"), "instructions"),
                type=_string(row.get("type"), "static_text"),
                priority=int(row.get("priority") or 100),
                version=_string(row.get("version"), "1.0.0"),
                description=_string(row.get("description")),
                enabled=_bool(row.get("enabled"), True),
                content=_string(row.get("content")),
                template_vars=[str(item) for item in _json_list(row.get("template_vars_json"), [])],
                resolver_ref=_string(row.get("resolver_ref")),
                bundle_refs=[str(item) for item in _json_list(row.get("bundle_refs_json"), [])],
                config=_json_object(row.get("config_json"), {}),
                tags=[str(item) for item in _json_list(row.get("tags_json"), [])],
                metadata=_json_object(row.get("metadata_json"), {}),
                created_at=_string(row.get("created_at")),
                updated_at=_string(row.get("updated_at")),
            )
            files[data_dir / "prompts" / "custom" / f"{prompt_id}.md"] = (
                render_prompt_definition_markdown(draft)
            )
        except (PromptDefinitionAdminError, ValueError) as exc:
            warnings.append(f"Skipped prompt {prompt_id!r}: {exc}")
    return files


def _persona_prompt_definition_ids(conn: sqlite3.Connection) -> set[str]:
    if not _table_exists(conn, "personas"):
        return set()
    return {
        _string(row.get("prompt_definition_uuid"))
        for row in _select_all(conn, "personas")
        if row.get("prompt_definition_uuid") is not None
    }


def _legacy_prompt_source_type(value: Any) -> str:
    source_type = _string(value, "legacy_migration")
    return "legacy_migration" if source_type == "persona" else source_type


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _select_all(conn: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    if not _table_exists(conn, table_name):
        return []
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(row) for row in rows]


def _json_object(value: Any, default: dict[str, Any]) -> dict[str, Any]:
    parsed = _json_value(value, default)
    return dict(parsed) if isinstance(parsed, Mapping) else dict(default)


def _json_list(value: Any, default: list[Any]) -> list[Any]:
    parsed = _json_value(value, default)
    return list(parsed) if isinstance(parsed, list) else list(default)


def _json_value(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _string(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off"}
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_prompt_stem(value: str, *, fallback: str = "prompt") -> str:
    return _safe_stem(value, fallback=fallback, regex=SAFE_STEM_RE)


def _safe_persona_stem(value: str, *, fallback: str = "persona") -> str:
    return _safe_stem(value, fallback=fallback, regex=PERSONA_SAFE_STEM_RE)


def _safe_agent_stem(value: str, *, fallback: str = "agent") -> str:
    return _safe_stem(value, fallback=fallback, regex=PERSONA_SAFE_STEM_RE)


def _safe_stem(value: str, *, fallback: str, regex: re.Pattern[str]) -> str:
    stem = regex.sub("-", value.strip()).strip(".-:")
    if not stem:
        stem = f"{fallback}-{utc_now_iso().replace(':', '').replace('+', '-')}"
    if not stem[0].isalnum():
        stem = f"{fallback}-{stem}"
    return stem


def _toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _drop_empty_private_keys(
    payload: dict[str, Any],
    *,
    private_keys: set[str],
) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in private_keys}


def _write_json_if_safe(
    path: Path,
    payload: dict[str, Any],
    *,
    empty_payload: dict[str, Any],
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise LegacyConfigMigrationError(
                f"Refusing to overwrite invalid existing file {path}"
            ) from exc
        if existing != empty_payload:
            raise LegacyConfigMigrationError(
                f"Refusing to overwrite non-empty existing file {path}; use --overwrite"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_toml_if_safe(
    path: Path,
    payload: dict[str, Any],
    *,
    empty_payload: dict[str, Any],
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        try:
            existing = tomllib.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise LegacyConfigMigrationError(
                f"Refusing to overwrite invalid existing file {path}"
            ) from exc
        if existing != empty_payload:
            raise LegacyConfigMigrationError(
                f"Refusing to overwrite non-empty existing file {path}; use --overwrite"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_toml_dumps(payload), encoding="utf-8")


def _write_text_if_safe(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise LegacyConfigMigrationError(
            f"Refusing to overwrite existing file {path}; use --overwrite"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _toml_dumps(payload: dict[str, Any]) -> str:
    return tomli_w.dumps(payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export legacy SQLite config tables into data/models.json, "
            "data/instance-configs.json and Markdown persona/prompt files."
        )
    )
    parser.add_argument("--data-dir", default="data", help="Runtime data directory.")
    parser.add_argument(
        "--db",
        default="",
        help="Legacy SQLite path. Defaults to DATA_DIR/db/shinbot.sqlite3.",
    )
    parser.add_argument("--apply", action="store_true", help="Write files. Default is dry-run.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing existing generated files.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON summary.")
    args = parser.parse_args(argv)

    plan = build_migration_plan(
        data_dir=Path(args.data_dir),
        db_path=Path(args.db) if args.db else None,
    )
    if args.apply:
        apply_migration(plan, overwrite=args.overwrite)

    summary = plan.summary()
    summary["applied"] = bool(args.apply)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        _print_summary(summary)
    return 0


def _print_summary(summary: dict[str, Any]) -> None:
    print(f"Legacy DB: {summary['dbPath']}")
    print(f"Models: {summary['providers']} providers, {summary['models']} models, {summary['routes']} routes")
    print(f"Instance configs: {summary['instanceConfigs']}")
    print(f"Persona files: {len(summary['personaFiles'])}")
    print(f"Prompt definition files: {len(summary['promptDefinitionFiles'])}")
    if summary["warnings"]:
        print("Warnings:")
        for warning in summary["warnings"]:
            print(f"- {warning}")


if __name__ == "__main__":
    raise SystemExit(main())
