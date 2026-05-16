#!/usr/bin/env python3
"""One-shot export of legacy SQLite config tables into the file-backed layout."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
    persona_files: dict[Path, str] = field(default_factory=dict)
    prompt_definition_files: dict[Path, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def models_path(self) -> Path:
        return self.data_dir / "models.json"

    @property
    def instance_configs_path(self) -> Path:
        return self.data_dir / "instance-configs.json"

    def summary(self) -> dict[str, Any]:
        return {
            "dbPath": str(self.db_path),
            "modelsPath": str(self.models_path),
            "providers": len(self.models_payload["providers"]),
            "models": len(self.models_payload["models"]),
            "routes": len(self.models_payload["routes"]),
            "instanceConfigsPath": str(self.instance_configs_path),
            "instanceConfigs": len(self.instance_configs_payload["configs"]),
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


def _safe_stem(value: str, *, fallback: str, regex: re.Pattern[str]) -> str:
    stem = regex.sub("-", value.strip()).strip(".-:")
    if not stem:
        stem = f"{fallback}-{utc_now_iso().replace(':', '').replace('+', '-')}"
    if not stem[0].isalnum():
        stem = f"{fallback}-{stem}"
    return stem


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


def _write_text_if_safe(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise LegacyConfigMigrationError(
            f"Refusing to overwrite existing file {path}; use --overwrite"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


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
