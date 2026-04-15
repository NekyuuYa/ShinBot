"""Plugin management router: /api/v1/plugins"""

from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, ok

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/plugins",
    tags=["plugins"],
    dependencies=AuthRequired,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _adapter_config_schema(bot: Any, plugin_id: str) -> dict[str, Any] | None:
    module = bot.plugin_manager._modules.get(plugin_id)
    if module is None:
        return None

    cfg_cls = getattr(module, "__plugin_config_class__", None)
    if cfg_cls is None or not hasattr(cfg_cls, "model_json_schema"):
        return None

    try:
        return cfg_cls.model_json_schema()
    except Exception:
        logger.exception("Failed to build config schema for plugin %s", plugin_id)
        return None


def _plugin_locales(bot: Any, plugin_id: str) -> dict[str, dict[str, str]]:
    module = bot.plugin_manager._modules.get(plugin_id)
    if module is None:
        return {}
    payload = getattr(module, "__plugin_locales__", {})
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, str]] = {}
    for locale, entries in payload.items():
        if not isinstance(locale, str) or not isinstance(entries, dict):
            continue
        result[locale] = {
            str(key): str(value)
            for key, value in entries.items()
            if isinstance(key, str) and isinstance(value, str)
        }
    return result


def _request_locale(request: Request) -> list[str]:
    header = request.headers.get("accept-language", "")
    locales: list[str] = []
    for chunk in header.split(","):
        code = chunk.split(";")[0].strip()
        if code:
            locales.append(code)
    locales.extend(["zh-CN", "en-US"])
    return locales


def _resolve_translations(locales: dict[str, dict[str, str]], requested: list[str]) -> dict[str, str]:
    for locale in requested:
        if locale in locales:
            return locales[locale]
        base = locale.split("-")[0].lower()
        for candidate, entries in locales.items():
            if candidate.split("-")[0].lower() == base:
                return entries
    return {}


def _translate_schema_properties(
    properties: dict[str, Any] | None,
    translations: dict[str, str],
    parent: str = "",
) -> None:
    if not isinstance(properties, dict):
        return
    for key, value in properties.items():
        if not isinstance(value, dict):
            continue
        path = f"{parent}.{key}" if parent else key
        label_key = f"config.fields.{path}.label"
        description_key = f"config.fields.{path}.description"
        if label_key in translations:
            value["title"] = translations[label_key]
        if description_key in translations:
            value["description"] = translations[description_key]
        if value.get("type") == "object":
            _translate_schema_properties(value.get("properties"), translations, path)
        items = value.get("items")
        if isinstance(items, dict) and items.get("type") == "object":
            _translate_schema_properties(items.get("properties"), translations, path)


def _translate_plugin_schema(schema: dict[str, Any] | None, translations: dict[str, str]) -> dict[str, Any] | None:
    if schema is None:
        return None
    translated = deepcopy(schema)
    if "config.title" in translations:
        translated["title"] = translations["config.title"]
    if "config.description" in translations:
        translated["description"] = translations["config.description"]
    _translate_schema_properties(translated.get("properties"), translations)
    return translated


def _plugin_config_class(bot: Any, plugin_id: str) -> type[Any] | None:
    module = bot.plugin_manager._modules.get(plugin_id)
    if module is None:
        return None
    cfg_cls = getattr(module, "__plugin_config_class__", None)
    return cfg_cls if cfg_cls is not None and hasattr(cfg_cls, "model_validate") else None


def _plugin_config_store(boot: Any) -> dict[str, Any]:
    store = boot.config.setdefault("plugin_configs", {})
    if not isinstance(store, dict):
        store = {}
        boot.config["plugin_configs"] = store
    return store


def _plugin_saved_config(boot: Any, plugin_id: str) -> dict[str, Any]:
    store = boot.config.get("plugin_configs", {})
    if not isinstance(store, dict):
        return {}
    config = store.get(plugin_id, {})
    return dict(config) if isinstance(config, dict) else {}


def _unflatten_config(raw: dict[str, Any]) -> dict[str, Any]:
    nested: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key:
            continue

        cursor = nested
        parts = [part for part in key.split(".") if part]
        if not parts:
            continue

        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child

        cursor[parts[-1]] = value

    return nested


def _normalize_plugin_config(bot: Any, plugin_id: str, config: dict[str, Any]) -> dict[str, Any]:
    cfg_cls = _plugin_config_class(bot, plugin_id)
    if cfg_cls is None:
        return _unflatten_config(config)

    try:
        validated = cfg_cls.model_validate(_unflatten_config(config))
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "code": EC.INVALID_ACTION,
                "message": exc.errors()[0].get("msg", "Invalid plugin configuration"),
            },
        ) from exc

    return validated.model_dump(exclude_none=True)


def _plugin_dict(
    bot: Any,
    p: Any,
    boot: Any | None = None,
    *,
    translations: dict[str, str] | None = None,
) -> dict:
    module = bot.plugin_manager._modules.get(p.id)
    cfg_schema = _adapter_config_schema(bot, p.id)
    status = "enabled" if p.state.value in ("active", "loaded", "running") else "disabled"
    resolved_translations = translations or {}

    metadata: dict[str, Any] = {}
    if cfg_schema is not None:
        metadata["config_schema"] = _translate_plugin_schema(cfg_schema, resolved_translations)
    if boot is not None:
        metadata["config"] = _plugin_saved_config(boot, p.id)
    if module is not None and hasattr(module, "__plugin_adapter_platform__"):
        metadata["adapter_platform"] = module.__plugin_adapter_platform__

    return {
        "id": p.id,
        "name": resolved_translations.get("meta.name", p.name),
        "version": p.version,
        "description": resolved_translations.get("meta.description", p.description),
        "author": p.author,
        "role": p.role.value,
        "status": status,
        "state": p.state.value,
        "commands": p.commands,
        "event_types": p.event_types,
        "data_dir": p.data_dir,
        "metadata": metadata,
    }


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_plugins(request: Request, bot=BotDep, boot=BootDep):
    """List all loaded plugins with their metadata and config schema."""
    requested = _request_locale(request)
    return ok(
        [
            _plugin_dict(
                bot,
                p,
                boot,
                translations=_resolve_translations(_plugin_locales(bot, p.id), requested),
            )
            for p in bot.plugin_manager.all_plugins
        ]
    )


@router.get("/{plugin_id}/schema")
async def get_plugin_schema(plugin_id: str, request: Request, bot=BotDep):
    """Get config schema for a non-adapter plugin that declares __plugin_config_class__."""
    plugin = bot.plugin_manager.get_plugin(plugin_id)
    if plugin is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": f"Plugin {plugin_id!r} not found"},
        )

    if plugin.role.value == "adapter":
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose plugin-level configuration",
            },
        )

    schema = _adapter_config_schema(bot, plugin_id)
    if schema is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose a config schema",
            },
        )
    translations = _resolve_translations(_plugin_locales(bot, plugin_id), _request_locale(request))
    return ok(_translate_plugin_schema(schema, translations))


async def _rescan_plugins(bot: Any, boot: Any):
    """Rescan data/plugins/ for new plugins and load them (hot-add)."""
    plugins_dir = (Path(boot.data_dir) / "plugins").resolve()
    if not plugins_dir.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_RESCAN_FAILED,
                "message": f"Plugins directory not found: {plugins_dir}",
            },
        )
    try:
        loaded = await bot.plugin_manager.load_plugins_from_metadata_dir_async(plugins_dir)
        return ok({"loaded_count": len(loaded), "plugins": [_plugin_dict(bot, p) for p in loaded]})
    except Exception as e:
        logger.exception("Plugin rescan failed")
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RESCAN_FAILED, "message": str(e)},
        ) from e


@router.post("/reload")
async def reload_plugins(bot=BotDep, boot=BootDep):
    return await _rescan_plugins(bot, boot)


@router.post("/rescan")
async def rescan_plugins(bot=BotDep, boot=BootDep):
    return await _rescan_plugins(bot, boot)


@router.patch("/{plugin_id}/config")
async def update_plugin_config(plugin_id: str, config: dict[str, Any], bot=BotDep, boot=BootDep):
    """Persist plugin configuration for a specific plugin by ID."""
    plugin = bot.plugin_manager.get_plugin(plugin_id)
    if plugin is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": f"Plugin {plugin_id!r} not found"},
        )

    if plugin.role.value == "adapter":
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PLUGIN_NOT_FOUND,
                "message": f"Plugin {plugin_id!r} does not expose plugin-level configuration",
            },
        )

    normalized_config = _normalize_plugin_config(bot, plugin_id, config)
    store = _plugin_config_store(boot)
    store[plugin_id] = normalized_config

    if not boot.save_config():
        raise HTTPException(
            status_code=500,
            detail={
                "code": EC.CONFIG_WRITE_FAILED,
                "message": f"Failed to persist configuration for plugin {plugin_id!r}",
            },
        )

    return ok(_plugin_dict(bot, plugin, boot))


@router.post("/{plugin_id}/disable")
async def disable_plugin(plugin_id: str, bot=BotDep):
    """Disable a specific plugin while keeping its metadata visible to the UI."""
    try:
        meta = await bot.plugin_manager.disable_plugin_async(plugin_id)
        return ok(_plugin_dict(bot, meta))
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": str(e)},
        ) from e
    except Exception as e:
        logger.exception("Disable failed for plugin %s", plugin_id)
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RELOAD_FAILED, "message": str(e)},
        ) from e


@router.post("/{plugin_id}/enable")
async def enable_plugin(plugin_id: str, bot=BotDep):
    """Enable a previously disabled plugin."""
    try:
        meta = await bot.plugin_manager.enable_plugin_async(plugin_id)
        return ok(_plugin_dict(bot, meta))
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.PLUGIN_NOT_FOUND, "message": str(e)},
        ) from e
    except Exception as e:
        logger.exception("Enable failed for plugin %s", plugin_id)
        raise HTTPException(
            status_code=500,
            detail={"code": EC.PLUGIN_RELOAD_FAILED, "message": str(e)},
        ) from e
