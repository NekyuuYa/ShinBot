"""Frontend-facing runtime configuration workspace API."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, Envelope, ok
from shinbot.core.application.boot_preflight import run_boot_preflight
from shinbot.core.application.config_sections import (
    iter_adapter_instance_records,
    normalize_adapter_instance_record,
)
from shinbot.core.application.provider_config_validation import validate_provider_configs
from shinbot.core.config_provider import (
    ConfigProviderDefinition,
    ConfigProviderKind,
    ConfigProviderRegistry,
    ConfigValidationIssue,
)

router = APIRouter(
    prefix="/config",
    tags=["config"],
    dependencies=AuthRequired,
)


class ValidateConfigRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)


class SaveConfigRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)
    validateBeforeSave: bool = True


class SaveAdapterInstancesRequest(BaseModel):
    adapterInstances: list[dict[str, Any]] = Field(default_factory=list)
    validateBeforeSave: bool = True


class SaveBotsRequest(BaseModel):
    bots: list[dict[str, Any]] = Field(default_factory=list)
    validateBeforeSave: bool = True


class ConfigValidationIssuePayload(BaseModel):
    """A single configuration validation issue."""

    path: str = ""
    message: str = ""
    code: str = ""
    source: str = ""


class ConfigValidationResult(BaseModel):
    """Result of a configuration validation pass."""

    valid: bool = False
    issues: list[ConfigValidationIssuePayload] = Field(default_factory=list)
    normalized: dict[str, Any] = Field(default_factory=dict)


class RuntimeStatus(BaseModel):
    """Runtime mount and adapter status."""

    modelMounted: bool = False
    modelEnabled: bool = False
    agentMounted: bool = False
    adapterInstances: list[dict[str, Any]] = Field(default_factory=list)
    requiresRestartAfterSave: bool = True


class ConfigWorkspace(BaseModel):
    """Full configuration workspace returned to the dashboard."""

    version: int = 1
    configPath: str = ""
    dataDir: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    validation: ConfigValidationResult = Field(default_factory=ConfigValidationResult)
    runtime: RuntimeStatus = Field(default_factory=RuntimeStatus)
    templates: dict[str, Any] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)
    providers: dict[str, Any] = Field(default_factory=dict)
    plugins: list[dict[str, Any]] = Field(default_factory=list)


class SaveConfigResult(BaseModel):
    """Result returned after saving configuration."""

    saved: bool = True
    requiresRestart: bool = True
    validation: ConfigValidationResult = Field(default_factory=ConfigValidationResult)
    workspace: ConfigWorkspace = Field(default_factory=ConfigWorkspace)


def _config_validation_result(
    *,
    config: dict[str, Any],
    bot: Any,
    boot: Any,
) -> dict[str, Any]:
    preflight = run_boot_preflight(
        config,
        data_dir=boot.data_dir,
        raise_on_error=False,
    )
    issues = [
        *_issue_payloads(preflight.issues, source="boot"),
        *_issue_payloads(
            validate_provider_configs(config, bot.config_provider_registry),
            source="provider",
        ),
    ]
    return {
        "valid": not issues,
        "issues": issues,
        "normalized": {
            "adapterInstances": [
                normalize_adapter_instance_record(item)
                for item in iter_adapter_instance_records(config)
            ],
            "bots": [asdict(item) for item in preflight.bot_service_configs],
        },
    }


def _issue_payloads(
    issues: tuple[ConfigValidationIssue, ...] | list[ConfigValidationIssue],
    *,
    source: str,
) -> list[dict[str, str]]:
    payloads: list[dict[str, str]] = []
    for issue in issues:
        payload = issue.to_dict()
        payload["source"] = source
        payloads.append(payload)
    return payloads


def _config_workspace(
    *,
    bot: Any,
    boot: Any,
) -> dict[str, Any]:
    config = deepcopy(boot.config)
    validation = _config_validation_result(config=config, bot=bot, boot=boot)
    model_runtime_required = _model_runtime_effectively_enabled(config, validation)
    return {
        "version": 1,
        "configPath": str(getattr(boot, "config_path", "")),
        "dataDir": str(getattr(boot, "data_dir", "")),
        "config": config,
        "validation": validation,
        "runtime": {
            "modelMounted": getattr(bot, "model_runtime", None) is not None,
            "modelEnabled": model_runtime_required,
            "agentMounted": getattr(bot, "agent_runtime", None) is not None,
            "adapterInstances": _adapter_instance_runtime_payload(bot=bot, config=config),
            "requiresRestartAfterSave": True,
        },
        "templates": _config_templates(),
        "options": _config_options(bot=bot),
        "providers": _provider_catalog(bot.config_provider_registry),
        "plugins": _plugin_catalog(bot),
    }


def _adapter_instance_runtime_payload(
    *,
    bot: Any,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    manager = getattr(bot, "adapter_manager", None)
    if manager is None:
        return []

    runtime_items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in iter_adapter_instance_records(config):
        instance_id = str(item.get("id") or "")
        if not instance_id:
            continue
        seen_ids.add(instance_id)
        runtime_items.append(
            {
                "id": instance_id,
                "running": bool(manager.is_running(instance_id)),
                "connected": bool(manager.is_connected(instance_id)),
                "available": bool(manager.is_available(instance_id)),
            }
        )
    for adapter in getattr(manager, "all_instances", []):
        if adapter.instance_id in seen_ids:
            continue
        runtime_items.append(
            {
                "id": adapter.instance_id,
                "running": bool(manager.is_running(adapter.instance_id)),
                "connected": bool(manager.is_connected(adapter.instance_id)),
                "available": bool(manager.is_available(adapter.instance_id)),
            }
        )
    return runtime_items

def _save_config_payload(
    *,
    bot: Any,
    boot: Any,
    next_config: dict[str, Any],
    validate_before_save: bool,
) -> dict[str, Any]:
    validation = _config_validation_result(config=next_config, bot=bot, boot=boot)
    if validate_before_save and not validation["valid"]:
        raise HTTPException(
            status_code=422,
            detail={
                "code": EC.CONFIG_VALIDATION_FAILED,
                "message": "Configuration validation failed",
                "issues": validation["issues"],
            },
        )

    boot.config.clear()
    boot.config.update(deepcopy(next_config))
    if not boot.save_config():
        raise HTTPException(
            status_code=500,
            detail={
                "code": EC.CONFIG_WRITE_FAILED,
                "message": "Failed to persist configuration",
            },
        )

    return ok(
        {
            "saved": True,
            "requiresRestart": True,
            "validation": validation,
            "workspace": _config_workspace(bot=bot, boot=boot),
        }
    )


def _provider_catalog(registry: ConfigProviderRegistry) -> dict[str, list[dict[str, Any]]]:
    return {
        "adapters": [
            _provider_payload(registry, provider)
            for provider in registry.list(ConfigProviderKind.ADAPTER)
        ],
        "plugins": [
            _provider_payload(registry, provider)
            for provider in registry.list(ConfigProviderKind.PLUGIN)
        ],
        "agents": [
            _provider_payload(registry, provider)
            for provider in registry.list(ConfigProviderKind.AGENT)
        ],
    }


def _provider_payload(
    registry: ConfigProviderRegistry,
    provider: ConfigProviderDefinition,
) -> dict[str, Any]:
    payload = provider.to_dict()
    payload["defaults"] = registry.default_config(provider.kind, provider.id)
    payload["schemaRef"] = f"/api/v1/config-providers/{provider.kind.value}/{provider.id}"
    payload["defaultsRef"] = (
        f"/api/v1/config-providers/{provider.kind.value}/{provider.id}/defaults"
    )
    payload["validateRef"] = (
        f"/api/v1/config-providers/{provider.kind.value}/{provider.id}/validate"
    )
    return payload


def _config_options(*, bot: Any) -> dict[str, Any]:
    adapter_provider_ids = {
        provider.id for provider in bot.config_provider_registry.list(ConfigProviderKind.ADAPTER)
    }
    return {
        "agentModes": ["none", "simple", "full"],
        "adapterPlatforms": sorted(
            set(getattr(bot.adapter_manager, "registered_platforms", [])) | adapter_provider_ids
        ),
        "pluginIds": sorted(meta.id for meta in bot.plugin_manager.all_plugins),
    }


def _plugin_catalog(bot: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for meta in sorted(bot.plugin_manager.all_plugins, key=lambda item: item.id):
        provider = bot.config_provider_registry.get(ConfigProviderKind.PLUGIN, meta.id)
        result.append(
            {
                "id": meta.id,
                "name": meta.name,
                "module": meta.module_path,
                "role": meta.role.value,
                "state": meta.state.value,
                "configurable": provider is not None,
                "schemaRef": (
                    f"/api/v1/config-providers/plugin/{meta.id}" if provider is not None else ""
                ),
            }
        )
    return result


def _config_templates() -> dict[str, Any]:
    return {
        "runtime": {"agent": True},
        "logging": {
            "level": "INFO",
            "third_party_noise": "off",
            "file": {
                "enabled": True,
                "path": "logs/shinbot.log",
                "when": "midnight",
                "interval": 1,
                "backup_count": 14,
                "max_bytes": 10485760,
            },
        },
        "database": {"url": "sqlite:///data/db/shinbot.sqlite3", "snapshot_ttl": 10800},
        "adapterInstance": {
            "id": "",
            "name": "",
            "adapter": "",
            "enabled": True,
            "config": {},
        },
        "plugin": {"id": "", "module": "", "enabled": True, "config": {}},
        "bot": {
            "id": "",
            "display_name": "",
            "enabled": True,
            "commands": {"enabled": True, "prefixes": ["/"]},
            "plugins": {
                "enabled": True,
                "enabled_plugins": ["*"],
                "disabled_plugins": [],
            },
            "agent": {"mode": "none", "config": ""},
            "bindings": [],
        },
        "botBinding": {
            "id": "",
            "adapter_instance_id": "",
            "session_patterns": ["group:*"],
            "enabled": True,
            "priority": 0,
        },
    }


def _model_runtime_effectively_enabled(
    config: dict[str, Any],
    validation: dict[str, Any],
) -> bool:
    runtime = config.get("runtime")
    if isinstance(runtime, dict):
        if runtime.get("model") is False:
            return False
        if runtime.get("model") is True:
            return True
        if runtime.get("agent") is False:
            return False

    for bot_config in validation["normalized"]["bots"]:
        if (
            bot_config.get("enabled") is not False
            and bot_config.get("agent", {}).get("mode") != "none"
        ):
            return True
    return False


@router.get("", response_model=Envelope[ConfigWorkspace])
async def get_config_workspace(bot=BotDep, boot=BootDep):
    """Return the full configuration workspace for the dashboard."""
    return ok(_config_workspace(bot=bot, boot=boot))


@router.post("/validate", response_model=Envelope[ConfigValidationResult])
async def validate_config(body: ValidateConfigRequest, bot=BotDep, boot=BootDep):
    """Validate a configuration payload without persisting it."""
    return ok(_config_validation_result(config=body.config, bot=bot, boot=boot))


@router.put("", response_model=Envelope[SaveConfigResult])
async def save_config(body: SaveConfigRequest, bot=BotDep, boot=BootDep):
    """Save the full configuration and return updated workspace."""
    return _save_config_payload(
        bot=bot,
        boot=boot,
        next_config=deepcopy(body.config),
        validate_before_save=body.validateBeforeSave,
    )


@router.put("/adapter-instances", response_model=Envelope[SaveConfigResult])
async def save_adapter_instances(
    body: SaveAdapterInstancesRequest,
    bot=BotDep,
    boot=BootDep,
):
    """Replace adapter instances in config and save."""
    next_config = deepcopy(boot.config)
    next_config["adapter_instances"] = deepcopy(body.adapterInstances)
    return _save_config_payload(
        bot=bot,
        boot=boot,
        next_config=next_config,
        validate_before_save=body.validateBeforeSave,
    )


@router.put("/bots", response_model=Envelope[SaveConfigResult])
async def save_bots(body: SaveBotsRequest, bot=BotDep, boot=BootDep):
    """Replace bot definitions in config and save."""
    next_config = deepcopy(boot.config)
    next_config["bots"] = deepcopy(body.bots)
    return _save_config_payload(
        bot=bot,
        boot=boot,
        next_config=next_config,
        validate_before_save=body.validateBeforeSave,
    )
