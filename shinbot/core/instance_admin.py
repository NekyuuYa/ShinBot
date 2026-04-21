"""Administrative helpers for instance management flows."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

from shinbot.core.bot_config_admin import serialize_bot_config


@dataclass(slots=True)
class InstanceAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def persist_instance_record(boot: Any, record: dict[str, Any]) -> None:
    instances = boot.config.setdefault("instances", [])
    for index, item in enumerate(instances):
        if item.get("id") == record["id"]:
            instances[index] = record
            break
    else:
        instances.append(record)


def find_instance_record(
    boot: Any, instance_id: str
) -> tuple[int, dict[str, Any]] | tuple[None, None]:
    for index, item in enumerate(boot.config.get("instances", [])):
        if item.get("id") == instance_id:
            return index, item
    return None, None


def resolve_instance_config(body: Any) -> dict[str, Any]:
    if body.config:
        return dict(body.config)
    return {}


def runtime_config(adapter: Any) -> dict[str, Any]:
    config = getattr(adapter, "config", None)
    if hasattr(config, "model_dump"):
        return config.model_dump()
    if is_dataclass(config):
        return asdict(config)
    if isinstance(config, dict):
        return dict(config)
    return {}


def serialize_bot_config_summary(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    serialized = serialize_bot_config(payload)
    return {
        "uuid": serialized["uuid"],
        "defaultAgentUuid": serialized["defaultAgentUuid"],
        "mainLlm": serialized["mainLlm"],
        "mediaInspectionLlm": serialized["mediaInspectionLlm"],
        "responseProfile": serialized["responseProfile"],
        "responseProfilePrivate": serialized["responseProfilePrivate"],
        "responseProfilePriority": serialized["responseProfilePriority"],
        "responseProfileGroup": serialized["responseProfileGroup"],
        "tags": serialized["tags"],
    }


def bot_config_by_instance_id(database: Any) -> dict[str, dict[str, Any]]:
    return {item["instance_id"]: item for item in database.bot_configs.list()}


def serialize_instance_record(
    item: dict[str, Any],
    mgr: Any,
    bot_configs_by_instance_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    instance_id = item.get("id")
    adapter = mgr.get_instance(instance_id) if instance_id else None
    status = (
        "running"
        if instance_id and adapter is not None and mgr.is_running(instance_id)
        else "stopped"
    )
    config = item.get("config", {})
    if not config and adapter is not None:
        config = runtime_config(adapter)

    return {
        "id": instance_id,
        "name": item.get("name", instance_id),
        "adapterType": item.get("adapterType", getattr(adapter, "platform", "satori")),
        "status": status,
        "config": config,
        "botConfig": serialize_bot_config_summary(bot_configs_by_instance_id.get(str(instance_id))),
        "createdAt": item.get("createdAt", 0),
        "lastModified": item.get("lastModified", 0),
    }


def serialize_runtime_instance(
    adapter: Any,
    mgr: Any,
    bot_configs_by_instance_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "id": adapter.instance_id,
        "name": adapter.instance_id,
        "adapterType": adapter.platform,
        "status": "running" if mgr.is_running(adapter.instance_id) else "stopped",
        "config": runtime_config(adapter),
        "botConfig": serialize_bot_config_summary(
            bot_configs_by_instance_id.get(adapter.instance_id)
        ),
        "createdAt": 0,
        "lastModified": 0,
    }


def list_instance_payloads(*, bot: Any, boot: Any) -> list[dict[str, Any]]:
    mgr = bot.adapter_manager
    bot_configs = bot_config_by_instance_id(bot.database)
    records: list[dict[str, Any]] = []

    for item in boot.config.get("instances", []):
        records.append(serialize_instance_record(item, mgr, bot_configs))

    seen_ids = {item["id"] for item in records}
    for adapter in mgr.all_instances:
        if adapter.instance_id in seen_ids:
            continue
        records.append(serialize_runtime_instance(adapter, mgr, bot_configs))

    return records


def validate_new_instance(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    platform: str,
) -> None:
    if not instance_id:
        raise InstanceAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Instance id/name is required",
        )
    if (
        bot.adapter_manager.get_instance(instance_id) is not None
        or find_instance_record(boot, instance_id)[1] is not None
    ):
        raise InstanceAdminError(
            status_code=409,
            code="INSTANCE_ALREADY_EXISTS",
            message=f"Instance {instance_id!r} already exists",
        )
    if platform not in bot.adapter_manager.registered_platforms:
        raise InstanceAdminError(
            status_code=400,
            code="UNSUPPORTED_PLATFORM",
            message=f"Platform {platform!r} is not registered",
        )


def build_instance_record(
    *,
    instance_id: str,
    name: str,
    platform: str,
    config: dict[str, Any],
    created_at: int | None = None,
) -> dict[str, Any]:
    now = created_at if created_at is not None else int(time.time())
    return {
        "id": instance_id,
        "name": name or instance_id,
        "adapterType": platform,
        "platform": platform,
        "config": config,
        "createdAt": now,
        "lastModified": now,
    }


def get_instance_for_update(
    *, bot: Any, boot: Any, instance_id: str
) -> tuple[Any | None, int | None, dict[str, Any] | None]:
    adapter = bot.adapter_manager.get_instance(instance_id)
    index, inst = find_instance_record(boot, instance_id)
    return adapter, index, inst


def ensure_persisted_instance_record(
    *, boot: Any, instance_id: str, adapter: Any
) -> tuple[int, dict[str, Any]]:
    inst = build_instance_record(
        instance_id=instance_id,
        name=instance_id,
        platform=adapter.platform,
        config=runtime_config(adapter),
        created_at=0,
    )
    boot.config.setdefault("instances", []).append(inst)
    index = len(boot.config["instances"]) - 1
    return index, inst


def apply_instance_patch(*, inst: dict[str, Any], body: Any) -> dict[str, Any]:
    if body.name is not None:
        inst["name"] = body.name
    if body.adapterType is not None:
        inst["adapterType"] = body.adapterType
        inst["platform"] = body.adapterType

    config_patch = resolve_instance_config(body)
    if config_patch:
        inst_config = inst.setdefault("config", {})
        inst_config.update(config_patch)

    inst["lastModified"] = int(time.time())
    return config_patch


def apply_runtime_config_patch(*, adapter: Any, config_patch: dict[str, Any]) -> None:
    if not config_patch or not hasattr(adapter, "config"):
        return
    if hasattr(adapter.config, "model_copy"):
        adapter.config = adapter.config.model_copy(update=config_patch)
        return
    if is_dataclass(adapter.config):
        for key, value in config_patch.items():
            if hasattr(adapter.config, key):
                setattr(adapter.config, key, value)


def get_runtime_instance_or_raise(mgr: Any, instance_id: str) -> Any:
    adapter = mgr.get_instance(instance_id)
    if adapter is None:
        raise InstanceAdminError(
            status_code=404,
            code="INSTANCE_NOT_FOUND",
            message=f"Instance {instance_id!r} not found",
        )
    return adapter


def create_instance_runtime(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    platform: str,
    name: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    validate_new_instance(
        bot=bot,
        boot=boot,
        instance_id=instance_id,
        platform=platform,
    )
    bot.add_adapter(
        instance_id=instance_id,
        platform=platform,
        **config,
    )
    record = build_instance_record(
        instance_id=instance_id,
        name=name,
        platform=platform,
        config=config,
    )
    persist_instance_record(boot, record)
    return record


def update_instance_runtime(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    body: Any,
) -> dict[str, Any]:
    adapter, index, inst = get_instance_for_update(bot=bot, boot=boot, instance_id=instance_id)
    if adapter is None and inst is None:
        raise InstanceAdminError(
            status_code=404,
            code="INSTANCE_NOT_FOUND",
            message=f"Instance {instance_id!r} not found",
        )

    if inst is None and adapter is not None:
        index, inst = ensure_persisted_instance_record(
            boot=boot,
            instance_id=instance_id,
            adapter=adapter,
        )

    assert inst is not None
    assert index is not None

    config_patch = apply_instance_patch(inst=inst, body=body)
    boot.config["instances"][index] = inst

    if adapter is not None:
        apply_runtime_config_patch(adapter=adapter, config_patch=config_patch)

    return inst


async def delete_instance_runtime(*, bot: Any, boot: Any, instance_id: str) -> None:
    mgr = bot.adapter_manager
    adapter = mgr.get_instance(instance_id)
    index, _inst = find_instance_record(boot, instance_id)

    if adapter is None and index is None:
        raise InstanceAdminError(
            status_code=404,
            code="INSTANCE_NOT_FOUND",
            message=f"Instance {instance_id!r} not found",
        )

    if adapter is not None:
        await mgr.delete_instance(instance_id)

    if index is not None:
        del boot.config["instances"][index]


async def control_instance_runtime(*, mgr: Any, instance_id: str, action: str) -> str:
    get_runtime_instance_or_raise(mgr, instance_id)

    if action == "start":
        if mgr.is_running(instance_id):
            raise InstanceAdminError(
                status_code=409,
                code="INSTANCE_ALREADY_RUNNING",
                message=f"Instance {instance_id!r} is already running",
            )
        await mgr.start_instance(instance_id)
        return "running"

    if not mgr.is_running(instance_id):
        raise InstanceAdminError(
            status_code=409,
            code="INSTANCE_NOT_RUNNING",
            message=f"Instance {instance_id!r} is not running",
        )
    await mgr.stop_instance(instance_id)
    return "stopped"
