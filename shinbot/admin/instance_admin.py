"""Administrative helpers for instance management flows."""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

from shinbot.admin.instance_config_admin import serialize_instance_config
from shinbot.core.application.config_sections import (
    adapter_instance_store,
    append_adapter_instance_record,
    iter_adapter_instance_records,
    normalize_adapter_instance_record,
    replace_adapter_instance_record,
    set_adapter_instance_platform,
    timestamp_now,
)


@dataclass(slots=True)
class InstanceAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def persist_instance_record(boot: Any, record: dict[str, Any]) -> None:
    """Persist or replace an adapter instance record in the boot config.

    Args:
        boot: The application boot controller.
        record: The instance record dict to persist.
    """
    _section, instances = adapter_instance_store(boot.config, create=True)
    for index, item in enumerate(instances):
        if item.get("id") == record["id"]:
            replace_adapter_instance_record(boot.config, index, record)
            break
    else:
        append_adapter_instance_record(boot.config, record)


def find_instance_record(
    boot: Any, instance_id: str
) -> tuple[int, dict[str, Any]] | tuple[None, None]:
    """Find an adapter instance record by its ID.

    Args:
        boot: The application boot controller.
        instance_id: The instance identifier to search for.

    Returns:
        A tuple of (index, record) if found, or (None, None) otherwise.
    """
    _section, instances = adapter_instance_store(boot.config)
    for index, item in enumerate(instances):
        if not isinstance(item, dict):
            continue
        if item.get("id") == instance_id:
            return index, item
    return None, None


def resolve_instance_config(body: Any) -> dict[str, Any]:
    """Extract the config dict from a request body, defaulting to empty.

    Args:
        body: The request body object.

    Returns:
        The config dict, or an empty dict if not present.
    """
    if body.config:
        return dict(body.config)
    return {}


def runtime_config(adapter: Any) -> dict[str, Any]:
    """Extract a serialisable config dict from a running adapter.

    Args:
        adapter: The adapter instance.

    Returns:
        A dict representation of the adapter's config, or empty dict.
    """
    config = getattr(adapter, "config", None)
    if hasattr(config, "model_dump"):
        return config.model_dump()
    if is_dataclass(config):
        return asdict(config)
    if isinstance(config, dict):
        return dict(config)
    return {}


def serialize_instance_config_summary(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Build a condensed instance-config summary for API responses.

    Args:
        payload: The full instance config payload, or ``None``.

    Returns:
        A summary dict, or ``None`` if the input is ``None``.
    """
    if payload is None:
        return None
    serialized = serialize_instance_config(payload)
    return {
        "uuid": serialized["uuid"],
        "mainLlm": serialized["mainLlm"],
        "explicitPromptCacheEnabled": serialized["explicitPromptCacheEnabled"],
        "mediaInspectionLlm": serialized["mediaInspectionLlm"],
        "mediaInspectionPrompt": serialized["mediaInspectionPrompt"],
        "stickerSummaryLlm": serialized["stickerSummaryLlm"],
        "stickerSummaryPrompt": serialized["stickerSummaryPrompt"],
        "contextCompressionLlm": serialized["contextCompressionLlm"],
        "maxContextTokens": serialized["maxContextTokens"],
        "contextEvictRatio": serialized["contextEvictRatio"],
        "contextCompressionMaxChars": serialized["contextCompressionMaxChars"],
        "responseProfile": serialized["responseProfile"],
        "responseProfilePrivate": serialized["responseProfilePrivate"],
        "responseProfilePriority": serialized["responseProfilePriority"],
        "responseProfileGroup": serialized["responseProfileGroup"],
        "tags": serialized["tags"],
    }


def instance_config_by_instance_id(database: Any) -> dict[str, dict[str, Any]]:
    """Return a mapping of instance_id to instance-config record.

    Args:
        database: The application database handle.

    Returns:
        A dict keyed by instance ID.
    """
    return {item["instance_id"]: item for item in database.instance_configs.list()}


def serialize_instance_record(
    item: dict[str, Any],
    mgr: Any,
    instance_configs_by_instance_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Serialise a persisted adapter instance record for the API.

    Args:
        item: Raw adapter instance record from config.
        mgr: The adapter manager for runtime status lookups.
        instance_configs_by_instance_id: Mapping of instance IDs to configs.

    Returns:
        A serialised instance dict.
    """
    normalized = normalize_adapter_instance_record(item)
    instance_id = normalized["id"]
    adapter = mgr.get_instance(instance_id) if instance_id else None
    running = bool(instance_id and adapter is not None and mgr.is_running(instance_id))
    connected = bool(instance_id and adapter is not None and mgr.is_connected(instance_id))
    available = bool(instance_id and adapter is not None and mgr.is_available(instance_id))
    status = "running" if running else "stopped"
    config = normalized["config"]
    if not config and adapter is not None:
        config = runtime_config(adapter)

    return {
        "id": instance_id,
        "name": normalized["name"],
        "adapter": normalized["adapter"],
        "status": status,
        "running": running,
        "connected": connected,
        "available": available,
        "config": config,
        "instanceConfig": serialize_instance_config_summary(
            instance_configs_by_instance_id.get(str(instance_id))
        ),
        "createdAt": normalized["createdAt"],
        "lastModified": normalized["lastModified"],
    }


def serialize_runtime_instance(
    adapter: Any,
    mgr: Any,
    instance_configs_by_instance_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Serialise a live adapter that has no persisted config record.

    Args:
        adapter: The running adapter instance.
        mgr: The adapter manager.
        instance_configs_by_instance_id: Mapping of instance IDs to configs.

    Returns:
        A serialised instance dict with zeroed timestamps.
    """
    running = bool(mgr.is_running(adapter.instance_id))
    connected = bool(mgr.is_connected(adapter.instance_id))
    available = bool(mgr.is_available(adapter.instance_id))
    return {
        "id": adapter.instance_id,
        "name": adapter.instance_id,
        "adapter": adapter.platform,
        "status": "running" if running else "stopped",
        "running": running,
        "connected": connected,
        "available": available,
        "config": runtime_config(adapter),
        "instanceConfig": serialize_instance_config_summary(
            instance_configs_by_instance_id.get(adapter.instance_id)
        ),
        "createdAt": 0,
        "lastModified": 0,
    }


def list_instance_payloads(*, bot: Any, boot: Any) -> list[dict[str, Any]]:
    """Build the full list of instance payloads for the API.

    Combines persisted config records with live adapter instances.

    Args:
        bot: The running application.
        boot: The application boot controller.

    Returns:
        A list of serialised instance dicts.
    """
    mgr = bot.adapter_manager
    instance_configs = instance_config_by_instance_id(bot.database)
    records: list[dict[str, Any]] = []

    for item in iter_adapter_instance_records(boot.config):
        records.append(serialize_instance_record(item, mgr, instance_configs))

    seen_ids = {item["id"] for item in records}
    for adapter in mgr.all_instances:
        if adapter.instance_id in seen_ids:
            continue
        records.append(serialize_runtime_instance(adapter, mgr, instance_configs))

    return records


def validate_new_instance(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    platform: str,
) -> None:
    """Validate that a new instance can be created.

    Args:
        bot: The running application.
        boot: The application boot controller.
        instance_id: The desired instance ID.
        platform: The platform type for the adapter.

    Raises:
        InstanceAdminError: If the ID is empty, already exists, or the
            platform is not registered.
    """
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
    """Construct a new adapter instance record dict.

    Args:
        instance_id: The instance identifier.
        name: Human-readable name.
        platform: The adapter platform type.
        config: Adapter configuration dict.
        created_at: Optional creation timestamp; defaults to now.

    Returns:
        A new instance record dict.
    """
    now = created_at if created_at is not None else timestamp_now()
    return {
        "id": instance_id,
        "name": name or instance_id,
        "adapter": platform,
        "config": config,
        "createdAt": now,
        "lastModified": now,
    }


def get_instance_for_update(
    *, bot: Any, boot: Any, instance_id: str
) -> tuple[Any | None, int | None, dict[str, Any] | None]:
    """Retrieve the adapter, config index, and record for an update operation.

    Args:
        bot: The running application.
        boot: The application boot controller.
        instance_id: The instance identifier.

    Returns:
        A tuple of (adapter_or_None, index_or_None, record_or_None).
    """
    adapter = bot.adapter_manager.get_instance(instance_id)
    index, inst = find_instance_record(boot, instance_id)
    return adapter, index, inst


def ensure_persisted_instance_record(
    *, boot: Any, instance_id: str, adapter: Any
) -> tuple[int, dict[str, Any]]:
    """Create and persist an instance record for a runtime-only adapter.

    Args:
        boot: The application boot controller.
        instance_id: The instance identifier.
        adapter: The live adapter instance.

    Returns:
        A tuple of (index, record) after persistence.
    """
    inst = build_instance_record(
        instance_id=instance_id,
        name=instance_id,
        platform=adapter.platform,
        config=runtime_config(adapter),
        created_at=0,
    )
    return append_adapter_instance_record(boot.config, inst)


def apply_instance_patch(*, inst: dict[str, Any], body: Any) -> dict[str, Any]:
    """Apply an API request patch to an instance record in-place.

    Args:
        inst: The instance record to modify.
        body: The API request body with optional fields.

    Returns:
        The config patch dict that was applied.
    """
    if body.name is not None:
        inst["name"] = body.name
    adapter = body.adapter
    if adapter is None:
        adapter = getattr(body, "adapterType", None)
    if adapter is not None:
        set_adapter_instance_platform(inst, adapter)

    config_patch = resolve_instance_config(body)
    if config_patch:
        inst_config = inst.setdefault("config", {})
        inst_config.update(config_patch)

    inst["lastModified"] = timestamp_now()
    return config_patch


def apply_runtime_config_patch(*, adapter: Any, config_patch: dict[str, Any]) -> None:
    """Push a config patch into a running adapter's config object.

    Args:
        adapter: The running adapter instance.
        config_patch: Dict of config keys/values to apply.
    """
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
    """Retrieve a running adapter or raise a 404 error.

    Args:
        mgr: The adapter manager.
        instance_id: The instance identifier.

    Returns:
        The adapter instance.

    Raises:
        InstanceAdminError: If the instance is not found.
    """
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
    """Validate, create, and persist a new adapter instance.

    Args:
        bot: The running application.
        boot: The application boot controller.
        instance_id: The desired instance ID.
        platform: The adapter platform type.
        name: Human-readable name.
        config: Adapter configuration dict.

    Returns:
        The persisted instance record.

    Raises:
        InstanceAdminError: On validation failure or duplicate ID.
    """
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
    """Update an existing adapter instance's persisted config and runtime state.

    Args:
        bot: The running application.
        boot: The application boot controller.
        instance_id: The instance identifier.
        body: The API request body with update fields.

    Returns:
        The updated instance record.

    Raises:
        InstanceAdminError: If the instance is not found.
    """
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
    inst = replace_adapter_instance_record(boot.config, index, inst)

    if adapter is not None:
        apply_runtime_config_patch(adapter=adapter, config_patch=config_patch)

    return inst


async def delete_instance_runtime(*, bot: Any, boot: Any, instance_id: str) -> None:
    """Stop and remove an adapter instance from runtime and persisted config.

    Args:
        bot: The running application.
        boot: The application boot controller.
        instance_id: The instance identifier to delete.

    Raises:
        InstanceAdminError: If the instance is not found.
    """
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
        _section, instances = adapter_instance_store(boot.config)
        del instances[index]


async def control_instance_runtime(*, mgr: Any, instance_id: str, action: str) -> str:
    """Start or stop an adapter instance.

    Args:
        mgr: The adapter manager.
        instance_id: The instance identifier.
        action: Either ``"start"`` or ``"stop"``.

    Returns:
        The resulting status string (``"running"`` or ``"stopped"``).

    Raises:
        InstanceAdminError: If the instance is already in the requested state.
    """
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
