"""Satori Adapter built-in plugin.

Registers the Satori WebSocket adapter factory with the framework via the
plugin lifecycle hook.  The core engine never imports this module directly;
it is discovered and loaded by PluginManager through metadata.json.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from shinbot.core.plugins.context import Plugin
from shinbot.utils.resource_ingress import DEFAULT_MAX_RESOURCE_BYTES


class SatoriPluginConfig(BaseModel):
    host: str = Field(
        default="localhost:5140", description="Satori gateway host, e.g. localhost:5140"
    )
    token: str = Field(default="", description="Authorization token")
    path: str = Field(default="/v1/events", description="WebSocket events path")
    reconnect_delay: float = Field(default=5.0, ge=0.0, description="Reconnect delay in seconds")
    max_reconnects: int = Field(
        default=-1, description="Maximum reconnect attempts, -1 for infinite"
    )
    auto_download_media: bool = Field(
        default=True, description="Cache image and video resources to local temp cache"
    )
    download_file_resources: bool = Field(
        default=False, description="Download file attachments to local temp cache"
    )
    max_resource_bytes: int = Field(
        default=DEFAULT_MAX_RESOURCE_BYTES,
        gt=0,
        description="Maximum bytes allowed for one cached resource",
    )
    resource_cache_dir: str = Field(
        default="data/temp/resources", description="Local cache directory for downloaded resources"
    )
    silent_reconnect: bool = Field(default=True, description="Suppress frequent reconnect warnings")
    reconnect_log_interval: float = Field(
        default=30.0, ge=1.0, description="Minimum seconds between reconnect warning logs"
    )


__plugin_config_class__ = SatoriPluginConfig
__plugin_adapter_platform__ = "satori"


def setup(plg: Plugin) -> None:
    """Register the Satori adapter factory with the AdapterManager."""
    from .adapter import SatoriAdapter, SatoriConfig

    def _satori_factory(
        instance_id: str,
        platform: str,
        *,
        host: str = "localhost:5140",
        token: str = "",
        path: str = "/v1/events",
        reconnect_delay: float = 5.0,
        max_reconnects: int = -1,
        auto_download_media: bool | None = None,
        download_file_resources: bool = False,
        max_resource_bytes: int = DEFAULT_MAX_RESOURCE_BYTES,
        download_resources: bool | None = None,
        resource_cache_dir: str = "data/temp/resources",
        silent_reconnect: bool = True,
        reconnect_log_interval: float = 30.0,
        **_: object,
    ) -> SatoriAdapter:
        """Construct a SatoriAdapter from flat keyword arguments.

        This factory is what boot.py (and the instances API) call with
        raw config dict kwargs, so neither has to import SatoriConfig.
        """
        resolved_auto_download_media = (
            auto_download_media
            if auto_download_media is not None
            else (download_resources if download_resources is not None else True)
        )
        cfg = SatoriConfig(
            host=host,
            token=token,
            path=path,
            reconnect_delay=reconnect_delay,
            max_reconnects=max_reconnects,
            auto_download_media=resolved_auto_download_media,
            download_file_resources=download_file_resources,
            max_resource_bytes=max_resource_bytes,
            resource_cache_dir=resource_cache_dir,
            silent_reconnect=silent_reconnect,
            reconnect_log_interval=reconnect_log_interval,
        )
        return SatoriAdapter(instance_id=instance_id, platform=platform, config=cfg)

    plg.register_adapter_factory("satori", _satori_factory)


async def on_disable(plg: Plugin) -> None:
    """Unregister the Satori factory on plugin unload / hot-reload."""
    if plg._adapter_manager is not None:
        plg._adapter_manager.unregister_adapter("satori")
