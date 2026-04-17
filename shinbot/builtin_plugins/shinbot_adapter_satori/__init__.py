"""Satori Adapter built-in plugin.

Registers the Satori WebSocket adapter factory with the framework via the
plugin lifecycle hook.  The core engine never imports this module directly;
it is discovered and loaded by PluginManager through metadata.json.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from shinbot.core.plugins.context import Plugin


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
    download_resources: bool = Field(
        default=False, description="Download media resources to local temp cache"
    )
    resource_cache_dir: str = Field(
        default="data/temp/resources", description="Local cache directory for downloaded resources"
    )
    silent_reconnect: bool = Field(default=True, description="Suppress frequent reconnect warnings")
    reconnect_log_interval: float = Field(
        default=30.0, ge=1.0, description="Minimum seconds between reconnect warning logs"
    )


__plugin_config_class__ = SatoriPluginConfig


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
        download_resources: bool = False,
        resource_cache_dir: str = "data/temp/resources",
        silent_reconnect: bool = True,
        reconnect_log_interval: float = 30.0,
        **_: object,
    ) -> SatoriAdapter:
        """Construct a SatoriAdapter from flat keyword arguments.

        This factory is what boot.py (and the instances API) call with
        raw config dict kwargs, so neither has to import SatoriConfig.
        """
        cfg = SatoriConfig(
            host=host,
            token=token,
            path=path,
            reconnect_delay=reconnect_delay,
            max_reconnects=max_reconnects,
            download_resources=download_resources,
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
