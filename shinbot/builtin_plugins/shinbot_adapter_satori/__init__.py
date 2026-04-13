"""Satori Adapter built-in plugin.

Registers the Satori WebSocket adapter factory with the framework via the
plugin lifecycle hook.  The core engine never imports this module directly;
it is discovered and loaded by PluginManager through metadata.json.
"""

from __future__ import annotations

from shinbot.core.plugin import PluginContext

__plugin_name__ = "Satori Adapter (Official)"
__plugin_version__ = "1.0.0"
__plugin_author__ = "ShinBot Team"
__plugin_description__ = "The standard Satori protocol driver for ShinBot."

from shinbot.core.plugin import PluginRole

__plugin_role__ = PluginRole.ADAPTER


def setup(ctx: PluginContext) -> None:
    """Register the Satori adapter factory with the AdapterManager."""
    from .adapter import SatoriAdapter, SatoriConfig

    def _satori_factory(
        instance_id: str,
        platform: str,
        *,
        host: str = "localhost:5140",
        token: str = "",
        reconnect_delay: float = 5.0,
        **_: object,
    ) -> SatoriAdapter:
        """Construct a SatoriAdapter from flat keyword arguments.

        This factory is what boot.py (and the instances API) call with
        raw config dict kwargs, so neither has to import SatoriConfig.
        """
        cfg = SatoriConfig(host=host, token=token, reconnect_delay=reconnect_delay)
        return SatoriAdapter(instance_id=instance_id, platform=platform, config=cfg)

    ctx.register_adapter_factory("satori", _satori_factory)


async def on_disable(ctx: PluginContext) -> None:
    """Unregister the Satori factory on plugin unload / hot-reload."""
    if ctx._adapter_manager is not None:
        ctx._adapter_manager.unregister_adapter("satori")
