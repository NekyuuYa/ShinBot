"""Satori Adapter built-in plugin.

Registers the Satori WebSocket adapter factory with the framework via the
plugin lifecycle hook.  The core engine never imports this module directly;
it is discovered and loaded by PluginManager through metadata.json.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from shinbot.core.plugins.plugin import PluginContext

__plugin_name__ = "Satori Adapter (Official)"
__plugin_version__ = "1.0.0"
__plugin_author__ = "ShinBot Team"
__plugin_description__ = "The standard Satori protocol driver for ShinBot."
__plugin_adapter_platform__ = "satori"
__plugin_locales__ = {
    "zh-CN": {
        "meta.name": "Satori 适配器（官方）",
        "meta.description": "ShinBot 标准 Satori 协议适配器。",
        "config.title": "Satori 连接配置",
        "config.description": "配置 Satori 网关连接与资源缓存行为。",
        "config.fields.host.label": "网关地址",
        "config.fields.host.description": "Satori 网关地址，例如 localhost:5140",
        "config.fields.token.label": "鉴权令牌",
        "config.fields.token.description": "连接 Satori 网关时使用的令牌",
        "config.fields.path.label": "事件路径",
        "config.fields.path.description": "WebSocket 事件订阅路径",
        "config.fields.reconnect_delay.label": "重连间隔",
        "config.fields.reconnect_delay.description": "断线后的重连间隔秒数",
        "config.fields.max_reconnects.label": "最大重连次数",
        "config.fields.max_reconnects.description": "最大重连次数，-1 表示无限重试",
        "config.fields.download_resources.label": "下载资源",
        "config.fields.download_resources.description": "将媒体资源下载到本地临时缓存",
        "config.fields.resource_cache_dir.label": "资源缓存目录",
        "config.fields.resource_cache_dir.description": "下载资源的本地缓存目录",
        "config.fields.silent_reconnect.label": "静默重连日志",
        "config.fields.silent_reconnect.description": "抑制高频重连告警日志",
        "config.fields.reconnect_log_interval.label": "重连日志间隔",
        "config.fields.reconnect_log_interval.description": "两次重连告警日志之间的最小秒数",
    },
    "en-US": {
        "meta.name": "Satori Adapter (Official)",
        "meta.description": "The standard Satori protocol driver for ShinBot.",
        "config.title": "Satori Connection Settings",
        "config.description": "Configure Satori gateway connectivity and local resource caching.",
        "config.fields.host.label": "Gateway Host",
        "config.fields.host.description": "Satori gateway host, for example localhost:5140",
        "config.fields.token.label": "Auth Token",
        "config.fields.token.description": "Authorization token used to connect to the Satori gateway",
        "config.fields.path.label": "Events Path",
        "config.fields.path.description": "WebSocket path used to subscribe to events",
        "config.fields.reconnect_delay.label": "Reconnect Delay",
        "config.fields.reconnect_delay.description": "Delay in seconds before reconnecting",
        "config.fields.max_reconnects.label": "Max Reconnects",
        "config.fields.max_reconnects.description": "Maximum reconnect attempts, -1 means unlimited",
        "config.fields.download_resources.label": "Download Resources",
        "config.fields.download_resources.description": "Download media resources into the local temp cache",
        "config.fields.resource_cache_dir.label": "Resource Cache Directory",
        "config.fields.resource_cache_dir.description": "Local cache directory for downloaded resources",
        "config.fields.silent_reconnect.label": "Silent Reconnect Logs",
        "config.fields.silent_reconnect.description": "Suppress noisy reconnect warning logs",
        "config.fields.reconnect_log_interval.label": "Reconnect Log Interval",
        "config.fields.reconnect_log_interval.description": "Minimum seconds between reconnect warning logs",
    },
}

from shinbot.core.plugins.plugin import PluginRole

__plugin_role__ = PluginRole.ADAPTER


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


def setup(ctx: PluginContext) -> None:
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

    ctx.register_adapter_factory("satori", _satori_factory)


async def on_disable(ctx: PluginContext) -> None:
    """Unregister the Satori factory on plugin unload / hot-reload."""
    if ctx._adapter_manager is not None:
        ctx._adapter_manager.unregister_adapter("satori")
