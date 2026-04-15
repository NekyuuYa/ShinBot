"""OneBot v11 Adapter built-in plugin."""

from __future__ import annotations

from pydantic import BaseModel, Field

from shinbot.core.plugins.context import PluginContext
from shinbot.core.plugins.types import PluginRole

__plugin_name__ = "OneBot v11 Adapter (Official)"
__plugin_version__ = "1.0.0"
__plugin_author__ = "ShinBot Team"
__plugin_description__ = "OneBot v11 forward WebSocket bridge for ShinBot."
__plugin_role__ = PluginRole.ADAPTER
__plugin_adapter_platform__ = "onebot_v11"


class OneBotV11PluginConfig(BaseModel):
    mode: str = Field(
        default="forward",
        description="Connection mode",
        json_schema_extra={"modes": ["forward", "reverse"]},
    )
    url: str = Field(
        default="ws://127.0.0.1:3001",
        description="OneBot v11 forward WS URL",
        json_schema_extra={"modes": ["forward"]},
    )
    reverse_host: str = Field(
        default="0.0.0.0",
        description="Reverse WebSocket listener host",
        json_schema_extra={"modes": ["reverse"]},
    )
    reverse_port: int | None = Field(
        default=None,
        description="Reverse WebSocket listener port (None means shared management port)",
        json_schema_extra={"modes": ["reverse"]},
    )
    reverse_path: str = Field(
        default="/onebot/v11",
        description="Reverse WebSocket path or full ws://host:port/path URL",
        json_schema_extra={"modes": ["reverse"]},
    )
    self_id: str = Field(
        default="",
        description="Expected X-Self-ID for reverse connection validation",
        json_schema_extra={"modes": ["reverse"]},
    )
    access_token: str = Field(default="", description="Access token for WS auth")
    reconnect_delay: float = Field(default=5.0, ge=0.0, description="Reconnect delay in seconds")
    max_reconnects: int = Field(
        default=-1, description="Maximum reconnect attempts, -1 for infinite"
    )
    request_timeout: float = Field(
        default=20.0, gt=0.0, description="API request timeout in seconds"
    )
    forward_max_depth: int = Field(
        default=3,
        ge=0,
        description="Maximum nested forward-message expansion depth",
    )
    auto_download_media: bool = Field(
        default=False,
        description="Cache image/video/file resources to local temp before pipeline",
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


__plugin_config_class__ = OneBotV11PluginConfig


def setup(ctx: PluginContext) -> None:
    """Register the OneBot v11 adapter factory with the AdapterManager."""
    from .adapter import OneBotV11Adapter, OneBotV11Config

    def _onebot_factory(
        instance_id: str,
        platform: str,
        *,
        mode: str = "forward",
        url: str = "ws://127.0.0.1:3001",
        reverse_host: str = "0.0.0.0",
        reverse_port: int | None = None,
        reverse_path: str = "/onebot/v11",
        self_id: str = "",
        access_token: str = "",
        reconnect_delay: float = 5.0,
        max_reconnects: int = -1,
        request_timeout: float = 20.0,
        forward_max_depth: int = 3,
        auto_download_media: bool = False,
        download_resources: bool = False,
        resource_cache_dir: str = "data/temp/resources",
        silent_reconnect: bool = True,
        reconnect_log_interval: float = 30.0,
        **_: object,
    ) -> OneBotV11Adapter:
        cfg = OneBotV11Config(
            mode=mode,
            url=url,
            reverse_host=reverse_host,
            reverse_port=reverse_port,
            reverse_path=reverse_path,
            self_id=self_id,
            access_token=access_token,
            reconnect_delay=reconnect_delay,
            max_reconnects=max_reconnects,
            request_timeout=request_timeout,
            forward_max_depth=forward_max_depth,
            auto_download_media=auto_download_media,
            download_resources=download_resources,
            resource_cache_dir=resource_cache_dir,
            silent_reconnect=silent_reconnect,
            reconnect_log_interval=reconnect_log_interval,
        )
        return OneBotV11Adapter(instance_id=instance_id, platform=platform, config=cfg)

    ctx.register_adapter_factory("onebot_v11", _onebot_factory)


async def on_disable(ctx: PluginContext) -> None:
    from .adapter import _GATEWAY

    await _GATEWAY.shutdown_all()
    if ctx._adapter_manager is not None:
        ctx._adapter_manager.unregister_adapter("onebot_v11")
