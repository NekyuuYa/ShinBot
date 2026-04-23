"""OneBot v11 Adapter built-in plugin."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from shinbot.core.plugins.context import Plugin
from shinbot.utils.resource_ingress import DEFAULT_MAX_RESOURCE_BYTES


class OneBotV11PluginConfig(BaseModel):
    mode: Literal["forward", "reverse"] = Field(
        default="forward",
        description="Connection method",
        json_schema_extra={
            "modes": ["forward", "reverse"],
            "enum_titles": ["Forward WebSocket", "Reverse WebSocket"],
        },
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
        json_schema_extra={"modes": ["reverse"], "ui_group": "advanced"},
    )
    self_id: str = Field(
        default="",
        description="Expected X-Self-ID for reverse connection validation",
        json_schema_extra={"modes": ["reverse"], "ui_group": "advanced"},
    )
    access_token: str = Field(default="", description="Access token for WS auth")
    reconnect_delay: float = Field(
        default=5.0,
        ge=0.0,
        description="Reconnect delay in seconds",
        json_schema_extra={"ui_group": "advanced"},
    )
    max_reconnects: int = Field(
        default=-1,
        description="Maximum reconnect attempts, -1 for infinite",
        json_schema_extra={"ui_group": "advanced"},
    )
    request_timeout: float = Field(
        default=20.0,
        gt=0.0,
        description="API request timeout in seconds",
        json_schema_extra={"ui_group": "advanced"},
    )
    forward_max_depth: int = Field(
        default=3,
        ge=0,
        description="Maximum nested forward-message expansion depth",
        json_schema_extra={"ui_group": "advanced"},
    )
    auto_download_media: bool = Field(
        default=True,
        description="Cache image and video resources before entering the pipeline",
        json_schema_extra={"ui_group": "advanced"},
    )
    download_file_resources: bool = Field(
        default=False,
        description="Download file attachments before entering the pipeline",
        json_schema_extra={"ui_group": "advanced"},
    )
    max_resource_bytes: int = Field(
        default=DEFAULT_MAX_RESOURCE_BYTES,
        gt=0,
        description="Maximum bytes allowed for one cached resource",
        json_schema_extra={"ui_group": "advanced"},
    )
    resource_cache_dir: str = Field(
        default="data/temp/resources",
        description="Local cache directory for downloaded resources",
        json_schema_extra={"ui_group": "advanced"},
    )
    silent_reconnect: bool = Field(
        default=True,
        description="Suppress frequent reconnect warnings",
        json_schema_extra={"ui_group": "advanced"},
    )
    reconnect_log_interval: float = Field(
        default=30.0,
        ge=1.0,
        description="Minimum seconds between reconnect warning logs",
        json_schema_extra={"ui_group": "advanced"},
    )


__plugin_config_class__ = OneBotV11PluginConfig
__plugin_adapter_platform__ = "onebot_v11"


def setup(plg: Plugin) -> None:
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
        auto_download_media: bool | None = None,
        download_file_resources: bool = False,
        max_resource_bytes: int = DEFAULT_MAX_RESOURCE_BYTES,
        download_resources: bool | None = None,
        resource_cache_dir: str = "data/temp/resources",
        silent_reconnect: bool = True,
        reconnect_log_interval: float = 30.0,
        **_: object,
    ) -> OneBotV11Adapter:
        resolved_auto_download_media = (
            auto_download_media
            if auto_download_media is not None
            else (download_resources if download_resources is not None else True)
        )
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
            auto_download_media=resolved_auto_download_media,
            download_file_resources=download_file_resources,
            max_resource_bytes=max_resource_bytes,
            resource_cache_dir=resource_cache_dir,
            silent_reconnect=silent_reconnect,
            reconnect_log_interval=reconnect_log_interval,
        )
        return OneBotV11Adapter(instance_id=instance_id, platform=platform, config=cfg)

    plg.register_adapter_factory("onebot_v11", _onebot_factory)


async def on_disable(plg: Plugin) -> None:
    from .adapter import _GATEWAY

    await _GATEWAY.shutdown_all()
    if plg._adapter_manager is not None:
        plg._adapter_manager.unregister_adapter("onebot_v11")
