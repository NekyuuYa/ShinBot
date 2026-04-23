"""QQ Official Bot adapter built-in plugin."""

from __future__ import annotations

from pydantic import BaseModel, Field

from shinbot.core.plugins.context import Plugin
from shinbot.utils.resource_ingress import DEFAULT_MAX_RESOURCE_BYTES

DEFAULT_QQOFFICIAL_INTENTS = (1 << 9) | (1 << 12) | (1 << 25) | (1 << 30)


class QQOfficialPluginConfig(BaseModel):
    app_id: str = Field(default="", description="QQ bot AppID")
    app_secret: str = Field(default="", description="QQ bot AppSecret")
    intents: int = Field(
        default=DEFAULT_QQOFFICIAL_INTENTS,
        ge=0,
        description="Gateway intent bitmask",
    )
    sandbox: bool = Field(
        default=False,
        description="Use QQ sandbox API domain",
        json_schema_extra={"ui_group": "advanced"},
    )
    api_base: str = Field(
        default="",
        description="Override OpenAPI base URL (empty means official default)",
        json_schema_extra={"ui_group": "advanced"},
    )
    token_base: str = Field(
        default="https://bots.qq.com",
        description="Token service base URL",
        json_schema_extra={"ui_group": "advanced"},
    )
    ws_url: str = Field(
        default="",
        description="Override gateway WebSocket URL (empty means auto-discovery)",
        json_schema_extra={"ui_group": "advanced"},
    )
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
        description="HTTP request timeout in seconds",
        json_schema_extra={"ui_group": "advanced"},
    )
    heartbeat_jitter: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Randomized heartbeat jitter ratio",
        json_schema_extra={"ui_group": "advanced"},
    )
    auto_download_media: bool = Field(
        default=True,
        description="Cache image and video resources to local temp cache",
        json_schema_extra={"ui_group": "advanced"},
    )
    download_file_resources: bool = Field(
        default=False,
        description="Download file attachments to local temp cache",
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


__plugin_config_class__ = QQOfficialPluginConfig
__plugin_adapter_platform__ = "qqofficial"


def setup(plg: Plugin) -> None:
    """Register QQ Official adapter factories with the AdapterManager."""
    from .adapter import QQOfficialAdapter, QQOfficialConfig

    def _qqofficial_factory(
        instance_id: str,
        platform: str,
        *,
        app_id: str = "",
        app_secret: str = "",
        intents: int = DEFAULT_QQOFFICIAL_INTENTS,
        sandbox: bool = False,
        api_base: str = "",
        token_base: str = "https://bots.qq.com",
        ws_url: str = "",
        reconnect_delay: float = 5.0,
        max_reconnects: int = -1,
        request_timeout: float = 20.0,
        heartbeat_jitter: float = 0.05,
        auto_download_media: bool | None = None,
        download_file_resources: bool = False,
        max_resource_bytes: int = DEFAULT_MAX_RESOURCE_BYTES,
        download_resources: bool | None = None,
        resource_cache_dir: str = "data/temp/resources",
        **_: object,
    ) -> QQOfficialAdapter:
        resolved_auto_download_media = (
            auto_download_media
            if auto_download_media is not None
            else (download_resources if download_resources is not None else True)
        )
        cfg = QQOfficialConfig(
            app_id=app_id,
            app_secret=app_secret,
            intents=intents,
            sandbox=sandbox,
            api_base=api_base,
            token_base=token_base,
            ws_url=ws_url,
            reconnect_delay=reconnect_delay,
            max_reconnects=max_reconnects,
            request_timeout=request_timeout,
            heartbeat_jitter=heartbeat_jitter,
            auto_download_media=resolved_auto_download_media,
            download_file_resources=download_file_resources,
            max_resource_bytes=max_resource_bytes,
            resource_cache_dir=resource_cache_dir,
        )
        return QQOfficialAdapter(instance_id=instance_id, platform=platform, config=cfg)

    # Register both spellings to make migration easier from existing configs.
    plg.register_adapter_factory("qqofficial", _qqofficial_factory)
    plg.register_adapter_factory("qq_official", _qqofficial_factory)


async def on_disable(plg: Plugin) -> None:
    if plg._adapter_manager is not None:
        plg._adapter_manager.unregister_adapter("qqofficial")
        plg._adapter_manager.unregister_adapter("qq_official")
