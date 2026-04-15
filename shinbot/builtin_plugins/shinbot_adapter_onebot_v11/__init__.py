"""OneBot v11 Adapter built-in plugin."""

from __future__ import annotations

from pydantic import BaseModel, Field

from shinbot.core.plugins.plugin import PluginContext, PluginRole

__plugin_name__ = "OneBot v11 Adapter (Official)"
__plugin_version__ = "1.0.0"
__plugin_author__ = "ShinBot Team"
__plugin_description__ = "OneBot v11 forward WebSocket bridge for ShinBot."
__plugin_role__ = PluginRole.ADAPTER
__plugin_adapter_platform__ = "onebot_v11"
__plugin_locales__ = {
    "zh-CN": {
        "meta.name": "OneBot v11 适配器（官方）",
        "meta.description": "ShinBot 的 OneBot v11 WebSocket 桥接适配器。",
        "config.title": "OneBot v11 连接配置",
        "config.description": "配置 OneBot v11 的正向或反向 WebSocket 接入方式。",
        "config.fields.mode.label": "连接模式",
        "config.fields.mode.description": "选择正向或反向连接模式",
        "config.fields.url.label": "正向连接地址",
        "config.fields.url.description": "OneBot v11 正向 WebSocket 地址",
        "config.fields.reverse_host.label": "反向监听主机",
        "config.fields.reverse_host.description": "反向 WebSocket 监听地址",
        "config.fields.reverse_port.label": "反向监听端口",
        "config.fields.reverse_port.description": "反向 WebSocket 监听端口，空则复用管理端口",
        "config.fields.reverse_path.label": "反向路径",
        "config.fields.reverse_path.description": "反向 WebSocket 路径或完整 ws:// 地址",
        "config.fields.self_id.label": "预期 Self ID",
        "config.fields.self_id.description": "用于校验反向连接的 X-Self-ID",
        "config.fields.access_token.label": "访问令牌",
        "config.fields.access_token.description": "WebSocket 鉴权访问令牌",
        "config.fields.reconnect_delay.label": "重连间隔",
        "config.fields.reconnect_delay.description": "断线后的重连间隔秒数",
        "config.fields.max_reconnects.label": "最大重连次数",
        "config.fields.max_reconnects.description": "最大重连次数，-1 表示无限重试",
        "config.fields.request_timeout.label": "请求超时",
        "config.fields.request_timeout.description": "API 请求超时秒数",
        "config.fields.forward_max_depth.label": "转发展开深度",
        "config.fields.forward_max_depth.description": "转发消息的最大嵌套展开深度",
        "config.fields.auto_download_media.label": "自动下载媒体",
        "config.fields.auto_download_media.description": "进入消息管线前缓存图片/视频/文件资源",
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
        "meta.name": "OneBot v11 Adapter (Official)",
        "meta.description": "OneBot v11 forward WebSocket bridge for ShinBot.",
        "config.title": "OneBot v11 Connection Settings",
        "config.description": "Configure forward or reverse WebSocket access for OneBot v11.",
        "config.fields.mode.label": "Connection Mode",
        "config.fields.mode.description": "Choose forward or reverse connection mode",
        "config.fields.url.label": "Forward URL",
        "config.fields.url.description": "OneBot v11 forward WebSocket URL",
        "config.fields.reverse_host.label": "Reverse Host",
        "config.fields.reverse_host.description": "Reverse WebSocket listener host",
        "config.fields.reverse_port.label": "Reverse Port",
        "config.fields.reverse_port.description": "Reverse WebSocket listener port, empty means shared management port",
        "config.fields.reverse_path.label": "Reverse Path",
        "config.fields.reverse_path.description": "Reverse WebSocket path or full ws://host:port/path URL",
        "config.fields.self_id.label": "Expected Self ID",
        "config.fields.self_id.description": "Expected X-Self-ID used to validate reverse connections",
        "config.fields.access_token.label": "Access Token",
        "config.fields.access_token.description": "Access token for WebSocket authentication",
        "config.fields.reconnect_delay.label": "Reconnect Delay",
        "config.fields.reconnect_delay.description": "Delay in seconds before reconnecting",
        "config.fields.max_reconnects.label": "Max Reconnects",
        "config.fields.max_reconnects.description": "Maximum reconnect attempts, -1 means unlimited",
        "config.fields.request_timeout.label": "Request Timeout",
        "config.fields.request_timeout.description": "Timeout in seconds for API requests",
        "config.fields.forward_max_depth.label": "Forward Expansion Depth",
        "config.fields.forward_max_depth.description": "Maximum nested expansion depth for forwarded messages",
        "config.fields.auto_download_media.label": "Auto Download Media",
        "config.fields.auto_download_media.description": "Cache image, video, and file resources before entering the pipeline",
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
