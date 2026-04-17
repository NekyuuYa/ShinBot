# 示例代码

以下示例均按当前实现编写。

## 示例 1：最小命令插件

```python
from shinbot.core.plugins.context import Plugin


def setup(plg: Plugin) -> None:
    @plg.on_command("hello")
    async def hello(bot, args: str) -> None:
        name = args.strip() or "world"
        await bot.send(f"hello, {name}")
```

## 示例 2：正则命令

```python
from shinbot.core.dispatch.command import CommandPriority


def setup(plg):
    @plg.on_command(
        "roll",
        priority=CommandPriority.P2_REGEX,
        pattern=r"^(\d+)d(\d+)$",
    )
    async def roll(bot, args: str) -> None:
        match = bot.command_match.regex_match if bot.command_match else None
        if not match:
            return
        n = int(match.group(1))
        sides = int(match.group(2))
        await bot.send(f"rolled {n}d{sides}")
```

## 示例 3：消息事件与通知事件

```python
def setup(plg):
    @plg.on_message()
    async def on_message(bot) -> None:
        if "ping" in bot.text:
            await bot.reply("pong")

    @plg.on_event("guild-member-added")
    async def on_member_added(event) -> None:
        plg.logger.info("member added: %s", event.sender_id)
```

## 示例 4：多轮输入

```python
import asyncio


def setup(plg):
    @plg.on_command("ask")
    async def ask(bot, args: str) -> None:
        try:
            name = await bot.wait_for_input("你的名字是？", timeout=30)
        except asyncio.TimeoutError:
            await bot.send("超时，已取消")
            return

        await bot.send(f"你好，{name}")
```

## 示例 5：注册工具

```python
def setup(plg):
    @plg.tool(
        name="weather_query",
        description="Query weather by city",
        input_schema={
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
        permission="tools.weather.query",
    )
    async def weather_query(arguments, runtime):
        city = arguments["city"]
        return {
            "city": city,
            "weather": "sunny",
            "caller": runtime.caller,
        }
```

工具处理器也可以是同步函数；`ToolManager` 会统一处理同步/异步返回值。

## 示例 6：适配器插件骨架

```python
from shinbot.core.plugins.context import Plugin
from shinbot.core.plugins.types import PluginRole

__plugin_role__ = PluginRole.ADAPTER
__plugin_adapter_platform__ = "demo"


def setup(plg: Plugin) -> None:
    def factory(instance_id: str, platform: str, **kwargs):
        # 返回 BaseAdapter 子类实例
        raise NotImplementedError

    plg.register_adapter_factory("demo", factory)
```

## 示例 7：声明配置模型与本地化

```python
from pydantic import BaseModel, Field


class DemoConfig(BaseModel):
    api_key: str = Field(default="", description="API key")


__plugin_config_class__ = DemoConfig
__plugin_locales__ = {
    "zh-CN": {
        "meta.name": "演示插件",
        "meta.description": "示例",
        "config.title": "演示配置",
        "config.fields.api_key.label": "接口密钥",
    }
}
```

更多字段和签名见 [API 参考](./16_api_reference.md)。
