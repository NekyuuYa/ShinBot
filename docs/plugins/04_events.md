# 事件系统

ShinBot 现在把“消息”和“非消息事件”分成两条入口：

- 消息事件（`message-created` 等）进入 `MessageIngress`，再由 `RouteTable` 分发给命令、关键词、自定义 route 或 `agent_entry`。
- 非消息 notice / lifecycle 信号进入 `EventBus`，按优先级执行 `@plg.on_event(...)` 处理器。

`@plg.on_event("message-*")` 和旧的 `@plg.on_message()` 已移除；消息处理请使用 `on_command`、`on_keyword` 或 `on_route`。

## 1. Notice 事件

```python
from shinbot.core.plugins.context import Plugin


def setup(plg: Plugin) -> None:
    @plg.on_event("guild-member-added")
    async def on_member_added(event) -> None:
        plg.logger.info("member added: %s", event.sender_id)
```

notice 处理器收到的是 `UnifiedEvent`。

## 2. 消息入口

常见文本触发优先用专用 registry：

```python
@plg.on_command("ping")
async def ping(bot, args: str) -> None:
    await bot.send("pong")


@plg.on_keyword("help")
async def help_keyword(bot, match) -> None:
    await bot.send("需要帮助的话可以输入 /help")
```

复杂条件才直接注册 route：

```python
from shinbot.core.dispatch.routing import RouteCondition, RouteMatchMode


@plg.on_route(
    RouteCondition(
        event_types=frozenset({"message-created"}),
        platforms=frozenset({"qq"}),
    ),
    match_mode=RouteMatchMode.OBSERVE,
)
async def qq_message_route(context, rule) -> None:
    bot = context.require_message_context()
    plg.logger.info("qq message: %s", bot.text)
```

route 处理器收到 `(RouteDispatchContext, RouteRule)`。需要消息上下文时调用 `context.require_message_context()`。
只做记录、审计或指标时使用 `OBSERVE`；如果这个 route 要真正消费消息并阻止 `agent_entry` fallback，再使用 `NORMAL` 或 `EXCLUSIVE`。

## 3. 优先级

`EventBus` 的优先级是“数字越小越先执行”，默认 `priority=100`。

```python
@plg.on_event("guild-member-added", priority=10)
async def first(event):
    ...


@plg.on_event("guild-member-added", priority=200)
async def later(event):
    ...
```

消息 route 的优先级由 `RouteTable` 决定，规则是“数字越大越先评估”，并通过注册顺序保持稳定。

## 4. 通配监听

`"*"` 可以监听所有进入 EventBus 的非消息事件：

```python
@plg.on_event("*")
async def on_any_notice(event) -> None:
    plg.logger.info("notice=%s", event.type)
```

消息事件不会进入这个通配监听。

## 5. 中断传播

EventBus 处理器可以抛出 `StopPropagation` 阻断后续低优先级处理器：

```python
from shinbot.core.dispatch.event_bus import StopPropagation


@plg.on_event("guild-member-added", priority=10)
async def guard(event):
    if event.sender_id == "blocked-user":
        raise StopPropagation()
```

## 6. 断路器机制

`EventBus` 内置失败保护：

- 同一处理器连续失败 5 次后短暂熔断
- 熔断约 60 秒后会尝试半开恢复

这能防止异常处理器持续刷日志。

## 7. 与 Agent 调度的关系

未被命令、关键词或自定义消费型 route 命中的用户消息，会进入 `agent_entry` fallback。`agent_entry` 只发出最小 Agent 入口信号；Agent 模块自行读取消息库并维护内部上下文和调度状态。

如果消息处理器需要阻止后续 Agent 接管，应使用消费型 route 设计，而不是把消息挂到 EventBus。

## 8. 返回值约定

事件和 route 处理器返回值不会参与后续流程；如果需要输出，请显式调用 `await bot.send(...)`。

下一步：阅读 [数据存储](./05_storage.md)。
