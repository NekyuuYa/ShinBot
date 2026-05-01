# 事件系统

ShinBot 的事件分发由两部分组成：

- `MessagePipeline`：决定消息是否走命令还是走事件
- `EventBus`：按优先级执行已注册处理器

## 1. 注册方式

```python
from shinbot.core.plugins.context import Plugin


def setup(plg: Plugin) -> None:
    @plg.on_event("message-created")
    async def on_message(bot) -> None:
        await bot.send(f"收到: {bot.text}")

    @plg.on_message()
    async def on_message_alias(bot) -> None:
        plg.logger.info("message-created alias")
```

`@plg.on_message()` 是 `@plg.on_event("message-created")` 的别名。

## 2. 处理器参数类型

这个点必须分清：

- 消息事件（`message-created` 等）收到的是 `MessageContext`
- 通知事件（非消息事件）收到的是 `UnifiedEvent`

示例（通知事件）：

```python
@plg.on_event("guild-member-added")
async def on_member_added(event) -> None:
    plg.logger.info("member added: %s", event.sender_id)
```

## 3. 优先级

`EventBus` 规则是“数字越小越先执行”，默认 `priority=100`。

```python
@plg.on_event("message-created", priority=10)
async def first(bot):
    ...


@plg.on_event("message-created", priority=200)
async def later(bot):
    ...
```

## 4. 通配监听

`"*"` 可以监听所有事件：

```python
@plg.on_event("*")
async def on_any(event_obj) -> None:
    if hasattr(event_obj, "event"):
        event_type = event_obj.event.type
    else:
        event_type = event_obj.type
    plg.logger.info("event=%s", event_type)
```

消息轨道与通知轨道都会进这里，因此参数类型可能不同。

## 5. 中断传播

抛出 `StopPropagation` 可以阻断后续低优先级处理器：

```python
from shinbot.core.dispatch.event_bus import StopPropagation


@plg.on_event("message-created", priority=10)
async def guard(bot):
    if bot.text.startswith("/internal"):
        raise StopPropagation()
```

## 6. 断路器机制

`EventBus` 内置了失败保护：

- 同一处理器连续失败 5 次后短暂熔断
- 熔断约 60 秒后会尝试半开恢复

这能防止异常处理器持续刷日志。

## 7. 与命令系统的关系

对 `message-created` 而言：

- 先做命令解析
- 命中命令就执行命令处理器，并 `return`
- 未命中命令才进入 `EventBus.emit(event.type, bot)`

所以你写了 `on_message()` 后，不会收到已命中的命令消息。

## 8. 与 Attention 调度的关系

消息事件处理器会在 attention scheduler 之前运行。处理器可以通过 `await bot.send(...)`
主动回复，也可以用 `bot.stop()` 表示后续 attention 不应继续接管；但处理器不应假设“收到
`message-created` 事件”就代表 Bot 一定会自动响应。

自然语言响应是否触发，仍由 response profile、attention 阈值、是否已由插件回复等条件决定。
如果某段逻辑必须等到 attention 确认接管后再执行，应接入后续专门的 post-attention 回调，而不是放在通用事件处理器里。

## 9. 返回值约定

事件处理器返回值不会参与后续流程；如果需要输出，请显式调用 `await bot.send(...)`。

下一步：阅读 [数据存储](./05_storage.md)。
