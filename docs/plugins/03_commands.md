# 命令系统

ShinBot 的命令注册入口在 `Plugin.on_command()`，运行时解析由 `CommandRegistry` + `text_command_dispatcher` 完成。该 dispatcher 由 `MessageIngress` 通过 `RouteTable` 触发。

## 1. 基础注册

```python
from shinbot.core.plugins.context import Plugin


def setup(plg: Plugin) -> None:
    @plg.on_command("ping")
    async def ping(bot, args: str) -> None:
        await bot.send(f"pong: {args}" if args else "pong")
```

当前命令处理器实际签名：

```python
async def handler(bot, args: str) -> None:
    ...
```

`args` 的语义取决于命中类型：

- P0 前缀命令：命令词之后的原始文本
- P1 精确命令：空字符串
- P2 正则命令：整条原始文本

## 2. 触发优先级

`CommandRegistry.resolve()` 按以下顺序匹配：

1. `P0_PREFIX`：前缀命令（默认）
2. `P1_EXACT`：整句精确匹配
3. `P2_REGEX`：正则匹配

可用枚举值：

- `CommandPriority.P0_PREFIX`
- `CommandPriority.P1_EXACT`
- `CommandPriority.P2_REGEX`

## 3. 前缀命令（默认）

```python
from shinbot.core.dispatch.command import CommandPriority


@plg.on_command("echo", aliases=["say"], priority=CommandPriority.P0_PREFIX)
async def echo(bot, args: str) -> None:
    await bot.send(args or "(empty)")
```

- 会话默认前缀是 `/`。
- 输入 `/echo hello` 时，`args == "hello"`。

## 4. 精确匹配命令

```python
from shinbot.core.dispatch.command import CommandPriority


@plg.on_command("菜单", priority=CommandPriority.P1_EXACT)
async def menu(bot, args: str) -> None:
    await bot.send("显示菜单")
```

只有整句等于 `菜单` 才会触发，`菜单 更多` 不会触发。

## 5. 正则命令

```python
from shinbot.core.dispatch.command import CommandPriority


@plg.on_command(
    "dice",
    priority=CommandPriority.P2_REGEX,
    pattern=r"^(\d+)d(\d+)$",
)
async def dice(bot, args: str) -> None:
    match = bot.command_match.regex_match if bot.command_match else None
    if not match:
        return
    n = int(match.group(1))
    sides = int(match.group(2))
    await bot.send(f"roll {n}d{sides}")
```

注意：当前实现不会把正则分组自动作为函数参数传入，需要从 `bot.command_match.regex_match` 自取。

## 6. 权限控制

```python
@plg.on_command("admin", permission="admin.secret")
async def admin_only(bot, args: str) -> None:
    await bot.send("ok")
```

权限不足时，命令 dispatcher 会直接回复：`权限不足：需要 <permission>`，并且不会进入处理器。

## 7. 其他参数

`on_command` 还支持：

- `description`
- `usage`
- `aliases`
- `mode`

其中 `CommandMode` 目前有 `DELEGATED` 和 `MANAGED` 两个枚举值，但当前命令执行路径尚未按两种模式分流，通常按普通命令理解即可。

## 8. 常见误区

- 误区：处理器写成 `async def handler(bot)`。
  - 现状：命令 dispatcher 会传两个参数，少一个会报错。
- 误区：`pattern` 会自动提取参数到函数签名。
  - 现状：不会自动注入；需要手动读 `regex_match`。
- 误区：命令命中后 `@plg.on_event("message-created")` 仍会收到消息。
  - 现状：消息事件不再走 EventBus；命令是 `EXCLUSIVE` route，命中后也不会进入 Agent fallback。

## 9. 未命中时的行为

当前实现没有内置“未知命令”回复。命令未命中时会继续走后续消息路由，例如关键词、自定义 route 或 `agent_entry` fallback。

下一步：阅读 [事件系统](./04_events.md)。
