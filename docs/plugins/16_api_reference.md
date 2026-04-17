# API 参考

本页只记录当前代码中的有效接口。

## PluginContext

初始化时传入 `setup(ctx)`。

### 属性

```python
ctx.plugin_id: str
ctx.data_dir: Path
ctx.logger
```

### 命令注册

```python
ctx.on_command(
    name: str,
    *,
    aliases: list[str] | None = None,
    description: str = "",
    usage: str = "",
    permission: str = "",
    mode: CommandMode = CommandMode.DELEGATED,
    priority: CommandPriority = CommandPriority.P0_PREFIX,
    pattern: str | None = None,
) -> Callable
```

### 事件注册

```python
ctx.on_event(event_type: str, *, priority: int = 100) -> Callable
ctx.on_message(*, priority: int = 100) -> Callable
```

`on_message` 等价于监听 `message-created`。

### 工具注册

```python
ctx.tool(
    *,
    name: str,
    description: str,
    input_schema: dict[str, Any],
    display_name: str = "",
    output_schema: dict[str, Any] | None = None,
    permission: str = "",
    enabled: bool = True,
    visibility: ToolVisibility = ToolVisibility.SCOPED,
    timeout_seconds: float = 30.0,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Callable
```

工具处理器签名（同步/异步均可）：

```python
def handler(arguments: dict, runtime: ToolExecutionContext):
    ...

# or

async def handler(arguments: dict, runtime: ToolExecutionContext):
    ...
```

### 适配器工厂注册

```python
ctx.register_adapter_factory(name: str, factory: Callable) -> None
```

## MessageContext（命令/消息处理器的 bot）

### 常用字段

```python
bot.event               # UnifiedEvent
bot.message             # Message
bot.session             # Session
bot.adapter             # BaseAdapter
bot.permissions         # set[str]
bot.command_match       # CommandMatch | None
```

### 便捷属性

```python
bot.text: str
bot.elements: list[MessageElement]
bot.user_id: str
bot.session_id: str
bot.platform: str
bot.is_private: bool
bot.elapsed_ms: float
bot.is_stopped: bool
```

### 方法

```python
await bot.send(content)
await bot.reply(content)
await bot.kick(user_id, guild_id=None)
await bot.mute(user_id, duration, guild_id=None)
await bot.poke(user_id)
await bot.approve_friend(message_id)
await bot.get_member_list(guild_id=None)
await bot.set_group_name(name, guild_id=None)
await bot.delete_msg(message_id)

bot.has_permission(permission) -> bool
bot.stop() -> None
bot.mark_trigger_read() -> None
await bot.wait_for_input(prompt="", timeout=60.0) -> str
```

## 命令相关类型

### CommandMode

```python
CommandMode.DELEGATED
CommandMode.MANAGED
```

### CommandPriority

```python
CommandPriority.P0_PREFIX
CommandPriority.P1_EXACT
CommandPriority.P2_REGEX
```

### CommandMatch

```python
match.command
match.priority
match.raw_args
match.regex_match
```

## UnifiedEvent 关键字段

```python
event.type
event.self_id
event.platform
event.timestamp
event.user
event.operator
event.member
event.channel
event.guild
event.message
```

便捷属性：

```python
event.is_message_event
event.is_notice_event
event.is_private
event.sender_id
event.operator_id
event.channel_id
event.guild_id
event.message_content
```

## Message / MessageElement

### Message

```python
Message.from_text(text)
Message.from_elements(*elements)
Message.from_xml(xml)
message.get_text()
message.to_xml()
```

### MessageElement 构造

```python
MessageElement.text(...)
MessageElement.at(...)
MessageElement.sharp(...)
MessageElement.img(...)
MessageElement.emoji(...)
MessageElement.quote(...)
MessageElement.audio(...)
MessageElement.video(...)
MessageElement.file(...)
MessageElement.br()
```

## ToolVisibility

```python
ToolVisibility.PRIVATE
ToolVisibility.SCOPED
ToolVisibility.PUBLIC
```

## 生命周期钩子

```python
def setup(ctx): ...                       # 必需
def on_enable(): ...                      # 可选
def on_enable(ctx): ...                   # 可选
def on_disable(): ...                     # 可选
def on_disable(ctx): ...                  # 可选
def teardown(): ...                       # 可选，当前无参数调用
```

返回 [文档首页](./README.md)
