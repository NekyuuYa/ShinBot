# ShinBot 插件开发：核心能力 (Plugin Capabilities)

本文档面向插件开发者，介绍 ShinBot 提供的 SDK 能力与调用接口。

---

## 1. 显式响应机制 (Explicit Actions)

在 ShinBot 的 Workflow 模型中，响应通过 `MessageContext` 的方法主动触发。

### 1.1 `await ctx.send(content)`
向当前会话发送消息。`content` 可为 XML 字符串或 `Message` 对象。

### 1.2 `await ctx.reply(content)`
引用回复发送者的消息。

### 1.3 `await ctx.wait_for_input(prompt)`
挂起当前逻辑，等待用户发送下一条消息作为输入。

---

## 2. 平台资源管理 (Standard APIs)

插件可以通过 `ctx.adapter` 调用 Satori 规范的标准化 API，实现跨平台管理。

### 2.1 成员与频道管理
```python
# 禁言某人
await ctx.adapter.call_api("member.mute", {
    "guild_id": ctx.event.guild_id,
    "user_id": "12345",
    "duration": 600
})

# 修改频道名称
await ctx.adapter.call_api("channel.update", {"name": "新频道名"})
```

### 2.2 消息操作
```python
# 撤回某条消息
await ctx.adapter.call_api("message.delete", {"message_id": "999"})

# 获取历史记录
history = await ctx.adapter.call_api("message.list", {"channel_id": "..."})
```

---

## 3. 跨平台交互元素 (Interactive Elements)

虽然 Satori 尚未完全标准化所有交互，ShinBot 建议使用以下扩展标签：
- **戳一戳**: `<sb:poke id="user_id" />`
- **点赞**: `<sb:like id="user_id" count="10" />`

---

## 4. 持久化存储 (Storage)

- **`ctx.session.plugin_data`**: 绑定会话的存储。
- **`ctx.user.plugin_data`**: 绑定用户的跨会话存储。
- **`ctx.get_db()`**: 数据库连接。
