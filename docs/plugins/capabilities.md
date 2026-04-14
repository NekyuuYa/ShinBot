# ShinBot 插件开发：核心能力 (Plugin Capabilities)

本文档面向插件开发者，介绍 ShinBot 提供的 SDK 能力与调用接口。

---

## 1. 操作句柄 (The Bot Handle)

在 ShinBot 的 Workflow 模型中，每一个处理函数都会接收到一个 `bot` 对象（即操作句柄）。它是插件与系统交互的唯一桥梁。

### 1.1 核心数据 (听)
- **`bot.event`**: 当前事件对象。
- **`bot.session`**: 当前会话。
- **`bot.user`**: 发送者信息。

### 1.2 高阶响应 (说)
- **`await bot.send(content)`**: 发送消息。
- **`await bot.reply(content)`**: 引用回复。

### 1.3 高阶管理 (管)
- **`await bot.kick(user_id)`**
- **`await bot.mute(user_id, duration)`**
- **`await bot.poke(user_id)`**

---

## 2. 日志记录 (Logging)

插件必须使用框架注入的 Logger，严禁使用原生 `print()`。

- **获取方式**: 在 `setup(ctx)` 中通过 `ctx.logger` 访问。
- **特点**: 日志会自动携带插件 ID，并实时同步至 Dashboard 和审计文件。
- **用法示例**:
  ```python
  ctx.logger.info("插件已加载")
  ctx.logger.error("操作失败: %s", err)
  ```

---

## 3. 持久化存储 (Storage)

- **会话持久化**: `ctx.session.plugin_data`。
- **通用存储**: `ctx.data_dir` (指向 `data/plugin_data/{id}/`)。
