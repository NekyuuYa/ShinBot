# 数据存储

ShinBot 不强制插件使用某一种数据库，但有几条明确边界。

## 1. 插件专属数据目录

每个插件都会分配独立目录：

```text
data/plugin_data/<plugin_id>/
```

在代码中通过 `plg.data_dir` 使用：

```python
def setup(plg):
    plg.logger.info("data_dir=%s", plg.data_dir)
```

该目录由 `PluginManager` 在构建插件上下文时自动创建。

## 2. 会话级状态

命令/消息处理器拿到的 `bot.session`（`Session`）包含：

- `session.state`
- `session.plugin_data`

示例：

```python
@plg.on_command("counter")
async def counter(bot, args: str) -> None:
    current = int(bot.session.plugin_data.get("counter", 0))
    current += 1
    bot.session.plugin_data["counter"] = current
    await bot.send(f"count={current}")
```

在正常命令路径和事件路径结束时，管线会调用 `SessionManager.update(session)` 持久化状态。

## 3. 文件存储建议

### 小规模数据

```python
import json


def load_json(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
```

### 较重 I/O

避免阻塞事件循环，使用 `asyncio.to_thread()`：

```python
import asyncio


@plg.on_command("save")
async def save(bot, args: str) -> None:
    target = plg.data_dir / "notes.txt"
    await asyncio.to_thread(target.write_text, args, encoding="utf-8")
    await bot.send("saved")
```

## 4. 框架自身会持久化什么

由框架维护，不需要插件重复写入：

- 会话对象（`SessionManager`）
- 消息日志（`message_logs`，用户消息和助手消息）
- 审计日志（若启用）

## 5. 实践建议

- 插件业务数据放在 `plg.data_dir`
- 避免写入插件代码目录
- 高频更新建议用 SQLite
- 多协程共享写文件时加锁，避免覆盖

下一步：阅读 [配置系统](./06_configuration.md)。
