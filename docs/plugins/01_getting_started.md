# 快速开始：第一个可运行插件

本篇用最小可用示例，基于当前代码实现创建一个插件。

## 1. 创建目录

在 `data/plugins/` 下创建插件目录：

```text
data/plugins/hello_demo/
├── metadata.json
└── __init__.py
```

## 2. 编写 metadata.json

`PluginManager` 当前会强校验这几个点：

- `id`：非空字符串
- `entry`：相对路径，且文件存在于插件目录内
- `dependencies`：字符串列表（可选）

说明：`metadata.json` 里可以写 `name`、`version`、`author`、`description`、`permissions` 等字段，
通过 `metadata.json` 扫描加载时，插件运行态展示信息（`PluginMeta`）会直接使用这些字段。

示例：

```json
{
  "id": "hello_demo",
  "name": "Hello Demo",
  "version": "1.0.0",
  "author": "Your Name",
  "description": "Minimal hello plugin",
  "role": "logic",
  "entry": "__init__.py",
  "permissions": [],
  "dependencies": []
}
```

## 3. 编写入口代码

```python
from shinbot.core.plugins.context import Plugin


def setup(plg: Plugin) -> None:
    @plg.on_command("hello")
    async def hello(bot, args: str) -> None:
        # args 是命令名之后的原始文本
        name = args.strip() or "world"
        await bot.send(f"hello, {name}")

    plg.logger.info("hello_demo loaded")
```

关键点：

- `setup(plg)` 必须存在。
- 命令处理器签名是 `async def handler(bot, args: str)`。
- `bot` 是 `MessageContext`，用 `await bot.send(...)` 回复。
- 展示字段（`name`/`version`/`author`/`description`/`role`）建议统一在 `metadata.json` 中声明。

## 4. 让系统发现插件

启动 ShinBot 后，使用重扫接口（或 WebUI 的重扫按钮）：

```bash
curl -X POST http://localhost:3945/api/v1/plugins/rescan \
  -H "Authorization: Bearer <token>"
```

然后启用插件：

```bash
curl -X POST http://localhost:3945/api/v1/plugins/hello_demo/enable \
  -H "Authorization: Bearer <token>"
```

## 5. 测试命令

向机器人发送：

```text
/hello ShinBot
```

默认前缀来自会话配置，初始是 `/`。上面输入会触发命令并回复：

```text
hello, ShinBot
```

## 6. 你现在拿到了什么

- 一个可加载、可启用、可响应命令的插件
- 一个独立数据目录：`data/plugin_data/hello_demo/`
- 一个可扩展的 `Plugin` 入口

下一步建议阅读：

- [项目结构](./02_project_structure.md)
- [命令系统](./03_commands.md)
- [事件系统](./04_events.md)
