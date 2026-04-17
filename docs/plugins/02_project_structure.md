# 项目结构与命名

本文档描述 ShinBot 当前插件加载器真正依赖的文件结构。

## 最小可用结构

```text
data/plugins/my_plugin/
├── metadata.json
└── __init__.py
```

只要 `metadata.json` 合法且入口文件存在，就可以被扫描加载。

## 推荐结构

```text
data/plugins/my_plugin/
├── metadata.json
├── __init__.py
├── commands.py
├── events.py
├── services/
│   └── ...
├── locales/
│   ├── zh-CN.json
│   └── en-US.json
└── README.md
```

## metadata.json 当前校验规则

`PluginManager._validate_metadata()` 当前会验证：

1. `metadata.json` 必须是 JSON 对象。
2. `id` 必须是非空字符串。
3. `entry` 必须是非空相对路径。
4. `entry` 不能是绝对路径，不能包含 `..`。
5. `entry` 解析后必须仍在插件目录内，并且文件存在。
6. `dependencies`（如果提供）必须是字符串列表。
7. `name`、`version`、`author`、`description`（如果提供）必须是字符串。
8. `role`（如果提供）必须是 `logic` 或 `adapter`。

示例：

```json
{
    "id": "my_plugin",
    "name": "My Plugin",
    "version": "1.0.0",
    "author": "Your Name",
    "description": "Example plugin",
    "entry": "__init__.py",
    "permissions": ["cmd.my_plugin"],
    "dependencies": ["base_plugin"]
}
```

注意：通过 `metadata.json` 扫描加载时，`name`、`version`、`author`、`description` 会直接写入运行态 `PluginMeta`。
仅在直接调用 `load_plugin(plugin_id, module_path)`（不经过 metadata 扫描）时，才会回退到模块变量 `__plugin_*__`。

## 命名说明

- 用户插件目录当前不强制前缀。
- 内置插件目录（`shinbot/builtin_plugins/`）会强制：
    - `shinbot_plugin_`
    - `shinbot_adapter_`
    - `shinbot_debug_`

建议用户插件也保持前缀风格，便于团队识别。

## 依赖加载顺序

- `dependencies` 使用简单字符串列表。
- 加载器会做拓扑排序，尽量保证依赖先加载。
- 若依赖不存在，会记录 warning，但不一定阻止插件继续尝试加载。

## 角色字段说明

通过 `metadata.json` 扫描加载时：

- `metadata.json` 的 `role` 是运行态角色的最终来源。
- 未声明时默认为 `logic`。

如果你是直接调用 `load_plugin(plugin_id, module_path)` 加载插件（不走 metadata 扫描），
则角色会回退读取模块变量 `__plugin_role__`（默认 `PluginRole.LOGIC`）。

## 入口文件建议

在 `__init__.py` 里只保留注册逻辑，具体实现拆分到独立模块：

```python
from shinbot.core.plugins.context import Plugin
from .commands import register_commands
from .events import register_events


def setup(plg: Plugin) -> None:
    register_commands(plg)
    register_events(plg)
```

下一步：阅读 [命令系统](./03_commands.md)。
