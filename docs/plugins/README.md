# ShinBot 插件开发文档

本目录聚焦 **ShinBot 当前代码实现** 的插件开发能力，避免描述尚未落地的接口。

## 文档导航

### 入门
- [快速开始](./01_getting_started.md)
- [项目结构](./02_project_structure.md)
- [核心能力速览](./capabilities.md)

### 开发主题
- [命令系统](./03_commands.md)
- [事件系统](./04_events.md)
- [数据存储](./05_storage.md)
- [配置系统](./06_configuration.md)
- [本地化](./07_localization.md)
- [生命周期](./10_lifecycle.md)

### 参考与示例
- [API 参考](./16_api_reference.md)
- [示例代码](./17_examples.md)
- [常见问题](./18_faq.md)

## 这套文档基于哪些代码

以下文件是本文档的主要事实来源：

- `shinbot/core/plugins/context.py`
- `shinbot/core/plugins/manager.py`
- `shinbot/core/dispatch/command.py`
- `shinbot/core/dispatch/event_bus.py`
- `shinbot/core/dispatch/pipeline.py`
- `shinbot/core/plugins/config.py`

如果文档与源码不一致，以源码为准。

## 快速事实

- 插件入口必须提供 `setup(plg)`。
- 命令处理器当前签名是 `async def handler(bot, args: str)`。
- `@plg.on_message()` 等价于监听 `message-created`。
- 插件数据目录是 `data/plugin_data/<plugin_id>/`。
- `on_disable` 支持 0 或 1 个参数；`teardown` 当前按无参数调用。
- 通过 `metadata.json` 扫描加载时，运行态插件名/版本/作者/描述/角色来自 `metadata.json`。

## 推荐阅读顺序

1. [快速开始](./01_getting_started.md)
2. [项目结构](./02_project_structure.md)
3. [命令系统](./03_commands.md)
4. [事件系统](./04_events.md)
5. [API 参考](./16_api_reference.md)

开始开发：[快速开始 →](./01_getting_started.md)
