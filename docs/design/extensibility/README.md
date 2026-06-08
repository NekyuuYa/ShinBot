# Extensibility Design Docs

`docs/design/extensibility/` 记录 ShinBot 对外扩展面的长期设计，包括插件系统和平台适配器。

## 当前文档

- `plugin_system_design.md`
  - 插件系统职责、扩展能力和生命周期边界。
- `adapter_interface_spec.md`
  - 平台适配器接口、能力发现、内部 API 命名空间和语义补全策略。

## 放置规则

- 插件、adapter、driver、外部协议桥接的能力规格放在这里。
- 面向插件作者的具体开发指南放在 `../../plugins/`。
- 平台协议镜像资料放在 `../../references/`。
