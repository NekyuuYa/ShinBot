# ShinBot Docs

当前文档按“长期规范”和“实现说明”分层维护，尽量避免把一次性报告、阶段总结、临时检查单长期留在主文档树里。

## 目录结构

- `design/`
  - 产品与系统设计规范。
  - 回答“系统应该是什么样”。
  - 内部再按 `core/`、`runtime/`、`extensibility/`、`interfaces/`、`governance/` 分层。
- `internals/`
  - 关键实现机制说明。
  - 回答“当前代码是怎么做的”。
  - `internals/parameters/` 记录当前仍然重要、但暂不直接暴露给用户修改的内部参数与阈值。
- `plugins/`
  - 面向插件开发者的能力说明。
- `architecture/`
  - 跨文档抽象出的长期架构原则。

## 推荐阅读顺序

1. `../README.md`
2. `architecture/design_principles.md`
3. `design/README.md`
4. `design/core/00_core_philosophy.md`
5. `design/core/01_message_workflow.md`
6. `design/runtime/24_attention_driven_conversation_workflow.md`
7. `design/runtime/25_media_semantics_and_meme_handling.md`
8. `design/extensibility/07_plugin_system_design.md`
9. `design/extensibility/09_adapter_interface_spec.md`
10. `design/interfaces/13_webui_design_spec.md`
11. `plugins/capabilities.md`

## 维护规则

- 设计规范写进 `design/`，不要混入“这次改了什么”的工作纪要。
- 实现细节写进 `internals/`，但只保留仍然能帮助后续开发的内容。
- 阶段性报告、完成总结、重构计划、代码清理报告，不再保留在主文档树。
- 涉及真实环境、密钥、临时测试群号的检查单，不应进入长期文档。
