# ShinBot Docs

当前文档按“架构约束、领域设计、实现说明、开发者指南、外部参考”分层维护。

`docs/` 只保留长期有用的资料。一次性报告、阶段总结、临时检查单和施工笔记应留在 `.agent/` 或提交说明中，不进入主文档树。

## 目录结构

- `architecture/`
  - 当前有效的长期架构边界、模块分层和跨子系统原则。
  - 回答“系统为什么这样分层，以及各层不能越界做什么”。
- `design/`
  - 领域设计规范和能力规格。
  - 回答“某个子系统应该提供什么语义和能力”。
  - 内部再按 `core/`、`runtime/`、`extensibility/`、`interfaces/`、`governance/` 分层。
- `internals/`
  - 关键实现机制说明。
  - 回答“当前代码是怎么做的”。
  - `internals/parameters/` 记录当前仍然重要、但暂不直接暴露给用户修改的内部参数与阈值。
- `plugins/`
  - 面向插件开发者的能力说明。
- `dashboard/`
  - Dashboard / WebUI 相关补充说明。
- `references/`
  - 外部项目或协议资料镜像，仅作参考，不代表 ShinBot 当前设计。
- `archive/`
  - 已被新设计替代、但暂时仍有追溯价值的旧文档。

## 推荐阅读顺序

1. `../README.md`
2. `architecture/README.md`
3. `architecture/design_principles.md`
4. `architecture/agent_module_layers.md`
5. `architecture/agent_context_boundary.md`
6. `design/README.md`
7. `design/core/core_philosophy.md`
8. `design/core/message_workflow.md`
9. `design/runtime/agent_runtime_index.md`
10. `design/runtime/prompt_registry.md`
11. `design/runtime/tool_registry_and_manager.md`
12. `design/extensibility/plugin_system_design.md`
13. `plugins/capabilities.md`

## 维护规则

- 架构边界写进 `architecture/`，尤其是跨多个子系统的职责划分。
- 设计规范写进 `design/`，不要混入“这次改了什么”的工作纪要。
- 实现细节写进 `internals/`，但只保留仍然能帮助后续开发的内容。
- 面向插件作者的稳定接口写进 `plugins/`。
- 外部资料镜像写进 `references/`，不要混入 ShinBot 自己的设计结论。
- 被新架构替代但仍有参考价值的文档移入 `archive/`，并在文档顶部注明替代文档。
- 阶段性报告、完成总结、重构计划、代码清理报告，不再保留在主文档树。
- 涉及真实环境、密钥、临时测试群号的检查单，不应进入长期文档。

## 当前整理状态

- `architecture/agent_module_layers.md` 是 Agent 后续重构的当前有效分层依据。
- `architecture/agent_context_boundary.md` 是 Agent context 模块当前有效职责边界。
- `design/runtime/attention_driven_conversation_workflow.md` 等早期 Agent 文档仍有参考价值，但需要按新分层重新审计；在审计完成前，不应把其中内容视作全部现行实现约束。
- `archive/runtime/context_memory_architecture.md` 已归档；context 现行职责以 `architecture/agent_context_boundary.md` 为准。
