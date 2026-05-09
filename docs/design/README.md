# Design Docs

`docs/design/` 只放“某个子系统应该提供什么语义和能力”的长期规范，不放阶段性实现记录。

跨多个子系统的架构边界放在 `docs/architecture/`。例如 Agent 内部的 scheduler / coordinator / workflow / utils 分层，应以 `../architecture/agent_module_layers.md` 为准，而不是散落在 runtime 设计文档里。

## 目录分层

- `core/`
  - 核心交互模型、消息模型、资源模型。
  - 回答系统最底层的语义和流转方式。
- `runtime/`
  - 运行时机制，例如命令、会话、权限、启动生命周期。
- `extensibility/`
  - 插件、适配器和扩展能力边界。
- `interfaces/`
  - 对外界面的设计，包括 WebUI 与前后端通信。
- `governance/`
  - 术语、命名、文档级约束等治理性内容。

## 当前文件

### `core/`

- `core_philosophy.md`
- `message_workflow.md`
- `message_element_spec.md`
- `message_egress_spec.md`
- `resource_schema_spec.md`

### `runtime/`

- `agent_runtime_index.md` — Agent 运行时文档索引（阅读入口）。
- `active_chat_workflow.md` — Active Chat 双层触发模型、会话生命周期、兴趣衰减。
- `command_system.md`
- `session_management.md`
- `permission_system.md`
- `system_boot_lifecycle.md`
- `agent_model_runtime.md`
- `database_persistence_architecture.md`
- `prompt_registry.md`
- `prompt_registry_schema.md`
- `tool_registry_and_manager.md`
- `attention_driven_conversation_workflow.md`
- `media_semantics_and_meme_handling.md`
- `context_memory_architecture.md`

### `extensibility/`

- `plugin_system_design.md`
- `adapter_interface_spec.md`

### `interfaces/`

- `webui_design_spec.md`
- `api_communication_spec.md`
- `model_runtime_webui_spec.md`

### `governance/`

- `glossary.md`

## 新文档放置规则

- Agent 架构分层与模块边界：放 `../architecture/`
- Agent 内某个具体能力规格：放 `runtime/`
- 平台接入规范：放 `extensibility/`
- Dashboard 页面与交互：放 `interfaces/`
- 新的核心语义模型：放 `core/`
- 数据库、运行记录和存储边界：放 `runtime/`

## Agent 文档审计状态

以下文档写于早期 Agent 方案阶段，已完成审计：

- `runtime/attention_driven_conversation_workflow.md`
  - **状态**：部分现行。核心概念（SessionAttentionState、exponential decay、response profiles、tool-driven reply）已被 `scheduler/` 和 `active_chat/` 实现。SenderWeightState、Robust Interrupt 多因子累积等高级特性尚未实现。调度职责已迁移到 `scheduler/` + `active_chat/coordinator.py`，workflow 执行已迁移到 `workflow/`。
  - **保留原因**：仍可作为 attention 模型和 response profile 的设计参考。
- `runtime/media_semantics_and_meme_handling.md`
  - **状态**：现行。fingerprint/dedup、sticker vs image 分流、semantic cache、reanalysis 等核心设计均已实现于 `media/`。
  - **保留原因**：仍为媒体子系统的有效能力规格。
- `runtime/context_memory_architecture.md`
  - **状态**：部分现行。三级记忆模型（short/mid/long-term）、Block 投影分离、Prefix Cache 友好的前缀稳定原则仍为设计目标。实际实现中 `context/` 模块采用 ring buffer + alias table + projector 模式，与文档描述的 MemoryBlock/PromptBlock 分离尚未完全对齐。
  - **保留原因**：仍可作为上下文系统演进的设计参考。

跨模块分层约束以 `../architecture/agent_module_layers.md` 为准。
