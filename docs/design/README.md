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

- `00_core_philosophy.md`
- `01_message_workflow.md`
- `02_message_element_spec.md`
- `06_message_egress_spec.md`
- `17_resource_schema_spec.md`

### `runtime/`

- `03_command_system.md`
- `04_session_management.md`
- `05_permission_system.md`
- `12_system_boot_lifecycle.md`
- `18_agent_model_runtime.md`
- `19_database_persistence_architecture.md`
- `21_prompt_registry.md`
- `22_prompt_registry_schema.md`
- `23_tool_registry_and_manager.md`
- `24_attention_driven_conversation_workflow.md`
- `25_media_semantics_and_meme_handling.md`
- `26_context_memory_architecture.md`

### `extensibility/`

- `07_plugin_system_design.md`
- `09_adapter_interface_spec.md`

### `interfaces/`

- `13_webui_design_spec.md`
- `16_api_communication_spec.md`
- `20_model_runtime_webui_spec.md`

### `governance/`

- `10_glossary.md`

## 新文档放置规则

- Agent 架构分层与模块边界：放 `../architecture/`
- Agent 内某个具体能力规格：放 `runtime/`
- 平台接入规范：放 `extensibility/`
- Dashboard 页面与交互：放 `interfaces/`
- 新的核心语义模型：放 `core/`
- 数据库、运行记录和存储边界：放 `runtime/`

## 待审计文档

以下文档写于早期 Agent 方案阶段，仍有局部参考价值，但需要按当前 Agent 分层重新审计：

- `runtime/24_attention_driven_conversation_workflow.md`
- `runtime/26_context_memory_architecture.md`

审计完成后，如果内容仍是现行能力规格，则保留在 `design/runtime/`；如果只是历史方案，则移动到 `../archive/`；如果其中包含跨模块边界约束，则抽取到 `../architecture/`。
