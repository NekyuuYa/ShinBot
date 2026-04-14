# Design Docs

`docs/design/` 只放“系统应该怎样设计”的长期规范，不放阶段性实现记录。

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

- Agent 框架设计：优先放 `runtime/` 或 `extensibility/`
- 平台接入规范：放 `extensibility/`
- Dashboard 页面与交互：放 `interfaces/`
- 新的核心语义模型：放 `core/`
