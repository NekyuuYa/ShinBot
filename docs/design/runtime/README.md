# Runtime Design Docs

`docs/design/runtime/` 记录运行时子系统的长期能力规格。这里描述“运行时应该提供什么能力”，不记录一次性施工过程。

## 推荐入口

- `agent_runtime_index.md`
  - Agent 运行时文档索引，包含现行、部分现行和归档参考状态。

## 当前文档

- `active_chat_workflow.md`
- `agent_model_runtime.md`
- `attention_driven_conversation_workflow.md`
- `command_system.md`
- `database_persistence_architecture.md`
- `logging_observability.md`
- `media_semantics_and_meme_handling.md`
- `permission_system.md`
- `prompt_registry.md`
- `prompt_registry_schema.md`
- `session_management.md`
- `system_boot_lifecycle.md`
- `tool_registry_and_manager.md`

## 放置规则

- Agent、模型、工具、权限、会话、启动生命周期等运行时能力规格放在这里。
- Agent 内部跨模块分层边界放在 `../../architecture/`。
- 与源码结构强绑定的实现说明放在 `../../internals/`。
