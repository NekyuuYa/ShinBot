# Agent 运行时文档索引

本文件列出 Agent 子系统当前有效的设计文档、实现说明和历史参考。

## 架构分层（最高优先阅读）

- `../../architecture/agent_module_layers.md` — Agent Scheduler / Coordinator / Workflow / Utils / Runtime 分层约束与命名边界。

## 设计规范

| 文档 | 状态 | 说明 |
|------|------|------|
| `prompt_registry.md` | **现行** | PromptRegistry 七阶段装配协议。所有模型调用必须通过 PromptRegistry 组装 messages。 |
| `prompt_registry_schema.md` | **现行** | PromptRegistry 数据结构定义（PromptComponent、PromptAssemblyRequest、PromptSnapshot 等）。 |
| `tool_registry_and_manager.md` | **现行** | ToolRegistry + ToolManager 统一注册、宿主解耦、权限治理。 |
| `agent_model_runtime.md` | **现行** | ModelRuntime 统一接入层：Provider → Model → Route → Execution Runtime 四层架构。Model 接入独立于 Agent，可服务非 Agent 调用。 |
| `active_chat_workflow.md` | **现行** | Active Chat 双层触发模型（Interest + Attention）、会话生命周期、semantic wait、batch 处理。核心设计已实现于 `active_chat/` + `scheduler/`。 |
| `media_semantics_and_meme_handling.md` | **现行** | 媒体 fingerprint/dedup、sticker vs image 分流、semantic cache、reanalysis。核心设计已实现于 `media/`。 |
| `attention_driven_conversation_workflow.md` | **部分现行** | 核心概念（SessionAttentionState、exponential decay、response profiles、tool-driven reply）已实现。SenderWeightState、Robust Interrupt 多因子累积等高级特性尚未实现。调度职责已迁移到 `scheduler/` + `active_chat/coordinator.py`。 |
| `context_memory_architecture.md` | **部分现行** | 三级记忆模型（short/mid/long-term）和 Prefix Cache 友好原则仍为设计目标。实际实现采用 ring buffer + alias table + projector 模式，与 MemoryBlock/PromptBlock 分离尚未完全对齐。 |

## 实现说明

| 文档 | 说明 |
|------|------|
| `../../internals/workflow_engine.md` | 消息入口（MessageIngress）、路由表（RouteTable）、Agent 边界说明。 |
| `../../internals/parameters/context_management.md` | 上下文管理当前参数与阈值。 |

## 已完成的命名调整

- `ActiveChatWorkflow` → `ActiveChatCoordinator`（session 生命周期、pending buffer、semantic wait、round scheduling）
- `ReviewWorkflow` → `ReviewCoordinator`（4 阶段编排 + scheduler 回调）
- `workflow/conversation.py` WorkflowRunner 混合体 → 拆分为 `AttentionCoordinator`（coordinator）+ `WorkflowRunner`（纯 LLM 循环）
- `workflow/tool_loop.py` 和 `active_chat/tool_loop.py` 重复的 `_parse_tool_call` → 提取到 `tools/parsing.py`

## 历史参考（.agent/ 目录）

以下文件位于项目根目录 `.agent/`，为施工过程中的设计笔记，不进入主文档树：

| 文件 | 内容 |
|------|------|
| `active_chat_workflow_design.md` | Active Chat 完整设计笔记。长期价值已提取到 `docs/design/runtime/active_chat_workflow.md`。原文件保留为施工笔记，含更多实现细节和 Think Mode 设计。 |
| `agent_scheduler_refactor.md` | Scheduler 四状态机设计笔记：IDLE → REVIEW → ACTIVE_REPLY → ACTIVE_CHAT 流转、高提醒队列、review 间隔策略。 |
| `AGENT_SYSTEM_DESIGN.md` | 早期 Agent 管理系统设计（Persona/Skill/Agent 数据模型、Assembly Pipeline）。部分概念已演化为当前 PromptRegistry + ToolRegistry 架构。 |
| `agent_restructure_plan.md` | Agent 模块重构计划（9 阶段执行顺序、模块分类表、Active Chat 设计规则）。 |
