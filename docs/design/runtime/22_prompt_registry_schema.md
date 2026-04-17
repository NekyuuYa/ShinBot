# ShinBot 技术规范：PromptRegistry 数据结构 (PromptRegistry Schema)

本文档定义 PromptRegistry 实现所需的核心数据结构、字段约束与运行关系。

---

## 1. 设计目标

- 为 PromptRegistry 提供符合 Chat Completions 规范的实现骨架。
- 保证“阶段映射到 Role、Tool 原生绑定、Content 数组隔离”的设计能直接映射到代码。
- **持久化策略**：实行“输出记录，输入裁剪，短期全量”原则，兼顾可追溯性与存储效率。

---

## 2. 核心数据结构总览

PromptRegistry 包含以下核心数据结构：

1. `PromptComponent`: 最小注册单元。
2. `PromptSource`: 系统推导的来源。
3. `PromptProfile`: 默认启用方案。
4. `PromptAssemblyRequest`: 装配请求。
5. `PromptAssemblyResult`: 装配结果。
6. `PromptSnapshot`: 调用全量快照（短期保留）。
7. `PromptStageBlock`: 阶段组装块。
8. `PromptComponentRecord`: 组件使用记录。

---

## 3. PromptAssemblyResult

`PromptAssemblyResult` 表示对一次请求的完整组装输出。

建议字段：
- `profile_id: str`
- `caller: str`
- `messages: list[dict[str, Any]]`: 符合 Chat Completions 规范的消息列表。
- `tools: list[dict[str, Any]]`: 符合 API 规范的工具列表。
- `prompt_signature: str`: 基于 messages 和 tools 结构的稳定签名。
- `compatibility_used: bool`
- `truncation: dict[str, Any]`

---

## 4. PromptSnapshot (全量请求快照 - 短期存储)

`PromptSnapshot` 存储于独立的 `prompt_snapshots` 表中，用于开发调试和深度审计。

**持久化策略 (TTL Policy)**：
- **默认保留时间**：3 小时。
- **过期销毁**：系统定期自动清理超过保留时间的记录。
- **作用**：记录发送给模型的所有 `messages` 和 `tools` 全量原文。

建议字段：
- `id: str` (UUID)
- `timestamp: float`
- `full_messages: list[dict[str, Any]]`
- `full_tools: list[dict[str, Any]]`
- `prompt_signature: str`

---

## 5. ModelExecutionRecord (执行记录 - 长期存储)

执行记录存储于 `model_executions` 表中，用于长期统计、计费和基础审计。

**裁剪逻辑 (Pruning Logic)**：
- **输入部分**：仅记录最后一条 `user` 消息（包含 Stage 5, 6, 7 的 Injected Content）。**严禁记录完整的 Stage 3 历史记录**。
- **输出部分**：全量记录。包括模型返回的内容、Tool Call 链、思考过程（Reasoning/Thinking）。
- **关联性**：通过 `prompt_snapshot_id` 关联短期的全量快照。

建议字段：
- `id: str` (UUID)
- `prompt_snapshot_id: str`
- `injected_content: list[dict[str, Any]]`: 仅保留最后一条 User Message 的 Content 数组。
- `response_text: str`: 模型回复文本。
- `thought_process: str`: 模型思考过程（如果支持）。
- `tool_calls: list[dict[str, Any]]`: 完整的工具调用链。
- `usage: dict`: Token 消耗统计。
- `latency_ms: int`

---

## 6. 强制约束

- **存储分离**：全量请求与执行摘要必须分表存储。
- **自动清理**：Snapshot 表必须实现 TTL 机制。
- **输入剥离**：长期记录中不得包含历史对话上下文，以保护隐私并节省空间。
- **输出完整**：必须保留模型的所有思考和调用决策（Thinking & Tool Calls）。
