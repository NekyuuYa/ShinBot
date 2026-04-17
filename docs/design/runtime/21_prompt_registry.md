# ShinBot 技术规范：PromptRegistry（提示词注册表）

本文档定义 ShinBot 在 Agent、系统任务与插件侧的统一 Prompt 编排机制。

数据结构与实现字段定义见 `22_prompt_registry_schema.md`。

PromptRegistry 的目标不是“存几段 prompt 文本”，而是建立一套稳定、可组合、可审计、可缓存优化的 Prompt 组装协议，并将其映射到现代 LLM 的 **Chat Completions (Messages + Tools)** 规范。

---

## 1. 设计目标

### 1.1 统一编排与结构化映射
- **不再输出单一的纯文本字符串**，而是输出符合 Chat Completions 规范的 `messages` 列表与 `tools` 列表。
- 阶段顺序必须由 PromptRegistry 内部强制执行，映射到特定的消息角色（Role）。

### 1.2 最大化缓存命中 (Cache Optimization)
- 将高度稳定的组件（系统基底、身份、工具定义）前置。
- 将高度波动的组件（历史记录、当前任务）后置。
- 确保前缀部分的一致性，以最大化利用大模型商的 Prefix Cache。

### 1.3 高指令遵从与 Recency Bias 利用
- 越及时的指令与格式约束（Instructions / Constraints）映射到末尾 `user` 消息，利用模型的近因效应。

---

## 2. 七阶段装配与消息角色映射

PromptRegistry 严格按以下顺序装配。注意 **Abilities (Stage 3)** 优先于 **Context (Stage 4)**，以确保稳定前缀的连续性。

| 阶段 (Stage) | 映射目标 (Mapping Target) | 说明 | 稳定性 |
| :--- | :--- | :--- | :--- |
| **1. System Base** | `system` message (Top) | 基础协议、安全规则、输出格式基准。 | 极高 |
| **2. Identity** | `system` message (Initial) | 助手人格、风格、角色定位。 | 高 |
| **3. Abilities** | `tools` parameter | 工具/技能声明。**映射至 API 顶层参数**。 | 高 |
| **4. Context** | `user`/`assistant` sequence | 对话历史序列。包含历史消息与背景摘要。 | 低 (随轮次波动) |
| **5. Compatibility** | `user` message (Final, Part A) | 遗留逻辑注入，位于当前用户消息前部。 | 极低 |
| **6. Instructions** | `user` message (Final, Part B) | 当前任务目标、用户请求原文。 | 极低 |
| **7. Constraints** | `user` message (Final, Part C) | 执行约束。**强制位于最后一条消息的最末尾**。 | 极低 |

---

## 3. 阶段映射详解

### 3.1 Stage 1 & 2: 系统与身份 (System Roles)
- 作为消息列表最顶部的 `system` 角色消息。
- 使用多段 `{"type": "text", "text": "..."}` 块来区分不同的组件。

### 3.2 Stage 3: 能力 (Abilities / Tools)
- **映射方式**：直接映射到 API 请求的 `tools` 字段。
- **构建顺序意义**：在逻辑上，工具定义紧随系统设定之后。将其视为稳定前缀的一部分。

### 3.3 Stage 4: 上下文 (Context / History)
- **映射方式**：展开为一系列 `user` 和 `assistant` 角色的消息对。
- **要求**：Context Resolver 应返回结构化的消息对象列表。

### 3.4 Stage 5, 6 & 7: 任务执行包 (Task Payload)
- **映射方式**：合并作为消息列表中**最后一条 `user` 角色消息**。
- **内容组织 (Content Array)**：
  - 使用数组形式组织 `content`：`[{"type": "text", "text": "..."}, {"type": "text", "text": "..."}, ...]`。
  - **Stage 7 (Constraints)** 必须强制排在数组的最后一位。
- **隔离原则 (Isolation)**：
  - 在后续将本轮存入历史 Context 时，**只保留 Stage 6 的核心内容**。Stage 5 和 Stage 7 的补丁与约束在任务完成后即刻失效，不进入长期记忆。

---

## 4. 强制约束

- **禁止压扁**：不得将所有阶段压扁为单一字符串。
- **缓存优先排序**：Abilities 必须在 Context 之前被解析，确保前缀缓存稳定。
- **约束置尾**：Stage 7 必须位于请求的绝对末尾。
- **历史纯净度**：存入 Context 的历史消息必须剥离掉 Stage 5 和 Stage 7 的痕迹。
