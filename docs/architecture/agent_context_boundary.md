# Agent Context Boundary

本文定义 Agent `context` 模块的当前目标职责。旧的三级记忆大方案已归档到 `../archive/runtime/context_memory_architecture.md`，不能再作为现行模块边界依据。

## 核心定位

`context` 是上下文结构化与上下文管理层。

它接收已经被上游选定的消息、摘要、身份、媒体语义和记忆材料，在 token 预算与 PromptRegistry stage 约束下，输出 prompt-ready 的上下文包。

一句话定义：

> Context is a projection and packing layer. It does not decide what should be read, when a workflow starts, or how memory is generated.

## Context 应该负责

- 将消息记录转换为结构化 content blocks。
- 将已选定的消息窗口整理为 prompt-facing 的输入材料。
- 将已有的 alias、identity display name、媒体语义和摘要投影进上下文。
- 将已有短期/中期/长期记忆投影为 PromptRegistry 可消费的 context messages。
- 按 token budget 做裁剪、排序、去重和打包。
- 管理 context 自己需要的轻量状态，例如 alias prompt snapshot、短期上下文 buffer、cache 友好边界。
- 输出统一的 context bundle，供 PromptRegistry 继续组装最终 request messages。

## Context 不应该负责

- 不决定 unread/read 生命周期。
- 不决定 review 要扫描哪些消息。
- 不决定 active chat 何时触发 batch。
- 不执行 workflow 或 tool loop。
- 不直接调用模型生成回复。
- 不生成压缩摘要本身。
- 不生成图片转述本身。
- 不维护身份数据的事实来源。
- 不直接决定 PromptRegistry 的 stage 顺序。
- 不直接承担数据库查询策略。

## 与其他层的关系

| 能力 | 所属层 | 说明 |
| --- | --- | --- |
| 选择哪些消息进入 review | Coordinator | `ReviewCoordinator` 固定 unread snapshot、分批扫描和合并候选 |
| 选择 active batch 边界 | Coordinator | `ActiveChatCoordinator` 根据 attention、semantic wait 和 pending buffer 决定 |
| 查询消息数据库 | Store / Runtime Adapter | coordinator 定义 port，runtime 实现 SQLite/database adapter |
| 生成压缩摘要 | Runner | compression 是一次性 LLM stage，应作为 runner，不属于 context |
| 生成图片/媒体语义 | Media Service / Runner | context 只消费已经生成的媒体语义 |
| 维护身份事实 | Identity Service | context 只消费 identity 的 display projection |
| 组装最终 messages | PromptRegistry | context 输出材料，PromptRegistry 按 stage 顺序合成 request messages |
| 执行 tool loop | Workflow | context 不执行工具循环 |

## 压缩职责

上下文压缩分为两个动作：

1. **选择压缩源**
   - 可以由 coordinator、context packer 或 eviction policy 给出候选消息/上下文块。
   - 这一步是结构与预算决策，不调用模型。

2. **生成压缩摘要**
   - 应由 compression runner 执行。
   - runner 输入结构化源材料，输出摘要、覆盖范围、候选消息 id 和理由。
   - context 之后只负责把这份摘要作为已有材料投影进 prompt。

当前 `review_compression` runners 已符合这个方向。后续如果要做 active/context eviction 的压缩，也应新增对应 runner，而不是把 LLM 压缩逻辑放回 `context`。

## 目标输出结构

理想输出应收敛为类似结构：

```python
@dataclass
class ContextBundle:
    context_messages: list[dict]
    instruction_blocks: list[dict]
    constraint_text: str = ""
    metadata: dict = field(default_factory=dict)
```

其中：

- `context_messages`：稳定背景上下文，例如已读历史、压缩记忆、长期记忆、alias snapshot。
- `instruction_blocks`：本轮选定输入，例如 review batch、reply decision window、active chat pending batch。
- `constraint_text`：只放本轮必须遵守的上下文约束，例如 alias 使用约束。
- `metadata`：供 runner/coordinator 追踪来源，不直接作为业务决策入口。

## 当前迁移原则

- 可以先保留 `ContextManager` 作为 facade，避免大范围调用方同时改动。
- 新逻辑应优先落在明确子模块中：`builders/`、`projectors/`、`runtime/`、`state/`、`utils/`。
- 不再向 `context` 添加新的 LLM runner。
- 不再向 `context` 添加 workflow 触发判断。
- 不再让 `context` 直接决定 review/active 的消息范围。
- 旧三级记忆文档只作为历史参考，现行边界以本文和 `agent_module_layers.md` 为准。
