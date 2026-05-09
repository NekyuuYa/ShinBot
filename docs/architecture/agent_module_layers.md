# Agent Module Layers

本文定义 ShinBot Agent 系统后续重构时采用的模块分层。

核心原则：消息事件只是信号。Core dispatch 只负责把触发原因通知 Agent 入口，不在 Agent 外部构造上下文，也不直接连接具体 workflow。Agent 内部再由调度器、协调器、workflow、可复用能力和运行时服务分层协作。

## 分层概览

```text
core dispatch
  -> AgentEntrySignal
  -> agent scheduler
  -> coordinator
  -> workflow / runner / utils
  -> runtime services
```

| 层级 | 建议目录 | 职责 | 不应承担的职责 |
| --- | --- | --- | --- |
| Scheduler | `shinbot/agent/scheduler/` | 维护 Agent 状态机、定时器、入口信号、review/active 的触发条件、session 级状态转换 | 组装 prompt、调用模型、执行 tool loop、读取大量上下文 |
| Coordinator | `shinbot/agent/coordinators/` | 编排某个状态或流程，例如 review、active chat、active reply；决定调用哪些 workflow/util，并把结果反馈给 scheduler | 直接实现通用模型调用能力，或把自己做成聊天 runtime |
| Workflow | `shinbot/agent/workflows/` | 处理一个明确输入并执行一段完整工作流，例如聊天 runtime、tool loop、多步 repair | 决定何时被触发、维护全局状态机、扫描 unread |
| Runner | `shinbot/agent/runners/` | 执行单次、无连续会话语义的 LLM stage，例如 review scan、reply decision、bootstrap | 调度多个 stage、维护 scheduler 状态、持有 session 生命周期 |
| Utils | `shinbot/agent/utils/` | 可复用、无连续会话语义的能力，例如上下文压缩、图片转述、消息窗口整理、结构化转换 | 控制状态迁移、长期持有 session 状态、隐式触发回复 |
| Runtime / Services | `shinbot/agent/runtime/`、`shinbot/agent/services/` | ModelRuntime、PromptRegistry、ToolManager、ContextBuilder、消息存储访问等基础服务 | 知道 review/active 的业务策略 |

## 命名边界

### Scheduler

Scheduler 是 Agent 的状态控制面。它回答“现在处于什么状态、下一步应该由哪个流程接管”。

它可以维护：

- `IDLE` / `REVIEW` / `ACTIVE_REPLY` / `ACTIVE_CHAT` 等状态；
- review timer 和 active chat tick timer；
- attention、interest 等状态驱动参数；
- 高提醒事件队列与未读入口；
- session 级串行与重入保护。

它不应直接：

- 拼接 prompt；
- 访问模型；
- 执行 tool loop；
- 把消息历史预先包装成完整上下文。

### Coordinator

Coordinator 是某个状态或流程的编排层。它回答“为了完成这次状态任务，需要按什么顺序调用哪些能力”。

例如：

- `ReviewCoordinator`
  - 固定本次 review 的未读范围；
  - 分批读取消息；
  - 对 overflow 前半截调用压缩能力；
  - 调用 review scan runner 筛选值得回应的消息；
  - 调用 reply decision runner 决定回复；
  - 调用 active bootstrap runner 生成 active chat 初始语义档位；
  - 将结果提交给 scheduler 进入 active chat。
- `ActiveChatCoordinator`
  - 管理 active session；
  - 接收 active 状态期间的新消息；
  - 累计 attention；
  - 管理 pending batch、semantic wait 和 tick；
  - 在条件满足时调用 chat workflow；
  - 根据 workflow 结果调整 interest 或退出状态。

Coordinator 可以有流程状态，但它的状态服务于编排，不应变成通用模型调用框架。

### Workflow

Workflow 是可执行的一段 Agent 工作流。它回答“给我这批输入，我怎样完成一次模型驱动任务并返回结构化结果”。

典型 workflow：

- `FastChatWorkflow`
  - 构建当前轮聊天请求；
  - 使用 PromptRegistry 组装 messages；
  - 暴露本轮允许的 tools；
  - 执行 tool loop；
  - 处理无 tool call 的 repair；
  - 返回已消费消息、tool 行为、是否强制退出等结构化结果。

Workflow 不应自己决定：

- 哪些消息应该进入本次流程；
- 何时从 idle 进入 active；
- 是否扫描全局 unread；
- active chat 结束后下一次 review 间隔。

### Runner

Runner 是单次 stage 执行单元。它回答“给我一个已经准备好的 stage input，我调用一次模型或一次工具决策并返回本 stage 的结果”。

典型 runner：

- `ReviewScanStageRunner`
  - 输入一批 review 消息与压缩摘要；
  - 输出有回复价值的消息 id。
- `ReplyDecisionStageRunner`
  - 输入核心回复目标和附近上下文；
  - 使用 `send_reply` / `no_reply` / 可选 `send_poke` 等工具；
  - 返回 reply/no-reply 结果。
- `ActiveChatBootstrapStageRunner`
  - 输入 review 结果与 tail history；
  - 输出 active chat 初始语义档位。

Runner 不应自己决定：

- 哪些 unread range 应进入本次 review；
- stage 之间的执行顺序；
- review 结束后是否进入 active chat；
- active chat 的 session 生命周期。

Runner 和 workflow 都不应依赖 coordinator 的内部 models。跨层传递时应使用本层自己的输入输出 contract，由 coordinator 或 runtime adapter 做转换。

### Utils

Utils 是可复用的一次性能力。它可以调用 LLM，但不具有连续性、不拥有 tool loop、不维护状态机。

适合放入 utils 的能力：

- 消息压缩；
- 图片转述或缩略图语义提取；
- 消息窗口裁剪；
- 上下文结构化包装；
- token 估算；
- 通用 JSON repair；
- 将消息记录投影为 prompt-facing 结构。

为了避免 `utils` 变成杂物箱，进入此层的能力必须满足：

1. 输入输出清晰；
2. 不拥有 session 生命周期；
3. 不决定状态迁移；
4. 可被多个 coordinator/workflow 复用。

## Prompt 与 Tool 的位置

所有模型调用都应通过 PromptRegistry 得到最终 `messages` 数组，再由 ModelRuntime 发送请求。

PromptRegistry 负责：

- 按固定 stage 顺序组装 prompt；
- 接收 persona、workflow prompt、上下文消息、当前任务约束等输入；
- 返回用于模型请求的 `messages`；
- 不管理 tool 定义。

Tool 定义和可见性由 ToolRegistry / ToolManager 管理。Tool 注册代码不放在 `services/tools/`；`services/tools/` 只负责工具注册表、管理器、schema 和执行投影。具体 tool 的注册应像 prompt registration 一样放在拥有该语义的模块中，例如聊天动作工具放在 `workflows/chat_actions/`。

## 与 Core Dispatch 的关系

Core dispatch 只发送最小 Agent 入口信号：

- 触发消息 id；
- session / sender / platform；
- 触发原因或 profile；
- 必要的事件类型。

它不应包含：

- 批量历史消息；
- 已包装好的上下文；
- Agent 内部阅读状态；
- workflow 专用参数。

Agent 需要上下文时，由 Agent 内部通过消息数据库、上下文构建器和自身状态来读取。

## 建议目录形态

```text
shinbot/agent/
├── scheduler/
├── coordinators/
│   ├── review/
│   ├── active_chat/
│   └── dispatcher.py
├── workflows/
│   ├── active_chat/
│   └── chat_actions/
├── runners/
│   ├── review_scan/
│   ├── review_reply/
│   ├── review_compression/
│   └── review_bootstrap/
├── services/
│   ├── context/
│   ├── identity/
│   ├── media/
│   ├── model_runtime/
│   ├── prompt_engine/
│   └── tools/
├── utils/
├── runtime/
└── ...
```

该目录形态是目标结构，不要求一次性迁移完成。后续改动应优先避免继续把 coordinator、workflow 和 utils 混在同一个模块里。

## Context 模块内部分层

`context/` 模块跨越多个层级，内部应按以下方式分类：

| 子目录 | 目标层 | 说明 |
|--------|--------|------|
| `builders/` | Utils | 无状态构建器：context stage builder、instruction stage builder、image summary、message parts |
| `projectors/` | Utils | 无状态投影器：alias projector、compressed memory projector、long-term memory projector |
| `utils/` | Utils | token 估算、eviction 辅助 |
| `runtime/` | Runtime | 有状态会话操作：session runtime、pool runtime、eviction runtime、timeline runtime、prompt assembler |
| `state/` | Runtime | 有状态容器：ring buffer、alias table、active pool、state store |
| `manager.py` | Runtime | 顶层编排器，组装 builders、projectors、runtime |

物理文件移动风险较高，当前以文档分类为准。后续改动应避免在 `builders/` 和 `projectors/` 中引入会话状态。

## 当前整理状态

- `ActiveChatCoordinator`：session 生命周期、pending buffer、semantic wait、round scheduling。
- `ReviewCoordinator`：review 三阶段编排、overflow、unread 消费、scheduler 回调。
- `ActiveChatFastRunner`：active chat fast-mode 的模型请求与 tool loop。
- `review_*` runners：review 各 stage 的单次 LLM runner。
- `services/tools/`：只保留 ToolRegistry、ToolManager、schema、parsing 等工具管理基础设施。
- `workflows/chat_actions/`：聊天动作 tool 的注册位置。
