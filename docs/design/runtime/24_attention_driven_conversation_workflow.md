# ShinBot 技术规范：注意力驱动的群聊会话工作流 (Attention-Driven Conversation Workflow)

本文档定义 ShinBot 在群聊自然语言场景中的新会话工作流。

目标不是继续沿用“每条消息都尝试触发一次 LLM”的触发式兜底回复，而是引入一个由 **Session 全局注意力** 驱动的批处理工作流：

- 消息先进入会话的注意力累积过程，而不是立即触发模型
- 注意力达到阈值后，才将一批消息打包送入 workflow
- sender 的差异通过“对全局注意力的增量权重”体现，而不是把注意力绑定到某个人
- workflow 允许通过 Tool 有边界地调节 sender 权重和会话兴趣阈值
- 命令、等待输入、消息路由和非消息事件总线等现有基础设施继续保留，不引入双聊天引擎

本文档是后续实现“群聊 attention workflow”与替换逐消息 fallback responder 的主要设计依据。

---

## 1. 设计目标

### 1.1 降低逐消息触发成本
- 群聊自然语言消息默认不应每条都触发 LLM。
- 系统应优先累计消息与信号，再决定是否值得进入 workflow。
- LLM 的主要输入单位应从“单条消息”变为“会话内一段未消费消息批次”。

### 1.2 统一会话级注意力
- 注意力是 `session` 级全局变量，不与某个用户一一绑定。
- sender 差异只体现在“该用户消息对 attention 的贡献权重”上。
- `@bot`、回复 bot、连续话题等都应作为影响 contribution 的消息特征，而不是独立路由分支。

### 1.3 支持可治理的兴趣调节
- workflow 可以借助 Tool 调整某个 sender 的长期/短期权重。
- workflow 可以借助 Tool 调整当前会话的兴趣阈值，表达“暂时低兴趣”或“暂时高关注”。
- 所有修改都必须受边界限制、审计记录与回显提示约束。

### 1.4 与现有系统共存但不双轨聊天
- 现有 `MessageIngress` + `RouteTable` 继续承担 ingress、命令、权限、`wait_for_input`、消息路由与消息持久化。
- 群聊自然语言的“聊天引擎”只保留一套，即本 attention-driven workflow。
- 不再保留旧的逐消息 fallback responder 聊天链路。
- 所有“即时响应”需求也必须通过 workflow 配置表达，而不是回退到另一套 legacy responder。

---

## 2. 非目标
- 本文档不重新定义命令系统、插件系统、权限系统与 PromptRegistry 的完整设计。
- 本文档不要求首版就支持复杂的多 Agent 编排市场或远程工作流托管。
- 本文档不规定具体的模型提示词文本，只定义 workflow 的输入、状态与边界。
- 本文档不将注意力建模为“关注某个人的独立对象”；个人差异仅通过 sender 权重影响 session attention。

---

## 3. 定位与边界

新工作流应被视为 `agent_entry` 之后的“群聊自然语言调度层”，而不是独立替换整个消息基础设施。

### 3.1 保留的现有职责
以下职责继续由现有基础设施负责：
- `Adapter -> UnifiedEvent -> MessageElement AST` 的接入和归一化
- `Session` 识别与上下文装载
- `CommandRegistry` 的命令解析
- `wait_for_input` 的挂起与恢复
- `RouteTable` 的消息分发和 `EventBus` 的 notice / lifecycle 事件分发
- `message_logs`、审计日志、Prompt Snapshot 等持久化

### 3.2 被替换的职责
以下职责不再由“逐消息 fallback responder”承担，而应完全收敛到 workflow：
- 每条群聊消息单独判断是否拉起模型
- 模型直接产出文本后立刻发送
- `@bot` 作为单独的聊天触发分支

替代方式：
- 每条群聊消息先更新 `SessionAttentionState`
- 只有注意力达到有效阈值，才触发 workflow
- workflow 通过 Tool 决策回复、权重调整和兴趣调节

### 3.3 单一聊天引擎原则
系统在自然语言群聊场景中必须遵循：
- 同一类消息只能归一个聊天引擎处理
- 不允许新旧两套聊天引擎并行争抢同一条群聊自然语言消息
- “共存”仅指基础设施共存，不指两套聊天决策共存
- 即时响应、主动响应、被动响应都只是同一 workflow 的调度配置差异，而不是不同聊天引擎

### 3.4 迭代优先于兼容
- 当前阶段的目标是尽快收敛到单一工作流架构，而不是维持 legacy 行为兼容。
- 若兼容逻辑只为保留旧设计而存在、且会增加维护成本或理解复杂度，应优先删除而不是继续保留。
- 数据结构、调度行为与工具协议允许在迭代过程中发生不兼容调整。

---

## 4. 核心概念

### 4.1 SessionAttentionState
表示一个会话的全局注意力状态。
建议包含字段：
- `session_id`
- `attention_value`: 当前累计注意力值
- `base_threshold`: 会话的基础触发阈值
- `runtime_threshold_offset`: 会话短期阈值偏移（支持正负），会自动回归到 0
- `cooldown_until`: 冷却截止时间，在此之前即使达标也不触发
- `last_update_at`: 上次计算衰减的时间戳
- `last_consumed_msg_log_id`: Workflow 已经完全消费到的消息位置
- `last_trigger_msg_log_id`: 上次触发 Workflow 的消息位置
- `metadata`: 存储 `internal_summary` 等额外状态

有效阈值定义为：
```text
effective_threshold = clamp(
    base_threshold + runtime_threshold_offset,
    threshold_min,
    threshold_max,
)
```

### 4.2 SenderWeightState
表示某个 sender 在某个 session 中对全局 attention 的影响权重。
建议包含字段：
- `stable_weight`: 长期权重，不随时间衰减，表达长期信任、优先级或反感。
- `runtime_weight`: 短期权重，会平滑回归到 0，表达本轮会话中的临时关注。
- `last_runtime_adjust_at`: 上次短期调整时间。

### 4.3 中性点与投影规则
权重存储值的中性点定义为 `0`，而不是 `1`。
原因：
- `0` 更适合作为“没有偏好”的初始态。
- 允许向正（增强）和负（减弱）两个方向有对称的操作空间。

实际计算 contribution 时，权重值应通过投影映射为 sender factor：
```text
sender_score = clamp(stable_weight + runtime_weight, min, max)
sender_factor = weight_curve(sender_score)
```
要求：
- `weight_curve(0)` 必须映射到中性贡献系数（1.0）。
- 负权重应使系数趋近于 0，正权重应成倍提高系数。

### 4.4 Message Contribution 与 Robust Interrupt
单条消息对 attention 的贡献定义为：
```text
contribution = base_gain * sender_factor + feature_bonus
```
其中：
- `base_gain`: 消息的基础增量。
- **Robust Interrupt (破门机制)**：
  - `@bot` 或回复 Bot 不应直接推满注意力。
  - **多因子累积**：单次 `@bot` 仅提供适度 bonus。但如果在短时间内（例如 10s 内）出现多次近似发言、相同内容、或来自不同用户的连续提及（通过 Token Overlap 或 MinHash 等高效算法识别），`feature_bonus` 将呈非线性（如指数）增长。
  - 这种设计利用了群聊的“集体压力”特征，确保 Bot 响应的是真实的交互热度。

---

## 5. 状态更新与衰减规则

### 5.1 Attention 衰减 (Exponential Decay)
`attention_value` 必须支持时间衰减。
推荐规则：
- 使用**指数衰减**：`value = value * exp(-k * Δt)`。
- 这能让对话的“余热”更平滑地消退，符合人类对话的遗忘规律。
- 每次收到新消息前，先根据 `last_update_at` 计算衰减后的当前值。

### 5.2 状态回归
- `runtime_weight` 与 `runtime_threshold_offset` 必须随时间平滑回归到 `0`。

### 5.3 Reply Fatigue (回复疲劳机制)
- **阈值正向漂移**：当 Bot 短时间内连续触发 Workflow 并回复后，系统应自动向 `runtime_threshold_offset` 增加一个正向增量（即提高门槛）。
- 这种“疲劳值”会随 Bot 的静默时间而回归。
- 作用：物理级保护，防止模型通过调权重形成自我强化风暴，强制 Bot 在高频互动后进入“冷静观望”态。

### 5.4 Clamp 规则与显式反馈
所有状态写入前必须 Clamp 到预设边界。
**关键要求**：
- 在 Tool 的执行回执中，**必须明确说明是否发生了 Clamp**。
- 示例：如果 LLM 尝试将权重加到 5.0 但上限是 2.0，回执应返回 `"applied": "clamped_to_max"`。
- 这能辅助 LLM 判断当前的调节策略是否已经触顶，从而调整后续决策。

---

## 6. Workflow 触发规则

### 6.0 响应策略配置 (Response Profiles)
系统必须支持通过配置表达不同的响应风格，而不是保留 legacy 聊天链路。

推荐引入 `attention_profile` / `response_profile` 概念，例如：
- `disabled`
- `passive`
- `balanced`
- `immediate`

这些 profile 共享同一套 workflow，只调整下列参数：
- `base_threshold`
- `runtime_threshold_offset` 的默认范围
- `mention_bonus` / `reply_bonus`
- `semantic_wait_ms`
- `cooldown_seconds`
- 可选的 fast-dispatch 条件

目标：
- `disabled`: 不进入注意力调度，适合默认关闭私聊注意力
- `passive`: 倾向观望与批处理
- `balanced`: 默认群聊风格
- `immediate`: 接近 legacy 的即时响应体验，但仍然走 workflow

关键约束：
- `immediate` 不等于恢复“每条消息都直接调用 LLM”的旧链路
- 它只表示注意力更容易越过阈值、沉淀窗口更短、对高优先级信号更敏感
- 即使在 `immediate` 模式下，输出仍必须通过 Tool 执行

### 6.1 消息到达时的标准流程
1. 消息归一化并写入 `message_logs`。
2. 对当前 Session 状态做时间推进（衰减与回归）。
3. 计算 Contribution 并更新 `attention_value`。
4. **语义完备性等待 (Semantic Boundary)**：
   - 如果注意力达到 `effective_threshold`，系统不应立即调度 Workflow。
   - 引入一个短窗口（如 800ms~1200ms）。
   - 如果期间该 Sender 仍在持续发送消息，则等待其发送结束再进行 Batch Claim。
   - 作用：有效解决“分条发送”问题，确保 LLM 拿到完整的语义块。
5. Claim 消息批次并启动 Workflow。

### 6.1.1 Fast Dispatch 也是 Workflow
若用户需要“几乎即时”的响应体验，应通过 workflow 的快速调度策略实现，而不是恢复 legacy responder。

允许的实现方式包括：
- 降低 `base_threshold`
- 缩短 `semantic_wait_ms`
- 提高 `mention_bonus` / `reply_bonus`
- 允许某些高优先级信号直接把注意力推到阈值以上

但必须保持：
- 统一的 attention 状态管理
- 统一的 workflow runner
- 统一的 Tool 输出约束

### 6.2 触发后的 Attention 处理
- 触发后默认不应直接清零。
- 推荐：扣减本次消耗掉的 `effective_threshold` 值，保留剩余的残余注意力。

---

## 7. Workflow 运行模型

### 7.1 Cross-talk Mitigation (交错话题处理)
- **特征识别**：在 Claim Batch 后，系统可利用轻量算法（如关键词共现或简单的 Topic Hash）标注出批次内是否存在多个讨论线索。
- **上下文标注**：在送入 LLM 的 Prompt 中显式标注：“[系统提示：检测到当前批次可能包含 2 个不相关话题线索]”。
- **引导决策**：引导 LLM 决定是只回复主导话题，还是通过分条回复处理多个线索。

### 7.2 运行期消息插入 (Incremental Merging)
- `run_start_cursor`: 启动时 claim 的边界。
- `live_append_buffer`: 运行期间新到消息的暂存区。
- **合并时机**：限制在模型完成一次输出、或 Tool 执行完成、准备进入下一步决策的“空档期”。
- **原则**：不支持推理中途抢占插入，确保模型生成的 Assistant 文本与它看到的 Context 是语义一致的。

### 7.3 输出与 no_reply 的角色
- 发送动作必须通过 Tool 执行。
- **no_reply 增强**：`no_reply` 允许附带一个 `internal_summary`（对本次观察的摘要和不回复的原因）。
- 该摘要存入 Session Metadata，在下一轮触发时作为“短期记忆”提供给 LLM。

---

## 8. Attention Tool 设计

### 8.1 引导性数值参考 (Reference Values)
- 在 Tool 的 `description` 中，系统必须提供**建议修改的参考 delta 范围**。
- 例如：`"stable_delta: 建议范围 0.1 ~ 0.5，过大会导致响应偏置失控"`。
- 这不是硬性拦截，而是为没有物理“量感”的 LLM 提供操作指南，防止它一次性加减过大的数值导致系统振荡。

### 8.2 推荐 Tool 集
- `attention.inspect_state`:
  - 返回当前的注意力区间（如 `neutral`, `high`, `very_high`）。
  - 返回当前权重分布的离散 Band。
- `attention.adjust_sender_weight`:
  - 参数：`sender_id`, `stable_delta`, `runtime_delta`, `reason`。
  - 必须返回 Clamp 状态。
- `attention.adjust_session_threshold`:
  - 调整会话的兴趣门槛。
- `no_reply`:
  - 参数：`internal_summary`。

### 8.3 回显提示风格
推荐回执风格：
```json
{
  "target": "sender:123456",
  "applied": { "stable": "clamped_to_max", "runtime": "applied" },
  "current_band": { "stable": "high", "runtime": "positive" },
  "hint": "权重已达上限，继续增加将不再生效。"
}
```

---

## 9. 持久化建议

### 9.1 `session_attention_states`
- 保存 session 全局注意力状态、阈值参数、冷却状态。
- 必须记录 `last_update_at` 用于下次衰减计算。

### 9.2 `sender_weight_states`
- 保存 (session, sender) 对的权重快照。
- 长期不活跃的条目应支持定期清理或压缩。

### 9.3 `workflow_runs` (审计与观测)
- 记录每一轮的 Input Batch。
- 记录模型调用的所有 Tool 及其参数。
- 记录最终是否回复、回复了什么。
- 作用：用于复盘为什么 Bot 在某个群里变得“话痨”或“高冷”。

---

## 10. 与现有消息路由的集成

### 10.1 替换逻辑
- 用户消息路由的 `agent_entry` fallback 负责把未被消费型 route 命中的消息通知给 Agent 入口。
- 消息路由层只发出最小 `AgentEntrySignal`，包含触发消息的 `message_log_id`、session、sender、平台和 profile 等事由信息；它不预先构造 Agent 上下文，也不直接依赖 attention scheduler。
- `shinbot.agent.runtime.AgentRuntime` 作为 Agent 入口之后的内部实现接收该信号，再委托 attention scheduler 自行读取 `message_logs`、更新注意力并决定是否触发 workflow。
- 只有 Agent 侧触发逻辑判定为需要响应时，才拉起 Workflow 调度器。
- 命令与 `wait_for_input` 继续保留各自的原有控制流。
- 私聊、`@bot`、回复 Bot 等“高即时性”需求，应通过更激进的 workflow profile 实现，而不是保留第二套聊天 responder。

### 10.2 Legacy 下线原则
- legacy prompt pipeline 不应继续作为自然语言聊天的兜底链路存在。
- 若某些场景需要 legacy 级别的即时性，应先尝试用 profile / fast dispatch / 阈值配置实现。
- 只有命令系统、挂起输入恢复、事件总线等非聊天职责可以保留原控制流。

---

## 11. 实现约束与安全要求
- **自我强化保护**：物理级 Clamp 和疲劳机制是底线，不能依赖 LLM 的自觉。
- **观测性**：系统应提供一个内部接口或 Dashboard 页面，实时查看群聊注意力的“水位”波动。

---

## 12. 最小可行落地顺序
1. 定义数据表 Schema 并实现基础的衰减/回归逻辑。
2. 实现 Agent 入口 handler，接收 `AgentEntrySignal` 后委托 attention scheduler 完成注意力累积。（已由 `AgentRuntime` 承接）
3. 实现基于沉淀窗口的消息 Claim 逻辑。
4. 接入首版 `attention.*` Tool 集并实现显式的 Clamp 反馈。
5. 引入回复疲劳机制和 Cross-talk 标注。
6. 删除 legacy 自然语言 fallback responder，并补齐 `response_profile` 配置。
