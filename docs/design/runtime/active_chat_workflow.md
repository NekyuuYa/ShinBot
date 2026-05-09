# Active Chat Workflow 设计规格

本文档定义 Active Chat（AtC）的运行时设计，包括双层触发模型、会话生命周期和兴趣衰减机制。

分层约束以 `../../architecture/agent_module_layers.md` 为准。实现细节以代码为准。

---

## 1. 设计概览

Active Chat 是 Agent 持续参与对话的状态，由 Review 完成后进入。核心特征：

- **不逐条响应**：消息先累积 attention，达到阈值后经 semantic wait 批量处理。
- **双层模型**：Interest（宏观存活度）控制 AtC 整体寿命；Attention（微观触发器）控制单次 LLM 轮次触发。
- **Tool-driven 决策**：LLM 通过 `send_reply` / `no_reply` / `send_poke` / `exit_active` 等工具表达行为，不直接输出文本。

---

## 2. 双层触发模型

### 2.1 Interest（宏观，长周期）

Interest 决定 AtC 整体存活时间。由 LLM action 主要驱动，消息仅提供轻量修正。

**初始值**：Review 完成后由 `ReviewCoordinator` 设置，默认 `15.0`，half-life `20s`。

**消息对 Interest 的贡献**：

| 事件 | Interest delta |
|------|---------------|
| 普通消息 | `+1` |
| @ bot | `+8` |
| 回复 bot | `+5` |
| poke / @ 他人 / poke 他人 | `+0` |

**LLM Action 对 Interest 的贡献**：

| Action | Interest delta |
|--------|---------------|
| `send_reply(light)` | `+5` |
| `send_reply(engaged)` | `+10` |
| `no_reply` | `-5` |
| `no_reply(strong)` | `-10` |
| `send_poke` | `+3` |
| `request_think_mode` | `+6` |
| `retry_failed` | `-3` |
| `exit_active` | 强制退出 |

**衰减**：Interest 使用指数衰减，half-life 由 `ActiveChatPolicy` 管理。Interest 耗尽时 AtC 退出。

### 2.2 Attention（微观，短周期）

Attention 决定"现在是否值得触发一轮 LLM"。每条消息贡献权重，累积到动态阈值后触发。

**消息贡献**：

| 事件 | Attention 贡献 |
|------|---------------|
| 普通消息 | `+1.0` |
| @ bot | `+4.0` |
| 回复 bot | `+3.0` |
| poke bot | `+0.8` |
| @ 他人 | `+0.5` |
| poke 他人 | `+0.2` |
| bot 自己消息 | `+0.0` |

取所有匹配特征中的最大值，不叠加。

**动态阈值** = f(interest)：

```
threshold = base_threshold * (reference_interest / interest)
clamp to [threshold_min, threshold_max]
```

- interest 高 (60) → threshold 低 (2.5) → 普通对话轻松触发
- interest 中 (30) → threshold 中 (5.0) → 需要较多消息或 mention
- interest 低 (10) → threshold 高 (15.0) → 只有持续 @ 才触发

**衰减**：accumulated attention 使用指数衰减（`decay_k = 0.003`），比主 AttentionEngine 更慢。

**Post-round 冷却**：LLM 轮次完成后 `accumulated *= 0.25`，保留余波但降低再次触发概率。

---

## 3. 两阶段触发流程

```
阶段 1：权重累积（可能跨越多条消息、多个 sender）
  普通消息 +1.0, +1.0, +1.0... → 累积到阈值

阶段 2：阈值突破后，semantic wait 等人说完
  同一 sender → 重置 timer
  timer 到期 → 批量合并 → LLM
  LLM 轮次完成 → accumulated *= 0.25
```

Semantic wait 避免在人还在打字时就触发 LLM。默认等待 800ms。

---

## 4. 会话生命周期

### 4.1 启动（从 Review 进入）

```
Review 完成
  → scheduler.complete_review(enter_active_chat=True)
  → ActiveChatCoordinator.start(session_id)
      ├── 初始化 attention state
      ├── 吸收 review 期间到达的新消息
      └── 进入等待态；不立即发起 LLM call
```

AtC 启动时不传入完整上下文。上下文在真正触发 LLM 轮次时，通过 context builder 按需构建。

### 4.2 多轮循环

```
LLM Call + Tool Loop（一个 atomic 轮次）
  → 轮次结束，更新 interest
  → interest 耗尽？→ 退出 AtC
  → 保留 conversation state
  → Wait for Next Batch
  → 新消息 → attention 累积 → semantic wait → 触发下一轮
  → 循环，直到 interest 耗尽或显式 EXIT
```

### 4.3 重入与并发

同一 session 同时最多运行一个 LLM round。运行中到达的新消息只进入 pending buffer，不并行启动第二个 LLM 请求。当前 round 结束后检查 pending 是否达到阈值。

AtC 运行期间不被 Active Reply 打断。高提醒消息属于当前实时聊天的一部分，只提高 attention/interest。

### 4.4 失败与重试

LLM round 允许 repair 一次。重试时合入 retry 期间新到达的 pending messages。retry 仍失败则不发送回复，记录失败原因，回到等待态。

**消费语义**：
- 已成功构建 prompt、进入 LLM/tool loop 并得到可解释结果（如 `retry_failed`）→ 消费 batch，按失败 action 降温。
- workflow 异常、非法退出、未形成 batch → 恢复 pending buffer，不标记 consumed。

### 4.5 退出

```
interest 耗尽 / exit_active → scheduler 退出 ACTIVE_CHAT
  → 取消 pending timer
  → pending_buffer 中未消费消息回到 unread
  → 清空 conversation state
  → scheduler 转入 IDLE + 生成下一轮 review plan
```

---

## 5. 实现映射

| 概念 | 实现 |
|------|------|
| Attention 贡献计算 | `active_chat/attention.py` — `ActiveChatAttention` |
| Interest 效果映射 | `active_chat/actions.py` — `interest_effect_for_round()` |
| Interest 衰减与策略 | `scheduler/active_chat_policy.py` — `ActiveChatPolicy` |
| Tick timer | `scheduler/active_chat_timer.py` — `ActiveChatTickTimer` |
| 会话生命周期 | `active_chat/coordinator.py` — `ActiveChatCoordinator` |
| 单轮 LLM 执行 | `active_chat/runner.py` — `ActiveChatFastRunner` |
| Tool loop | `active_chat/tool_loop.py` — `ActiveChatToolLoop` |

---

## 6. 后续阶段（未实现）

- **Think Mode**：精细上下文 + 丰富工具 + 回复 subagent。`request_think_mode` 当前只是 terminal action 占位。
- **LLM call 中途 interrupt/cancel/降级策略**。
- **语义级 conversation summary / 中长期记忆 / persona 记忆增强**。
