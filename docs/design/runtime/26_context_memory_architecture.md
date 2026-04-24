# ShinBot 技术规范：上下文与三级记忆架构

本文档定义 ShinBot 在 Agent 运行时中的上下文构建、短中长三级记忆分层，以及面向 Prompt Cache 的块投影模型。

本文档讨论的是“系统应该怎样设计”，不是当前实现行为说明。当前实现中的参数与实际行为，应继续以 `docs/internals/parameters/01_context_management.md` 为准。

相关文档：

- `21_prompt_registry.md`
- `22_prompt_registry_schema.md`
- `24_attention_driven_conversation_workflow.md`
- `25_media_semantics_and_meme_handling.md`

---

## 1. 设计目标

### 1.1 保留块压缩优势，但解耦上下文真相与 Prompt 表示

ShinBot 现有上下文系统里，`Block` 设计对减少 message 数量、缩短缓存标记间隔、提高 Prefix Cache 命中率非常重要，这个能力必须保留。

但 `Block` 不应继续承担以下全部职责：

- 会话真相存储
- 短期记忆载体
- 中期压缩单元
- 长期记忆来源
- alias 快照容器
- Prompt message 直接表示

本设计要求：

- 保留 `Block` 作为 Prompt / Cache 友好的投影单元。
- 将“记忆事实”与“Prompt 表示”拆开。

### 1.2 为三级记忆编排提供稳定基础

上下文系统需要演化为三层记忆：

- **短期记忆**：最近会话块、当前开放块、未消费工作信息。
- **中期记忆**：会话压缩记忆、episode 级摘要、阶段性承诺与线索。
- **长期记忆**：稳定偏好、身份称呼、关系线索、长期任务与用户事实。

### 1.3 强化缓存稳定性与局部重建能力

系统应最大化保留稳定前缀，不因尾部变化而重建整个上下文。

因此上下文重建必须满足：

- 旧块默认稳定，不轻易变动。
- 当前工作区允许频繁变化。
- 失效时优先局部重建尾部，而不是全量销毁重建。

---

## 2. Prompt 装配顺序

PromptRegistry 仍然遵循 `21_prompt_registry.md` 中定义的七阶段顺序；本设计只重新定义 Stage 4 的内部结构，以及 Stage 6 与上下文系统的边界。

### 2.1 运行时分层视图

从运行时语义上，可将一次请求中的 Prompt 内容理解为五层：

1. **固定层**
   - System Base
   - Persona / Identity
2. **能力层**
   - Tools / Abilities
3. **记忆层**
   - 长期记忆
   - 中期记忆
   - 短期记忆
4. **工作层**
   - 本轮待处理输入
   - workflow 增量输入
   - 本轮任务说明
5. **约束层**
   - 当前轮行为边界
   - 输出要求
   - 活跃 alias 约束

### 2.2 映射到 PromptRegistry 的方式

- 固定层 -> Stage 1 / Stage 2
- 能力层 -> Stage 3
- 记忆层 -> Stage 4
- 工作层 -> Stage 6
- 约束层 -> Stage 7

说明：

- Stage 5 Compatibility 仍保留为兼容层，不参与长期记忆沉淀。
- Stage 7 不应被建模为“第二个 system message”；它仍然属于最后一条 `user` message 的尾部约束。
- Stage 4 只承载“稳定可复用的上下文投影”，不直接包含 workflow 运行中后续轮转的即时输入增量。

---

## 3. 核心原则

### 3.1 事实源、记忆、Prompt 投影三者分离

必须明确区分三种对象：

- **事实源**
  - 原始消息、媒体语义、身份信息、时间线事件
- **记忆对象**
  - Working Set、Episode、Semantic Fact
- **Prompt 投影**
  - 为模型请求构建的 `PromptBlock` 与 content block

任何一层都不得反向成为另一层的唯一真相来源。

### 3.2 只让尾部可变

系统应遵循：

- 前缀默认稳定
- 尾部允许重算
- 工作区始终可变

这比“全量重建上下文”更符合 Prefix Cache 的目标。

### 3.3 Block 保留，但只作为投影单位

本设计显式保留 `Block`，但重新定义其职责：

- `MemoryBlock`：记忆层片段，表达语义边界。
- `PromptBlock`：Prompt 层片段，表达缓存边界与 message 压缩边界。

二者可以一一对应，但不能视为同一个概念。

---

## 4. 三级记忆模型

### 4.1 短期记忆 (Short-Term Memory)

短期记忆负责承接最近会话和当前轮工作状态。

包含两部分：

- **当前开放块 (`OpenBlock`)**
  - 尚未 seal 的工作区
  - 可继续接收新消息
  - 可受 alias 变化、分段规则变化影响而重算
- **固化块队列 (`SealedBlockDeque`)**
  - 已 seal 的时间顺序块队列
  - 追加在尾部
  - 淘汰从头部开始
  - 前缀默认稳定

设计结论：

- 短期记忆的“构建过程”具有栈顶工作区特征。
- 短期记忆的“已固化块存储”更适合 `queue / deque`，而不是纯栈。

### 4.2 中期记忆 (Mid-Term Memory)

中期记忆负责沉淀会话级片段，而不是直接保存原始 Prompt block。

典型对象包括：

- `EpisodeMemory`
  - 一段对话阶段的摘要
  - 包含参与者、主要话题、未完成事项、情绪线索、关键图片/引用
- `CompressedMemory`
  - 从一批被淘汰的短期块中提炼出的压缩摘要
  - 用于后续快速回忆

中期记忆的生成方式：

- 从短期记忆中若干 `SealedBlock` 提升
- 由压缩器基于淘汰候选生成
- 可进一步合并，但不回写原始短期块

### 4.3 长期记忆 (Long-Term Memory)

长期记忆负责保存稳定语义事实，而不是历史消息本身。

典型对象包括：

- `SemanticFact`
  - 用户偏好
  - 稳定称呼
  - 关系与身份线索
  - 长期任务与承诺
  - 重要世界状态

长期记忆必须具备：

- 结构化字段
- 可显式更新 / 撤销
- 不依赖短期块是否仍在上下文中

---

## 5. Block 模型

### 5.1 `MemoryBlock`

`MemoryBlock` 是记忆层片段，表达“哪些事件应被视为同一语义段落”。

建议字段：

- `block_id`
- `session_id`
- `started_at_ms`
- `ended_at_ms`
- `participants`
- `event_refs`
- `semantic_tags`
- `summary`
- `sealed`
- `revision`
- `metadata`

### 5.2 `PromptBlock`

`PromptBlock` 是 Prompt 投影层片段，表达“哪些内容应该被合并成尽量少的 model messages，以利于缓存”。

建议字段：

- `prompt_block_id`
- `source_memory_block_ids`
- `role`
- `contents`
- `token_estimate`
- `cache_stable`
- `sealed`
- `projection_revision`
- `metadata`

### 5.3 Block 切分原则

`PromptBlock` 的切分目标不是还原事实边界，而是优化模型请求：

- 压缩 message 数量
- 缩短缓存标记间隔
- 尽量保留稳定前缀
- 控制单块 token 大小
- 控制时间间隔导致的语义跳变

因此：

- 一个 `MemoryBlock` 可以投影为一个或多个 `PromptBlock`
- 多个较小的 `MemoryBlock` 也可以在投影阶段拼成一个 `PromptBlock`

---

## 6. 数据结构选择

### 6.1 不推荐“整个上下文系统 = 栈”

完整上下文系统不应被建模为单一栈，因为：

- 长期记忆不是 LIFO 结构
- alias / identity 更接近索引
- PromptBlock 更关注缓存边界，而不是入栈顺序
- eviction 主要发生在最老块，不是最新块

### 6.2 推荐结构

推荐采用混合结构：

- `EventLog`
  - append-only 事实流
- `OpenBlock`
  - 当前工作区
- `SealedBlockDeque`
  - 已 seal 的短期块双端队列
- `EpisodeStore`
  - 中期片段存储
- `SemanticStore`
  - 长期事实索引

### 6.3 `deque` 的适用性

对已 seal 的短期块，`deque` 更适合：

- 新块在尾部追加
- 淘汰从头部移除
- 尾部少量块可失效重算
- 左侧稳定前缀可长期复用

这是 ShinBot 上下文缓存优化的核心结构基础。

---

## 7. 生命周期与状态转换

### 7.1 事件流入

每条新消息进入系统后，先被规范化为统一事件对象，再进入当前会话的 `OpenBlock`。

### 7.2 Seal

当满足以下条件之一时，当前 `OpenBlock` 应被 seal：

- token 预算达到上限
- 时间间隔超过阈值
- 发言者连续性被打断
- workflow 轮次边界要求封口
- 显式控制指令要求封口

seal 后：

- 生成 `MemoryBlock`
- 投入 `SealedBlockDeque`
- `OpenBlock` 重置为新的工作区

### 7.3 Promote

当短期块满足提升条件时：

- 一批 `MemoryBlock` 可提升为 `EpisodeMemory`
- 被淘汰候选可触发 `CompressedMemory`
- 稳定事实可提炼进 `SemanticStore`

### 7.4 Evict

Eviction 针对短期 `SealedBlockDeque` 的头部进行，不直接删除长期事实。

淘汰前可触发：

- 压缩摘要生成
- episode 提升
- 引用关系保留

### 7.5 Invalidate / Rebuild

系统应支持局部失效，而不是优先全量重建。

推荐操作：

- `invalidate_open_block()`
- `invalidate_tail(from_block_id)`
- `reproject_prompt_blocks(from_revision)`
- `rebuild_all()`

其中：

- alias 轻微变化通常只影响尾部投影
- schema 迁移或切分策略大改才触发全量重建

---

## 8. Alias、身份与记忆边界

alias 映射不能继续作为 `PromptBlock` 的内部副产品存在。

应将其拆为两个层面：

- **Participant Index**
  - 会话参与者索引
  - 保存平台 ID、显示名、活跃度、关系信息
- **Alias Projection**
  - 当前 Prompt 中采用何种简称
  - 仅影响渲染，不影响长期事实本体

这意味着：

- identity 更新不应直接篡改历史 `MemoryBlock`
- alias 变化可以触发相关 `PromptBlock` 重投影
- 长期记忆中的稳定称呼属于 `SemanticStore`

---

## 9. Context Runtime 设计

### 9.1 职责

新的上下文运行时应退化为控制面，而不是全能管理器。

`ContextRuntime` 负责：

- 管理会话 revision
- 编排消息流入、seal、promote、evict、invalidate
- 调用 projector 生成 Prompt bundle
- 维护局部重建边界

`ContextRuntime` 不负责：

- 内嵌 Prompt 具体渲染规则
- 直接持有所有 alias 业务规则
- 直接承担长期记忆的全部表达

### 9.2 建议接口

```python
class ContextRuntime:
    def append_event(self, session_id: str, event: ConversationEvent) -> None: ...
    def seal_open_block(self, session_id: str) -> MemoryBlock | None: ...
    def evict_head_blocks(self, session_id: str, *, budget: TokenBudget) -> EvictionResult: ...
    def invalidate_tail(self, session_id: str, *, from_block_id: str) -> None: ...
    def project_prompt_bundle(self, session_id: str, *, work_packet: WorkPacket) -> PromptMemoryBundle: ...
```

---

## 10. Prompt 投影模型

### 10.1 `PromptMemoryBundle`

上下文层对 PromptRegistry 的唯一输出，应是单一 bundle，而不是多个零散方法。

建议字段：

- `context_messages`
- `instruction_blocks`
- `constraint_blocks`
- `cacheable_message_count`
- `projection_revision`
- `metadata`

PromptRegistry 不应直接协调：

- 历史 context message 构建
- inactive alias message 构建
- unread instruction block 构建
- active alias constraint 构建

这些都应由上下文投影层一次性给出。

### 10.2 投影顺序

记忆层内部建议按以下顺序投影：

1. 长期记忆
2. 中期记忆
3. 短期已 seal 块
4. 当前开放块的必要投影

工作层输入不属于记忆层投影的一部分，而由 workflow 作为当前轮任务包注入。

### 10.3 投影状态

在迁移期，格式化过程仍然需要少量可变资源，例如短 `msgid` 分配、图片短 ID 分配和图片摘要引用。

这些资源必须集中在显式的投影状态对象中，例如 `ContextProjectionState`，而不是散落在各个 builder 的渲染分支里。

迁移目标：

- builder 只负责格式化。
- `ContextProjectionState` 暂时组合格式化所需的可变资源。
- 短 `msgid` 分配与图片引用解析应拆为独立 projector，例如 `MessageIdProjector` 与 `ImageReferenceProjector`。
- 后续可将 `ContextProjectionState` 替换为只读 snapshot 或独立 projection store。

### 10.4 Block 投影过渡层

在迁移期，短期上下文块仍需要落回旧的 `ContextBlockState`，因为现有持久化和淘汰逻辑仍依赖它。

但 builder 不应直接把所有内容一次性写成 Chat Completions content dict。推荐先生成 `PromptBlockProjection`：

- `text_parts`
- `token_estimate`
- `metadata`
- `kind`
- `sealed`

然后再由最后一跳转换为旧的 `ContextBlockState.contents`。

这一步的意义是先拆开：

- 文本块规划
- Prompt content dict 形态
- 持久化状态兼容层

---

## 11. 持久化要求

持久化层应保存领域状态，而不是直接保存 Prompt message 结构。

应优先持久化：

- `MemoryBlock`
- `EpisodeMemory`
- `CompressedMemory`
- `SemanticFact`
- `ParticipantIndex`
- `ContextRevision`

不推荐将以下对象作为长期主存：

- 直接可发给模型的 `contents: list[dict]`
- 带缓存标记语义的投影字段
- 仅为本轮 Prompt 临时存在的 stage patch

---

## 12. 与现有实现的迁移原则

### 12.1 保留能力

迁移过程中必须保留以下现有优势：

- 上下文块压缩 message 数量
- 尾部增量重建
- 上下文淘汰与压缩
- 活跃 alias 约束
- 未消费消息的指令层组织

### 12.2 优先拆分顺序

建议按以下顺序改造：

1. 引入 `PromptMemoryBundle`，收口 PromptRegistry 对上下文层的多点调用。
2. 将 `ContextStageBuilder` / `InstructionStageBuilder` 改为纯投影器，不再修改 session state。
3. 将短期块存储重构为 `OpenBlock + SealedBlockDeque`。
4. 引入中期 `EpisodeMemory / CompressedMemory`。
5. 将长期事实迁移到独立 `SemanticStore`。
6. 最后再拆除旧的巨型 `ContextManager`。

### 12.3 兼容原则

在迁移完成前，可以保留兼容门面：

- 对外仍暴露 `ContextManager` 风格接口
- 对内逐步转向 `ContextRuntime + Stores + Projectors`

但新功能不应继续堆入旧门面。

---

## 13. 强制约束

- `Block` 必须保留，但只能作为投影与缓存优化单位，不得继续充当上下文唯一真相。
- 短期已固化块必须使用时间有序结构；推荐 `deque`。
- 工作区与已固化块必须分离。
- PromptRegistry 对上下文层的交互必须收口为单一 bundle 接口。
- 长期记忆不得依赖短期 PromptBlock 是否仍然存在。
- workflow 增量输入属于工作层，不属于 Stage 4 长期上下文。

---

## 14. 术语表

- **OpenBlock**
  - 当前仍可继续写入的短期工作块。
- **SealedBlock**
  - 已封口、默认不可变的短期记忆块。
- **MemoryBlock**
  - 记忆层语义片段。
- **PromptBlock**
  - Prompt 层缓存友好的压缩片段。
- **EpisodeMemory**
  - 中期会话片段摘要。
- **SemanticFact**
  - 长期稳定语义事实。
- **PromptMemoryBundle**
  - 上下文层一次性产出的 Prompt 注入结果。

---

## 15. 总结

ShinBot 的下一代上下文架构应采用：

- **固定层 / 能力层 / 记忆层 / 工作层 / 约束层** 的 Prompt 组织方式
- **短期记忆 = OpenBlock + SealedBlockDeque**
- **中期记忆 = Episode + Compression**
- **长期记忆 = SemanticStore**
- **Block 保留，但降级为 Prompt / Cache 友好的投影单位**

这样既能保留现有块压缩与缓存命中优势，也能为后续三级记忆编排、局部失效重建与长期语义沉淀提供稳定基础。
