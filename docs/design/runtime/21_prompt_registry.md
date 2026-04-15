# ShinBot 技术规范：PromptRegistry（提示词注册表）

本文档定义 ShinBot 在 Agent、系统任务与插件侧的统一 Prompt 编排机制。

数据结构与实现字段定义见 `22_prompt_registry_schema.md`。

PromptRegistry 的目标不是“存几段 prompt 文本”，而是建立一套稳定、可组合、可审计、可缓存优化的 Prompt 组装协议。

它必须回答以下问题：

- Prompt 由哪些阶段组成
- 各阶段允许承载什么类型的信息
- 动态上下文应被放在哪一层
- Tool / Skill 如何被稳定注入
- 历史遗留或外部来源的未规范化 prompt 如何兼容
- 如何通过固定顺序与稳定序列化来最大化 cache 命中

---

## 1. 设计目标

### 1.1 统一编排

- 所有 Agent、系统任务、模型驱动插件，必须通过统一 PromptRegistry 组装 prompt。
- 禁止各调用方临时手拼 system prompt 主体结构。
- 上层只描述“我要注册哪些 prompt 组件”，不直接决定最终拼接顺序。
- 阶段顺序必须由 PromptRegistry 内部强制执行，而不是由注册方或调用方决定。
- 任何绕过 PromptRegistry 直接构造最终 system prompt 的实现都视为不合规。

### 1.2 高指令遵从

- 越底层、越稳定、越强约束的内容，必须放在越靠前且固定的阶段。
- 会影响模型行为边界的内容，禁止与业务上下文混杂。
- PromptRegistry 必须保证所有调用路径遵从相同的提示词骨架。

### 1.3 最大化缓存命中

- Prompt 主体必须具有稳定的阶段顺序。
- 高复用、低变化内容必须前置。
- 高波动内容必须后置并被局部化。
- 同一语义输入应尽量生成同一序列化输出，避免无意义 cache miss。

### 1.4 可观测与可审计

- 每次模型调用都应能回溯本次使用了哪些 prompt 组件。
- Prompt 组件的来源、版本、阶段、覆盖关系必须可记录。
- 出现越权、越界或行为偏移时，应能定位是哪个阶段注入了问题内容。
- 溯源信息应由系统主动推导与标记，而不是依赖注册方自由填写。

### 1.5 兼容演进

- 首版必须支持规范化组件与遗留注入并存。
- 兼容层存在的前提下，系统仍应鼓励逐步迁移到结构化 prompt。
- 兼容层不得破坏前置阶段的系统级约束。

---

## 2. 非目标

- 本文档不规定具体某个人设文案。
- 本文档不要求首版就实现复杂 Prompt DSL。
- 本文档不要求首版支持任意模板语言。
- 本文档不负责定义历史记忆的具体存储介质，仅规定其在 Prompt 中的装配位置。

---

## 3. 核心概念

### 3.1 PromptRegistry

PromptRegistry 是统一的提示词注册与编排中心。

职责包括：

- 注册 prompt 组件
- 按阶段归类组件
- 按规则解析启用集
- 输出稳定、可序列化的最终 prompt 结构
- 为模型运行时提供可缓存、可审计的 prompt 快照

### 3.2 Prompt Component

Prompt Component 是可被注册和组合的最小 prompt 单元。

每个组件至少包含：

- `id`
- `stage`
- `kind`
- `priority`
- `version`
- `content` 或 `resolver`
- `metadata`

其中：

- `stage` 决定组件位于哪一层
- `kind` 决定组件如何被解释与展开
- `priority` 决定同阶段内的相对顺序
- `version` 用于审计与缓存失效控制

说明：

- `content` 适用于静态文本或模板
- `resolver` 适用于函数式、运行时动态生成的组件
- 同一组件不要求同时存在 `content` 与 `resolver`

### 3.3 Prompt Assembly

Prompt Assembly 指一次模型调用前，根据调用上下文解析并生成最终 Prompt 的过程。

它的结果不是随意文本，而是一个有阶段边界的结构化产物。

### 3.4 Prompt Snapshot

Prompt Snapshot 指一次调用最终落到模型侧的 Prompt 快照。

它至少应记录：

- 调用使用的组件列表
- 各阶段展开后的内容
- 最终序列化结果
- 用于缓存的签名或哈希

### 3.5 Prompt Source

Prompt Source 表示系统识别出的组件来源。

该来源不是由注册方自由声明的业务标签，而是由 PromptRegistry 在注册或装配时基于宿主模块主动推导出的标准化来源信息。

典型来源包括：

- `builtin_system`
- `agent_plugin`
- `context_plugin`
- `tooling_module`
- `skill_module`
- `legacy_bridge`
- `external_injection`

来源信息至少应包含：

- `source_type`
- `source_id`
- `owner_plugin_id`
- `module_path`

### 3.6 Prompt Logger

Prompt Logger 是 PromptRegistry 的旁路记录器。

它的职责类似现有 audit/logger 体系，但面向 prompt 装配过程，而不是命令执行过程。

它至少负责记录：

- 本次装配请求摘要
- 选中的组件与来源
- 各阶段展开结果
- Compatibility 注入情况
- 最终 prompt 签名
- 可能的冲突、覆盖与裁剪行为

---

## 4. 七阶段装配顺序

PromptRegistry 必须严格按以下顺序装配：

1. `System Base`
2. `Identity`
3. `Context`
4. `Abilities`
5. `Compatibility`
6. `Instructions`
7. `Constraints`

任何调用方不得跳过排序直接拼接。

该顺序属于框架级协议，不提供“自定义阶段顺序”入口。

---

## 5. 各阶段定义

### 5.1 Stage 1: System Base（系统基座）

这是整个 Prompt 的最底层协议层。

用于承载：

- 安全控制
- 输出格式基准
- 工具调用协议
- 角色边界
- 不可绕过的系统级行为约束

特点：

- 最稳定
- 复用率最高
- 缓存价值最高
- 优先级最高

要求：

- 不得引用具体任务内容
- 不得注入一次性业务上下文
- 不得与人设、历史记忆、任务说明混写

典型内容：

- “必须遵循的安全规则”
- “必须输出 JSON / XML / 结构化字段”
- “不得伪造工具结果”
- “当信息不足时如何回应”

### 5.2 Stage 2: Identity（身份设定）

用于为模型定义稳定的人设或角色身份。

用于承载：

- 助手人格
- 语言风格
- 角色定位
- 长期行为偏好

特点：

- 稳定性较高
- 可能按 Agent 或任务类型变化
- 应与 System Base 分离，避免人设覆盖系统约束

要求：

- 不得承载一次性任务目标
- 不得重复定义底层安全协议
- 应尽量短小、稳定、可复用

### 5.3 Stage 3: Context（上下文 / 历史记忆）

用于承载运行时上下文。

用于承载：

- 短期对话历史
- 长期记忆摘要
- 会话状态
- 环境上下文
- 检索结果

特点：

- 动态性强
- 波动大
- 是 cache 命中的主要破坏源之一

要求：

- 必须通过专门的 Context 模块生成
- 首选注册形式应为函数式 `resolver`，而不是直接注册长文本块
- 不得直接把原始历史无限拼接进 Prompt
- 应优先注入结构化摘要，而不是无界原文
- 必须具备截断、摘要、优先级筛选或窗口控制机制

设计建议：

- 长期记忆、短期历史、环境态、检索结果应视为独立上下文提供器
- PromptRegistry 只负责按 Stage 3 装配，不负责承担记忆检索本身

### 5.4 Stage 4: Abilities（能力定义）

用于向模型声明当前调用可用的能力面。

用于承载：

- Tools 列表
- Skills 列表
- 工具调用约定
- 当前会话可启用的能力开关

特点：

- 动态变化
- 与具体上下文弱相关
- 需要尽量稳定序列化，避免工具顺序抖动导致 cache miss

要求：

- Tools / Skills 必须使用统一规范描述
- 相同能力集必须输出相同顺序
- 应按稳定键排序，不得按注册时机随机排列
- 不得把工具运行结果混入本阶段
- PromptRegistry 不得成为 Tool / Skill 真相源，只负责投影当前可见能力

设计建议：

- 后续可由独立可选模块控制 abilities 的启用与过滤
- PromptRegistry 只消费其输出结果并稳定装配

### 5.5 Stage 5: Compatibility（兼容层）

用于接纳特殊来源的未规范化 Prompt 注入。

适用来源包括：

- 旧插件遗留 prompt
- 外部系统传入的 system prompt 片段
- 暂未完成结构化迁移的业务模板

存在意义：

- 给系统留迁移缓冲层
- 避免历史能力在重构阶段全部失效

要求：

- Compatibility 必须晚于规范阶段注入
- 它可以补充说明，但不得覆盖 `System Base`
- 来源标记必须由系统主动附加，例如 `legacy_plugin`, `external_bridge`
- 应被视为技术债，并逐步迁移到前四阶段或后两阶段

治理要求：

- Compatibility 必须被单独统计
- 应能按来源插件、桥接模块或调用入口聚合
- 不允许以“兼容层”名义绕过前置规范阶段

### 5.6 Stage 6: Instructions（任务指令）

用于承载本次任务本身。

用于承载：

- 当前目标
- 待完成工作
- 用户请求
- 子任务说明

特点：

- 与单次调用最强相关
- 波动高
- 应与长期身份、上下文记忆严格分层

要求：

- 不得把系统级约束重复塞入本阶段
- 不得在这里重新定义能力边界
- 任务指令应尽量表达“做什么”，而不是重复“你是谁”

### 5.7 Stage 7: Constraints（执行约束）

用于在最终输出前再次强调本次执行必须满足的强制条件。

用于承载：

- 本次输出限制
- 结果格式硬约束
- 时间、篇幅、权限、资源预算等硬限制
- 特定任务下的禁止事项

特点：

- 靠后
- 强提醒
- 作用是“最后收束”，不是替代 System Base

要求：

- 必须是本次调用相关的约束
- 应简洁、明确、可执行
- 若与前置阶段冲突，以更高层级规范优先，并记录冲突

---

## 6. 注册类型

PromptRegistry 首版至少应支持以下注册类型。

### 6.1 Static Text

固定文本块。

适用于：

- 系统基座协议
- 稳定人设
- 固定格式约束

特点：

- 最简单
- 最稳定
- 最利于缓存

### 6.2 Template

带参数占位的模板组件。

适用于：

- 指令模板
- 任务骨架
- 约束模板

要求：

- 模板入参必须显式声明
- 缺失参数时应报错或降级，而不是静默输出残缺内容

### 6.3 Resolver

通过运行时回调动态生成内容的组件。

适用于：

- 上下文摘要
- 动态技能清单
- 会话状态注入
- 长期记忆与短期上下文提供器

要求：

- Resolver 输出必须是稳定字符串或结构化片段
- 不得依赖不可控随机序
- 应限制副作用，优先设计为纯函数式读取

### 6.4 Reference Bundle

引用其它组件集合的组合注册。

适用于：

- Agent 默认提示词包
- 某类任务的标准骨架
- 多租户或多角色共享配置

要求：

- Bundle 只声明依赖关系，不直接破坏阶段顺序
- 被引用组件仍按自身阶段进入最终组装

### 6.5 External Injection

来自外部调用者或兼容桥的直接注入。

适用于：

- 历史遗留接口
- 暂未结构化的系统桥接层

要求：

- 默认只能进入 `Compatibility` 或 `Instructions`
- 不得默认注入 `System Base`
- 外部注入的来源身份必须由系统桥接层主动标注

---

## 7. 组装规则

### 7.1 固定阶段优先

最终装配必须先按阶段排序，再按阶段内顺序排序。

阶段顺序不允许被注册参数、插件优先级或运行时调用者覆写。

### 7.2 阶段内顺序

同阶段内建议按以下键排序：

1. `priority`
2. `id`
3. `version`

要求：

- 排序规则必须稳定
- 不得依赖哈希表遍历顺序

### 7.3 覆盖与冲突

PromptRegistry 不鼓励“后者完全覆盖前者”的隐式行为。

规则应为：

- 默认追加，不默认覆盖
- 必须显式声明 `replace` / `disable` / `override`
- 涉及高优先级阶段时，覆盖应被记录并可审计
- 涉及 `System Base` 的覆盖默认应拒绝，而不是仅记录

### 7.4 缺省阶段策略

若调用方未提供某阶段内容：

- `System Base` 不允许为空
- `Identity` 可为空
- `Context` 可为空
- `Abilities` 可为空，但若存在 tool calling 则应显式输出能力声明
- `Compatibility` 可为空
- `Instructions` 通常不应为空
- `Constraints` 可为空

### 7.5 结构化输出优先

内部应优先维护结构化阶段对象，再在最后一步序列化为模型输入。

不建议从一开始就只保留扁平字符串。

### 7.6 来源推导优先

Prompt 组件的来源不得依赖注册方手工命名。

PromptRegistry 应主动从以下维度推导来源：

- 组件注册发生在哪个插件或内置模块中
- 当前 resolver 绑定在哪个模块对象上
- 当前注入是否来自外部桥接层
- 当前 abilities 是否来自工具/技能控制模块

这样可以避免：

- 同源模块使用不一致命名
- 注册方漏填来源
- 后期审计时来源口径混乱

若系统无法自动识别来源，应标记为 `unknown_source` 并记录宿主模块信息，而不是静默留空。

---

## 8. 缓存优化原则

### 8.1 稳定内容前置

以下内容应尽量前置并长期稳定：

- System Base
- Identity
- 低变化 Abilities

### 8.2 高波动内容后置

以下内容应尽量后置：

- 动态 Context
- 本次 Instructions
- 本次 Constraints

### 8.3 规范序列化

PromptRegistry 必须保证：

- 相同组件集合输出相同顺序
- 相同结构输出相同换行规则
- 相同工具集输出相同枚举顺序
- 避免时间戳、随机 ID、无意义空白进入 prompt 主体

### 8.4 Cache Key 构成建议

Prompt 缓存签名至少应考虑：

- 阶段顺序
- 组件 `id`
- 组件 `version`
- 模板参数摘要
- 上下文摘要签名
- 工具/技能集合签名

不建议直接只对整段最终文本做黑盒哈希而忽略来源结构。

### 8.5 上下文预算治理

PromptRegistry 虽不负责上下文检索，但必须为 Stage 3 预留预算治理接口。

建议至少支持：

- `max_context_tokens`
- `max_history_turns`
- `memory_summary_required`
- `truncate_policy`

目的不是让 PromptRegistry 直接做记忆系统，而是确保上下文进入 Prompt 前已被预算化。

---

## 9. 与 Model Runtime 的关系

PromptRegistry 属于 Agent / Plugin 调用 Model Runtime 之前的上游装配层。

推荐调用链：

`Agent / Plugin -> PromptRegistry -> Model Runtime -> Route Resolver -> LiteLLM`

其中：

- PromptRegistry 负责“喂给模型什么”
- Model Runtime 负责“由哪个模型执行”

二者不得互相替代。

---

## 10. 与 Context / Tool / Skill 模块的边界

### 10.1 Context 模块

Context 模块负责准备上下文，不负责决定最终 prompt 全局顺序。

它只向 PromptRegistry 交付 Stage 3 内容。

推荐形式：

- 以函数式 resolver 注册
- 按“短期历史 / 长期记忆 / 环境状态 / 检索结果”拆分
- 由系统在装配时统一调度

### 10.2 Tool / Skill 注册

ToolRegistry / SkillRegistry 负责能力元数据管理。

PromptRegistry 只负责把“当前可见的能力声明”装配到 Stage 4。

若后续 abilities 由独立模块控制启用与过滤，PromptRegistry 仍只消费投影结果，不接管该控制逻辑。

### 10.3 外部桥接层

兼容桥只允许把未规范化内容送入 Stage 5 或 Stage 6。

不得直接突破到 Stage 1。

桥接层还必须把来源身份交给 PromptRegistry 做系统级标记。

---

## 11. 来源与宿主模型

### 11.1 宿主优先

Prompt 组件的“拥有者”应优先由系统宿主关系确定，而不是由组件文本自报。

宿主对象可能包括：

- 内置系统模块
- Agent 插件
- 上下文管理插件
- Tool / Skill 控制模块
- 外部兼容桥

### 11.2 主动标记规则

系统在注册组件时应主动补全：

- `owner_plugin_id`
- `owner_module`
- `source_type`
- `source_id`

若组件来自 resolver，还应记录 resolver 所属函数或模块名。

### 11.3 统一口径

来源字段应使用系统定义的枚举或规范值，避免自由文本。

不应允许注册方直接提交随意命名，如：

- `pluginA`
- `Plugin-A`
- `agent plugin a`

这些命名差异都应由系统在登记层统一归一化。

## 12. 审计与可观测要求

每次模型调用至少应记录：

- `prompt_profile_id` 或等价配置标识
- 最终使用的组件列表
- 各组件阶段
- 组件版本
- 是否使用 compatibility 注入
- 最终 prompt 签名
- 上下文摘要长度
- abilities 数量
- 来源插件 / 来源模块 / 来源类型

这些记录建议通过独立 `Prompt Logger` 输出，设计风格可参考现有 audit/logger 体系：

- 结构化记录
- JSON 可落盘
- 可选接入数据库
- 可选旁路文件落盘
- 允许后续转发到监控/审计面板

必要时可附加：

- 最终 prompt 全文快照
- 脱敏后的阶段内容
- 被裁剪的上下文统计
- Compatibility 来源详情

### 12.1 Prompt Snapshot 建议字段

建议至少包含：

- `timestamp`
- `profile_id`
- `caller`
- `session_id`
- `instance_id`
- `route_id` 或 `model_id`
- `components`
- `stages`
- `prompt_signature`
- `cache_key`
- `compatibility_used`
- `truncation`
- `metadata`

### 12.2 Prompt Component Record 建议字段

建议至少包含：

- `component_id`
- `stage`
- `kind`
- `version`
- `priority`
- `source_type`
- `source_id`
- `owner_plugin_id`
- `module_path`
- `selected`

### 12.3 Prompt Logger 行为要求

- 不得影响主调用成功路径
- 记录失败不应阻断模型执行
- 支持脱敏模式，避免泄露敏感上下文
- 应支持按日滚动或数据库持久化

---

## 13. 首版实现建议

首版建议最小实现以下对象：

- `PromptRegistry`
- `PromptComponent`
- `PromptAssemblyRequest`
- `PromptAssemblyResult`
- `PromptSnapshot`
- `PromptLogger`
- `PromptSource`

首版建议先支持：

- Static Text
- Template
- Resolver
- Reference Bundle

并将 `External Injection` 明确限制在兼容入口。

此外首版实现建议同步定义：

- 组件宿主推导规则
- source 枚举
- PromptSnapshot 的结构化 schema
- 具体对象字段与接口签名

---

## 14. 迁移策略

从历史 prompt 迁移到 PromptRegistry 时，建议遵循以下顺序：

1. 先抽出 `System Base`
2. 再抽出稳定 `Identity`
3. 再把工具与技能迁移到 `Abilities`
4. 将历史长提示拆成 `Instructions`
5. 暂时无法结构化的部分放入 `Compatibility`
6. 逐步清空 `Compatibility`

目标不是“让旧 prompt 原样搬家”，而是把原本混杂在一起的语义拆回正确阶段。

---

## 15. 强制约束

- 所有面向 LLM 的系统级调用必须通过 PromptRegistry 装配。
- `System Base` 必须存在且位于最前。
- `Compatibility` 不得覆盖 `System Base`。
- `Instructions` 与 `Constraints` 必须位于后段。
- PromptRegistry 的排序与序列化必须稳定可复现。
- 阶段顺序必须由 PromptRegistry 强制控制，不允许调用方自定义。
- 来源标记必须由系统主动推导，不依赖注册方自由填写。
- Stage 3 上下文应优先以函数式 resolver 提供，而不是原始长文本拼接。
- Stage 4 abilities 只允许作为外部能力模块的投影，不得在 PromptRegistry 内部自成真相源。
- PromptSnapshot 与 PromptLogger 必须作为设计内建对象存在。
- 任何绕过 PromptRegistry 的直接 system prompt 拼接都视为不合规实现。
