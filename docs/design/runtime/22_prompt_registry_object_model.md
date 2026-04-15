# ShinBot 技术规范：PromptRegistry 对象模型 (PromptRegistry Object Model)

本文档定义 PromptRegistry 首版实现所需的核心对象、字段约束与运行关系。

它不是新的架构设计，而是 `21_prompt_registry.md` 的对象化落地版本，用于回答：

- PromptRegistry 运行时到底维护哪些对象
- 这些对象之间如何关联
- 哪些字段是必填、哪些字段可选
- 哪些字段由系统推导，哪些字段由调用方提供
- PromptSnapshot / PromptLogger 应如何建模

---

## 1. 设计目标

- 为 PromptRegistry 提供稳定的实现骨架。
- 保证“阶段顺序强制执行、来源系统主动推导、上下文以 resolver 为主”的设计能直接映射到代码。
- 为后续数据库持久化、审计、缓存和 WebUI 观察能力预留统一对象模型。

---

## 2. 核心对象总览

PromptRegistry 首版建议至少包含以下对象：

1. `PromptComponent`
2. `PromptSource`
3. `PromptProfile`
4. `PromptAssemblyRequest`
5. `PromptAssemblyResult`
6. `PromptStageBlock`
7. `PromptComponentRecord`
8. `PromptSnapshot`
9. `PromptLoggerRecord`

建议运行关系为：

`PromptProfile + PromptAssemblyRequest -> PromptRegistry -> PromptAssemblyResult -> PromptSnapshot -> PromptLogger`

---

## 3. 枚举与基础类型

### 3.1 PromptStage

`PromptStage` 必须为固定枚举，不允许动态扩展：

- `system_base`
- `identity`
- `context`
- `abilities`
- `compatibility`
- `instructions`
- `constraints`

要求：

- 枚举值应使用稳定 snake_case
- 最终排序由框架内部固定，不允许外部重排

### 3.2 PromptComponentKind

首版建议支持：

- `static_text`
- `template`
- `resolver`
- `bundle`
- `external_injection`

说明：

- `resolver` 主要服务于 Stage 3 Context，也可用于动态 abilities
- `external_injection` 默认只允许进入 `compatibility` 或 `instructions`

### 3.3 PromptSourceType

首版建议支持：

- `builtin_system`
- `agent_plugin`
- `context_plugin`
- `tooling_module`
- `skill_module`
- `legacy_bridge`
- `external_injection`
- `unknown_source`

该枚举由系统维护，不允许注册方自由扩展自由文本。

---

## 4. PromptSource

`PromptSource` 表示系统推导出的组件来源。

建议字段：

- `source_type: PromptSourceType`
- `source_id: str`
- `owner_plugin_id: str`
- `owner_module: str`
- `module_path: str`
- `resolver_name: str`
- `is_builtin: bool`
- `metadata: dict[str, Any]`

字段说明：

- `source_type`
  - 来源分类，由系统归一化
- `source_id`
  - 来源对象 ID，例如插件 ID、桥接器 ID、内置模块名
- `owner_plugin_id`
  - 若组件由插件宿主提供，则记录插件 ID
- `owner_module`
  - Python 模块名或逻辑宿主名
- `module_path`
  - 可选的完整模块路径，便于审计
- `resolver_name`
  - 若组件来自函数式 resolver，则记录函数名
- `is_builtin`
  - 是否为内置系统来源

要求：

- `PromptSource` 必须由系统主动推导
- 注册方不得直接覆盖 `source_type`
- 若无法识别来源，应使用 `unknown_source`

---

## 5. PromptComponent

`PromptComponent` 是 PromptRegistry 中最小的注册单元。

建议字段：

- `id: str`
- `stage: PromptStage`
- `kind: PromptComponentKind`
- `version: str`
- `priority: int`
- `enabled: bool`
- `cache_stable: bool`
- `content: str`
- `template_vars: list[str]`
- `resolver_ref: str`
- `bundle_refs: list[str]`
- `tags: list[str]`
- `metadata: dict[str, Any]`

### 5.1 字段约束

- `id`
  - 在注册表内唯一
- `stage`
  - 必须属于固定七阶段之一
- `kind`
  - 决定装配展开方式
- `version`
  - 用于审计、缓存签名与灰度迁移
- `priority`
  - 仅影响同阶段内排序
- `enabled`
  - 用于软禁用组件
- `cache_stable`
  - 标记该组件内容是否预期稳定，可用于 cache 策略提示
- `content`
  - `static_text` 或 `template` 时使用
- `template_vars`
  - 模板所需变量名清单
- `resolver_ref`
  - resolver 的系统注册引用，而不是任意自由对象
- `bundle_refs`
  - bundle 引用的子组件列表

### 5.2 组合规则

- `static_text` 必须有 `content`
- `template` 必须有 `content` 与 `template_vars`
- `resolver` 必须有 `resolver_ref`
- `bundle` 必须有 `bundle_refs`
- `external_injection` 不应长期持久化为常规组件，通常是一次性装配输入

### 5.3 不建议字段

首版不建议在组件对象中直接存：

- 最终展开文本
- 运行时 token 统计
- 调用级上下文值

这些应属于装配结果，而不是组件定义本身。

---

## 6. PromptProfile

`PromptProfile` 表示某类 Agent / 任务类型默认启用的 Prompt 组合方案。

它的作用是避免每次调用都手工列出完整组件集。

建议字段：

- `id: str`
- `display_name: str`
- `description: str`
- `enabled: bool`
- `base_components: list[str]`
- `default_constraints: list[str]`
- `default_metadata: dict[str, Any]`

典型例子：

- `agent.default`
- `agent.planner`
- `plugin.summary`
- `system.classifier`

规则：

- `PromptProfile` 只声明默认启用关系
- 不改变组件原有阶段
- 不提供重排阶段的能力

---

## 7. PromptAssemblyRequest

`PromptAssemblyRequest` 表示一次装配请求的输入。

建议字段：

- `profile_id: str`
- `caller: str`
- `session_id: str`
- `instance_id: str`
- `route_id: str`
- `model_id: str`
- `task_id: str`
- `component_overrides: list[str]`
- `disabled_components: list[str]`
- `instruction_payload: str | dict[str, Any]`
- `constraint_payload: str | dict[str, Any]`
- `template_inputs: dict[str, Any]`
- `context_inputs: dict[str, Any]`
- `abilities_inputs: dict[str, Any]`
- `compatibility_payloads: list[dict[str, Any]]`
- `metadata: dict[str, Any]`

### 7.1 字段职责

- `profile_id`
  - 指向默认 prompt profile
- `caller`
  - 当前调用方，例如 `agent.runtime`, `plugin.summary`
- `route_id` / `model_id`
  - 用于后续 PromptSnapshot 与 Model Runtime 关联
- `component_overrides`
  - 额外启用的组件 ID
- `disabled_components`
  - 本次禁用的组件 ID
- `instruction_payload`
  - 本次任务指令原始输入
- `constraint_payload`
  - 本次任务约束原始输入
- `template_inputs`
  - 模板组件展开时需要的变量
- `context_inputs`
  - 传给上下文 resolver 的上下文参数
- `abilities_inputs`
  - 传给 abilities 投影器的运行时输入
- `compatibility_payloads`
  - 兼容层的外部注入内容

### 7.2 不允许的能力

`PromptAssemblyRequest` 不应允许：

- 直接提交最终阶段顺序
- 自定义插入新 stage
- 自定义来源类型

---

## 8. PromptStageBlock

`PromptStageBlock` 表示某一阶段在本次装配后的结果。

建议字段：

- `stage: PromptStage`
- `components: list[PromptComponentRecord]`
- `rendered_text: str`
- `truncated: bool`
- `token_estimate: int`
- `metadata: dict[str, Any]`

作用：

- 保留阶段边界
- 便于调试、审计与差异分析
- 为 WebUI 后续查看 prompt 结构提供自然对象

---

## 9. PromptComponentRecord

`PromptComponentRecord` 表示某个组件在本次装配中的实际使用记录。

它不是静态定义，而是运行期选中结果。

建议字段：

- `component_id: str`
- `stage: PromptStage`
- `kind: PromptComponentKind`
- `version: str`
- `priority: int`
- `selected: bool`
- `source: PromptSource`
- `rendered_text: str`
- `text_hash: str`
- `cache_stable: bool`
- `truncated: bool`
- `metadata: dict[str, Any]`

用途：

- 支撑 PromptSnapshot
- 支撑 PromptLogger
- 支撑阶段内差异分析

---

## 10. PromptAssemblyResult

`PromptAssemblyResult` 表示 PromptRegistry 对一次请求的完整输出。

建议字段：

- `profile_id: str`
- `caller: str`
- `stages: list[PromptStageBlock]`
- `ordered_components: list[PromptComponentRecord]`
- `final_prompt: str`
- `prompt_signature: str`
- `cache_key: str`
- `compatibility_used: bool`
- `has_unknown_source: bool`
- `truncation: dict[str, Any]`
- `metadata: dict[str, Any]`

### 10.1 设计要求

- `final_prompt` 必须由阶段序列化得到
- `prompt_signature` 应与最终 prompt 稳定对应
- `cache_key` 可以在 `prompt_signature` 基础上叠加模型或 route 维度
- `compatibility_used` 便于快速统计技术债
- `has_unknown_source` 便于溯源质量监控

---

## 11. PromptSnapshot

`PromptSnapshot` 表示一次模型调用侧可存档、可审计、可查询的 prompt 快照。

建议字段：

- `id: str`
- `timestamp: str`
- `profile_id: str`
- `caller: str`
- `session_id: str`
- `instance_id: str`
- `route_id: str`
- `model_id: str`
- `prompt_signature: str`
- `cache_key: str`
- `components: list[PromptComponentRecord]`
- `stages: list[PromptStageBlock]`
- `final_prompt: str`
- `compatibility_used: bool`
- `truncation: dict[str, Any]`
- `metadata: dict[str, Any]`

### 11.1 存储建议

- `PromptSnapshot` 适合结构化落库
- `final_prompt` 可按配置选择是否全文保存
- 若存在敏感上下文，应支持脱敏版快照

### 11.2 与 Model Runtime 的关系

建议在模型执行记录中保存：

- `prompt_snapshot_id`
- `prompt_signature`

这样可以让一次模型调用与其 prompt 快照直接关联。

---

## 12. PromptLoggerRecord

`PromptLoggerRecord` 表示一次 prompt 装配旁路日志记录。

它更像 audit log，而不是完整业务对象。

建议字段：

- `timestamp: str`
- `entry_type: str`
- `profile_id: str`
- `caller: str`
- `session_id: str`
- `instance_id: str`
- `route_id: str`
- `model_id: str`
- `prompt_signature: str`
- `cache_key: str`
- `compatibility_used: bool`
- `selected_component_count: int`
- `unknown_source_count: int`
- `truncation_summary: dict[str, Any]`
- `metadata: dict[str, Any]`

建议默认 `entry_type = "prompt_assembly"`。

### 12.1 与 PromptSnapshot 的区别

- `PromptSnapshot` 偏完整、可回放
- `PromptLoggerRecord` 偏轻量、可流式记录

二者可以同时存在：

- Snapshot 用于调试与审计
- LoggerRecord 用于监控与索引

---

## 13. PromptRegistry 服务接口建议

首版建议提供如下接口：

### 13.1 注册接口

- `register_component(component)`
- `register_profile(profile)`
- `register_resolver(name, fn)`

### 13.2 查询接口

- `get_component(component_id)`
- `get_profile(profile_id)`
- `list_components(stage=None)`
- `list_profiles()`

### 13.3 装配接口

- `assemble(request) -> PromptAssemblyResult`
- `create_snapshot(result, request) -> PromptSnapshot`

### 13.4 记录接口

- `log_snapshot(snapshot)`
- `log_assembly(result, request)`

要求：

- `assemble()` 必须是唯一阶段排序入口
- 外部模块不得自行跳过 `assemble()` 拼接结果

---

## 14. Resolver 接口建议

由于 Stage 3 Context 与部分 Stage 4 Abilities 依赖函数式注册，建议定义统一 resolver 约定。

### 14.1 输入

resolver 至少应能访问：

- `PromptAssemblyRequest`
- 当前 `PromptComponent`
- 系统推导的 `PromptSource`

### 14.2 输出

resolver 建议输出：

- `text: str`
- `metadata: dict[str, Any]`
- `truncated: bool`

或等价结构化对象。

### 14.3 约束

- resolver 不得返回随机顺序内容
- resolver 应尽量避免副作用
- resolver 的失败应可记录并可降级

---

## 15. 序列化与签名建议

### 15.1 最终序列化

建议按固定格式序列化：

1. 依七阶段顺序遍历
2. 跳过空阶段
3. 阶段内部按稳定顺序串联组件
4. 使用统一换行规则

### 15.2 Prompt Signature

`prompt_signature` 建议由以下内容稳定生成：

- 阶段顺序
- 组件 ID
- 组件版本
- 展开文本 hash
- compatibility 标记

### 15.3 Cache Key

`cache_key` 可在 `prompt_signature` 基础上拼入：

- `route_id`
- `model_id`
- 需要区分的 response format

---

## 16. 首版实现优先级

最小可用版本建议先实现：

1. `PromptComponent`
2. `PromptSource`
3. `PromptProfile`
4. `PromptAssemblyRequest`
5. `PromptStageBlock`
6. `PromptAssemblyResult`
7. `PromptSnapshot`

第二阶段再补：

1. `PromptLoggerRecord`
2. 数据库存储
3. WebUI 可视化查看
4. Prompt diff 能力

---

## 17. 强制约束

- `PromptStage` 必须是固定枚举。
- `PromptSource` 必须由系统推导，不由注册方自由填写。
- `PromptAssemblyRequest` 不得携带自定义阶段顺序。
- `PromptAssemblyResult` 必须保留阶段边界，而不是只输出扁平字符串。
- `PromptSnapshot` 必须可与模型调用记录关联。
- `PromptLoggerRecord` 不得阻断主调用链。
