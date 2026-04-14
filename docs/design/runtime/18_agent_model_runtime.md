# ShinBot 技术规范：Agent 模型接入与路由运行时 (Agent Model Runtime)

本文档定义 ShinBot 在 Agent 与插件侧的统一模型接入层设计。

目标不是为每个插件分别接不同 SDK，而是建立一套稳定的模型运行时：

- 上层调用只面向 ShinBot 的统一接口。
- 底层模型接入统一通过 `LiteLLM SDK`。
- 模型资产按 **Provider → Model → Route** 三层组织。
- 每次模型调用都必须产生可审计、可统计、可回放的执行记录。

---

## 1. 设计目标

### 1.1 统一接入

- Agent、业务插件、系统内置能力不得各自直接接 OpenAI / Anthropic / Gemini SDK。
- 所有模型调用统一经由 `LiteLLM` 适配层。
- 上层只关心“调用哪条逻辑路由”和“需要什么能力”，不关心底层具体供应商差异。

### 1.2 供应商优先管理

- 系统必须先创建 **模型供应商 (Provider)**，再在其下创建 **模型 (Model)**。
- 同源模型必须在同一个 Provider 下集中管理。
- 禁止创建脱离 Provider 的孤立模型配置。

### 1.3 路由优先调用

- Agent 与插件默认不直接绑定具体模型，而是绑定 **模型路由 (Route)**。
- Route 负责在多个候选模型之间执行优先级、失败切换、权重或策略选择。
- 这样可以在不改业务代码的情况下替换底层模型。

### 1.4 全量可观测

- 每次模型调用都必须记录性能、容量、成本和缓存命中信息。
- 这些记录必须能进入后续的监控、审计和成本分析链路。
- 任何“只返回文本，不记录元数据”的模型调用都视为不合规。

---

## 2. 非目标

- 本文档不规定具体 Prompt 模板内容。
- 本文档不要求首版就支持所有 LiteLLM 能力，重点先覆盖文本生成与嵌入能力。
- 本文档不强制首版引入数据库；允许先用配置文件或本地持久化承载模型资产元数据。

---

## 3. 运行时分层

模型运行时分为四层：

1. **Provider Registry**
   - 管理供应商定义、密钥引用、默认参数和连通性状态。
2. **Model Registry**
   - 管理具体模型定义、能力标签、上下文窗口、默认参数和成本元数据。
3. **Route Registry**
   - 管理模型路由组、候选模型顺序、选择策略和故障切换规则。
4. **Execution Runtime**
   - 负责实际调用 LiteLLM、标准化响应、记录 metrics、写入审计和上报监控。

上层调用关系必须是：

`Agent / Plugin -> Model Runtime API -> Route Resolver -> LiteLLM -> Metrics/Audit Sink`

而不是：

`Agent / Plugin -> 第三方 SDK`

---

## 4. 核心概念

### 4.1 Provider

Provider 表示模型供应商或模型接入源。

典型示例：

- `openai-official`
- `anthropic-main`
- `azure-openai-prod`
- `openrouter-primary`
- `ollama-local`

Provider 必须至少包含：

- `id`
- `type`
  - 例如 `openai`、`anthropic`、`gemini`、`azure_openai`、`openrouter`、`ollama`
- `display_name`
- `base_url`
- `auth`
  - API Key 或其它认证引用
- `default_params`
  - 此 Provider 下模型共享的默认参数
- `enabled`

要求：

- 密钥不得在 UI 明文回显。
- Provider 允许被禁用；被禁用时其下模型不可参与路由选择。

### 4.2 Model

Model 表示某个 Provider 下的一条具体模型定义。

典型示例：

- `openai-official/gpt-4.1-mini`
- `openrouter-primary/claude-3.7-sonnet`
- `ollama-local/qwen2.5:14b`

Model 必须从属于某个 Provider，并至少包含：

- `id`
- `provider_id`
- `litellm_model`
  - 供 LiteLLM 调用的模型标识
- `display_name`
- `capabilities`
  - 例如 `chat`、`embedding`、`vision`、`tool_calling`、`json_mode`
- `context_window`
- `default_params`
- `cost_metadata`
  - 可为空，但结构必须预留
- `enabled`

### 4.3 Route

Route 是上层真正绑定的逻辑模型入口。

典型示例：

- `agent.default_chat`
- `agent.fast_chat`
- `plugin.summary`
- `plugin.embedding`
- `system.audit_classifier`

Route 必须至少包含：

- `id`
- `purpose`
- `strategy`
- `members`
- `enabled`

其中 `members` 是有序模型候选列表，每个成员可包含：

- `model_id`
- `priority`
- `weight`
- `conditions`
- `timeout_override`

### 4.4 Model Execution Record

每次模型调用都必须生成一条执行记录。

这是后续监控、审计、成本分析和路由调优的基础对象。

---

## 5. LiteLLM 作为统一接入端

### 5.1 统一原则

- ShinBot 不直接依赖具体云厂商 SDK。
- 统一通过 `LiteLLM SDK` 执行模型调用。
- LiteLLM 只作为“模型协议适配层”，不替代 ShinBot 的路由与审计逻辑。

### 5.2 统一调用抽象

ShinBot 内部应定义统一接口，例如：

- `generate()`
- `generate_stream()`
- `embed()`

后续如扩展到：

- `moderate()`
- `image_generate()`

也必须沿用同一运行时骨架。

### 5.3 调用入参

统一调用入参至少包含：

- `route_id` 或 `model_id`
- `caller`
  - 谁发起了调用，例如 `agent.runtime`、`plugin.weather`
- `session_id`
- `instance_id`
- `purpose`
- `messages` / `input`
- `tools`
- `response_format`
- `metadata`

要求：

- 直接传 `model_id` 仅用于调试或管理工具。
- 正常业务流必须优先走 `route_id`。

---

## 6. Provider → Model → Route 管理模型

### 6.1 Provider 先行

添加模型时必须先选择或创建 Provider。

管理流程固定为：

1. 创建 Provider
2. 在 Provider 下添加一个或多个 Model
3. 将多个 Model 编入一个或多个 Route
4. Agent / 插件只消费 Route

### 6.2 同源模型集中管理

例如：

- Provider: `openai-official`
  - `gpt-4.1-mini`
  - `gpt-4.1`
  - `text-embedding-3-large`

这样做的收益：

- 凭据与基础 URL 只维护一份
- 同源模型切换更直观
- 更方便按 Provider 聚合统计成本与成功率

### 6.3 UI 管理约束

WebUI 应体现三层关系：

- Provider 列表页
- Provider 详情页中的模型列表
- Route 列表页中的候选模型组

不应提供“脱离 Provider 直接添加模型”的入口。

---

## 7. 模型路由设计

### 7.1 路由是逻辑绑定点

Agent 与插件应绑定 Route，而不是绑定裸模型名。

例如：

- `AgentPlanner` 绑定 `agent.default_chat`
- 摘要插件绑定 `plugin.summary`
- 向量化插件绑定 `plugin.embedding`

### 7.2 首版必须支持的策略

#### `priority_failover`

- 按优先级顺序依次尝试。
- 首个可用且成功的模型即返回。
- 默认策略应为此项。

适用：

- 稳定生产调用
- 主模型 + 备用模型

#### `weighted_random`

- 在可用成员中按权重随机选择。
- 适合 A/B 测试和流量分配。

#### `sticky_session`

- 同一会话在一定时间窗口内尽量固定命中同一模型。
- 适合多轮对话，减少模型切换造成的行为漂移。

### 7.3 路由决策输入

Route Resolver 允许参考以下信息：

- 调用能力要求
  - 如必须支持 `vision` 或 `tool_calling`
- 会话上下文
  - 如 session 级模型偏好
- 显式策略参数
  - 如强制走低成本模型
- 模型健康状态
  - 近期错误率、超时率

### 7.4 故障切换规则

以下场景应允许自动切下一候选：

- 网络错误
- Provider 429 / 配额不足
- 超时
- 上游 5xx
- 路由策略允许的空响应 / 不可解析响应

以下场景默认不自动切换：

- 业务层明确拒绝
- 调用被本地校验拦截

---

## 8. 指标采集与执行记录

### 8.1 每次调用都必须采集

每次模型调用至少记录以下字段：

- `execution_id`
- `route_id`
- `selected_model_id`
- `selected_provider_id`
- `caller`
- `instance_id`
- `session_id`
- `requested_at`
- `dispatched_at`
- `first_token_at`
  - 非流式调用可为空
- `completed_at`
- `duration_ms`
- `queue_duration_ms`
- `provider_latency_ms`
- `status`
  - `success` / `failed` / `fallback_success` / `cancelled`

### 8.2 Token 与缓存指标

至少应采集：

- `input_tokens`
- `output_tokens`
- `total_tokens`
- `cached_input_tokens`
- `cache_hit`
- `cache_write_tokens`
  - 若上游提供
- `reasoning_tokens`
  - 若上游提供

要求：

- 若上游未返回某字段，记录为 `null`，不要伪造。
- 记录层必须保留原始 usage 字段快照，便于后续兼容不同 Provider。

### 8.3 成本指标

运行时应在执行记录中预留：

- `estimated_cost_input`
- `estimated_cost_output`
- `estimated_cost_total`
- `currency`

说明：

- 首版允许以“本地估算”为准。
- 如果某 Provider 提供真实计费反馈，可额外记录 `provider_reported_cost`。

### 8.4 路由过程指标

如果发生 fallback，还应记录：

- `attempt_count`
- `attempts`
  - 每次尝试的模型、开始时间、结束时间、错误原因
- `final_strategy`

---

## 9. 审计与监控整合

### 9.1 审计要求

模型执行记录应进入独立审计流，不与普通命令日志混淆。

审计至少包含：

- 谁发起了调用
- 选择了哪条 Route
- 最终命中了哪个模型
- 消耗了多少 token
- 花费了多长时间
- 是否 fallback
- 是否命中缓存

### 9.2 监控要求

Dashboard 后续至少应能展示：

- 按 Provider 的调用量、错误率、平均耗时
- 按 Model 的 token 消耗与成本估算
- 按 Route 的命中分布与 fallback 次数
- 缓存命中率
- 最近失败请求

### 9.3 成本分析要求

系统应支持按以下维度聚合：

- Provider
- Model
- Route
- Plugin / Agent Caller
- Session
- 时间窗口

---

## 10. Agent 与插件的使用约束

### 10.1 Agent 使用规则

Agent 不得硬编码具体 Provider / Model 名称。

Agent 仅允许：

- 指定默认 `route_id`
- 在必要时声明能力要求
- 在少数调试场景下覆盖为 `model_id`

### 10.2 插件使用规则

插件如需调用模型，必须通过框架注入的模型运行时接口。

不允许：

- 插件自行 import LiteLLM
- 插件自行管理模型密钥
- 插件在自己的配置里重复维护模型供应商信息

### 10.3 权限边界

模型调用能力应视为系统能力，而不是普通插件自带权限。

后续建议增加：

- `model.invoke`
- `model.invoke.high_cost`
- `model.manage`

---

## 11. 配置结构建议

首版可先采用配置文件或管理面持久化，逻辑结构应等价于：

```toml
[[model_providers]]
id = "openai-official"
type = "openai"
display_name = "OpenAI"
base_url = "https://api.openai.com/v1"
enabled = true

[model_providers.auth]
api_key = "${OPENAI_API_KEY}"

[[models]]
id = "gpt-4.1-mini"
provider_id = "openai-official"
litellm_model = "gpt-4.1-mini"
display_name = "GPT-4.1 mini"
capabilities = ["chat", "tool_calling", "json_mode"]
context_window = 128000
enabled = true

[[model_routes]]
id = "agent.default_chat"
purpose = "general_chat"
strategy = "priority_failover"
enabled = true

[[model_routes.members]]
model_id = "gpt-4.1-mini"
priority = 100
```

要求：

- Provider、Model、Route 必须分别持久化，不要把它们揉成一层配置。
- `members` 必须引用已有 `model_id`，不得内联完整模型配置。

---

## 12. 首版实现建议

### Phase A

- 建立 Provider / Model / Route 三类配置模型
- 建立基于 LiteLLM 的统一调用服务
- 支持 `priority_failover`
- 支持基础 metrics 采集

### Phase B

- Dashboard 管理页
- 路由健康状态与失败切换统计
- 成本估算与聚合面板

### Phase C

- 更复杂路由策略
- 会话粘性
- 缓存命中分析
- Agent 级模型策略覆盖

---

## 13. 强制约束总结

- 所有模型调用统一走 LiteLLM。
- 所有模型必须隶属于某个 Provider。
- 所有业务调用默认走 Route，而不是裸模型。
- 每次调用必须产生结构化执行记录。
- 执行记录必须可用于监控、审计和成本分析。
- Agent / 插件不得各自维护独立模型接入栈。
