# 日志与可观测性设计

本文定义 ShinBot 后端日志的目的、层级和字段约定。日志不是为了把所有内部变量打印出来，而是为了在真实部署中回答几个直接问题：

- 消息是否进入了系统，落到了哪个 `session_id` 和 `message_log_id`。
- 消息路由为什么命中或跳过，最终交给了哪个 route target。
- Agent 是否收到了统一信号，信号属于消息、review timer、active chat timer 还是 bootstrap。
- Scheduler 当时处于什么状态，为什么启动或跳过 review、active reply、active chat。
- 后台 timer 是否扫描到了到期任务，是否把信号送进了统一入口。
- 模型调用走了哪个 route/provider/model，是否成功，耗时和 token 用量如何。

## 设计原则

### 日志必须有调查目的

每条新增日志都应服务于生产排障或行为审计。常规路径上的细节使用 `DEBUG`，重要生命周期和状态转换使用 `INFO`，异常和不可恢复问题使用 `WARNING` 或 `ERROR`。

避免记录“函数被调用”这种没有上下文的日志。应记录事件名、关键 ID 和决策原因，例如：

```text
agent.signal.decision | kind=message | session_id=bot:group:room | state=idle | accepted=true
```

### 普通日志不承载正文

普通日志不得输出用户消息正文、模型 request/response 正文、token、密钥或原始媒体数据。需要查看模型请求与返回时，应通过模型审计 payload 文件读取。这样可以避免日志文件再次成为大体积数据源，也避免敏感内容混入 console 和 WebSocket log。

### 事件名稳定，字段可追加

结构化日志使用 `format_log_event(event, **fields)` 输出紧凑 key/value。事件名应稳定，便于 grep、WebUI 过滤和未来采集系统解析。字段可以追加，但不应随意改名。

### INFO 少而明确

`INFO` 只用于可被操作者理解的生命周期事件，例如 boot phase、adapter start/stop、review timer 启动、active chat 退出到 idle。逐消息、逐 tick、逐模型尝试默认进入 `DEBUG`。

### 第三方噪声默认不污染业务日志

uvicorn、websockets 等第三方低级别日志由日志管理器按策略处理。默认只在 DEBUG 场景展示这些噪声，生产 INFO 视图应优先显示 ShinBot 自身事件。

## 关键事件

### 消息入口与路由

- `message.ingress`: 消息已归一化并持久化，包含 `instance_id`、`session_id`、`message_log_id`、`bot_id`、`binding_id`、`modality`。
- `message.routing`: 路由决策结果，包含 `status`、`skipped_reason`、`rules`、`targets`。
- `route.target.scheduled`: route target 是异步任务并已调度。
- `route.target.missing` / `route.target.error` / `route.target.cancelled`: route target 层异常或取消。
- `agent.signal.created`: `agent_entry` target 已构造 Agent 统一信号。
- `agent.signal.dropped`: 没有 Agent handler 时丢弃信号。

### Agent 统一入口与状态机

- `agent.runtime.signal`: AgentRuntime 收到统一信号并完成 profile 选择。
- `agent.signal.entry`: AgentScheduler 接收信号时的初始状态。
- `agent.review.plan.created`: 首次为 session 创建 idle review plan。
- `agent.signal.decision`: Scheduler 的最终决策，包含 `state`、`skipped_reason`、`review_started`、`active_reply_started`、`active_chat_started`、`returned_to_idle` 等字段。
- `agent.active_chat.exit`: active chat 返回 idle，并记录下一次 review 的计划时间。

### 后台 timer

- `agent.review_timer.started`: review due timer 启动。
- `agent.review_timer.scan`: 一轮扫描发现的到期数量。
- `agent.review_timer.dispatch`: timer 将 `REVIEW_DUE` 信号送入 AgentRuntime。
- `agent.review_timer.skip`: 到期计划被跳过，例如同 session 已在处理。
- `agent.active_chat_timer.started` / `agent.active_chat_timer.tick` / `agent.active_chat_timer.exit`: active chat per-session tick 生命周期。

### Workflow 与模型调用

- `agent.review.workflow.start` / `agent.review.workflow.finish`: review workflow 开始与结束。
- `agent.active_chat.workflow.start` / `agent.active_chat.workflow.message`: active chat workflow 启动和消息通知。
- `agent.idle_review_planning.start` / `agent.idle_review_planning.finish`: ACTIVE_CHAT 退出 IDLE 前由 LLM 生成下一次 review 间隔。
- `model.call.start`: 模型调用尝试开始，记录 route/provider/model、caller、purpose、session。
- `model.call.finish`: 单次模型调用尝试结束，记录状态、耗时和 token 用量。
- `model.call.failed`: 所有模型尝试失败后的最终摘要。

## 排障建议

调查一条消息时，优先按 `message_log_id` 搜索；如果还没有落库 ID，则按 `session_id` 搜索。

调查状态机一睡不起时，按 `session_id` 依次查看：

1. `message.ingress`
2. `message.routing`
3. `agent.signal.created`
4. `agent.runtime.signal`
5. `agent.signal.entry`
6. `agent.signal.decision`
7. `agent.review_timer.scan` / `agent.review_timer.dispatch`

调查模型费用或重复调用时，按 `session_id`、`purpose`、`model_id` 搜索 `model.call.start` 和 `model.call.finish`，再用 `execution_id` 去模型审计页面查看 request/response payload。
