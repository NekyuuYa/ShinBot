# ShinBot 技术规范：异步消息工作流 (Message Workflow)

本文档定义了消息在 ShinBot 内部的流转与处理逻辑。ShinBot 采用 **异步工作流 (Workflow)** 模式，核心调度逻辑与底层协议驱动完全解耦。

---

## 1. 核心理念：非必然产出 (Non-guaranteed Output)

消息处理不是 $f(event) \rightarrow response$ 的简单函数。处理链是一个 **观察者与处理器集合**。
- **解耦发送**: 消息的发送由 `MessageContext` 调用 **已注册的适配器实例** 触发，而非函数返回。
- **多路产出**: 一个输入事件可能触发零个、一个或多个 `send` 行为。
- **异步终结**: 处理器可以在不产生任何输出的情况下结束，支持后台长时任务回调。

---

## 2. 核心流程细节 (Detailed Lifecycle)

### 2.1 接入与归一化 (Ingress & Normalization)
- **触发**: 由驱动插件 (Adapter Plugin) 捕获原始协议流量。
- **解析**: 适配器将 Payload 翻译为 `UnifiedEvent`，同时将 XML 内容转换为 **MessageElement AST 序列**。
- **补全**: 官方适配器（如 OB11 Bridge）负责在此阶段主动拉取合并转发内容或转换戳一戳语义。

### 2.2 上下文增强 (Context Enrichment)
- **会话识别**: 根据事件字段解析出复合 `session_id` (`instance:type:target`)。
- **对象装载**: 从数据库/缓存并行加载 `Session` 与 `User` 实例。
- **权限合并**: 计算该用户在当前环境下的 **合并权限集** (Global | Session-local)。
- **上下文注入**: 将上述对象封装进 `MessageContext`。

### 2.3 工作流分发 (Workflow Dispatching)
- **拦截器 (Interceptors)**: 检查频率限制、黑名单、会话是否静音。
- **分发决策**:
    - **交互状态**: 若 Session 处于 `WaitingForInput`，将消息定向至挂起的 Handler。
    - **指令匹配**: 优先检索 `session.config.prefixes`，进行 P0/P1/P2 匹配。
    - **Agent 引擎**: 若非指令，根据唤起配置决定是否交由 LLM 决策。

### 2.4 后处理与持久化 (Post-processing)
- **主动发送**: 执行期间产生的 `ctx.send()` 调用由适配器异步完成。
- **状态同步**: 自动持久化 `session.state` 与 `plugin_data` 的变更。
- **审计记录**: 记录耗时、Token 消耗及成功状态。
