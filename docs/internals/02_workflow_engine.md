# ShinBot 实现内幕：消息入口与异步工作流引擎

本文档剖析 ShinBot 消息处理的核心调度逻辑及其解耦的上下文模型。

## 1. 消息入口：MessageIngress

位于 `shinbot/core/dispatch/ingress.py`。

### 1.1 阶段化调度实现

当前消息入口分为四个阶段：

1. **归一化与会话识别**：调用 `Message.from_xml` 将事件内容转化为 AST，并通过 `SessionManager` 建立 session。
2. **上下文构建**：加载权限、构建 `MessageContext`，并处理 `wait_for_input` 的挂起恢复。
3. **持久化与路由评估**：消息与 notice 先写入 `message_logs(PENDING)`，再执行过期检查、静音/拦截器（消息专用）和 `RouteTable.match(...)`。
4. **目标派发**：命中 route 后标记 `DISPATCHED` 并异步调度 route target；无命中或被过滤时标记 `SKIPPED`。

### 1.2 健壮性设计

- **持久化旁路保障**：外部消息先落库，实时调度不依赖轮询数据库。
- **异常隔离**：matcher、拦截器和 route target 的异常都会被记录，不阻断整条消息入口。
- **状态边界清晰**：`message_logs.routing_status` 只表达路由层生命周期，下游模块自行维护处理状态与阅读状态。状态值由 `shinbot.schema.routing.MessageRoutingStatus` / `MessageRoutingSkipReason` 固化。
- **Agent 边界**：ingress 不主动维护 Agent 上下文；Agent 入口只接收触发信号，并可在自身模块内按需读取 `message_logs` 构建上下文。

---

## 2. 路由表：RouteTable

位于 `shinbot/core/dispatch/routing.py`。

`RouteTable` 是纯匹配层，只返回命中的 `RouteRule`，不直接执行业务逻辑。内置 route target 包括：

- `text_command_dispatcher`
- `keyword_dispatcher`
- `notice_dispatcher`
- `agent_entry`

消息 route 的优先级数字越大越先评估；同优先级下按注册顺序稳定排序。`FALLBACK` 必须通过条件声明自己的适用大类，用户消息的默认 fallback 是 `agent_entry`。

---

## 3. 上下文门面：Bot 句柄

`MessageContext` 位于 `shinbot/core/dispatch/message_context.py`。

### 3.1 实现亮点

- **资源抽象**：`bot.send()` 和 `bot.reply()` 通过当前 adapter 发送，不关心具体平台协议。
- **语义化管理 API**：`bot.kick()`、`bot.mute()`、`bot.poke()` 等方法自动从 `bot.event` 中提取上下文。
- **交互式补全**：`wait_for_input` 通过 `WaitingInputRegistry` 将当前 `session_id` 与一个 Future 绑定，下一条同会话消息到达时由 ingress 优先恢复。

### 3.2 解耦性评估

`bot` 句柄充当核心引擎与业务插件之间的边界。插件开发者只需面对 `bot` 提供的精简接口，不能直接触碰底层适配器细节。

---

## 4. 双轨分发逻辑

1. **消息流**：消息事件进入 `MessageIngress`，解析 AST 后交给 `RouteTable`，再派发给命令、关键词、自定义 route 或 `agent_entry`。
2. **通知流**：notice 事件先以 `role="system"` 写入 `message_logs`，再经过 `RouteTable`；只有 `EventBus` 上存在对应处理器或 wildcard 处理器时，`notice_dispatcher` 才会命中并转发。
