# ShinBot 实现内幕：异步工作流引擎 (Workflow Engine)

本文档剖析了 ShinBot 消息处理的核心调度逻辑及其高度解耦的上下文模型。

## 1. 消息流水线：MessagePipeline
位于 `shinbot/core/pipeline.py`。

### 1.1 阶段化调度实现
代码严谨地实现了设计文档中的四个阶段：
1. **归一化 (Normalization)**: 调用 `Message.from_xml` 将事件内容转化为 AST。
2. **增强 (Enrichment)**: 通过 `SessionManager` 和 `PermissionEngine` 并行加载环境，构建 `MessageContext`。
3. **分发 (Dispatch)**: 
   - 依次运行 **拦截器 (Interceptors)** 链。
   - 调用 `CommandRegistry.resolve` 进行 P0/P1/P2 指令匹配。
   - 若命中指令且权限通过，则执行 Command Handler。
   - 否则，通过 `EventBus` 发送原始事件（供 LLM 或其他监听器消费）。
4. **后处理 (Post-processing)**: 统一执行会话持久化和审计。

### 1.2 健壮性设计
- **异常隔离**: 每一个拦截器和指令处理器的执行都包裹在 `try-except` 中，防止单个插件崩溃导致整个工作流中断。
- **流程熔断**: 拦截器通过返回 `False` 或调用 `ctx.stop()` 可以立即终止后续处理，保证了风控和静音逻辑的强制性。

---

## 2. 上下文门面：Bot 句柄 (The Bot Handle)

### 2.1 实现亮点
- **资源抽象**: `bot.send()` 和 `bot.reply()` 并不关心具体的平台协议，而是通过持有的 `adapter` 引用直接调用抽象接口。
- **语义化管理 API**: 实现了 `bot.kick()`, `bot.mute()`, `bot.poke()` 等高阶方法。这些方法自动从 `bot.event` 中提取上下文（如 `guild_id`），极大地简化了插件编写。
- **交互式补全 (wait_for_input)**: 
  - 实现逻辑：通过在 `MessageContext` 中集成一个 `WaitingInputRegistry`。
  - 当调用 `wait_for_input` 时，它会创建一个异步 `Future` 并将其与当前的 `session_id` 绑定，然后 `await` 该 Future。
  - 当下一条属于该会话的消息进入时，Pipeline 会优先检查该注册表，如果存在挂起的 Future，则直接 `set_result` 恢复执行逻辑。

### 2.2 解耦性评估
- `bot` 句柄成功充当了核心引擎与业务插件之间的“防火墙”。插件开发者只需面对 `bot` 提供的精简接口，无法直接触碰底层的适配器细节。


---

## 3. 双轨分发逻辑 (Dual-Track Logic)

为了支持“方案一”的设计，Pipeline 实现了分层处理：
1. **消息流 (Message Flow)**: 当 `event.type` 为消息创建时，调用 `SatoriParser` 生成 `Message` 对象，支持富文本指令。
2. **事件流 (Notice Flow)**: 当 `event.type` 为系统通知时，绕过 XML 解析，直接映射结构化资源 JSON。通过 `@on_notice` 装饰器实现精准分发。
