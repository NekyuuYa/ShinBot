# ShinBot 核心思想：三位一体架构

本文档阐述 ShinBot 如何通过三条标准链路收敛复杂的平台交互。

## 1. 链路收敛与隐式路由 (Implicit Routing)

ShinBot 的核心优势在于开发者无需关心“我在和哪个平台说话”。

### 1.1 自动绑定机制
当事件从适配器（如 `shinbot_adapter_satori`）进入系统时，框架会创建一个 **操作句柄 (bot)**：
- 该句柄内部**永久持有**产生该事件的适配器引用。
- 所有的 `bot.send()` 或 `bot.call_api()` 都会自动通过该引用“原路返回”。

## 2. 三大函数模型 (The Three Pillars)

我们将所有交互行为封装在名为 `bot` 的高阶对象中：

### 2.1 听 (Listen / Ingress) —— 通过 `bot.event`
适配器负责将一切信号转换为 `UnifiedEvent` 存入 `bot.event`。包含：
- **叙事轨**: `bot.event.message` (AST 结构)。
- **资源轨**: `bot.event.user`, `bot.event.guild` (JSON 对象)。

### 2.2 说 (Speak / Egress) —— 通过 `bot.send()`
插件产生平台无关的 **MessageElement 流**。
- 示例: `await bot.send("你好")`。

### 2.3 管 (Action / Control) —— 通过 `bot.call_api()`
一个万能的 RPC 接口，涵盖所有非消息流操作。
- 语义化封装: `bot.kick()`, `bot.mute()` 等高阶函数均基于此链路实现。
