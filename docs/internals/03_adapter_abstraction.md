# ShinBot 实现内幕：适配器抽象与驱动管理 (Adapter Abstraction)

本文档剖析了 ShinBot 的“驱动容器模式”，分析核心如何通过抽象接口与各种 IM 协议通信。

## 1. 核心契约：BaseAdapter
位于 `shinbot/core/platform/adapter_manager.py`。

### 1.1 实现方法
- **抽象方法集**: 严格定义了 `start`, `shutdown`, `send`, `call_api`, `get_capabilities`。
- **听说管知模型**: 接口设计高度统一，强制适配器实现能力探测（Capabilities），实现了“逻辑面”与“协议面”的完美隔离。
- **MessageHandle**: `send` 接口返回一个句柄对象，通过持有适配器引用实现了 `handle.edit()` 和 `handle.recall()`，使得消息的后续操作变得极其自然。

---

## 2. 注册工厂：AdapterManager

### 2.1 实现方法
- **动态注册**: 采用工厂模式 (`register_adapter`)。驱动插件在加载时注入自己的类，而不是由核心 import。
- **多实例管理**: 通过 `create_instance` 方法，用户可以在 `config.toml` 的 `[[instances]]` 中为同一驱动开启多个具有独立 `instance_id` 的连接实例。

---

## 3. 实战实现：SatoriAdapter
位于 `shinbot/builtin_plugins/shinbot_adapter_satori/adapter.py`。

### 3.1 健壮性分析
- **生命周期自动化**: 实现了 WebSocket 的自动连接循环 (`_connection_loop`)、指数退避重连机制以及心跳保活 (`PING/PONG`)。
- **协议对齐**: 完美处理了 Satori 的 `READY` 握手，并从中提取 `self_id` 和平台特性。
- **语义转换**: 实现了 `_decode_session_id` 辅助方法，将复杂的 URN 格式 SessionID 还原为平台所需的 `channel_id`。

### 3.2 解耦性评估
- 该类封装了所有 `httpx` 和 `websockets` 的细节。如果未来要从 `websockets` 库切换到 `aiohttp`，只需修改此文件，核心 `pipeline` 无需任何变动。
