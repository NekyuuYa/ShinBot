# ShinBot 技术规范：插件系统与动态加载 (Plugin System)

ShinBot 采用“微内核 + 动态插件”架构，支持运行时热重载。

## 1. 插件角色定义 (Plugin Roles)

根据功能职责，插件可分为两类：

### 1.1 业务插件 (Logic Plugins)
- **职责**: 实现具体功能（如天气、翻译、模型交互）。
- **注册**: 使用 `@on_command`, `@on_message` 等装饰器。

### 1.2 驱动插件 (Adapter Plugins)
- **职责**: 实现底层平台协议（如 OneBot, Discord, 腾讯官方协议）。
- **注册**: 在插件加载时调用 `adapter_manager.register(platform_name, AdapterClass)`。
- **翻译**: 负责将私有协议与 Satori 标准 AST 相互转换。

## 2. 指令与事件注册机制

框架通过装饰器实现逻辑绑定，支持在插件卸载时自动清理注册表：
- `@on_command`: 注册指令处理器。
- `@on_message`: 订阅消息事件，支持过滤器链。

## 3. 动态加载机制 (Dynamic Loading)

`PluginManager` 维护插件全生命周期：
- **热重载**: 使用 `importlib` 动态加载。重载时，旧插件的指令、事件监听及 **适配器驱动** 都会被一并注销，由新版本重新注册。
- **隔离性**: 建议插件利用 `Session.plugin_data` 存储持久化状态，以应对重载带来的内存清空。
