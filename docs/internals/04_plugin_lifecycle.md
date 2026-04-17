# ShinBot 实现内幕：插件系统与热重载 (Plugin Lifecycle)

本文档剖析了 ShinBot 插件系统的动态加载机制，以及它是如何实现“无残留卸载”的。

## 1. 插件载入器：PluginManager
位于 `shinbot/core/plugins/manager.py`。

### 1.1 实现方法
- **模块动态导入**: 利用 `importlib.util` 和 `sys.modules` 操作，实现在运行时从指定目录发现并加载 Python 模块。
- **Entrypoint 规范**: 统一采用 `setup(plg: Plugin)` 作为插件入口，这种设计优于零散的全局代码执行，能更好地控制加载时机。
- **命名空间隔离**: 每一个插件都被映射为一个独立的 `Plugin` 对象，内部持有了该插件的所有元数据。

### 1.2 热重载与资源回收 (Hot-Reload)
- **所有权追踪**: 这是 ShinBot 最核心的健壮性设计之一。
  - 每一个通过装饰器注册的指令（Command）或事件监听器（Event Listener）都会在 `Plugin` 对象中记录一份“存根”。
  - 当执行 `unload_plugin()` 时，管理器会遍历这些存根，主动从全局的 `CommandRegistry` 和 `EventBus` 中将其注销。
- **结果**: 实现了真正的热重载，彻底解决了“重载插件后出现两个相同指令”的经典 Bug。

---

## 2. 开发者接口：Plugin

### 2.1 设计亮点
- **资源注入**: 插件在 `setup` 时拿到的 `plg` 是受限的。它只能注册属于自己的指令和监听器。
- **声明式开发**: 开发者只需通过 `@plg.on_command` 注册，底层的注销逻辑由框架自动处理，极大地降低了开发者的心智负担。

### 2.2 健壮性分析
- **依赖检查**: 现有实现已经支持基于 `metadata.json` 的依赖声明与拓扑排序，但仍应避免构造过深的插件耦合链。
