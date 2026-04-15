# ShinBot 技术规范：插件系统与动态加载 (Plugin System)

ShinBot 采用“微内核 + 全量插件化”架构，支持运行时热重载。

## 1. 核心理念
系统核心仅负责逻辑编排，所有的平台接入（适配器）与业务功能均以插件形式存在。

## 2. 统一命名规范 (Naming Convention)

为了保证目录整洁与 Python 模块命名空间安全，所有插件文件夹及其 ID 必须遵循以下前缀：

### 2.1 业务插件 (Logic Plugins)
- **格式**: `shinbot_plugin_{name}`
- **示例**: `shinbot_plugin_weather`, `shinbot_plugin_admin`。

### 2.2 驱动插件 (Adapter Plugins)
- **格式**: `shinbot_adapter_{platform}`
- **示例**: `shinbot_adapter_satori`, `shinbot_adapter_onebot_v11`。

### 2.3 调试与诊断插件 (Debug Plugins)
- **格式**: `shinbot_debug_{name}`
- **示例**: `shinbot_debug_message`。
- **用途**: 用于核心状态监控、流量拦截审计等非业务功能。

## 3. 插件物理分布与角色 (Plugin Distribution)

根据存放位置，插件分为两类：

### 3.1 内置插件 (Built-in Plugins)
- **存放路径**: `shinbot/builtin_plugins/`
- **定位**: 随框架分发的官方组件（含官方适配器）。优先加载。

### 3.2 外部插件 (External Plugins)
- **存放路径**: `data/plugins/`
- **定位**: 用户自行安装的扩展功能。支持动态安装/卸载。

## 4. 插件结构规范 (Structure)

每个插件必须拥有独立文件夹：
- `metadata.json`: 定义插件 ID (必须与文件夹名一致)、名称、版本、入口文件。
- `__init__.py`: 插件逻辑入口，必须导出 `setup(ctx)`。

## 5. 数据持久化 (Data Persistence)
- **代码目录**: 仅限只读操作。
- **数据目录**: `data/plugin_data/{plugin_id}/`。
- 框架通过 `ctx.data_dir` 注入该路径，插件所有的持久化资产必须存放于此。

## 6. 依赖管理 (Dependency Management)

ShinBot 支持插件间的层级依赖，允许开发者通过“依赖插件”来扩展平台能力或共享逻辑。

### 6.1 硬依赖 (Required)
- **行为**: 若声明的依赖项未加载或加载失败，当前插件将**拒绝启动**并报错。
- **用途**: 确保核心库或必须的中间件（如数据库插件）已就绪。

### 6.2 软依赖 (Optional)
- **行为**: 系统在加载时尝试解析该依赖。若缺失，当前插件**正常启动**。
- **用途**: 实现“渐进式功能增强”。例如：某个通用插件在检测到 QQ 适配器存在时，额外开启“戳一戳响应”功能。

### 6.3 声明规范 (metadata.json)
```json
{
    "dependencies": {
        "required": ["shinbot_plugin_db_orm"],
        "optional": ["shinbot_adapter_qq_pro"]
    }
}
```

## 7. 热重载原则
- 插件停用或卸载时，框架先触发 `on_disable(ctx)`。
- 若插件定义了 `teardown()`，框架在注销命令和事件后继续调用它完成最终资源释放。
- 所有由该插件注册的命令、事件监听器和适配器工厂都必须可追踪、可撤销。
