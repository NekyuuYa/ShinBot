# ShinBot 技术规范：系统引导与生命周期 (System Boot & Lifecycle)

本文档定义了 ShinBot 从进程启动到正式提供服务期间的标准化引导序列。

---

## 1. 引导控制器 (Boot Controller) 的目标

- **确定性排序**: 解决组件间的拓扑依赖，防止“消息早于 Handler”进入。
- **状态透明化**: 为 Dashboard 提供系统当前的实时加载进度。
- **异常收敛**: 确保在任何启动阶段发生崩溃时，都能执行安全的资源回滚。

---

## 2. 标准启动序列 (Boot Sequence)

引导过程分为五个严格的原子阶段：

### Phase 1: 环境探测 (Environment)
- **动作**: 读取 `config.toml`，初始化日志系统，检查 `data/` 目录下运行时子目录的读写权限。
- **失败影响**: 核心无法运行，直接抛出系统级错误并退出。

### Phase 2: 基础设施就绪 (Infrastructure)
- **动作**: 
    - 建立数据库连接池。
    - **前端分发**: 检测 `dashboard/dist` 目录。若存在，则通过 FastAPI 的 `StaticFiles` 挂载至根路径，并配置 SPA 回退逻辑（Fallback to index.html）。
- **目的**: 确保用户只需启动主程序即可访问 WebUI。
- **失败影响**: 若 `dist` 不存在，仅提供 API 服务，并在日志中警告。

### Phase 3: 核心引擎实例化 (Kernel Load)
- **动作**: 实例化 `MessagePipeline`、`CommandRegistry`、`PermissionEngine` 和 `SessionManager`。
- **失败影响**: 系统无法处理逻辑，Dashboard 展示核心崩溃详情。

### Phase 4: 插件注入 (Plugin Loading)
- **动作**: 调用 `PluginManager` 先扫描 `shinbot/builtin_plugins/`，再扫描 `data/plugins/`。
- **子任务**:
    1. 解析 `metadata.json`。
    2. 导入 Python 模块并调用其 `setup(plg)`。
    3. 此时所有指令和事件监听器已注册进核心引擎。
- **失败影响**: 对应插件标记为 `LOAD_FAILED`，系统继续引导。

### Phase 5: 适配器激活 (Adapter Activation)
- **动作**: 根据配置实例化适配器驱动，调用其 `start()` 接口。
- **意义**: 只有此时，网络端口才会打开，流量才开始涌入已就绪的工作流。

---

## 3. 系统状态机 (State Machine)

| 状态 | 说明 |
| :--- | :--- |
| `UNINITIALIZED` | 进程刚启动。 |
| `BOOTING` | 正在执行上述 5 个阶段。 |
| `RUNNING` | 所有适配器正常运行，系统负载中。 |
| `DEGRADED` | 部分核心组件或关键插件失效，但服务未中断。 |
| `STOPPING` | 正在执行优雅关机。 |

---

## 4. 优雅关机序列 (Graceful Shutdown)

关机顺序应与启动顺序**严格相反**：
1. **停止适配器**: 立即切断外部流量。
2. **通知插件**: 触发 `on_disable()`，必要时再执行 `teardown()` 释放插件自有资源。
3. **状态持久化**: `SessionManager` 将内存会话刷新到持久化层。
4. **关闭基础设施**: 收敛数据库、静态资源和其他运行时基础设施。
