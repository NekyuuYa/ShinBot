"""
# 插件系统深度适配实现总结

## 完成的三大步骤

### Step 1: 重构插件加载器 (shinbot/core/plugin.py)

#### 新增方法: `load_plugins_from_metadata_dir()`

**功能:**
- 扫描 `data/plugins/` 目录结构
- 读取每个插件子目录中的 `metadata.json`
- 根据元数据中的 `entry` 字段确定入点模块
- 基于 `plugin_id` 加载插件
- 执行权限交叉校验

**元数据格式:**
```json
{
    "id": "com.shinbot.demo",
    "name": "Demo",
    "version": "1.0.0",
    "author": "ShinBot Team",
    "description": "示例插件",
    "entry": "__init__.py",
    "permissions": ["cmd.ping", "cmd.echo"]
}
```

**权限交叉校验:**
- 验证插件通过 `@ctx.on_command(permission=...)` 声明的权限
- 与 `metadata.json` 中的 `permissions` 数组进行比对
- 输出警告日志如果存在未声明的权限使用

**插件目录结构:**
```
data/plugins/
├── demo_plugin/
│   ├── metadata.json
│   ├── __init__.py        # entry: "__init__.py" => module "plugins.demo_plugin"
│   └── ...
└── another_plugin/
    ├── metadata.json
    ├── main.py            # entry: "main.py" => module "plugins.another_plugin.main"
    └── ...
```

---

### Step 2: 数据隔离与持久化

#### SessionManager 初始化 (shinbot/core/session.py)

**改进:**
- SessionManager 现在接受 `data_dir` 参数
- 会话自动持久化到 `data/sessions/{session_id}.json`
- 会话恢复: 从磁盘加载已有会话，无则新建

**数据流:**
```
Session 创建/更新 → SessionManager.update()
                   ├─ memory: _sessions 字典
                   └─ disk: data/sessions/{id}.json
```

#### 会话文件格式 (JSON):
```json
{
    "id": "instance:type:target",
    "instance_id": "mybot",
    "session_type": "group",
    "platform": "satori",
    "channel_id": "12345",
    "display_name": "Test Group",
    "permission_group": "default",
    "created_at": 1712950440.123,
    "last_active": 1712950500.456,
    "config": {
        "prefixes": ["/"],
        "llm_enabled": true,
        "is_muted": false,
        "audit_enabled": false
    },
    "state": {},
    "plugin_data": {}
}
```

---

### Step 3: 运行时热重载与审计日志

#### 热重载 API (shinbot/api/admin.py)

**新增 FastAPI 端点:**

1. **GET `/admin/plugins`**
   - 列表所有已加载插件
   - 返回: 插件ID、名称、版本、命令列表、事件订阅

2. **POST `/admin/plugins/{plugin_id}/reload`**
   - 重新加载指定插件
   - 清理旧的命令/事件注册
   - 返回: 重新加载后的命令和事件列表

3. **POST `/admin/plugins/rescan`**
   - 重新扫描 `data/plugins/` 目录
   - 加载新插件（无需重启）
   - 返回: 新加载的插件列表

**集成方式:**
```python
from shinbot.api.admin import create_admin_router

app = FastAPI()
admin_router = create_admin_router(bot)
app.include_router(admin_router)
```

#### 审计日志 (shinbot/core/audit.py)

**AuditLog 条目结构:**
```python
@dataclass
class AuditLog:
    timestamp: str                      # ISO 8601
    command_name: str                   # 执行的命令
    plugin_id: str                      # 命令所属插件
    user_id: str                        # 执行用户
    session_id: str                     # 会话ID
    instance_id: str                    # Bot实例
    permission_required: str            # 需要的权限
    permission_granted: bool            # 是否已授予
    execution_time_ms: float            # 执行耗时（毫秒）
    success: bool                       # 是否成功
    error: str                          # 错误信息（如有）
    metadata: dict[str, Any]            # 额外元数据
```

**持久化:**
- 日志记录到 `data/audit/audit_YYYY-MM-DD.jsonl`
- 每天一个文件，按日期轮转
- JSON Lines 格式，每行一条记录

**集成到消息处理管道 (shinbot/core/pipeline.py):**
- 命令处理前记录权限检查结果
- 命令执行时记录耗时
- 记录执行成功/失败和错误信息

**审计日志示例:**
```json
{
    "timestamp": "2026-04-12T13:34:00.595319",
    "command_name": "ping",
    "plugin_id": "com.shinbot.demo",
    "user_id": "user123",
    "session_id": "mybot:group:12345",
    "instance_id": "mybot",
    "permission_required": "cmd.ping",
    "permission_granted": true,
    "execution_time_ms": 10.5,
    "success": true,
    "error": "",
    "metadata": {"raw_args": "test", "message_count": 1}
}
```

---

## 应用启动流程

**main.py 改进:**

1. 创建 `ShinBot(data_dir="data")` 实例
   - SessionManager 初始化数据目录
   - AuditLogger 初始化审计目录

2. 调用 `_load_plugins()` 加载插件
   - 优先尝试扫描 `data/plugins/`（元数据驱动）
   - 回退支持配置文件方式（向后兼容）

3. 设置权限绑定
4. 启动所有适配器

**配置示例 (config.toml):**
```toml
[logging]
level = "INFO"

[[instances]]
id = "mybot"
platform = "satori"

[instances.satori]
host = "localhost:5140"
token = "your-token"

# 如需配置文件加载方式（可选）
[[plugins]]
id = "legacy_plugin"
module = "plugins.legacy"
```

---

## 核心特性

### 1. 数据目录隔离
- ✓ 所有用户数据存储在 `data/` 目录
- ✓ 核心代码无硬编码路径
- ✓ 支持多实例配置

### 2. 动态性与热重载
- ✓ 运行时通过 API 重新加载插件
- ✓ 自动清理旧注册（命令、事件）
- ✓ 支持扫描新插件无需重启

### 3. 审计与监控
- ✓ 每个命令执行都被记录
- ✓ 记录执行耗时便于性能分析
- ✓ 权限检查结果被审计
- ✓ 持久化到 jsonl 格式便于日志分析

### 4. 权限管理
- ✓ 元数据中声明权限需求
- ✓ 装饰器注册时验证权限
- ✓ 运行时权限检查与审计

---

## 测试验证

已创建的测试脚本：

1. **test_init.py** - 基础初始化测试
   ```bash
   python test_init.py
   ```
   结果: ✓ 所有系统初始化成功

2. **test_integration.py** - 集成测试
   ```bash
   python test_integration.py
   ```
   结果: ✓ 所有集成测试通过

3. **test_sessions.py** - 会话持久化测试
   ```bash
   python test_sessions.py
   ```
   结果: ✓ 所有会话持久化测试通过

---

## 文件变更清单

### 修改
- `shinbot/core/plugin.py` - 添加元数据驱动加载和权限校验
- `shinbot/core/app.py` - 支持 data_dir 参数，集成审计日志
- `shinbot/core/pipeline.py` - 集成审计日志记录
- `main.py` - 支持元数据驱动加载和 data_dir 初始化

### 新增
- `shinbot/core/audit.py` - 审计日志系统
- `shinbot/api/admin.py` - 管理 API（热重载）

---

## 使用建议

### 开发环境
```bash
# 清除旧数据（可选）
rm -rf data/sessions data/audit

# 运行应用
python main.py

# 查看审计日志
cat data/audit/audit_*.jsonl
```

### 生产环境
- 定期备份 `data/sessions/` 和 `data/audit/`
- 监控 `data/audit/` 文件大小
- 使用日志分析工具合并多日期审计文件
- 通过 `/admin/plugins/rescan` API 部署新插件无需重启
"""
