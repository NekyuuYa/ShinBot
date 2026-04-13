# ShinBot 插件系统快速参考

## 快速开始

### 1. 启动应用
```bash
python main.py --config config.toml --log-level INFO
```

应用会自动：
- 从 `data/plugins/` 加载所有元数据插件
- 从 config.toml 加载配置的插件（可选）
- 初始化 SessionManager（会话持久化）
- 初始化 AuditLogger（审计日志）

### 2. 查看加载的插件
```bash
# 通过日志查看
grep "Loaded plugin" console_output.log

# 或查看审计日志
ls -la data/audit/
```

---

## 目录结构

```
data/
├── plugins/              # 元数据驱动的插件
│   ├── demo_plugin/
│   │   ├── metadata.json          # ⚠️ 必需！
│   │   ├── __init__.py            # 入点文件
│   │   └── utils.py               # 其他文件
│   └── another_plugin/
│       ├── metadata.json
│       ├── main.py
│       └── ...
├── sessions/             # 会话存储（自动创建）
│   └── *.json
└── audit/                # 审计日志（自动创建）
    └── audit_YYYY-MM-DD.jsonl
```

---

## 元数据格式

**必需**: metadata.json 在每个插件根目录

```json
{
    "id": "com.example.plugin",
    "name": "Plugin Display Name",
    "version": "1.0.0",
    "author": "Your Name",
    "description": "Plugin description",
    "entry": "__init__.py",
    "permissions": [
        "cmd.command1",
        "cmd.command2"
    ]
}
```

| 字段 | 必需 | 说明 |
|-----|-----|-----|
| `id` | ✅ | 插件唯一标识（反向域名制） |
| `name` | ✅ | 显示名称 |
| `version` | ❌ | 版本号 |
| `author` | ❌ | 作者 |
| `description` | ❌ | 描述 |
| `entry` | ✅ | 入点文件相对路径 |
| `permissions` | ✅ | 声明的权限列表 |

---

## 权限系统

### 权限树结构
```
cmd.*                   # 所有命令
├── cmd.ping
├── cmd.ask
└── cmd.weather

tools.*                 # 工具类
├── tools.translate
└── tools.weather

sys.*                   # 系统类
├── sys.reboot
└── sys.config
```

### 权限校验
```python
# 在 metadata.json 中声明
"permissions": ["cmd.ping"]

# 在 setup() 中使用
@ctx.on_command("ping", permission="cmd.ping")
async def ping_handler(c, args):
    await c.reply("pong!")
```

**⚠️ 重要**: metadata.json 中的权限必须包含所有 decorator 中使用的权限。

---

## 会话管理

### 会话自动持久化
```python
# 应用自动处理：
# 1. 会话创建时 → 内存 + 磁盘 (data/sessions/xxx.json)
# 2. 会话更新时 → 内存 + 磁盘
# 3. 程序重启 → 从磁盘恢复
```

### 会话数据结构
```json
{
    "id": "mybot:group:12345",
    "instance_id": "mybot",
    "session_type": "group",
    "platform": "satori",
    "channel_id": "12345",
    "permission_group": "default",
    "state": {},
    "plugin_data": {}
}
```

---

## 审计日志

### 日志位置
```
data/audit/audit_2026-04-12.jsonl

# 每行一条 JSON 记录
{"timestamp": "...", "command_name": "ping", ...}
{"timestamp": "...", "command_name": "ask", ...}
```

### 日志内容示例
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
    "metadata": {}
}
```

### 查询示例
```bash
# 查看所有日志
cat data/audit/audit_*.jsonl | jq '.'

# 查看特定用户
cat data/audit/audit_*.jsonl | jq 'select(.user_id == "user123")'

# 查看失败的命令
cat data/audit/audit_*.jsonl | jq 'select(.success == false)'

# 查看权限被拒绝的
cat data/audit/audit_*.jsonl | jq 'select(.permission_granted == false)'

# 统计平均执行时间
cat data/audit/audit_*.jsonl | jq '.execution_time_ms' | jq -s 'add/length'

# 查找慢命令（>1秒）
cat data/audit/audit_*.jsonl | jq 'select(.execution_time_ms > 1000)'
```

---

## 热重载 API

### API 端点

#### 1. 列表插件
```bash
GET /admin/plugins

# 响应
[
    {
        "id": "com.shinbot.demo",
        "name": "Demo",
        "version": "1.0.0",
        "state": "active",
        "commands": ["ping", "echo", "ask"],
        "event_types": ["message-created"]
    }
]
```

#### 2. 重新加载插件
```bash
POST /admin/plugins/{plugin_id}/reload

# 示例
curl -X POST http://localhost:8000/admin/plugins/com.shinbot.demo/reload

# 响应
{
    "status": "ok",
    "plugin_id": "com.shinbot.demo",
    "commands": ["ping", "echo", "ask"],
    "event_types": ["message-created"]
}
```

#### 3. 扫描新插件
```bash
POST /admin/plugins/rescan

# 示例
curl -X POST http://localhost:8000/admin/plugins/rescan

# 响应
{
    "status": "ok",
    "loaded_count": 2,
    "plugins": [
        {"id": "com.example.new", "name": "New Plugin", "commands": ["cmd1"]}
    ]
}
```

### 与 FastAPI 集成

```python
from fastapi import FastAPI
from shinbot.core.app import ShinBot
from shinbot.api.admin import create_admin_router

app = FastAPI()
bot = ShinBot(data_dir="data")

# 添加管理 API 路由
admin_router = create_admin_router(bot)
app.include_router(admin_router)

# 现在可以访问：
# GET  /admin/plugins
# POST /admin/plugins/{plugin_id}/reload
# POST /admin/plugins/rescan
```

---

## 常见任务

### 添加新插件（无需重启）

```bash
# 1. 创建目录
mkdir -p data/plugins/my_plugin

# 2. 创建 metadata.json
cat > data/plugins/my_plugin/metadata.json << 'EOF'
{
    "id": "com.example.my_plugin",
    "name": "My Plugin",
    "version": "1.0.0",
    "entry": "__init__.py",
    "permissions": ["cmd.mycommand"]
}
EOF

# 3. 创建 __init__.py
cat > data/plugins/my_plugin/__init__.py << 'EOF'
def setup(ctx):
    @ctx.on_command("mycommand", permission="cmd.mycommand")
    async def mycommand(c, args):
        await c.reply("Hello from my plugin!")
EOF

# 4. 通过 API 加载
curl -X POST http://localhost:8000/admin/plugins/rescan

# 5. 验证
curl http://localhost:8000/admin/plugins | jq 'map(.id)'
```

### 更新现有插件

```bash
# 1. 修改 __init__.py（或其他文件）
vim data/plugins/demo_plugin/__init__.py

# 2. 重新加载
curl -X POST http://localhost:8000/admin/plugins/com.shinbot.demo/reload

# 3. 验证
cat data/audit/audit_*.jsonl | jq 'select(.plugin_id == "com.shinbot.demo")' | tail -5
```

### 查看插件执行统计

```bash
# 查看插件执行的命令数
cat data/audit/audit_*.jsonl | jq -r '.plugin_id' | sort | uniq -c

# 查看插件的平均响应时间
cat data/audit/audit_*.jsonl | jq '[group_by(.plugin_id)[] | {plugin: .[0].plugin_id, avg_time: (map(.execution_time_ms) | add / length)}]'

# 查看失败率最高的插件
cat data/audit/audit_*.jsonl | jq '[group_by(.plugin_id)[] | {plugin: .[0].plugin_id, fail_rate: (map(select(.success == false)) | length / length * 100)}]'
```

---

## 故障排除

### 插件加载失败

**症状**: 看不到预期的命令
**解决**:
1. 检查 metadata.json 格式
2. 检查 entry 文件是否存在
3. 查看日志: `grep "Failed to load" console_output.log`

### 权限不足

**症状**: 收到"权限不足"消息
**解决**:
1. 检查 metadata.json 中的 permissions 列表
2. 检查装饰器中 permission 参数是否在列表中
3. 检查用户权限绑定关系

### 会话丢失

**症状**: 程序重启后会话数据消失
**解决**:
1. 检查 data/sessions/ 目录是否存在
2. 确认 `.gitignore` 不会排除 data/ 目录备份
3. 检查写入权限: `ls -la data/`

---

## 性能建议

- **插件数量**: 支持数百个插件
- **会话数量**: 数千个会话无压力
- **审计日志**: 每天日志大小取决于命令频率，建议定期归档
- **热重载**: 延迟通常 < 100ms

---

## 相关文件

- 实现详情: `PLUGIN_SYSTEM_IMPL.md`
- 完成报告: `COMPLETION_SUMMARY.md`
- 设计文档: `docs/design/07_plugin_system_design.md`
- 示例插件: `data/plugins/demo_plugin/`
