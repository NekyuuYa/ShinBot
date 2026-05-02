# ShinBot 实现内幕：业务逻辑基础设施 (Business Logic Infra)

本文档分析了指令、权限和会话这三大核心模块的代码实现细节。

## 1. 指令解析：CommandRegistry
位于 `shinbot/core/message_routes/command.py`。

### 1.1 实现方法
- **三层匹配算法**: 严格实现了 `03_command_system.md` 规定的优先级：
  1. **P0 (Prefix)**: 头部匹配前缀，失败则截断。
  2. **P1 (Exact)**: 全量匹配指令名或别名。
  3. **P2 (Regex)**: 编译后的正则表达式匹配。
- **元数据存储**: 支持存储参数 Schema 和权限节点需求，为后续的自动补全和自动化文档打下基础。

---

## 2. 权限引擎：PermissionEngine
位于 `shinbot/core/security/permission.py`。

### 2.1 实现方法
- **树状路径匹配**: 实现了一个递归的路径校验算法。支持 `tools.*` 这种通配符，极大增强了权限管理的灵活性。
- **显式拒绝逻辑**: 代码中明确了以 `-` 开头的权限点具有最高优先级。
- **多层合并算法**: 严格实现了 `Global | Session-local | Session-base` 的合并规则，确保了权限判定的确定性。

---

## 3. 会话管理：SessionManager
位于 `shinbot/core/state/session.py`。

### 3.1 实现方法
- **URN 生成器**: 实现了标准化的 `instance:type:target` ID 生成逻辑。
- **自动持久化**: `SessionManager` 可挂接持久化仓储，当前实现已经支持数据库记录持久化，并保留运行时内存态作为处理缓存。

## 4. 健壮性总结
- **状态隔离**: 会话数据按 `instance_id` 进行物理目录隔离，防止多账号运行下的数据污染。
- **容错性**: 权限校验在拦截器层级进行，确保了所有未授权请求在进入业务逻辑前被物理阻断。
