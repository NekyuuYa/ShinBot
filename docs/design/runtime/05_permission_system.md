# ShinBot 技术规范：权限系统 (Permission System)

权限系统采用基于角色的访问控制 (RBAC) 理念，支持精细到“会话+用户”维度的权限绑定。

## 1. 权限绑定标识符 (Binding Identifiers)

为了实现精细化控制，我们定义了两种层级的权限主体：

### 1.1 会话内绑定 (Session-scoped)
- **格式**: `{session_id}.{user_id}`
- **说明**: 权限仅在特定的 `Session` (如某个具体的群聊) 中对该用户生效。
- **场景**: 某用户在群 A 是“群管”，可以调用清理消息指令；但在群 B 只是普通用户。

### 1.2 平台级绑定 (Global-scoped)
- **格式**: `{instance_id}:{user_id}` (映射为 `User.{instance_id}:{user_id}`)
- **说明**: 权限跨越该 Bot 实例的所有会话。
- **场景**: Bot 的超级管理员 (Owner)，无论在哪个群组或私聊中，都拥有最高权限。

## 2. 权限树结构 (Permission Tree)
权限是以点分字符串表示的树状结构，例如：
- `tools.weather` (查询天气的根权限)
- `tools.weather.admin` (管理天气的子权限)
- `*` (全量通配符)

## 3. 权限合并与校验算法 (Consolidation)

当用户在一个 `Session` 中发起请求时，系统会按以下优先级合并权限集：

1.  **加载全局权限 (Global)**: 从 `{instance_id}:{user_id}` 检索权限组。
2.  **加载会话权限 (Session-local)**: 从 `{session_id}.{user_id}` 检索权限组。
3.  **加载会话基础权限 (Session-base)**: 从 `Session.permission_group` (即该群默认权限) 检索。
4.  **计算最终合集**: `FinalPermissions = Global | Session-local | Session-base`。

### 校验规则
- **显式拒绝优先**: 如果任何一个层级包含 `-tools.weather` (负向权限)，则该请求被立即拦截。
- **最小权限原则**: 只有在 `FinalPermissions` 中显式包含（或通配符匹配）了所需权限点，操作才被允许。

## 4. 权限组配置 (Permission Groups)
权限组是权限点的集合，可以被重用：
- `DefaultGroup`: 包含 `cmd.help`, `cmd.ping`。
- `AdminGroup`: 包含 `tools.*`, `sys.reboot`。
