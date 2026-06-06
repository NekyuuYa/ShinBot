# Permission Groups 功能规划

## 1. 现状分析

ShinBot 当前权限链路已经具备基础 RBAC 形态，但管理能力和持久化边界仍然偏固定。

当前核心实现位于 `shinbot/core/security/permission.py`：

- `PermissionGroup` 是内存模型，包含 `id`、`name`、`permissions`。
- 内置三组权限：
  - `default`：基础命令，如 `cmd.help`、`cmd.ping`、`cmd.about`、`cmd.whoami`。
  - `admin`：`tools.*`、`sys.*`、`cmd.*`。
  - `owner`：`*`。
- `PermissionEngine` 在内存中维护 `_groups` 和 `_bindings`，支持：
  - 添加/删除权限组。
  - 按 binding key 绑定权限组。
  - 合并全局、会话、会话默认权限。
  - 支持通配符和显式拒绝，例如 `tools.*`、`-tools.dangerous`。

当前权限解析发生在消息入口 `MessageIngress`：

- adapter 事件进入后，根据 bot service routing 选择 bot。
- 调用 `permission_scope_for_event()` 生成权限作用域：
  - 无 bot router 时保持 adapter/session 作用域。
  - 有 bot router 时使用 `{bot_id}` 和 `{bot_id}:{session_key}`。
- `PermissionEngine.resolve()` 计算用户有效权限，并写入 `MessageContext.permissions`。
- 后续指令调用 `bot.has_permission(permission)`，只检查这份已解析集合。

当前指令权限链路位于 `shinbot/core/message_routes/command.py` 和 `shinbot/core/plugins/context.py`：

- 插件通过 `@plg.on_command(..., permission="cmd.xxx")` 声明指令所需权限。
- `CommandDef.permission` 是单个权限点字符串。
- `TextCommandDispatcher` 执行前检查 `match.command.permission`。
- 权限不足时直接回复 `权限不足：需要 <permission>`，不会进入 handler。
- `CommandRegistry` 目前只支持命令启停覆盖，不支持修改命令所需权限。

当前配置和持久化状态：

- `config.example.toml` / `config.reference.toml` 只示例了 `[permissions].bindings`。
- `BootController._setup_permissions()` 只从配置读取 binding，不读取自定义 group。
- `apply_bot_admin_bindings()` 会根据 `[[bots]].administrators` 生成 bot-scoped admin 绑定。
- SQLite schema 目前只有：
  - `sessions.permission_group`：会话默认权限组 ID。
  - `audit_logs.permission_required` / `permission_granted`：审计记录。
- 没有权限组、权限绑定、指令权限覆盖的专用表。
- Dashboard API 当前有 `/commands`，可列出命令并切换 enabled；没有 `/permissions/groups` 实现。

### 主要局限

1. 权限组不可持久化管理

   `PermissionEngine.add_group()` 虽然存在，但没有配置加载、配置文件持久化、API 或 WebUI 入口。实际生产只能使用内置 `default/admin/owner`。

2. 权限绑定只来自配置和 bot administrators

   `[permissions].bindings` 可写 binding，但需要改 TOML 并重启/重新加载。运行时缺少增删改查接口，也没有审计化变更记录。

3. 一个 binding 只能绑定一个 group

   `_bindings: dict[str, str]` 使用户在一个 scope 下只能归属一个权限组。复杂场景需要复制权限组，例如“群管 + 搜索工具”无法组合授权。

4. 指令权限由插件硬编码声明

   `CommandDef.permission` 来自插件注册参数。管理员无法把某个指令移动到自定义权限组，除非修改插件代码或让权限组包含该指令的权限点。

5. 指令权限点和权限组之间缺少显式归属关系

   当前模型本质是“指令需要某权限点，用户权限组包含该权限点”。缺少“某指令属于哪些权限组”的管理视图，也缺少对命令权限覆盖的持久化模型。

6. 缺少内置组保护策略

   内置 `default/admin/owner` 当前只是普通内存 group。未来开放 API 后，需要防止误删 `owner`、清空 `admin`、破坏默认启动能力。

7. 多 bot 场景需要更清晰的 scope 语义

   现有 bot service 已引入 `{bot_id}:{user_id}` 和 `{bot_id}:{session_key}.{user_id}`，但配置注释中 session key 示例和代码实际拼接细节需要统一文档化，避免 adapter user id 是否带前缀造成混乱。

## 2. 目标和非目标

### 目标

- 支持管理员创建、修改、删除自定义权限组。
- 支持为权限组配置权限点、显式拒绝权限点、描述和系统保护标记。
- 支持运行时管理用户/会话 scope 到权限组的绑定。
- 支持将指令所需权限从插件默认值覆盖为管理员配置。
- 支持通过权限组管理指令可见性和可调用性。
- 保持现有插件 API、adapter、`MessageContext.has_permission()` 基本兼容。
- 提供可回滚、可降级的实施路径。

### 非目标

- 不在第一阶段实现复杂 ABAC 条件表达式，例如时间、频率、地理位置、消息内容条件。
- 不替换现有 permission node 语义；继续使用点分字符串、通配符和负向权限。
- 不要求 adapter 感知权限组；adapter 仍只负责事件归一化和发送。
- 不改变插件声明权限的基本方式；插件默认权限仍由 `@plg.on_command(permission=...)` 提供。

## 3. 权限组功能设计

### 3.1 核心概念

权限系统分为四层：

1. Permission Node

   点分权限字符串，例如：

   - `cmd.help`
   - `cmd.mute`
   - `tools.search.query`
   - `sys.*`
   - `-cmd.mute`

2. Permission Group

   可复用权限集合。可被用户、会话默认权限、bot 管理员机制引用。

3. Permission Binding

   将主体绑定到一个或多个权限组。

   主体 key 沿用当前格式：

   - 全局 bot scope：`{bot_id}:{user_id}`
   - 会话 bot scope：`{bot_id}:{session_key}.{platform_user_id}`
   - 兼容 adapter scope：`{adapter_instance_id}:{user_id}`
   - 兼容旧 session scope：`{session_id}.{user_id}`

4. Command Permission Override

   管理员对 `CommandDef.permission` 的持久化覆盖。用于改变某个命令实际需要的权限点。

### 3.2 数据模型

核心原则是尽量使用非数据库存储。权限配置以 TOML 文件为权威来源，方便管理员直接手动修改、审阅和用 git 追踪变更。SQLite 仅保留天然适合数据库的内容：

- 审计日志：时序数据，量大，适合查询和归档。
- `sessions.permission_group`：现有会话默认权限组字段继续保留在 `sessions` 表中。

不新增以下 SQLite 表：

- `permission_groups`
- `permission_bindings`
- `command_permission_overrides`

#### 权限组配置

权限组定义存储在 TOML 中：

```toml
[[permissions.groups]]
id = "moderator"
name = "Moderator"
description = "群管理常用指令"
permissions = [
  "cmd.mute",
  "cmd.help",
  "cmd.whoami",
]
```

约束：

- `id` 使用稳定 slug，只允许 `[a-zA-Z0-9_.:-]`。
- 内置组 `default/admin/owner` 由代码注册并可在 TOML 中覆盖展示字段或追加权限，但仍受保护策略约束。
- `owner` 禁止删除，禁止移除 `*`。
- `default` 禁止删除，但允许追加权限；是否允许移除基础命令由产品策略决定。

#### 权限绑定配置

权限绑定存储在 TOML 中：

```toml
[[permissions.bindings]]
key = "full-agent:group:20001.qq-main:123456789"
groups = ["moderator", "search_user"]
```

说明：

- 新模型允许同一 `scope_key` 绑定多个 group。
- `[[bots]].administrators` 派生的 admin binding 不写入 TOML，仍在启动时动态应用。
- 手动配置和管理 API 修改的 binding 写回 TOML，保持文件可读和可追踪。
- 继续兼容旧字段 `group = "admin"`；新字段 `groups = [...]` 优先。

#### 指令权限覆盖配置

指令权限覆盖存储在 TOML 中：

```toml
[[permissions.command_overrides]]
command = "mute"
permission = "cmd.moderation.mute"
```

说明：

- `permission = ""` 表示无权限要求。
- 无配置时使用插件声明的 `CommandDef.permission`。
- 有配置时覆盖运行时 `CommandDef.permission`。
- 删除 override 配置即恢复插件默认值。

### 3.3 内存 API

扩展 `PermissionEngine`，保持现有方法兼容：

- 保留 `bind(key, group_id)`，语义为兼容单组绑定。
- 新增 `bind_group(key, group_id, source='manual')`。
- 新增 `unbind_group(key, group_id, source=None)`。
- 新增 `groups_for_key(key) -> tuple[str, ...]`。
- 新增 `set_groups_for_key(key, group_ids)`，用于管理端整体替换。
- `resolve()` 合并同一 key 下所有 group。
- `binding_keys()` 继续返回所有 scope key。

为了兼容旧代码，可以内部从 `_bindings: dict[str, str]` 迁移为 `_bindings: dict[str, set[str]]`，并让 `get_binding()` 在多组场景下返回第一个或仅用于兼容。新代码不再依赖 `get_binding()`。

新增服务层 `PermissionGroupService`，避免 API 和 boot 直接操作 engine：

- `list_groups()`
- `get_group(group_id)`
- `create_group(payload)`
- `update_group(group_id, payload)`
- `delete_group(group_id)`
- `list_bindings(scope_key=None, group_id=None)`
- `set_binding(scope_key, group_ids)`
- `remove_binding(scope_key, group_id=None)`
- `load_from_toml(config)`
- `save_to_toml(config_path)`
- `load_into_engine(engine)`

服务层负责：

- 校验 group id 和 permission node 格式。
- 保护内置组。
- 读写 TOML 配置文件，并保持稳定排序和清晰格式。
- 刷新 `PermissionEngine`。
- 记录管理审计。

### 3.4 存储策略

推荐优先级：

1. TOML `[permissions]` 是权限组、权限绑定和指令权限覆盖的权威存储。
2. `PermissionEngine` 是运行时缓存，启动时从 TOML 加载，管理 API 修改后刷新内存。
3. SQLite 只保留审计日志和 `sessions.permission_group`。

启动流程建议：

1. 创建 `PermissionEngine`，注册内置组。
2. 数据库初始化现有 schema，只包含会话和审计等业务表；不创建权限组、权限绑定、指令权限覆盖表。
3. 读取 TOML：
   - `[[permissions.groups]]` 加载自定义权限组。
   - `[[permissions.bindings]]` 加载 scope 到 group 的绑定。
   - `[[permissions.command_overrides]]` 加载 command 到 permission 的覆盖。
4. 校验 TOML 中引用的 group 是否存在；不存在时记录 warning，并跳过对应 binding 或回退到 `default`。
5. 将 groups/bindings 加载到 `PermissionEngine`。
6. 应用 bot administrators 派生绑定。
7. 应用 command permission overrides。

运行时写入策略：

- 管理 API 对权限组、权限绑定、指令权限覆盖的修改先通过服务层校验。
- 校验通过后写回 TOML 文件，再刷新 `PermissionEngine` 和 command registry。
- 写回时保持 TOML 清晰、人类可读：按 group id、binding key、command name 稳定排序；权限数组去重并稳定排序；避免写入运行时派生的 bot administrator binding。
- 写回前可先写临时文件再原子替换，避免进程中断导致配置文件损坏。
- 变更仍写入审计日志，方便追踪管理动作。

配置扩展示例：

```toml
[permissions]

[[permissions.groups]]
id = "moderator"
name = "Moderator"
description = "群管理常用指令"
permissions = [
  "cmd.help",
  "cmd.mute",
  "cmd.whoami",
]

[[permissions.groups]]
id = "search_user"
name = "Search User"
permissions = ["tools.search.*"]

[[permissions.bindings]]
key = "full-agent:group:20001.qq-main:123456789"
groups = ["moderator", "search_user"]

[[permissions.command_overrides]]
command = "mute"
permission = "cmd.moderation.mute"
```

兼容规则：

- 继续支持旧字段 `group = "admin"`。
- 新字段 `groups = ["admin", "search_user"]` 优先。
- 同一 binding 同时有 `group` 和 `groups` 时记录 warning，只使用 `groups`。

## 4. 指令权限归属机制

### 4.1 基本原则

指令权限不直接存“指令属于某权限组”，而是通过稳定 permission node 做中间层：

- 指令声明或覆盖实际需要的 permission node。
- 权限组包含 permission node。
- 用户绑定权限组后获得调用权。

原因：

- 保持 `bot.has_permission("cmd.xxx")` 不变。
- 工具权限、系统权限、未来 API 权限可以共用同一 RBAC 引擎。
- 支持通配符和显式拒绝。

管理界面可以呈现为“把指令加入权限组”，但持久化时实际是给 TOML 中的 group 增加该命令当前 required permission。

### 4.2 指令默认权限

插件仍通过当前方式声明默认权限：

```python
@plg.on_command("mute", permission="cmd.mute")
```

加载插件后，系统从 `CommandRegistry.all_commands` 收集：

- `command.name`
- `command.aliases`
- `command.owner`
- `command.description`
- `command.usage`
- `command.permission`
- `command.enabled`

这组数据作为“命令目录”，不需要单独持久化完整命令定义。

### 4.3 指令权限覆盖

新增 command permission override：

- API 可以把 `mute` 的 required permission 从 `cmd.mute` 改成 `cmd.group.moderation`。
- `TextCommandDispatcher` 无需感知 override 来源，只读取最终的 `CommandDef.permission`。
- 插件 reload 后重新注册命令时，`CommandRegistry.register()` 或启动阶段服务重新应用 override。

建议扩展 `CommandAdmin`：

- `load_command_permission_overrides(config)`
- `apply_command_permission_overrides(command_registry, store)`
- `set_command_permission_or_raise(command_registry, command_name, permission)`
- `clear_command_permission_override(command_registry, command_name)`

### 4.4 将指令绑定到权限组

管理动作“将指令 A 绑定到权限组 G”定义为：

1. 找到命令 A 的最终 required permission。
2. 将该 permission 加入权限组 G 的 `permissions`。
3. 如果命令没有 permission，则先生成默认权限点：
   - 推荐格式：`cmd.{owner}.{command_name}` 或 `cmd.{command_name}`。
   - 为避免跨插件重名，内部建议使用 `cmd.{plugin_id}.{command_name}`。
   - 展示层可显示为 `/command`。
4. 写回 TOML 中的 command override，使命令开始要求该权限点。
5. 刷新 engine 中 group 权限。

管理动作“从权限组 G 移除指令 A”定义为：

1. 找到命令 A 的最终 required permission。
2. 从 G 的 `permissions` 删除该 permission。
3. 不自动删除 command permission override，因为其他 group 可能仍使用该权限点。
4. 如果没有任何 group 引用该 override，可提示管理员是否恢复插件默认权限。

### 4.5 帮助列表和可见性

当前 `/help` 会列出所有命令，没有按权限过滤。建议后续改为：

- 默认只显示当前用户有权限调用的命令。
- 管理员可通过 `/help all` 或 Dashboard 查看完整命令目录。
- 对无权限命令可以选择隐藏，减少普通用户误触。

这属于体验层增强，不影响第一阶段权限正确性。

## 5. 与现有系统的集成方案

### 5.1 core/security

改造重点：

- 扩展 `PermissionEngine` 支持多 group binding。
- 内置组声明继续在 `permission.py` 中保留，作为系统默认和测试基线。
- 校验逻辑 `check_permission()` 不变。
- `merge_permissions()` 不变。
- 新增 permission node 校验工具，避免 API 写入空字符串、非法通配符或重复负号。

兼容要求：

- 现有测试中 `permission_engine.bind("inst1:user1", "admin")` 必须继续通过。
- `resolve()` 对旧单 group binding 的结果保持一致。
- 显式 deny 优先级保持一致。

### 5.2 command system

改造重点：

- `CommandDef.permission` 继续作为执行时唯一判断字段。
- `CommandRegistry` 增加 permission override store，类似现有 `_enabled_overrides`。
- `register()` 时如果存在 permission override，覆盖新注册的 `CommandDef.permission`。
- `/api/v1/commands` 返回：
  - `permission`：当前最终权限。
  - `defaultPermission`：插件声明默认权限。
  - `permissionOverridden`：是否被管理员覆盖。

需要注意：

- 当前 `CommandDef` 没有保存 default permission。需要新增字段或在 registry 中记录原始值。
- 插件 reload、disable、enable 时命令会重新注册，override 必须可重复应用。

### 5.3 plugin system

插件 API 保持兼容：

- `plg.on_command(..., permission=...)` 不变。
- `metadata.json.permissions` 继续作为插件声明权限清单。
- `PluginManager._validate_permissions()` 继续检查插件使用了未声明权限的情况。

扩展建议：

- 允许 `metadata.json` 增加可选 `permission_groups`，用于插件建议默认组，但不自动授予高危权限：

```json
{
  "permissions": ["cmd.foo.admin"],
  "permission_groups": [
    {
      "id": "foo_admin",
      "name": "Foo Admin",
      "permissions": ["cmd.foo.admin"]
    }
  ]
}
```

加载策略：

- 插件建议组只作为“可写入 TOML 的导入模板”，默认不覆盖已有同名组。
- 插件卸载不删除管理员已导入的权限组，避免误删授权。
- 插件禁用后，命令消失，但权限组中遗留 permission node 可保留，Dashboard 标记为 orphan permission。

### 5.4 adapter

adapter 不需要改造权限逻辑。

需要补齐的是文档和 key 规范：

- adapter 仍提供 `adapter.instance_id`、`event.sender_id`、`guild_id`、`channel_id`。
- bot routing 层继续把平台事件映射成 bot scope。
- 权限系统只消费标准化后的 `identity_id`、`session_id`、`user_id`。

风险点：

- 现有 `candidate_user_keys()` 同时支持带 adapter 前缀和裸 user id。开放管理接口时，必须在 UI/API 中明确推荐使用带平台前缀的 user id，避免多 adapter 用户 ID 碰撞。

### 5.5 bot config

`[[bots]].administrators` 继续保留，作为快捷授权：

- 启动时派生 `bot_admin` source binding。
- 默认绑定到 `admin`。
- 后续可扩展：

```toml
[[bots]]
id = "full-agent"
administrators = ["qq-main:123456789"]
administrator_group = "owner"
```

建议先不引入 `administrator_group`，避免扩大配置面。第一阶段维持 administrator -> admin。

`sessions.permission_group` 继续表示会话默认权限组：

- 当会话默认权限组 ID 不存在时，回退到 `default` 并记录 warning。
- Dashboard 可提供“设置当前会话默认权限组”的能力。

### 5.6 API 和 Dashboard

后端 API 建议：

- `GET /api/v1/permissions/groups`
- `POST /api/v1/permissions/groups`
- `GET /api/v1/permissions/groups/{group_id}`
- `PATCH /api/v1/permissions/groups/{group_id}`
- `DELETE /api/v1/permissions/groups/{group_id}`
- `GET /api/v1/permissions/bindings?scopeKey=&groupId=`
- `PUT /api/v1/permissions/bindings/{scope_key}`
- `DELETE /api/v1/permissions/bindings/{scope_key}`
- `PATCH /api/v1/commands/{command_name}/permission`
- `DELETE /api/v1/commands/{command_name}/permission`

Dashboard 页面建议：

- 权限组列表：显示名称、权限数量、绑定数量、是否内置/保护。
- 权限组详情：
  - 权限点编辑器。
  - 指令选择器。
  - 显式拒绝权限列表。
  - 当前绑定主体列表。
- 指令页面增加“所需权限”和“加入权限组”操作。
- 用户/会话绑定页支持复制 `/whoami` 输出的 binding key。

### 5.7 审计和观测

当前 command audit 已记录：

- `permission_required`
- `permission_granted`
- `command_name`
- `plugin_id`
- `user_id`
- `session_id`
- `instance_id`

新增管理审计建议：

- `permission_group.created`
- `permission_group.updated`
- `permission_group.deleted`
- `permission_binding.updated`
- `command_permission.updated`

审计 metadata 至少包含：

- actor admin user。
- old/new group permissions。
- old/new bindings。
- old/new command permission。

## 6. 分阶段实施计划

### Phase 0：设计确认和测试基线

交付：

- 补充权限组规划文档。
- 明确 binding key 标准示例。
- 为现有行为补测试快照：
  - 内置组权限。
  - 单 group binding。
  - bot administrators 派生 admin。
  - command permission denied。

验收：

- 不改变现有运行行为。

### Phase 1：权限组持久化和多组绑定

交付：

- TOML 增加 `[[permissions.groups]]` 和 `[[permissions.bindings]]`。
- 新增配置读写 service。
- `PermissionEngine` 支持一个 scope 多个 group。
- TOML `group` 兼容，新增 `groups`。
- 启动时从 TOML 加载权限组和绑定。

验收：

- 旧配置可无修改启动。
- `default/admin/owner` 自动存在。
- 同一用户绑定 `moderator + search_user` 后获得两个 group 的合集权限。
- 显式 deny 仍优先于 grant。

### Phase 2：管理 API

交付：

- 新增 `/permissions/groups` 和 `/permissions/bindings` API。
- API 输入校验、内置组保护、错误码。
- 管理动作写回 TOML 并刷新 engine。
- 管理动作审计。

验收：

- 可通过 API 创建 group、添加权限、绑定用户、立即生效。
- 不能删除 `owner/default/admin`。
- 不能绑定不存在的 group。

### Phase 3：指令权限覆盖

交付：

- TOML 增加 `[[permissions.command_overrides]]`。
- `CommandRegistry` 支持默认权限和最终权限。
- commands API 返回默认/最终权限和 override 状态。
- commands API 支持修改/恢复命令权限。
- 插件 reload 后 override 仍生效。

验收：

- 管理员可把 `/mute` 从 `cmd.mute` 改为 `cmd.moderation.mute`。
- 用户只有新权限点时可调用；只有旧权限点时不可调用。
- 删除 override 后恢复插件声明权限。

### Phase 4：Dashboard 权限组管理

交付：

- 权限组页面。
- 绑定管理。
- 指令加入/移出权限组。
- 命令页展示权限覆盖状态。

验收：

- 管理员既可通过 Dashboard 完成常见授权，也可直接编辑 TOML。
- 普通操作有确认和错误反馈。
- 内置保护限制在 UI 中明确禁用。

### Phase 5：体验和治理增强

交付：

- `/help` 按当前用户权限过滤。
- orphan permission 检测。
- 插件建议权限组导入。
- 权限变更审计查询。
- 可选导出权限配置到 JSON，TOML 本身就是权威配置。

验收：

- 权限组可迁移、可备份。
- 禁用/卸载插件后，权限组不产生误授权。

## 7. 风险和回滚策略

### 7.1 兼容性风险

风险：

- `_bindings` 从单值变多值后，依赖 `get_binding()` 的旧代码可能语义变化。

缓解：

- 保留 `bind()`、`unbind()`、`get_binding()` 兼容方法。
- 新功能使用 `groups_for_key()`。
- 测试覆盖旧 API。

回滚：

- 若多组绑定出现问题，可在服务层临时限制每个 scope 只写入一个 group，engine 保持可读多组但实际退回旧行为。

### 7.2 权限扩大风险

风险：

- 管理员误把 `cmd.*` 或 `*` 加入普通组。
- 插件建议组自动导入导致过度授权。

缓解：

- API 对高危权限点提示确认。
- `*`、`sys.*`、`cmd.*` 标记为 high risk。
- 插件建议组不自动授权用户，只创建模板。
- 修改 `owner/admin` 需二次确认或只允许 owner 操作。

回滚：

- TOML 配置可通过 git diff 或备份恢复旧权限列表。
- 提供禁用 group 的 `enabled = false` 快速止血。

### 7.3 锁死管理员风险

风险：

- 错误修改 `owner/admin/default` 可能导致无法管理系统。

缓解：

- `owner` 组不可删除，必须包含 `*`。
- `[[bots]].administrators` 派生 admin 绑定始终在启动时应用。
- 提供本地配置 emergency binding：

```toml
[[permissions.bindings]]
key = "full-agent:qq-main:123456789"
group = "owner"
```

回滚：

- 停机编辑 TOML 增加 owner binding 后重启。
- 如果 TOML 文件损坏，可从备份或 git 恢复；数据库损坏不影响权限组和绑定配置。

### 7.4 配置写回风险

风险：

- 管理 API 写回 TOML 时覆盖管理员刚刚手动编辑的内容。
- TOML 写入过程中进程中断导致文件不完整。
- 旧部署没有数据库或数据库初始化失败。

缓解：

- 写回前检查文件 mtime 或内容 hash，发现外部修改时拒绝覆盖并提示重新加载。
- 使用临时文件 + 原子替换写入 TOML。
- 权限组、权限绑定、指令覆盖不依赖数据库；数据库不可用时只影响审计日志和会话默认权限组。

回滚：

- 从 git 或备份恢复 TOML 配置。
- 删除新增 TOML 段落即可退回内置组和旧 binding 行为。

### 7.5 指令权限覆盖风险

风险：

- 管理员把命令权限改为空，导致敏感命令无权限门槛。
- 插件升级改名后 override 指向不存在命令。

缓解：

- 对空权限和高危命令要求确认。
- Dashboard 标记 orphan override。
- 内置敏感命令可设置 `requires_permission=true`，禁止清空权限。

回滚：

- 删除 TOML 中对应 `[[permissions.command_overrides]]` 记录即可恢复插件默认权限。
- 提供 API 一键清除某插件所有 command override。

## 8. 推荐优先实现边界

第一版应优先保证后端权限正确性：

1. 自定义权限组持久化。
2. 多组绑定。
3. API 管理并即时生效。
4. 指令权限覆盖。

Dashboard 和 `/help` 过滤可以后置。这样最小改动集中在权限引擎、TOML 读写、启动加载、管理 API 和命令 registry，不需要改动 adapter，也不需要重写插件系统。
