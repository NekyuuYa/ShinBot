# ShinBot 技术规范：Tool 注册接口与管理器 (Tool Registry & Manager)

本文档定义 ShinBot 中 Tool 能力的注册接口、运行时管理器与执行边界。

目标不是只给模型传一组 `tools` JSON，而是建立一套稳定的 Tool 基础设施：

- Tool 定义有统一真相源
- 插件与内置模块可以安全注册 Tool
- Agent / Prompt / Model Runtime 只消费规范化后的 Tool 投影
- Tool 调用具备权限控制、审计记录、生命周期管理与热卸载能力

本文档是后续实现“tools 注册接口和管理器”的首要设计依据。

---

## 1. 设计目标

### 1.1 统一注册

- 所有 Tool 必须经统一注册中心登记。
- 禁止 Agent、插件或外部桥接层直接向模型运行时临时塞入未登记 Tool。
- Tool 的名称、参数模式、权限点、宿主来源必须可被统一读取。

### 1.2 宿主解耦

- Tool 可以由内置系统模块、业务插件、适配器桥接层或后续 Skill 模块提供。
- PromptRegistry 与 Model Runtime 不拥有 Tool 定义，只消费 ToolRegistry 的投影结果。
- PluginManager 不直接承担 Tool 编排职责，只负责宿主生命周期与卸载联动。

### 1.3 可治理

- 每个 Tool 必须能声明权限要求、可见性、调用超时与审计策略。
- 管理器必须支持按会话、实例、调用方与权限结果过滤 Tool 可见集。
- Tool 卸载后不得残留“幽灵注册项”。

### 1.4 面向运行时

- 首版重点支持函数调用类 Tool。
- 设计必须兼容后续扩展到：
  - 只读查询 Tool
  - 需要用户确认的高风险 Tool
  - 长任务 / 异步任务 Tool
  - WebUI 可管理 Tool

---

## 2. 非目标

- 本文档不定义 SkillRegistry 的完整设计。
- 本文档不要求首版就实现复杂的 Tool 市场、版本仓库或远程安装。
- 本文档不要求首版支持 MCP 等外部协议接入，但数据结构应预留桥接空间。
- 本文档不直接规定具体某个 Tool 的业务行为。

---

## 3. 模块定位与边界

Tool 相关职责应拆成三层：

1. `ToolRegistry`
   - 管理 Tool 定义元数据
   - 管理宿主归属与注册状态
   - 提供查询、列举、过滤入口
2. `ToolManager`
   - 负责运行时执行、参数校验、权限判定、审计封装
   - 负责将 ToolDefinition 转为模型可消费的 `tools` 投影
   - 负责处理调用超时、异常映射和执行结果封装
3. `Tool Host`
   - 即具体提供 Tool 的宿主模块
   - 如内置系统模块、插件、桥接器
   - 只负责实现 handler，不负责全局编排

必须遵循以下依赖方向：

`Plugin / Builtin Module -> ToolRegistry -> ToolManager -> Model Runtime / Agent`

而不是：

`Plugin -> Model Runtime`

### 3.1 与 PluginManager 的边界

- PluginManager 负责插件加载、停用、热卸载。
- ToolRegistry 负责登记“这个插件提供了哪些 Tool”。
- 插件卸载时，PluginManager 必须通知 ToolRegistry / ToolManager 清理该宿主名下的 Tool。

### 3.2 与 PromptRegistry 的边界

- PromptRegistry 只负责把“当前可见 Tool 声明”装配进 `Abilities` 阶段。
- Tool 的定义、权限与启停状态不由 PromptRegistry 管理。

### 3.3 与 Model Runtime 的边界

- Model Runtime 负责把规范化 Tool 投影传给底层模型。
- Tool 实际执行必须回到 ToolManager，而不是由 Model Runtime 直接调宿主函数。

### 3.4 与 PermissionEngine 的边界

- PermissionEngine 负责判断某调用上下文是否有权使用某 Tool。
- ToolManager 负责把 Tool 权限声明转成实际校验请求，并处理拒绝结果。

---

## 4. 核心概念

### 4.1 ToolDefinition

`ToolDefinition` 是 ToolRegistry 中的标准注册对象。

建议至少包含：

- `id`
- `name`
- `display_name`
- `description`
- `input_schema`
- `output_schema`
- `handler_ref`
- `owner_type`
- `owner_id`
- `permission`
- `enabled`
- `visibility`
- `timeout_seconds`
- `risk_level`
- `tags`
- `metadata`

字段约束：

- `id`
  - 注册表内唯一，建议稳定格式：`{owner_id}.{tool_name}`
- `name`
  - 面向模型的稳定函数名，全局唯一
- `input_schema`
  - JSON Schema 风格结构，供模型调用与运行时校验共用
- `output_schema`
  - 首版可选，但建议预留，便于后续结构化结果与 WebUI 展示
- `handler_ref`
  - 指向真实 handler 的系统引用，不直接暴露任意对象
- `permission`
  - 如 `tools.weather.query`
- `visibility`
  - 见后文枚举
- `risk_level`
  - 如 `low`、`medium`、`high`

### 4.2 ToolSource

`ToolSource` 表示 Tool 的宿主来源。

建议字段：

- `owner_type`
  - `builtin_module` / `plugin` / `adapter_bridge` / `skill_module` / `external_bridge`
- `owner_id`
- `owner_module`
- `is_builtin`
- `metadata`

要求：

- 来源字段由系统补全，不依赖宿主自由填写任意字符串。
- Tool 宿主必须可追踪，便于热卸载与审计。

### 4.3 ToolVisibility

首版建议支持：

- `private`
  - 不对模型自动暴露，仅允许内部显式调用
- `scoped`
  - 需要结合会话、实例、调用方过滤
- `public`
  - 可默认参与可见集，但仍受权限与启用状态约束

### 4.4 ToolCallRequest

表示一次标准化 Tool 执行请求。

建议字段：

- `tool_name`
- `arguments`
- `caller`
- `session_id`
- `instance_id`
- `user_id`
- `trace_id`
- `run_id`
- `dry_run`
- `metadata`

### 4.5 ToolCallResult

表示一次 Tool 执行结果。

建议字段：

- `tool_name`
- `success`
- `output`
- `error_code`
- `error_message`
- `started_at`
- `finished_at`
- `latency_ms`
- `audit_id`
- `metadata`

---

## 5. 注册接口设计

首版建议同时提供两层注册接口：

1. 显式 API
2. 宿主友好的装饰器 / 上下文包装

### 5.1 Registry 级显式接口

建议核心接口如下：

- `register_tool(definition: ToolDefinition) -> None`
- `unregister_tool(tool_id: str) -> None`
- `unregister_owner(owner_type: str, owner_id: str) -> int`
- `get_tool_by_name(name: str) -> ToolDefinition | None`
- `get_tool(tool_id: str) -> ToolDefinition | None`
- `list_tools(...) -> list[ToolDefinition]`
- `list_owner_tools(owner_type: str, owner_id: str) -> list[ToolDefinition]`
- `export_model_tools(...) -> list[dict[str, Any]]`

要求：

- Tool 名冲突必须拒绝注册。
- 同一宿主重复注册同名 Tool 必须报错，不允许静默覆盖。
- Registry 应支持按 `enabled`、`visibility`、`owner_id`、`tag` 等条件筛选。

### 5.2 插件上下文接口

为了与现有插件注册体验保持一致，建议在 `PluginContext` 补一层包装接口：

- `ctx.tool(...)`
- 或 `ctx.register_tool(...)`

推荐风格：

```python
@ctx.tool(
    name="weather_query",
    description="查询城市天气",
    permission="tools.weather.query",
    input_schema={
        "type": "object",
        "properties": {
            "city": {"type": "string"},
        },
        "required": ["city"],
    },
)
async def weather_query(args, runtime):
    ...
```

约束：

- 插件侧不得直接操作全局字典。
- 包装接口最终仍应调用 `ToolRegistry.register_tool()`。
- `owner_type` 与 `owner_id` 由上下文自动补全。

### 5.3 内置模块注册

内置系统模块可以直接使用显式接口注册，但也应走统一宿主标识。

例如：

- `owner_type = "builtin_module"`
- `owner_id = "shinbot.agent.default_tools"`

### 5.4 延迟注册与热卸载

要求：

- 插件 `setup()` 成功完成前，允许暂存注册项。
- 若插件加载失败，相关 Tool 注册必须整体回滚。
- 插件停用、卸载或重载时，必须按 `owner_id` 批量清理。

---

## 6. ToolManager 职责

`ToolManager` 不应只是 `registry + execute()` 的薄封装，而应承担标准运行时职责。

### 6.1 可见集解析

给定调用上下文，ToolManager 应能解析“当前可见 Tool 集合”。

过滤维度至少包括：

- Tool `enabled`
- Tool `visibility`
- 宿主是否启用
- 权限校验是否通过
- 调用方类别
  - `agent.runtime`
  - `plugin.xxx`
  - `admin.console`
- 可选标签
  - 如 `read_only`
  - 如 `chat_session`

### 6.2 模型投影

ToolManager 必须负责把 ToolDefinition 转为模型可消费结构。

首版建议输出 OpenAI/LiteLLM 兼容形态：

```json
{
  "type": "function",
  "function": {
    "name": "weather_query",
    "description": "查询城市天气",
    "parameters": {
      "type": "object",
      "properties": {
        "city": {"type": "string"}
      },
      "required": ["city"]
    }
  }
}
```

注意：

- 投影结果不是新的真相源。
- PromptRegistry 与 Model Runtime 只消费该投影结果。

### 6.3 参数校验

ToolManager 在执行前必须：

1. 查找 ToolDefinition
2. 校验 Tool 是否启用
3. 校验调用权限
4. 按 `input_schema` 校验参数
5. 构造标准运行时上下文
6. 执行 handler
7. 记录审计与执行结果

若校验失败，必须返回结构化错误，而不是直接把 Python 异常暴露给模型。

### 6.4 超时与异常映射

每个 Tool 可声明：

- `timeout_seconds`
- `risk_level`
- `idempotent`
- `requires_confirmation`

首版至少实现：

- 超时中断
- 参数错误
- 权限拒绝
- Tool 不存在
- 宿主不可用
- 内部执行异常

### 6.5 审计与记录

每次 Tool 调用至少应输出：

- `tool_name`
- `owner_id`
- `caller`
- `session_id`
- `instance_id`
- `user_id`
- `success`
- `error_code`
- `latency_ms`
- `arguments_summary`
- `result_summary`
- `trace_id`

后续数据库建议与 `19_database_persistence_architecture.md` 中的 `agent_tool_calls` 对齐。

---

## 7. 生命周期与热重载规则

### 7.1 注册时机

- 内置 Tool：在应用启动时注册
- 插件 Tool：在插件 `setup(ctx)` 阶段注册
- 桥接 Tool：在桥接模块完成能力发现后注册

### 7.2 停用规则

若宿主插件被禁用：

- Tool 不应继续出现在可见集
- 已缓存的模型 Tool 投影必须失效
- 正在执行中的调用可以按策略允许完成，但新调用必须阻断

### 7.3 重载规则

- 宿主重载时，旧 ToolDefinition 必须先整体卸载
- 再注册新定义
- 不允许旧新版本并存，除非后续显式引入版本化策略

---

## 8. 推荐实现骨架

结合当前工程结构，建议首版按以下模块落地：

- `shinbot/agent/tools/schema.py`
  - `ToolDefinition`
  - `ToolCallRequest`
  - `ToolCallResult`
- `shinbot/agent/tools/registry.py`
  - `ToolRegistry`
- `shinbot/agent/tools/manager.py`
  - `ToolManager`
- `shinbot/agent/tools/__init__.py`
  - 对外导出

应用装配建议：

- 在 `ShinBot` 顶层实例中持有 `tool_registry` 与 `tool_manager`
- `PluginContext` 注入 tool 注册接口
- `PromptRegistry` 后续从 `ToolManager.export_model_tools()` 获取 `Abilities`
- `Model Runtime` 在调用时接收筛选后的 `tools`

---

## 9. 与现有系统的对齐要求

### 9.1 权限系统

Tool 权限命名应继续沿用现有树状风格：

- `tools.weather.query`
- `tools.weather.admin`
- `tools.system.reboot`

其中：

- `tools.*` 可表示工具通配权限
- 显式拒绝规则继续由 PermissionEngine 处理

### 9.2 插件系统

插件设计文档已要求“所有注册项可追踪、可撤销”。

Tool 注册也必须满足同一原则：

- 可列举
- 可按宿主批量撤销
- 可热卸载

### 9.3 PromptRegistry

`21_prompt_registry.md` 已明确：

- ToolRegistry 负责能力元数据管理
- PromptRegistry 只消费当前可见能力投影

本设计即为该约束的具体化。

### 9.4 Model Runtime

`18_agent_model_runtime.md` 已预留 `tools` 调用入参。

需要补充的实现约束是：

- 正常业务流中的 `tools` 列表必须来自 ToolManager
- 不允许业务代码直接手写任意 `tools` payload 绕过注册中心

---

## 10. API 与管理面预留

首版可以先不做完整 WebUI，但接口设计应预留管理面能力。

建议后续支持：

- 查询 Tool 列表
- 查询 Tool 来源与启用状态
- 查询 Tool schema
- 查看 Tool 最近调用记录
- 手动启停某个 Tool

注意：

- Tool 的“启停”与插件的“启停”是两层状态
- 若宿主插件停用，则其下 Tool 即使单独标记 enabled 也不得实际生效

---

## 11. 首版实现切片

建议按以下顺序推进：

1. 数据结构与 Registry
   - 先完成 `ToolDefinition` / `ToolRegistry`
   - 支持注册、查找、按宿主卸载
2. ToolManager 最小执行闭环
   - 参数校验
   - 权限校验
   - 超时与异常映射
3. 插件上下文接入
   - 在 `PluginContext` 提供 `ctx.tool(...)`
   - 插件卸载联动批量清理
4. 模型运行时接入
   - 由 ToolManager 导出模型可见 Tool 集
   - 接到 `ModelRuntimeCall.tools`
5. 审计与数据库
   - 补 Tool 调用记录
   - 与 Agent run / step / tool call 结构对齐

---

## 12. 最小验收标准

当以下条件满足时，可认为“tools 注册接口和管理器”首版成立：

- 插件可以通过统一接口注册 Tool
- Tool 名冲突可被阻止
- 插件卸载后 Tool 会被清理
- Agent 可按上下文获取筛选后的模型 Tool 列表
- Tool 调用前会做权限与参数校验
- Tool 调用结果有统一错误模型与审计记录

---

## 13. 后续扩展方向

- Tool 版本化与灰度发布
- 基于风险等级的确认流
- 长任务 Tool 与任务句柄
- Tool result schema 强校验
- Tool 调用缓存
- 外部协议桥接，如 MCP / HTTP tool provider
- Dashboard Tool 管理页

首版不必一次性覆盖上述全部能力，但数据结构和职责分层不得阻碍这些演进。
