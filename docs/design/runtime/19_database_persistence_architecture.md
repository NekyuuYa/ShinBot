# ShinBot 技术规范：数据库与持久化架构 (Database Persistence Architecture)

本文档定义 ShinBot 引入数据库后的持久化边界、数据分层与演进路径。

目标不是“把所有数据都塞进数据库”，而是建立一套清晰、可迁移、可审计的持久化体系：

- 可查询、可关联、可统计、需要事务一致性的数据进入数据库。
- 原始日志、调试转储、媒体资源等大对象继续留在文件系统。
- Agent、模型运行时、插件、会话、审计最终共享同一套持久化基础设施。

---

## 1. 设计目标

### 1.1 统一持久化骨架

- 不再长期依赖“每个子系统各自写 JSON/TOML/JSONL”。
- 建立统一的数据库访问层、迁移机制和数据模型边界。
- 上层业务不直接拼 SQL，也不直接自行决定落盘位置。

### 1.2 面向 Agent 与模型运行时

- 数据库设计必须能承载 Agent 会话、运行记录、模型路由与调用指标。
- LiteLLM 运行时产出的执行记录应可直接进入数据库，用于审计、监控和成本分析。

### 1.3 本地优先，后续可扩展

- 首版默认支持本地单机部署，默认数据库应易于启动与分发。
- 后续应允许平滑迁移到更强的数据库后端，而不重写业务层。

### 1.4 混合持久化

- 数据库负责结构化元数据。
- 文件系统负责原始文件、资源缓存、JSONL 调试流与导出工件。
- 二者通过稳定引用关系协同，而不是互相替代。

---

## 2. 非目标

- 首版不要求把现有所有配置立即迁移进数据库。
- 首版不要求数据库承担媒体二进制存储。
- 首版不要求把全部系统日志改造成数据库日志。
- 首版不要求引入分布式数据库、高可用集群或跨节点一致性方案。

---

## 3. 核心原则

### 3.1 什么应该进数据库

符合以下任一条件的数据，应优先进入数据库：

- 需要按字段查询、筛选、分页、聚合。
- 存在实体间关联关系。
- 需要事务一致性或并发写入保护。
- 需要长期统计、分析、审计。
- 需要被 WebUI/API 稳定管理。

典型示例：

- Provider / Model / Route
- Session 元数据与会话配置
- 插件实例、适配器实例及其配置快照
- Agent 运行记录与消息线程
- 模型调用执行记录与成本指标
- 结构化审计记录

### 3.2 什么不应该强行进数据库

以下数据默认继续使用文件系统：

- 资源文件与媒体缓存
- 大体积原始事件转储
- JSONL 调试流
- 普通应用日志
- 导入导出工件

原因：

- 这类数据通常是 append-only、大对象、以顺序读取为主。
- 数据库存储它们的收益低，迁移和清理成本反而更高。

### 3.3 数据库保存“引用”，文件系统保存“内容”

例如：

- 消息图片资源本体保存在 `data/temp/resources/`
- 数据库只保存资源元数据、哈希、来源和本地路径
- 调试事件全文可以继续写 JSONL，但数据库可记录索引、摘要和关联对象

---

## 4. 技术选型原则

### 4.1 默认数据库

默认后端建议为 `SQLite`。

原因：

- 本地部署零额外服务，适合当前项目阶段。
- 对单机 Bot、个人部署、开发调试足够友好。
- 便于 WebUI 和主程序共用同一个数据源。

### 4.2 可扩展后端

数据库访问层必须预留切换到 `PostgreSQL` 的能力。

要求：

- 不在业务层写死 SQLite 特性。
- 避免依赖仅 SQLite 才有的非必要行为。
- 表结构、索引和迁移策略尽量兼容 PostgreSQL。

### 4.3 访问层

建议使用：

- `SQLAlchemy 2.x`
- `Alembic` 进行 schema migration

约束：

- Pydantic 领域模型不直接等价于数据库模型。
- 数据库模型只用于持久化层。
- 业务逻辑通过 repository / service 访问数据库。

---

## 5. 总体架构

持久化层建议拆为以下结构：

1. `shinbot/persistence/engine.py`
   - 负责数据库 URL、engine、session factory 初始化。
2. `shinbot/persistence/models/`
   - SQLAlchemy ORM 模型定义。
3. `shinbot/persistence/repos/`
   - repository 层，负责查询与写入。
4. `shinbot/persistence/migrations/`
   - Alembic 迁移脚本。
5. `shinbot/persistence/services/`
   - 面向业务的聚合写入，例如模型执行记录、会话更新、审计落库。

调用关系应为：

`API / Runtime / Plugin / Agent -> Service -> Repository -> DB Session -> Database`

而不是：

`业务代码 -> 直接 open(json)`  
`业务代码 -> 到处散落 SQL`

---

## 6. 数据分层与边界

### 6.1 启动级配置

首版建议保留一份最小化文件配置，作为启动 bootstrap：

- HTTP/WebUI 监听地址
- 日志级别
- 数据目录
- 数据库连接串
- 首个管理员或 bootstrap 认证参数

原因：

- 程序必须先知道如何连接数据库，数据库自身不能负责“告诉程序数据库在哪”。

结论：

- `config.toml` 不会立即消失。
- 但它应逐步收敛为 bootstrap config，而不是长期承载全部业务配置。

### 6.2 运行级配置

以下配置应逐步迁移为数据库主存：

- 适配器实例配置
- 插件实例配置
- Session 配置
- 模型 Provider / Model / Route 配置
- Agent 绑定关系

要求：

- WebUI 修改此类数据时，默认写数据库。
- 配置变更应有更新时间和版本戳。

### 6.3 运行记录

以下记录应优先进入数据库：

- Agent run / step / tool call
- 模型执行记录
- 结构化审计日志
- 任务状态与失败原因

原始 payload 可根据类型选配文件副本或 JSONL 旁路记录。

---

## 7. 领域数据规划

### 7.1 Session

当前 Session 仅支持内存 + 单文件 JSON 持久化，这已经不适合后续规模。

建议拆成三部分：

- `sessions`
  - 会话主实体、平台信息、显示名、活跃时间
- `session_configs`
  - 前缀、静音、LLM 开关、审计开关等稳定配置
- `session_state`
  - 较动态的 KV/JSON 状态

原则：

- 高频临时状态允许缓存于内存。
- 需要跨重启保留的状态落库。
- `plugin_data` 初期可用 JSON 字段承载，后续再按插件演进为专表。

### 7.2 Plugin / Adapter / Instance

建议数据库中显式管理：

- 插件元信息快照
- 启用/禁用状态
- 插件实例与适配器实例
- 配置 schema 版本
- 配置内容与最后更新时间

注意：

- 插件源代码本体仍在文件系统。
- 数据库存储的是“运行注册与配置状态”，不是插件包内容。

### 7.3 Model Runtime

应与 `18_agent_model_runtime.md` 对齐，至少落库：

- `model_providers`
- `model_definitions`
- `model_routes`
- `model_route_members`
- `model_execution_records`

其中 `model_execution_records` 至少应包含：

- `id`
- `route_id`
- `provider_id`
- `model_id`
- `caller`
- `session_id`
- `instance_id`
- `started_at`
- `first_token_at`
- `finished_at`
- `latency_ms`
- `time_to_first_token_ms`
- `input_tokens`
- `output_tokens`
- `cache_hit`
- `cache_read_tokens`
- `cache_write_tokens`
- `success`
- `error_code`
- `error_message`
- `fallback_from_model_id`
- `fallback_reason`
- `estimated_cost`
- `currency`
- `metadata`

这些字段是后续监控、审计、成本分析的基础。

### 7.4 Audit

当前审计写 JSONL，建议演进为“双轨”：

- 数据库保存结构化审计记录，支持查询与统计
- JSONL 可作为旁路导出或冷备日志

结构化审计至少覆盖：

- 命令执行
- 权限判定
- 插件调用
- 模型调用
- Agent 关键动作
- 管理操作（如启用/禁用实例、修改模型路由）

### 7.5 Agent Runtime

若开始实现 Agent，首版就应将以下对象纳入数据库设计：

- `agent_definitions`
- `agent_threads`
- `agent_messages`
- `agent_runs`
- `agent_run_steps`
- `agent_tool_calls`

这样可以避免后续 Agent 对话历史与运行轨迹再次经历“先文件、后迁移”的返工。

---

## 8. 文件系统保留的数据面

以下数据继续保留在文件系统，数据库仅存引用或摘要：

- `data/temp/resources/` 中的资源文件
- `data/plugin_data/**` 中的插件调试转储
- `data/audit/*.jsonl` 这类旁路审计文件
- 大体积原始事件流
- 导出报告与快照

对这些对象建议统一补充一个资源索引层，至少记录：

- `resource_id`
- `kind`
- `sha256`
- `mime_type`
- `size_bytes`
- `storage_path`
- `created_at`
- `source`

这样数据库与文件系统之间能建立稳定关联。

---

## 9. 迁移策略

### 9.1 分阶段迁移

建议按以下顺序推进：

1. 引入数据库基础设施
   - engine、session factory、迁移框架
2. 迁移模型运行时元数据
   - Provider / Model / Route
3. 迁移 Session 与实例配置
4. 迁移结构化审计与模型执行记录
5. 再接入 Agent 线程、运行记录与工具调用

这样做的原因：

- 模型运行时和 Agent 是后续新功能，先落数据库成本最低。
- Session 和审计已经有现成结构，迁移收益高。
- 调试 JSONL、资源文件可以晚一点再统一索引。

### 9.2 双写过渡

对于已存在文件持久化的模块，迁移期可采用：

- 数据库主写
- 文件旁路写
- 对读路径逐步切换到数据库

但双写只应作为过渡策略，不应长期存在于所有模块。

### 9.3 数据迁移工具

需要提供一次性迁移工具，将现有数据导入数据库，例如：

- Session JSON -> `sessions`
- Audit JSONL -> `audit_logs`
- 模型配置文件 -> Provider / Model / Route 表

该工具应支持幂等导入与 dry-run。

---

## 10. 一致性与并发要求

### 10.1 单请求事务

单次 API 操作或单次运行态关键写入，应在一个数据库事务内完成。

例如：

- 创建 Provider 与其默认 Model
- 启用实例并更新状态
- 记录模型执行完成并写回统计数据

### 10.2 避免把数据库会话泄露到业务层

要求：

- repository/service 控制事务边界
- 业务对象不持有裸 session
- 插件不直接访问数据库连接

### 10.3 缓存只是加速层

- 内存缓存不能成为唯一真相来源
- SessionManager、Route Registry 等缓存对象应以数据库为主存
- 缓存失效后必须可从数据库重建

---

## 11. 观测性要求

数据库落地后，以下能力必须可做：

- 按实例、插件、会话、模型、供应商统计调用量
- 统计平均耗时、P95、失败率
- 统计输入/输出 token、缓存命中与成本
- 查询某条 Agent run 的完整轨迹
- 查询某个会话最近的模型调用和审计事件

这也是为什么模型执行记录不能只写日志、不落结构化数据库。

---

## 12. 安全要求

- 密钥、令牌等敏感字段不得明文回显到 WebUI。
- 数据库中敏感字段应支持加密存储或最小化保存。
- 审计表与模型执行表中的请求元数据必须允许脱敏。
- 导出与备份流程需能排除敏感列。

---

## 13. 首版实现建议

建议的第一阶段实现范围：

1. 引入 `SQLAlchemy + Alembic`
2. 提供统一数据库配置与初始化
3. 先落这四组表：
   - Provider / Model / Route
   - Session
   - Audit Log
   - Model Execution Record
4. 保持资源文件、调试 JSONL、普通日志继续走文件系统

这是一个比较稳的切入点：

- 能直接服务 Agent / LiteLLM 设计
- 能提升 WebUI 管理能力
- 不会一次性重写整个项目的 I/O 习惯

---

## 14. 与现有系统的关系

对当前代码基线，建议做如下演进判断：

- `SessionManager`
  - 从“内存 + JSON 文件”演进为“内存缓存 + 数据库主存”
- `AuditLogger`
  - 从“JSONL 旁路”演进为“数据库主写 + JSONL 可选旁路”
- `config.toml`
  - 从“主配置中心”收敛为“bootstrap 配置”
- `shinbot_debug_message`
  - 继续保留文件输出，不强行数据库化

---

## 15. 后续文档拆分建议

本文档只定义总体持久化原则。

后续应继续补充：

- `interfaces/` 下的数据库管理 API 与 WebUI 设计
- `runtime/` 下的 Agent 运行对象模型设计
- `core/` 下的资源索引与存储抽象设计

这样数据库不会只是“换个存储介质”，而是成为整个 Agent 时代的数据底座。
