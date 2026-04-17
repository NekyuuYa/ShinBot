# ShinBot

> The agent-oriented bot orchestration framework.

ShinBot 是一个面向多实例 Bot、Agent Runtime 和插件化扩展的 Python 框架。当前代码库已经按职责拆分为应用生命周期、消息分发、平台适配、插件系统、权限与会话、Agent 运行时和持久化几个独立子系统。

## 核心能力

- 面向 `UnifiedEvent` / `MessageElement` AST 的统一消息模型
- 基于 `MessagePipeline` 的事件分发、命令解析和权限校验
- 内置插件与外部插件并存的热加载机制
- 可管理的模型运行时与 Prompt Registry
- FastAPI 管理 API 与 Vue 3 Dashboard
- SQLite 优先的持久化骨架与运行时数据目录

## 当前代码结构

```text
ShinBot/
├── docs/                        # 设计、实现与插件文档
├── dashboard/                   # Vue 3 + Vite 管理面板
├── shinbot/
│   ├── agent/                   # Agent 运行时、Prompt Registry
│   ├── api/                     # FastAPI 管理 API
│   ├── builtin_plugins/         # 内置插件与适配器插件
│   ├── core/
│   │   ├── application/         # ShinBot 应用装配与 BootController
│   │   ├── dispatch/            # Command/Event/Pipeline
│   │   ├── platform/            # AdapterManager 与 BaseAdapter
│   │   ├── plugins/             # PluginManager 与 Plugin
│   │   ├── security/            # PermissionEngine 与 AuditLogger
│   │   └── state/               # SessionManager 与会话模型
│   ├── models/                  # 领域模型与协议资源模型
│   ├── persistence/             # 数据库引擎、记录与仓储
│   └── utils/                   # 通用工具
├── tests/                       # Python 测试
├── config.example.toml          # 启动配置模板
└── main.py                      # 进程入口
```

## 启动方式

要求：

- Python `>=3.12`
- `uv`

常用命令：

```bash
uv run pytest
uv run ruff check .
python main.py --config config.toml
```

默认配置模板见 [config.example.toml](config.example.toml)。

## 文档入口

- 项目文档总入口：[docs/README.md](docs/README.md)
- 设计文档分层：[docs/design/README.md](docs/design/README.md)
- 插件能力说明：[docs/plugins/capabilities.md](docs/plugins/capabilities.md)
