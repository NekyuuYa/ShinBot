# ShinBot

面向多平台交互的拟人 Agent Bot 框架。

ShinBot 通过结构化的 Prompt 编排、统一的 Tool 治理体系和注意力驱动的工作流，让 Bot 具备主动决策能力，而非传统的「艾特才回复」模式。

## 快速开始

### 环境依赖

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) (Python 包管理器)
- [pnpm](https://pnpm.io/) (前端包管理器，仅 Dashboard 开发/构建需要)

### 安装与启动

```bash
# 克隆仓库
git clone https://github.com/NekyuuYa/Shinbot.git
cd Shinbot

# 安装依赖
uv sync

# 复制配置文件
mkdir -p data/agents
cp config.example.toml data/config.toml
cp agent.example.toml data/agents/full-agent.toml
# 编辑 data/config.toml 和 data/agents/full-agent.toml 填入你的配置

# 启动 Bot
uv run main.py
# API: http://localhost:3945
```

### Dashboard 开发

```bash
cd dashboard
pnpm install
pnpm dev
```

## 架构

```text
ShinBot/
├── shinbot/
│   ├── agent/                   # Agent 决策引擎、Prompt Registry、Tool 系统
│   ├── api/                     # FastAPI 管理接口
│   ├── builtin_plugins/         # 内置插件与平台适配层
│   ├── core/
│   │   ├── application/         # 应用生命周期 (BootController)
│   │   ├── dispatch/            # 消息分发管线
│   │   ├── platform/            # 平台适配管理 (AdapterManager)
│   │   ├── plugins/             # 插件管理 (PluginManager)
│   │   ├── security/            # 权限控制与审计
│   │   └── state/               # 会话状态管理
│   ├── persistence/             # SQLite 数据持久化
│   └── utils/                   # 工具函数
├── dashboard/                   # Vue 3 + Vuetify 可视化面板
├── docs/                        # 文档、设计说明与参考资料
├── tests/                       # 测试集
├── agent.example.toml           # Agent 配置模板
├── config.example.toml          # 配置模板
└── main.py                      # 启动入口
```

### 启动流程

`main.py` → `BootController` → 5 阶段启动：环境校验 → 基础设施初始化 → 内核加载 → 插件加载 → 适配器激活。

### 核心设计理念

- 结构化 Prompt：插件通过 PromptRegistry 注入内容，禁止直接修改系统提示词
- 交互即 Tool：Agent 可调用行为通过 ToolRegistry 暴露为工具，支持权限、审计和风险等级控制
- 注意力驱动：基于对话活跃度衰减决策是否响应，支持批量聚合阅读

## 文档

- [文档索引](docs/README.md)
- [架构设计](docs/architecture/README.md)
- [插件开发](docs/plugins/README.md)
- [运行时设计](docs/design/runtime/agent_runtime_index.md)

## 命令

### 测试

```bash
# 运行全部测试
uv run --group dev python -m pytest

# 运行快速单元测试（CI 使用）
uv run --group dev python -m pytest -m "not integration and not slow and not e2e"

# 运行单个测试文件
uv run --group dev python -m pytest tests/unit/agent/workflows/test_agent_active_chat_tool_loop.py
```

### 代码检查

```bash
# 代码检查
uv run --group dev ruff check .

# 代码检查并自动修复
uv run --group dev ruff check . --fix
```

### 启动

```bash
# 启动 Bot
uv run main.py
# API: http://localhost:3945

# 启动 Bot（带 Operator CLI）
uv run main.py --operator-cli
```

### Dashboard

```bash
# 安装依赖
cd dashboard
pnpm install

# Dashboard 开发服务器
pnpm dev

# Dashboard 生产构建
pnpm build
```
