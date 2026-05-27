# ShinBot

面向多平台交互的拟人 Agent Bot 框架。

ShinBot 通过结构化的 Prompt 编排、统一的 Tool 治理体系和注意力驱动的工作流，让 Bot 具备主动决策能力，而非传统的"艾特才回复"模式。

## 快速开始

**环境依赖：**

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/) (Python 包管理器)
- [pnpm](https://pnpm.io/) (前端包管理器，仅 Dashboard 开发/构建需要)

**安装与启动：**

```bash
# 克隆仓库
git clone https://github.com/your-org/shinbot.git
cd shinbot

# 安装依赖
uv sync

# 复制配置文件
cp config.example.toml data/config.toml
# 编辑 data/config.toml 填入你的配置

# 启动 Bot
uv run main.py
```

**Dashboard 开发：**

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
├── tests/                       # 测试集
├── config.example.toml          # 配置模板
└── main.py                      # 启动入口
```

**启动流程：** `main.py` → `BootController` → 5 阶段启动：环境校验 → 基础设施初始化 → 内核加载 → 插件加载 → 适配器激活。

**核心设计理念：**

- **结构化 Prompt** — 插件通过 PromptRegistry 注入内容，禁止直接修改系统提示词
- **交互即 Tool** — 所有行为暴露为工具，支持权限、审计和风险等级控制
- **注意力驱动** — 基于对话活跃度衰减决策是否响应，支持批量聚合阅读

## 命令

```bash
# 运行全部测试
uv run --group dev python -m pytest

# 运行快速单元测试（CI 使用）
uv run --group dev python -m pytest -m "not integration and not slow and not e2e"

# 运行单个测试文件
uv run --group dev python -m pytest tests/test_attention_engine.py

# 代码检查
uv run --group dev ruff check .

# 代码检查并自动修复
uv run --group dev ruff check . --fix

# 启动 Bot
uv run main.py

# 启动 Bot（带 Operator CLI）
uv run main.py --operator-cli

# Dashboard 开发服务器
cd dashboard && pnpm dev

# Dashboard 生产构建
cd dashboard && pnpm build
```
