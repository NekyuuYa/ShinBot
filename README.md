# ShinBot

> 面向 Agent 与多平台交互的 Bot 编排框架。

ShinBot 不是把大模型直接塞进聊天机器人里，而是把一整套“消息输入、Prompt 编排、Tool 交互、指令执行、插件扩展、运行时治理”拆成清晰可控的基础设施。你可以把它理解为一个专门给 Bot 和 Agent 场景设计的运行框架，而不是一个只负责调用模型接口的薄封装。

## ShinBot 想解决什么

很多 Bot 项目一开始都很顺手：收消息，拼 prompt，调模型，回消息。但随着能力增长，问题会很快暴露出来：

- prompt 越堆越长，来源不清，调试困难
- 交互逻辑散落在业务代码里，模型“能做什么”边界模糊
- 插件想扩展能力时，只能继续拼字符串、打补丁
- 指令、权限、会话、工具调用之间缺少统一抽象

ShinBot 的目标就是把这些问题系统化处理，让 Bot 从“能跑”变成“能长期进化”。

## 核心卖点

### 1. 高度定制化的 Prompt 编排，请求结构清晰可见

ShinBot 将 Prompt 视为一等公民，而不是运行时临时拼接出来的一段大字符串。

- 通过 Prompt Registry 管理 prompt 的结构、阶段顺序、来源和快照
- 明确区分系统约束、角色设定、上下文、工具可见集和运行时注入信息
- Agent 与插件不能绕过统一编排层直接拼最终 prompt，避免“谁都能偷偷改系统提示词”
- 更适合调试、审计和后续演进，出了问题可以追踪“模型到底看到了什么”

这意味着你在做复杂 Agent 行为设计时，可以直接理解 Agent 是如何请求的。

### 2. 把交互抽象成 Tool，让智能行为更可控

在 ShinBot 里，交互不是随手写几段 if/else 或直接让模型自由发挥，而是显式抽象成 Tool。

- 模型能做的事通过 Tool 暴露，边界更清晰
- Tool 调用过程可以记录、审计、复现，而不是藏在 prompt 技巧里
- 发送消息、媒体分析、状态调整、外部能力调用都可以进入统一工具体系
- Tool 的权限、可见性、风险等级和执行上下文可以单独治理

这套设计的重点不是“让模型会调用函数”，而是让 Bot 的交互能力具备工程化约束。

### 3. 强大的指令系统，让插件可以自然声明自己的功能

ShinBot 内建独立的指令系统，优先处理高确定性、强操作性的请求。

- 指令拥有清晰的解析优先级与匹配规则
- 能处理确定性任务，也能与会话和权限系统协同工作
- 插件可以注册自己的指令、事件处理器和能力入口

### 4. 为长期演进准备的运行时骨架

除了 Prompt、Tool 和指令系统，ShinBot 还补齐了一个 Bot 框架真正需要的基础设施：

- 统一消息模型，面向 `UnifiedEvent` 与 `MessageElement` AST
- 多平台适配与可扩展的 AdapterManager
- 插件热加载、内置插件与外部插件并存
- FastAPI 管理 API 与 Vue 3 Dashboard
- SQLite 优先的持久化层、会话状态与审计数据存储

项目的重点不是做一个“功能列表很长”的机器人，而是做一个结构足够稳、后续能力足够好接的框架。

## 适合谁

ShinBot 更适合下面这类场景：

- 你想做一个可持续扩展的 Agent Bot，而不是一次性 Demo
- 你需要精细控制 prompt 结构，而不是把所有规则塞进 system prompt
- 你希望将模型交互落到工具层，获得更清晰的权限和执行边界
- 你准备编写插件，给不同实例、平台或工作流挂接自己的能力

如果你只是想要一个最小可用的“聊天机器人壳子”，ShinBot 可能会显得偏重；但如果你关心可维护性、可治理性和后续演进空间，它就是按这个目标设计的。

## 当前代码结构

```text
ShinBot/
├── docs/                        # 设计、实现与插件文档
├── dashboard/                   # Vue 3 + Vite 管理面板
├── shinbot/
│   ├── agent/                   # Agent 运行时、Prompt Registry、Tool 系统
│   ├── api/                     # FastAPI 管理 API
│   ├── builtin_plugins/         # 内置插件与适配器插件
│   ├── core/
│   │   ├── application/         # 应用装配与 BootController
│   │   ├── dispatch/            # Command/Event/Pipeline
│   │   ├── platform/            # AdapterManager 与 BaseAdapter
│   │   ├── plugins/             # PluginManager 与 Plugin
│   │   ├── security/            # PermissionEngine 与 AuditLogger
│   │   └── state/               # SessionManager 与会话模型
│   ├── persistence/             # 数据库引擎、记录与仓储
│   └── utils/                   # 通用工具
├── tests/                       # Python 测试
├── config.example.toml          # 启动配置模板
└── main.py                      # 进程入口
```

## 迭代状态

项目目前仍处于高速迭代阶段，当前优先级是架构收敛与能力成型，而不是保持早期接口绝对稳定。

- 暂时不承诺良好的向后兼容性
- 数据结构、配置字段、运行时协议可能在短周期内调整
- 升级前建议默认做好数据迁移失败、缓存清理和本地状态重建的准备

## 快速开始

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
