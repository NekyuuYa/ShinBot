# ShinBot

> **A Next-Generation, Modular, and High-Performance Bot Orchestration Framework.**

ShinBot 是一款旨在彻底解决传统 Bot 框架痛点的新一代机器人框架。基于 **异步工作流 (Workflow)** 和 **Satori 协议** 思想，它提供了极致的解耦能力、精细化的权限管理以及原生的大模型 (LLM) 接入支持。

---

## ✨ 核心特性

- 🛡️ **精细化权限体系**：支持“会话+用户”维度的双层权限绑定，具备显式拒绝优先的权限树合并算法。
- 🧩 **一切皆插件**：核心层（Pure Core）零网络代码，适配器（OneBot, Satori, Discord 等）作为驱动插件热插拔。
- 🌳 **Satori AST 架构**：内部消息统一采用符合 Satori 规范的 **MessageElement AST**，告别转义噩梦，天然支持多模态嵌套。
- 🔄 **异步工作流模型**：打破线性管道限制，支持多路响应、异步回调和复杂的交互式上下文管理。
- 🤖 **原生 AI 增强**：集成 **LiteLLM** 接入层，支持多模型自动路由、Token 审计与工具调用（Tool Use）。
- 📊 **Vue 3 现代化看板**：提供可视化的实例管理、实时日志流、权限组管理及模型配额设置。

---

## 🏗️ 架构分层

| 层级 | 技术栈 | 职责 |
| :--- | :--- | :--- |
| **ShinBot Core** | Python 3.10+ (uv) | 流程编排、指令解析、会话状态管理、权限引擎。 |
| **Dashboard** | Vue 3 (Vite + Pinia) | 实例可视化配置、实时日志流、权限组管理。 |
| **Adapters** | Python Plugins | 协议翻译（如 OneBot v11 ⇌ Satori AST）。 |
| **AI Layer** | LiteLLM | 模型接入、审计、成本控制。 |

---

## 📂 项目结构

```text
ShinBot/
├── docs/design/          # 核心设计规范 (系统图纸)
├── shinbot/              # Python 主程序 (核心引擎)
│   ├── core/             # 工作流、权限、插件管理器
│   ├── models/           # Pydantic 数据模型 (AST, Event)
│   ├── adapters/         # 适配器接口与官方驱动
│   └── api/              # 后端管理接口 (FastAPI)
├── dashboard/            # Vue 3 前端管理工程
├── data/                 # 运行时持久化数据 (Git 忽略)
├── plugins/              # 业务插件存放目录
└── main.py               # 程序总入口
```

---

## 📚 快速查阅 (Design Docs)

1. [消息工作流](./docs/design/01_message_workflow.md)
2. [消息元素 (AST) 规范](./docs/design/02_message_element_spec.md)
3. [指令系统](./docs/design/03_command_system.md)
4. [会话隔离与管理](./docs/design/04_session_management.md)
5. [权限绑定策略](./docs/design/05_permission_system.md)
6. [适配器接口契约](./docs/design/09_adapter_interface_spec.md)

---

## 🚀 开发路线图 (Current: Phase 1)

- [x] **Phase 1: 架构设计与协议对齐** (完成)
- [ ] **Phase 2: 核心基石实现** (进行中)
    - [ ] `MessageElement` Pydantic 模型
    - [ ] `Satori XML` ⇌ `AST` 转换器
    - [ ] `AdapterManager` 注册工厂
- [ ] **Phase 3: 官方适配器开发** (Pure Satori & OB11 Bridge)
- [ ] **Phase 4: 插件系统与工作流引擎**
- [ ] **Phase 5: Vue 3 Dashboard 与 API 联动**

---

## 🤝 参与贡献

本项目欢迎任何形式的贡献。在提交代码前，请务必阅读 `docs/design/` 下的相关规范，确保逻辑与框架核心理念一致。

---

**ShinBot** - *Build your bots, better and faster.*
