# ShinBot

> **The Agentic-Native Bot Orchestration Framework.**

ShinBot 是一款从底层为 **智能体 (Agent)** 设计的新一代机器人框架。通过 **异步工作流 (Workflow)** 编排和 **Satori 协议** 的深度集成，它为复杂的多模态交互、长链条决策和跨平台自动化提供了稳健的工程底座。

---

## ✨ 核心特性

- 🤖 **Agentic-Native (智能体原生)**：深度对齐 LLM 的决策逻辑，消息全量保留 AST 结构。
- 🔄 **Workflow Orchestration (工作流编排)**：基于异步状态机，彻底解决消息流乱序与 Prompt 污染。
- 🎨 **Vue 3 现代化看板**：采用 **Vuetify 3 (Material Design 3)** 构建，清新淡黄主题与极致大圆角设计。
- 🛡️ **精细化权限体系**：支持“会话+用户”维度的双层权限绑定，具备显式拒绝优先策略。
- 🧩 **全插件化内核**：核心零网络依赖，适配器（OneBot, Satori 等）作为驱动插件热插拔。

---

## 🏗️ 架构分层

| 层级 | 技术栈 | 职责 |
| :--- | :--- | :--- |
| **ShinBot Core** | Python 3.10+ (uv) | 流程编排、指令解析、权限引擎、会话状态管理。 |
| **Dashboard** | Vue 3 (Vite + Pinia) | 实例可视化配置、实时监控、美观的管理面板。 |
| **Adapters** | Python Plugins | 协议翻译与语义补全 (如 Satori, OB11 Bridge)。 |

---

## 📂 项目结构

```text
ShinBot/
├── docs/                 # 项目文档总入口 (设计 / 实现 / 插件 / 参考)
├── shinbot/              # 程序源代码 (Core Layer)
│   ├── core/             # 逻辑引擎 (Workflow, Auth, Session)
│   ├── builtin_plugins/  # 官方插件 (含适配器驱动)
│   └── models/           # 数据模型 (MessageElement AST)
├── data/                 # 持久化资产 (插件、数据、会话、审计)
├── dashboard/            # Vue 3 前端工程
└── main.py               # 引导入口 (端口: 3945)
```

文档入口： [docs/README.md](docs/README.md)  
设计分层： [docs/design/README.md](docs/design/README.md)

---

## 🙏 致谢 (Acknowledgements)

- **NoneBot**: 提供了优秀的插件依赖解析与解耦思路参考。
- **AstrBot**: 在前端管理与目录结构规范上提供了宝贵的实践启发。
- **Satori Protocol**: 提供了跨平台标准化语义的基石。

---

<p align="center"><b>ShinBot</b> - <i>Shinku in Shiny Shinbot.</i></p>
