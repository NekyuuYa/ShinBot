# ShinBot 技术栈与语言分工 (Architecture Stacks)

本文档明确了 ShinBot 框架开发的技术栈及其职能边界。

---

## 1. 核心引擎 (Kernel & SDK) —— Python 3.10+

**职能范围**：工作流编排、指令解析、会话状态管理、权限引擎、模型路由。

### 1.1 核心选型
- **包管理**: **uv** (现代、高性能 Python 包管理工具)。
- **模型接入**: **LiteLLM** (统一模型调用、费用审计)。
- **数据校验**: **Pydantic v2** (AST 模型的严格定义)。
- **网络与接口**: `FastAPI` (Dashboard 管理接口) + `httpx` / `websockets`。
- **数据库**: `SQLAlchemy` (支持 SQLite/PostgreSQL)。

### 1.2 为什么是 Python？
- **AI 生态**: LiteLLM 原生支持最全。
- **兼容性**: 完美加载 AstrBot-Lite 的旧版 Python 插件。

---

## 2. 管理面板 (Dashboard) —— TypeScript (Vue 3)

**职能范围**：实例可视化配置、实时日志流、权限组管理、模型配额设置。

### 2.1 核心选型
- **前端框架**: **Vue 3** (Composition API)。
- **状态管理**: **Pinia**。
- **组件库**: `Element Plus` 或 `Naive UI`。
- **构建工具**: `Vite`。

---

## 3. 协作机制 (IPC)

- **控制面**: Dashboard (Vue) 通过 REST API 操作 Core (Python)。
- **数据面**: 核心通过 WebSocket 实时推送 `UnifiedEvent` 摘要与日志流至前端展示。
- **统一语言**: 消息内容在前后端均以 **Satori XML / AST** 形式传输。
