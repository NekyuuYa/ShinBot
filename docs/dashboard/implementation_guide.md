# ShinBot WebUI: 前端开发实施指引 (Implementation Guide)

本文档面向前端开发者（或 AI），指导如何基于 Vue 3 + Vuetify 构建管理后台。

---

## 1. 工程基座
- **包管理**: pnpm
- **样式**: Vuetify 3 (Material Design) + TailwindCSS (用于布局微调)。
- **状态管理**: 
    - `useAuthStore`: 管理 JWT Token 与用户信息。
    - `useConfigStore`: 管理全局 UI 设置（如侧边栏折叠、欢迎页可见性）。
    - `useInstanceStore`: 实时同步各 Bot 实例的状态。

---

## 2. 路由规划 (Routes)
所有路由必须经过 `AuthGuard` 守卫：
- `/login`: 独立页面，无导航栏。
- `/welcome`: 引导流程。
- `/dashboard`: 概览。
- `/instances`: 机器人管理。
- `/plugins/manage`: 插件管理。
- `/permissions/groups`: 权限组。
- `/monitoring/logs`: 实时日志。

---

## 3. Vuetify 专题规范
- **主题**: 支持 Dark/Light 切换。
- **密度**: 紧凑模式 (`density="comfortable"`)，确保信息密度适中。
- **交互**: 
    - 提交按钮必须绑定 `loading` 状态。
    - 危险操作必须触发 `v-dialog` 二次确认。

---

## 4. 协作约束
- **i18n**: 必须严格遵守 `docs/dashboard/i18n/structure.md` 的扁平化规范。
- **组件**: 必须严格遵守 `docs/design/15_webui_component_spec.md` 的卡片网格规范。
