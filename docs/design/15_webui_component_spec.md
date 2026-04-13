# ShinBot 技术规范：WebUI 组件与模板规范 (Component Spec)

本文档定义了 ShinBot 管理后台的可复用组件标准，旨在消除页面间的割裂感，实现高度统一的交互体验。

---

## 1. 布局模板 (Layout Templates)

### 1.1 标准页面结构
所有功能页面必须遵循以下垂直结构：
1.  **Page Header**: 标题 + 面包屑导航 + 右侧全局操作（如“新建”、“刷新”）。
2.  **Control Bar**: 视图切换（卡片/列表） + 搜索框 + 统一筛选器。
3.  **Main Content**: 动态渲染的网格或列表。
4.  **Pagination**: 底部导航（若有）。

---

## 2. 统一卡片模式 (The ShinCard)

卡片是 ShinBot 最核心的展示单元（用于机器人、插件、权限组）。

### 2.1 卡片结构规范 (Anatomy)
- **Header**: 
    - 左侧：图标/头像（如 Bot 头像或插件 Logo）。
    - 中间：主标题 + 状态标签（Chip）。
    - 右侧：状态开关（Switch）或 更多操作（Menu）。
- **Body**: 
    - 关键信息摘要（2-3 行数据展示）。
- **Footer/Actions**: 
    - 统一的“编辑”、“配置”、“删除”按钮组。

### 2.2 卡片网格 (Grid)
- **响应式**: 使用 Vuetify 的 `v-col` 布局。
- **断点**: `xs=12`, `sm=6`, `md=4`, `lg=3`。确保在大屏幕下整齐排列。

---

## 3. 视图切换器与过滤器 (View Controllers)

### 3.1 视图切换 (View Switcher)
所有展示类页面（机器人管理、插件管理）必须在顶部工具栏提供 **Card/List 切换按钮**：
- **Card View**: 适合概览、监控和快速开关。
- **List View**: 适合批量操作、详细参数对比和排序。

### 3.2 统一筛选组件 (Unified Filter)
过滤器应作为一个独立组件 `SbFilterBar`：
- **左侧**: 实时搜索输入框（Debounced Search）。
- **中间**: 分类筛选（Dropdown/Select），如“仅显示已启用”、“按平台筛选”。
- **右侧**: 排序规则选择。

---

## 4. 状态反馈模板 (Feedback Patterns)

为了保证视觉统一，以下状态必须使用标准模板：
- **Empty State**: 统一的插画 + “暂无数据”文案 + “去创建”按钮。
- **Loading State**: 骨架屏 (Skeletons) 优先，禁止使用全屏大转圈。
- **Error State**: 红色轻量化警示条 + 错误堆栈查看按钮。

---

## 5. 交互叠层 (Overlays)

- **侧边栏 (Drawer)**: 用于轻量级设置（如查看日志详情）。
- **对话框 (Dialog/Modal)**: 用于关键修改（如编辑机器人配置）。
- **气泡通知 (Snackbar)**: 所有的 API 成功/失败反馈。
