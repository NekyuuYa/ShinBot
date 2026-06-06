# WebUI 插件安装规划

本文规划 ShinBot Dashboard 中的插件安装能力。目标是让管理员可以在 WebUI 中从可信来源安装、更新、启用插件，同时保持现有 `PluginManager` 的职责边界清晰。

## 当前状态

现有插件管理链路只覆盖已发现插件：

- 后端路由：`shinbot/api/routers/plugins.py`
- 管理 helper：`shinbot/admin/plugin_admin.py`
- 插件生命周期：`shinbot/core/plugins/manager.py`
- 前端页面：`dashboard/src/views/Plugins.vue`
- 前端 store/API：`dashboard/src/stores/plugins.ts`、`dashboard/src/api/plugins.ts`

当前 Dashboard 支持：

- 列出已加载插件
- reload/rescan `data/plugins`
- enable/disable 插件
- 编辑插件配置

当前缺口：

- 没有插件来源记录
- 没有安装/更新/卸载文件的 API
- 没有安装任务状态
- 没有安装前校验和风险提示
- 没有 WebUI 插件目录或远程仓库入口

## 设计目标

第一版目标：

- 支持从 GitHub 仓库 URL 安装插件。
- 支持从 `.zip` 上传安装插件。
- 安装结果落到 `<data_dir>/plugins/<plugin_id>`。
- 安装后触发现有 `rescan`，复用 `PluginManager` 加载逻辑。
- 在 WebUI 显示安装进度、失败原因、来源和版本。
- 禁止路径逃逸、非法插件 id、缺失 `metadata.json`、缺失入口文件。
- 默认只允许管理员操作。

暂不做：

- 自动安装 Python 依赖。
- PyPI 安装。
- 多版本并存。
- 远程插件市场搜索。
- 插件运行沙箱。

## 职责边界

`PluginManager` 继续只负责：

- 发现 `metadata.json`
- 校验插件元数据
- import 模块
- 调用 `setup(plg)` / `on_enable` / `on_disable` / `teardown`
- 管理 command/event/route/tool 注册和注销

新增安装层负责：

- 下载或接收插件包
- 解包到临时目录
- 识别插件根目录
- 校验 `metadata.json`
- 原子替换到 `data/plugins/<plugin_id>`
- 记录安装来源和任务状态
- 调用已有 rescan/load 流程

建议新增模块：

```text
shinbot/admin/plugin_install.py
shinbot/api/routers/plugin_installs.py
```

前端继续复用插件页，但把“安装”作为独立 dialog/side panel：

```text
dashboard/src/api/plugins.ts
dashboard/src/stores/plugins.ts
dashboard/src/views/Plugins.vue
dashboard/src/components/plugins/PluginInstallDialog.vue
```

## 数据模型

第一版可以先使用 JSON manifest，避免马上扩展 SQLite schema：

```text
<data_dir>/plugin_install_manifest.json
```

结构：

```json
{
  "plugins": {
    "shinbot_plugin_example": {
      "plugin_id": "shinbot_plugin_example",
      "source_type": "github",
      "source_url": "https://github.com/NekyuuYa/shinbot-plugin-example",
      "ref": "main",
      "installed_at": 1780750000,
      "updated_at": 1780750000,
      "installed_version": "0.1.0",
      "commit": "abc123",
      "managed_by_webui": true
    }
  }
}
```

后续如果需要审计、并发任务查询、历史记录，再迁到 SQLite repository。

## API 规划

新增路由挂载在 `/api/v1/plugin-installs`。

```http
GET /api/v1/plugin-installs
```

返回已安装来源记录，和当前插件列表合并展示。

```http
POST /api/v1/plugin-installs/github
```

请求：

```json
{
  "url": "https://github.com/NekyuuYa/shinbot-plugin-example",
  "ref": "main",
  "enable_after_install": true
}
```

返回：

```json
{
  "task_id": "plugin-install-...",
  "status": "queued"
}
```

```http
POST /api/v1/plugin-installs/archive
```

上传 zip 包，返回安装任务。

```http
GET /api/v1/plugin-installs/tasks/{task_id}
```

返回任务状态：

```json
{
  "task_id": "plugin-install-...",
  "status": "running",
  "stage": "validating",
  "message": "Validating metadata.json",
  "plugin_id": "shinbot_plugin_example",
  "error": null
}
```

```http
POST /api/v1/plugin-installs/{plugin_id}/update
```

从 manifest 记录的来源更新。

```http
DELETE /api/v1/plugin-installs/{plugin_id}
```

卸载 WebUI 管理的用户插件。流程是先 disable/unload，再删除 `data/plugins/<plugin_id>`，最后更新 manifest。第一版不删除 `data/plugin_data/<plugin_id>`，只在 UI 中提示保留数据。

## 安装流程

GitHub 安装：

1. 校验 URL 只允许 `https://github.com/<owner>/<repo>` 或 `git@github.com:<owner>/<repo>.git`。
2. 下载源码 archive 到临时目录。
3. 解压到 `<data_dir>/plugin_install_tmp/<task_id>`。
4. 查找插件根目录，要求根目录或一级子目录包含 `metadata.json`。
5. 校验 `metadata.json`：
   - `id` 非空。
   - `id` 以 `shinbot_plugin_`、`shinbot_adapter_` 或 `shinbot_debug_` 开头。
   - 入口文件存在。
   - `entry` 不能是绝对路径，不能包含 `..`。
6. 校验目标目录：
   - 目标必须在 `<data_dir>/plugins` 内。
   - 如果目标已存在，必须是 WebUI 管理的插件，或者用户显式确认覆盖。
7. 写入 staging 目录。
8. 原子替换到 `<data_dir>/plugins/<plugin_id>`。
9. 更新 manifest。
10. 调用 `rescan_plugins()`。
11. 如果 `enable_after_install=true`，调用 `enable_plugin_or_raise()`。

上传 zip 安装同样从第 3 步开始。

## 安全策略

第一版必须做到：

- 所有文件操作都基于 `Path.resolve()` 后确认仍在允许目录内。
- 解压 zip 时拒绝绝对路径、`..`、symlink、hardlink。
- 单个 archive 最大大小可配置，默认 20 MB。
- 解压后文件总大小可配置，默认 100 MB。
- 不执行安装脚本。
- 不自动运行 `pip install`。
- 安装前展示插件 metadata、来源、权限声明和覆盖风险。
- adapter 插件安装时提示需要配置实例后才会生效。

依赖处理建议：

- 第一版只检测依赖，不安装依赖。
- 如果导入失败是 `ModuleNotFoundError`，API 返回缺失模块名，UI 引导用户在服务端环境手动安装。
- 后续可增加“允许安装 Python 依赖”的显式开关，并通过受控 task 执行 `uv pip install`。

## WebUI 交互

插件页顶部新增主要操作：

- 安装插件
- 刷新
- 重载
- 扫描本地插件

安装 dialog 使用 tabs：

- GitHub URL
- 上传 ZIP

GitHub tab 字段：

- 仓库 URL
- 分支/tag/commit，默认 `main`
- 安装后启用
- 覆盖已有插件

上传 tab 字段：

- ZIP 文件
- 安装后启用
- 覆盖已有插件

插件卡片新增来源信息：

- 内置插件：`Builtin`
- 本地插件：`Local`
- WebUI 管理：显示 GitHub owner/repo 或 `Uploaded archive`

WebUI 管理的插件菜单新增：

- 更新
- 卸载

非 WebUI 管理的用户插件只显示“扫描/启用/禁用/配置”，避免误删手动放入的插件。

## 后端实现分阶段

### Phase 1: 安装服务和 API

- 新增 `PluginInstallError`。
- 新增 `PluginInstallManifest` 读写 helper。
- 新增 `PluginInstallService`：
  - `install_from_github()`
  - `install_from_archive()`
  - `update_plugin()`
  - `uninstall_plugin()`
- 新增 task registry，先用内存存储任务状态。
- 新增 API router 和 schema。
- 给 `plugin_dict()` 增加来源 metadata 合并。

### Phase 2: Dashboard 安装入口

- 扩展 `dashboard/src/api/plugins.ts`。
- 扩展 `dashboard/src/stores/plugins.ts`。
- 新增 `PluginInstallDialog.vue`。
- `Plugins.vue` 增加安装按钮、任务轮询和来源筛选。
- `PluginCard.vue` 增加 update/uninstall 菜单项。
- 补齐 `zh-CN` 和 `en-US` 文案。

### Phase 3: 测试和稳定性

- API 单测：
  - GitHub URL 校验。
  - zip 路径逃逸拒绝。
  - metadata 缺失拒绝。
  - 成功安装后写入 manifest 并触发 rescan。
  - 卸载时保留 plugin data。
- integration 测试：
  - 从本地 archive 安装可加载插件。
  - 覆盖已有 WebUI 管理插件。
  - 拒绝覆盖非 WebUI 管理插件。
- Dashboard 构建：
  - `pnpm build`

## 关键取舍

第一版不做自动依赖安装，是为了避免 WebUI 变成任意代码下载和执行入口。插件本身在启用时已经会执行 Python 代码，所以安装入口必须让“来源确认”和“实际启用”两个动作清晰可见。

第一版用 JSON manifest 而不是 SQLite，是因为来源记录只服务安装管理，不影响 bot 核心运行。等出现审计、历史、并发任务持久化需求后再迁移到数据库。

安装逻辑不进入 `PluginManager`，是为了保持当前架构中“安装文件”和“加载运行时”的边界。这样后续即使支持 PyPI、marketplace 或私有 registry，也不会污染插件生命周期代码。
