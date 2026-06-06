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
- 区分必须前置插件和可选前置插件。
- 默认只允许管理员操作。
- 不新增数据库表，不引入 plugin install repository。

暂不做：

- 自动安装 Python 依赖。
- PyPI 安装。
- 多版本并存。
- 远程插件市场搜索。
- 插件运行沙箱。
- 安装历史审计。
- 服务重启后恢复安装任务。

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

## 文件布局

第一版只使用文件系统和内存状态，不使用数据库。

```text
<data_dir>/
├── plugins/
│   └── <plugin_id>/                 # PluginManager 当前扫描目录
├── plugin_data/
│   └── <plugin_id>/                 # 插件运行数据，卸载时默认保留
├── plugin_install_manifest.json     # WebUI 管理来源清单
└── plugin_install_tmp/
    └── <task_id>/                   # 安装临时目录，任务结束后清理
```

`plugin_install_manifest.json` 是本机安装清单，不是审计日志，也不记录历史版本：

```text
<data_dir>/plugin_install_manifest.json
```

建议结构：

```json
{
  "schema_version": 1,
  "plugins": {
    "shinbot_plugin_example": {
      "plugin_id": "shinbot_plugin_example",
      "source_type": "github",
      "source_url": "https://github.com/NekyuuYa/shinbot-plugin-example",
      "ref": "main",
      "resolved_ref": "abc123",
      "installed_at": 1780750000,
      "updated_at": 1780750000,
      "installed_version": "0.1.0",
      "managed_by_webui": true,
      "archive_sha256": "..."
    }
  }
}
```

字段说明：

- `schema_version`：manifest 文件格式版本，第一版固定为 `1`。
- `plugin_id`：必须等于插件 `metadata.json` 的 `id`。
- `source_type`：`github` 或 `archive`。
- `source_url`：GitHub URL；上传 ZIP 时可为空字符串或 `uploaded_archive`。
- `ref`：用户输入的 branch/tag/commit；上传 ZIP 时为空。
- `resolved_ref`：GitHub archive 对应 commit；如果无法解析则为空。
- `installed_version`：安装时读取的 `metadata.version`。
- `managed_by_webui`：只有为 `true` 时，WebUI 才允许覆盖、更新、卸载插件目录。
- `archive_sha256`：安装包内容摘要，用于展示和排查，不作为安全信任根。

Manifest 写入要求：

- 使用临时文件加 `Path.replace()` 原子写入。
- JSON 使用 `ensure_ascii=False` 和稳定缩进，方便人工查看。
- 读取失败时不自动覆盖，API 返回 `PLUGIN_INSTALL_MANIFEST_INVALID`。
- 缺失 manifest 时按空清单处理。

任务状态只放内存：

```python
PluginInstallTask = {
    "task_id": str,
    "status": "queued" | "running" | "succeeded" | "failed",
    "stage": str,
    "message": str,
    "plugin_id": str | None,
    "error": {"code": str, "message": str} | None,
    "created_at": float,
    "updated_at": float,
}
```

服务重启后任务状态丢失是 V1 可接受行为；已完成安装结果以 manifest 和 `data/plugins` 为准。

## 插件前置依赖语义

当前 `metadata.json` 只有 `dependencies` 字符串列表。现有加载器只用它做拓扑排序；依赖缺失时只记录 warning，不会阻止插件加载。WebUI 安装需要把“必须前置”和“可选前置”分清楚。

建议从新安装能力开始支持以下字段：

```json
{
  "required_dependencies": ["shinbot_plugin_base"],
  "optional_dependencies": ["shinbot_plugin_extra"],
  "dependencies": ["legacy_soft_dependency"]
}
```

语义：

- `required_dependencies`：必须前置插件。缺失时 WebUI preview 返回 `can_install=false`，安装 API 拒绝安装；后续运行时加载器也应升级为缺失即拒绝加载。
- `optional_dependencies`：可选前置插件。存在时参与拓扑排序；缺失时只显示 warning，不阻止安装和加载。
- `dependencies`：legacy 软依赖字段。为了兼容旧插件，V1 不改变其运行时行为，仍按“加载顺序提示”处理；WebUI preview 显示为 `legacy_dependencies` 并给出兼容提示。

兼容迁移规则：

- 新插件应使用 `required_dependencies` 和 `optional_dependencies`，不要再新增 `dependencies`。
- 旧插件只声明 `dependencies` 时，WebUI 不把它当强依赖阻断安装。
- 文档和脚手架后续应把 `dependencies` 标记为 legacy。
- 运行时 `PluginManager` 可分两步升级：先识别新字段并参与排序，再在下一步对 `required_dependencies` 缺失执行 load failure。

## API 规划

新增路由挂载在 `/api/v1/plugin-installs`。

```http
GET /api/v1/plugin-installs
```

返回 WebUI 管理的安装来源清单。

响应：

```json
{
  "plugins": [
    {
      "plugin_id": "shinbot_plugin_example",
      "source_type": "github",
      "source_url": "https://github.com/NekyuuYa/shinbot-plugin-example",
      "ref": "main",
      "resolved_ref": "abc123",
      "installed_version": "0.1.0",
      "managed_by_webui": true
    }
  ]
}
```

插件列表接口 `/api/v1/plugins` 可把对应记录合并到每个插件的 `metadata.install_source`：

```json
{
  "metadata": {
    "install_source": {
      "source_type": "github",
      "source_url": "https://github.com/NekyuuYa/shinbot-plugin-example",
      "managed_by_webui": true,
      "can_update": true,
      "can_uninstall": true
    }
  }
}
```

```http
POST /api/v1/plugin-installs/github/preview
```

下载并校验 GitHub archive，但不写入 `data/plugins`，用于安装前确认。

请求：

```json
{
  "url": "https://github.com/NekyuuYa/shinbot-plugin-example",
  "ref": "main"
}
```

响应：

```json
{
  "plugin_id": "shinbot_plugin_example",
  "name": "Example Plugin",
  "version": "0.1.0",
  "description": "Example ShinBot plugin.",
  "author": "NekyuuYa",
  "role": "logic",
  "required_dependencies": [],
  "optional_dependencies": [],
  "legacy_dependencies": [],
  "missing_required_dependencies": [],
  "missing_optional_dependencies": [],
  "permissions": [],
  "source_type": "github",
  "source_url": "https://github.com/NekyuuYa/shinbot-plugin-example",
  "ref": "main",
  "resolved_ref": "abc123",
  "archive_sha256": "...",
  "target_exists": false,
  "target_managed_by_webui": false,
  "can_install": true,
  "warnings": []
}
```

```http
POST /api/v1/plugin-installs/archive/preview
```

请求体使用 raw zip bytes，`Content-Type: application/zip`。这样不需要为 FastAPI 额外引入 `python-multipart`。

查询参数：

- `filename`：可选，用于 UI 展示。

接口校验 metadata，但不写入 `data/plugins`。

响应结构与 GitHub preview 相同，`source_type` 为 `archive`。

```http
POST /api/v1/plugin-installs/github
```

请求：

```json
{
  "url": "https://github.com/NekyuuYa/shinbot-plugin-example",
  "ref": "main",
  "enable_after_install": true,
  "allow_overwrite": false
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

请求体使用 raw zip bytes，`Content-Type: application/zip`。

查询参数：

- `enable_after_install`：布尔值，默认 `true`。
- `allow_overwrite`：布尔值，默认 `false`。
- `filename`：可选，用于 manifest 或 UI 展示。

返回安装任务。

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

GitHub 来源插件可更新；archive 来源插件第一版不可自动更新，只能重新上传 ZIP 覆盖安装。

```http
DELETE /api/v1/plugin-installs/{plugin_id}
```

卸载 WebUI 管理的用户插件。流程是先 disable/unload，再删除 `data/plugins/<plugin_id>`，最后更新 manifest。第一版不删除 `data/plugin_data/<plugin_id>`，只在 UI 中提示保留数据。

### API 错误码

安装层统一抛出 `PluginInstallError`，router 转换为 `HTTPException`，由 app-level handler 包装成 Envelope。

建议错误码：

- `PLUGIN_INSTALL_INVALID_SOURCE`：URL、ref 或上传文件类型不合法。
- `PLUGIN_INSTALL_DOWNLOAD_FAILED`：下载失败。
- `PLUGIN_INSTALL_ARCHIVE_INVALID`：zip 无法读取或违反解压安全规则。
- `PLUGIN_INSTALL_METADATA_NOT_FOUND`：找不到 `metadata.json`。
- `PLUGIN_INSTALL_METADATA_INVALID`：metadata 字段非法。
- `PLUGIN_INSTALL_ENTRY_NOT_FOUND`：metadata 指向的入口文件不存在。
- `PLUGIN_INSTALL_ID_INVALID`：插件 id 不是允许前缀。
- `PLUGIN_INSTALL_REQUIRED_DEPENDENCY_MISSING`：必须前置插件缺失。
- `PLUGIN_INSTALL_TARGET_EXISTS`：目标目录已存在且不允许覆盖。
- `PLUGIN_INSTALL_TARGET_UNMANAGED`：目标插件不是 WebUI 管理，拒绝覆盖或卸载。
- `PLUGIN_INSTALL_LOAD_FAILED`：文件已安装，但 rescan/load 失败。
- `PLUGIN_INSTALL_MANIFEST_INVALID`：manifest 文件损坏或不可解析。

## 安装流程

GitHub 安装：

1. 校验 URL 只允许 `https://github.com/<owner>/<repo>` 或 `git@github.com:<owner>/<repo>.git`。
2. 校验 `ref` 只允许常见 branch/tag/commit 字符，拒绝空白、路径分隔逃逸和 shell 元字符。
3. 下载源码 archive 到临时目录。
4. 计算 archive sha256。
5. 解压到 `<data_dir>/plugin_install_tmp/<task_id>/extract`。
6. 查找插件根目录，要求根目录或一级子目录包含 `metadata.json`。
7. 校验 `metadata.json`：
   - `id` 非空。
   - `id` 以 `shinbot_plugin_`、`shinbot_adapter_` 或 `shinbot_debug_` 开头。
   - 入口文件存在。
   - `entry` 不能是绝对路径，不能包含 `..`。
   - `role` 如存在，只能是 `logic` 或 `adapter`。
   - `dependencies` 如存在，必须是字符串列表。
   - `required_dependencies` 如存在，必须是字符串列表。
   - `optional_dependencies` 如存在，必须是字符串列表。
8. 校验目标目录：
   - 目标必须在 `<data_dir>/plugins` 内。
   - 如果目标已存在，必须是 WebUI 管理的插件，或者用户显式确认覆盖。
9. 校验 `required_dependencies` 全部已经存在于当前 loaded plugins、本次安装包、或 `data/plugins` 可发现目录。
10. 如果当前已加载同 id 插件，先调用 `disable_plugin_or_raise()` 再 `bot.plugin_manager.unload_plugin_async(plugin_id)`。
11. 写入 staging 目录 `<data_dir>/plugins/.installing-<plugin_id>-<task_id>`。
12. 原子替换到 `<data_dir>/plugins/<plugin_id>`。
13. 写入插件启用状态配置：
    - `enable_after_install=true` 时设置 `plugins[].enabled=true`。
    - `enable_after_install=false` 时设置 `plugins[].enabled=false`。
14. 更新 manifest。
15. 调用 `rescan_plugins()`。
16. 如果 `enable_after_install=false` 且插件已被 rescan 短暂加载，立即调用 `disable_plugin_or_raise()`。
17. 清理临时目录。

上传 zip 安装同样从第 4 步开始。

启用状态说明：

- ShinBot 当前启动流程会先 `load_all_async(data/plugins)`，再根据 `boot.config["plugins"][].enabled` 应用启用/禁用覆盖。
- 安装服务必须复用 `set_plugin_saved_enabled()` 并调用 `boot.save_config()`，保证安装后的状态和重启后的状态一致。
- `enable_after_install=false` 不能只依赖“不调用 enable”，因为现有 rescan/load 会默认激活插件。

覆盖策略：

- 目标不存在：允许安装。
- 目标存在且 manifest 中 `managed_by_webui=true`：只有 `allow_overwrite=true` 才允许覆盖。
- 目标存在但 manifest 没有记录：拒绝覆盖。
- 目标存在且是内置插件：拒绝覆盖。

回滚策略：

- 在替换目标目录前失败：删除 staging 和 tmp，不修改 manifest。
- 替换目标目录后 load 失败：保留文件和 manifest，但任务标记 `failed`，并返回 `PLUGIN_INSTALL_LOAD_FAILED`；UI 显示“已安装但加载失败”，让管理员查看错误并修复。
- 覆盖旧版本时，可先把旧目录移动到 `.backup-<plugin_id>-<task_id>`；新版本 load 成功后删除备份，load 失败则恢复备份。

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
- 禁止 archive 写出 `data/plugins` 和 `plugin_install_tmp` 之外。
- 禁止 archive 内包含 symlink 目标。
- 禁止安装目录名和 `metadata.id` 不一致的包，除非插件根目录被识别后重命名到 `<plugin_id>`。

依赖处理建议：

- 第一版只检测依赖，不安装依赖。
- 如果导入失败是 `ModuleNotFoundError`，API 返回缺失模块名，UI 引导用户在服务端环境手动安装。
- 设计案不内置依赖安装按钮；依赖安装应作为单独安全评审项处理。

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

安装确认页展示：

- 插件名、id、版本、作者、描述。
- 插件 role。
- 必须前置插件、可选前置插件、legacy 软依赖。
- 缺失的必须前置插件和缺失的可选前置插件。
- 声明的 permissions。
- 来源 URL/ref 或上传文件名。
- 覆盖目标提示。
- “安装后启用”开关。

任务状态展示：

- queued：等待开始。
- downloading：下载中。
- extracting：解压中。
- validating：校验插件。
- installing：写入插件目录。
- loading：扫描并加载插件。
- succeeded：安装完成。
- failed：安装失败，展示错误码和 message。

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
- 新增 `PluginInstallManifest` 文件读写 helper，不接入数据库。
- 新增 `PluginInstallTaskRegistry`，使用进程内内存 dict。
- 新增 `PluginInstallService`：
  - `preview_github()`
  - `preview_archive()`
  - `install_from_github()`
  - `install_from_archive()`
  - `update_plugin()`
  - `uninstall_plugin()`
- 新增 API router 和 schema。
- 给 `plugin_dict()` 增加来源 metadata 合并。

### Phase 2: Dashboard 安装入口

- 扩展 `dashboard/src/api/plugins.ts`。
- 扩展 `dashboard/src/stores/plugins.ts`。
- 新增 `PluginInstallDialog.vue`。
- `Plugins.vue` 增加安装按钮、任务轮询和来源筛选。
- `PluginCard.vue` 增加 update/uninstall 菜单项。
- 补齐 `zh-CN` 和 `en-US` 文案。

### Phase 3: 测试

- API 单测：
  - GitHub URL 校验。
  - zip 路径逃逸拒绝。
  - zip symlink 拒绝。
  - metadata 缺失拒绝。
  - metadata id 前缀拒绝。
  - `required_dependencies` 缺失时 preview 和 install 均拒绝。
  - `optional_dependencies` 缺失时只返回 warning。
  - legacy `dependencies` 缺失时只返回兼容提示。
  - 成功安装后写入 manifest 并触发 rescan。
  - 拒绝覆盖非 WebUI 管理插件。
  - 卸载时保留 plugin data。
- integration 测试：
  - 从本地 archive 安装可加载插件。
  - 覆盖已有 WebUI 管理插件。
  - 覆盖失败时恢复旧插件目录。
- Dashboard 构建：
  - `pnpm build`

## 关键取舍

第一版不做自动依赖安装，是为了避免 WebUI 变成任意代码下载和执行入口。插件本身在启用时已经会执行 Python 代码，所以安装入口必须让“来源确认”和“实际启用”两个动作清晰可见。

第一版不用数据库。来源记录只服务本机安装管理，不影响 bot 核心运行，也不需要查询能力、历史审计或跨进程任务恢复。JSON manifest 更容易人工检查和修复，符合当前复杂度。

安装逻辑不进入 `PluginManager`，是为了保持当前架构中“安装文件”和“加载运行时”的边界。这样后续即使支持 PyPI、marketplace 或私有 registry，也不会污染插件生命周期代码。
