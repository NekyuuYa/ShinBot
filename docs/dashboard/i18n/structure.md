# ShinBot WebUI: i18n 目录与命名空间规范 (Namespace Guide)

ShinBot 采用 **“目录即命名空间 (Directory as Namespace)”** 的物理分层策略，并限制逻辑深度以保证性能与维护性的平衡。

---

## 1. 物理目录结构 (Physical Layout)
i18n 文件按业务大模块拆分并存放于对应子目录。

```text
src/locales/zh-CN/
├── common/
│   └── actions.json     # 全局操作 (save, cancel)
├── layout/
│   └── main.json        # 框架 UI (sidebar, header)
└── pages/
    ├── welcome.json     # 欢迎页文案
    ├── instances.json   # 机器人实例管理 (包含列表、编辑、详情)
    ├── plugins.json     # 插件、市场、编组
    └── auth.json        # 登录与鉴权
```

---

## 2. 键名生成规则 (Key Mapping)
系统将根据文件路径自动拼接键名：
- **格式**: `{目录}.{文件名}.{内部Group}.{内部Key}`
- **示例**: `pages.instances.form.labelToken`

---

## 3. JSON 内部规范 (Depth: Max 1)

为了防止 JSON 结构过于复杂，同时避免碎片化文件过多，ShinBot 规定：**JSON 内部最多允许一层逻辑嵌套。**

### ✅ 推荐结构 (1层嵌套)
```json
{
  "header": {
    "title": "机器人列表",
    "subtitle": "管理所有的接入实例"
  },
  "table": {
    "colName": "名称",
    "colStatus": "状态"
  }
}
```

### ❌ 禁止结构 (深层嵌套)
```json
{
  "form": {
    "adapter": {
      "settings": {
        "port": "端口"  // 严禁此类深度
      }
    }
  }
}
```

---

## 4. 协作同步
1. **多语言对齐**: 开发者在 `zh-CN` 目录下创建/修改文件后，必须在 `en-US` 下进行镜像操作。
2. **键名引用**: Vue 组件中通过 `$t('pages.instances.header.title')` 进行调用。
