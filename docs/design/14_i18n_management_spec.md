# ShinBot 技术规范：i18n 国际化管理 (i18n Management)

为了保证 ShinBot 全球化部署的能力，前端禁止出现任何非变量的硬编码文本。

---

## 1. 目录结构规范
i18n 文件按业务模块进行物理拆分，存放于 `src/locales/{lang}/` 目录下：

```text
src/locales/zh-CN/
├── common.json      # 通用文本
├── nav.json         # 导航菜单与页面标题
├── instances.json   # 机器人管理相关
├── plugins.json     # 插件与市场相关
├── auth.json        # 登录、权限、鉴权
└── monitoring.json  # 日志、审计、成本分析
```

---

## 2. 键名命名规范
使用 **小驼峰** 命名法，层级清晰：
- **页面标题**: `nav.welcome`, `nav.instances.edit`
- **操作按钮**: `common.action.save`, `common.action.delete`
- **表单 Label**: `instances.form.adapterType.label`

---

## 3. 开发禁令
1. **禁止硬编码**: 严禁在 `.vue` 模版中直接写中文或英文。必须使用 `$t('key')`。
2. **多语言同步**: 增加键值后必须跨语种对齐。
3. **动态参数**: 利用占位符处理动态文本，禁止手动拼接。
