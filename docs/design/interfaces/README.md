# Interface Design Docs

`docs/design/interfaces/` 记录对外界面和前后端通信契约的长期设计。

## 当前文档

- `webui_design_spec.md`
  - Dashboard / WebUI 的整体信息架构和交互规格。
- `api_communication_spec.md`
  - 管理 API 通信约定、Envelope 响应和错误语义。
- `model_runtime_webui_spec.md`
  - Model runtime 管理界面的页面与交互规格。

## 放置规则

- 长期 API 契约、页面信息架构和前后端通信规范放在这里。
- Dashboard 实现维护说明放在 `../../dashboard/`。
- API router 当前实现说明放在 `../../internals/` 或源码附近测试中。
