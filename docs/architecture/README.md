# Architecture Docs

`docs/architecture/` 记录 ShinBot 当前有效的长期架构边界。

这里的文档不追踪某一次重构做了什么，而是描述后续代码应当长期遵守的层级关系、职责边界和命名约束。

## 当前文档

- `design_principles.md`
  - 跨系统设计原则。
- `agent_module_layers.md`
  - Agent 调度、协调、工作流、工具型能力和运行时服务的分层约束。

## 放置规则

- 一个结论会影响多个目录或多个子系统时，优先写在这里。
- 单个领域的能力规格仍放在 `docs/design/`。
- 与当前代码强绑定的实现说明仍放在 `docs/internals/`。
- 临时重构计划和阶段总结不放在这里。
