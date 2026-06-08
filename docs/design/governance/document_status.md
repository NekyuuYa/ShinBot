# Document Status

本文维护 `docs/` 中长期文档的当前可信状态。状态只描述文档与当前代码/架构的一致性，不评价文档价值。

## 状态定义

- **现行**：可以作为当前设计或实现边界依据。
- **部分现行**：核心概念仍有价值，但存在已迁移、未实现或被新文档覆盖的部分；阅读时必须结合文档顶部审计说明。
- **历史参考**：用于理解早期设计来源，不作为当前实现约束。
- **已归档**：被新文档替代，只保留追溯价值。
- **外部参考**：第三方资料镜像，不代表 ShinBot 自己的实现承诺。

## 优先入口

- `../README.md`：设计文档目录索引。
- `../../architecture/README.md`：跨模块架构边界。
- `../runtime/agent_runtime_index.md`：Agent runtime 文档状态索引。
- `../../internals/README.md`：当前实现说明索引。
- `../../plugins/README.md`：插件开发者文档入口。

## 当前重点状态

| 文档 | 状态 | 说明 |
|------|------|------|
| `../../architecture/agent_module_layers.md` | 现行 | Agent scheduler / coordinator / workflow / runner / service 分层依据。 |
| `../../architecture/agent_context_boundary.md` | 现行 | Agent context 当前职责边界。 |
| `../runtime/agent_runtime_index.md` | 现行 | Agent runtime 文档状态入口。 |
| `../runtime/prompt_registry.md` | 现行 | PromptRegistry 七阶段装配协议。 |
| `../runtime/prompt_registry_schema.md` | 现行 | PromptRegistry 当前核心数据结构。 |
| `../runtime/tool_registry_and_manager.md` | 现行 | Agent tool registry / manager 能力规格。 |
| `../runtime/active_chat_workflow.md` | 现行 | Active chat 双层触发和会话生命周期。 |
| `../runtime/media_semantics_and_meme_handling.md` | 现行 | 媒体语义、fingerprint、dedup 和 reanalysis。 |
| `../runtime/database_persistence_architecture.md` | 部分现行 | 持久化边界有效；早期 ORM/Alembic 规划已由 SQLite repository 实现取代。 |
| `../runtime/attention_driven_conversation_workflow.md` | 部分现行 | Attention 核心概念保留；SenderWeightState 等高级特性未实现。 |
| `glossary.md` | 部分现行 | 通用术语可用；Attention 相关条目保留历史说明。 |
| `../../architecture/design_principles.md` | 历史参考 | 早期设计原则提炼，不作为唯一现行约束。 |
| `../../archive/runtime/context_memory_architecture.md` | 已归档 | 旧 context / 三级记忆大方案，当前边界见 `agent_context_boundary.md`。 |
| `../../references/satori-docs/` | 外部参考 | Satori 文档镜像，不代表 ShinBot 当前支持范围。 |

## 维护规则

- 文档顶部出现审计状态时，应优先相信审计状态，而不是正文中的旧计划语气。
- 修改源码结构后，同步检查对应文档中的反引号路径。
- 若文档只有局部概念仍有效，应标为“部分现行”，不要让读者误以为全文都是现行实现约束。
- 被替代文档应移入 `../../archive/`，并在顶部注明替代文档和归档原因。
