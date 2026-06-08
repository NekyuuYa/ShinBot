# ShinBot 名词解释 (Glossary)

> **审计状态 (2026-06-08)**：部分现行。核心、插件、权限和持久化术语已按当前实现修正；Attention 相关条目保留为历史 attention workflow 概念，当前 active chat 实现以 `../runtime/active_chat_workflow.md` 和 `../../architecture/agent_module_layers.md` 为准。

## 1. 实例 (Instance / instance_id)
- **定义**: 每一个独立的 Bot 账号或连接端点。
- **作用**: 用于会话隔离和路由。用户在配置中指定其使用的 **适配器类型**。

## 2. 适配器 (Adapter)
- **定义**: **协议翻译器插件**。负责将特定平台（如 OneBot）的原始消息转换为 ShinBot 内部的 `UnifiedEvent` (AST Elements)。
- **存在形式**: 它不是核心组件，而是由驱动插件向框架注册的一种 **“接入能力”**。

## 3. 会话 (Session / session_id)
- **定义**: 消息流转的逻辑单位。由适配器提供的 `channel_id` 与 `guild_id` 结合生成的复合 ID。

## 4. 消息元素 (MessageElement)
- **定义**: 消息内容的最小语义单元。ShinBot 内部统一采用 Satori 规范的 AST 表示。

## 5. 消息工作流 (Message Workflow)
- **定义**: 消息在系统内的非线性处理流程。由拦截器、分发器和业务处理器组成的异步执行链。

## 6. 插件上下文 (Plugin)
- **定义**: 插件的独立运行环境代理。每个插件获得一个 `Plugin` 实例，提供注册命令、监听事件、访问会话数据等核心能力。
- **作用**: 隔离插件间的副作用，确保插件卸载时所有注册资源可被完整清理。

## 7. 消息句柄 (MessageHandle)
- **定义**: `send()` 调用返回的不透明引用，封装了平台侧消息的标识信息（如消息 ID）。
- **作用**: 供后续操作（撤回、引用回复）使用，无需插件感知平台细节。

## 8. 统一事件 (UnifiedEvent)
- **定义**: 适配器将平台原始事件翻译后产生的通用数据结构，使用 Satori AST 表示消息内容。
- **作用**: 消除平台差异，使插件逻辑与具体协议完全解耦。

## 9. 命令注册表 (CommandRegistry)
- **定义**: 维护插件注册的所有命令处理器的中央索引，按命令前缀和名称路由消息。
- **作用**: 支持多插件共享命令空间，提供命令冲突检测和权限声明绑定。

## 10. 事件总线 (EventBus)
- **定义**: 系统内部的发布-订阅中间件，用于在核心与插件之间分发生命周期事件和平台事件。
- **作用**: 解耦事件生产者和消费者，允许多个插件同时订阅同一事件类型。

## 11. 权限引擎 (PermissionEngine)
- **定义**: 负责维护用户/会话身份与权限组的绑定关系，并在命令执行前进行权限校验的核心组件。
- **作用**: 实现 RBAC（基于角色的访问控制），统一处理跨平台的授权决策。

## 12. 审计日志 (AuditLog)
- **定义**: 对每次命令执行（包括权限结果、耗时、成功/失败状态）所产生的结构化日志记录。
- **存储**: 当前可写入数据库 `audit_logs`，并可旁路持久化到 `data/audit/audit_YYYY-MM-DD.jsonl`，按日轮转。

## 13. 管道 (Pipeline)
- **定义**: 消息从适配器进入系统后经历的有序处理阶段序列，包含 Ingress、路由、权限校验、命令分发和 Egress 等阶段。
- **作用**: 提供统一的消息处理生命周期，允许在各阶段插入拦截器逻辑。

## 14. 注意力 (Attention)
- **定义**: Active Chat 中的短周期触发状态，用于表示 pending 消息是否值得触发一轮 LLM。
- **作用**: 累积多条消息的价值信号，达到动态阈值后进入 semantic wait 并批量触发 active chat round。

## 15. Sender Weight
- **定义**: 早期 attention workflow 设计中的概念，表示某个 sender 在某个 `Session` 内对全局 attention 增量的影响权重。
- **状态**: 当前 active chat 尚未实现稳定的 per-sender weight 状态；消息贡献主要由 mention、reply、poke、bot 自己消息等特征计算。

## 16. Attention Batch
- **定义**: active chat attention 达到阈值并经过 semantic wait 后，由 pending buffer 形成的 `ActiveChatBatch`。
- **作用**: 将 LLM 输入单位从“单条消息”提升为“成批消息片段”，降低逐消息触发成本。

## 17. Media Fingerprint
- **定义**: 对媒体资源计算得到的稳定识别信息，通常包含 `raw_hash` 与视觉近似用的 `strict_dhash`。
- **作用**: 服务于缓存复用、重复检测、meme 聚类与原图回看。

## 18. Meme Digest
- **定义**: 对表情包或梗图的短文本语义摘要，默认控制在 50 字以内。
- **作用**: 在不反复重新看图的前提下，把 meme 的核心语义注入上下文。

## 19. Sliding TTL
- **定义**: 一种“每次再次命中时重置过期时间”的缓存保留策略。
- **作用**: 让高频出现的资源自然常驻缓存，让冷门资源自然淘汰。
