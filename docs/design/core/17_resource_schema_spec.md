# ShinBot 技术规范：资源模型与非消息事件 (Resource & Event Schema)

本文档定义了 ShinBot 内部处理结构化数据（User, Guild, Member 等）的标准 JSON Schema，旨在实现“感知（听）”与“操控（管）”的数据对齐。

## 1. 核心资源模型 (Core Models)

所有资源对象在 Ingress (听) 和 Action (管) 链路中保持字段一致。

### 1.1 用户 (User)
- `id`: 平台唯一标识
- `name`: 用户名
- `nick`: 昵称
- `avatar`: 头像 URL
- `is_bot`: 是否为机器人

### 1.2 群组 (Guild) / 频道 (Channel)
- 对齐 Satori 规范，包含 ID、名称、类型等。

## 2. 交互与通知事件 (Notice/Request)

非消息类事件的载体是结构化 JSON，而非 XML AST。

| 事件前缀 | 常用类型 | 载荷结构 |
| :--- | :--- | :--- |
| `guild-member-` | `added`, `deleted` | `guild`, `member/user`, `operator` |
| `friend-` | `request` | `user`, `message` |
| `interaction/` | `poke` (自定义) | `user` (发起者), `target` (被戳者) |

## 3. 逻辑价值
插件在监听到 `guild-member-added` 事件时，拿到的 `event.guild.id` 可以直接作为 `ctx.call_api("member.kick", {"guild_id": ...})` 的参数，无需二次转换。
