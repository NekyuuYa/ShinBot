# ShinBot 技术规范：会话管理 (Session Management)

会话 (Session) 是 ShinBot 逻辑处理、上下文维护和权限绑定的最小核心单位。

## 1. 会话标识协议 (Session Identity)

ShinBot 的会话识别完全对齐 Satori 的资源模型，确保跨平台、跨实例的唯一性。

### 1.1 生成规则 (Identity URN)
`session_id = {instance_id}:{type}:{target_id}`

- **instance_id**: 接入实例唯一标识 (由用户配置，如 `qq_main`)。
- **type**: 
    - `group`: 代表群聊、频道或多人场景。对应 Satori 的 `channel`。
    - `private`: 代表个人私聊。对应 Satori 的 `user`。
- **target_id**:
    - **群聊**: 
        - 单层结构 (如 QQ): 直接使用 `channel_id`。
        - 嵌套结构 (如 Discord): 使用 `{guild_id}:{channel_id}`。
    - **私聊**: 使用 `user_id`。

## 2. 核心对象定义 (Session Schema)

```python
class Session:
    # --- 身份标识 ---
    id: str                # 全局唯一标识符 (URN)
    instance_id: str       # 归属实例 ID
    platform: str          # 来源平台 (onebot, discord, satori...)
    guild_id: str          # 顶级容器 ID (服务器 ID) - 可选
    channel_id: str        # 目标容器 ID (群号或频道 ID)
    
    # --- 元数据 (持久化) ---
    display_name: str      # 会话名称 (如群名、用户昵称)
    permission_group: str  # 关联的权限组 ID (默认为 Default)
    created_at: int        # 创建时间戳
    last_active: int       # 最后活跃时间戳

    # --- 运行配置 (Session-bound Config) ---
    config: {
        "prefixes": List[str],  # 该会话生效的指令标记符 (如 ["/", "#"])
        "llm_enabled": bool,    # 是否允许在此会话触发 LLM
        "is_muted": bool,       # 是否禁言机器人
        "audit_enabled": bool   # 是否记录详细审计日志
    }
    
    # --- 动态数据 (KV Storage) ---
    state: Dict[str, Any]       # 运行时状态机 (如等待输入的中间状态)
    plugin_data: Dict[str, Any] # 插件私有存储空间 (Key 由插件 ID 隔离)
```

## 3. 会话隔离与安全原则

1.  **实例级隔离**: 即使两个 Bot 账号加入同一个群，由于 `instance_id` 不同，它们的 `session_id` 必然不同，从而保证了大脑、记忆和权限的完全隔离。
2.  **数据沙箱**: 插件应优先利用 `Session.plugin_data` 存储数据，确保业务逻辑在不同群聊间互不干扰。
3.  **环境感知**: 框架在处理消息前，必须先加载对应的 `Session` 对象，并将 `config.prefixes` 注入指令解析器。
