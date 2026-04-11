# ShinBot 技术规范：适配器与驱动接口 (Adapter & Driver Interfaces)

为了保持核心的纯净与高度可移植性，ShinBot 核心 (Core) 完全剥离了具体的协议实现。核心通过通用的 **RPC 风格接口** 与适配器通信。

## 1. 适配器的核心定位：协议翻译器 (Protocol Translator)

适配器负责将外部平台的私有协议与 ShinBot 的通用规范进行双向翻译：
- **事件翻译**: `Raw Payload` -> `UnifiedEvent` (Satori AST)。
- **API 翻译**: `ShinBot call_api` -> `Platform Native API`。

## 2. 平台适配器接口规范 (BaseAdapter)

任何适配器必须实现以下契约：

```python
class BaseAdapter(ABC):
    instance_id: str   # 接入端唯一标识 (Instance ID)
    platform: str      # 平台名称 (如 "onebot_v11", "discord")

    @abstractmethod
    async def start(self):
        """建立连接并开始监听事件"""
        pass

    @abstractmethod
    async def shutdown(self):
        """安全断开网络连接"""
        pass

    @abstractmethod
    async def send(self, target_session: str, elements: List[MessageElement]) -> MessageHandle:
        """发送消息的快捷入口"""
        pass

    @abstractmethod
    async def call_api(self, method: str, params: dict) -> Any:
        """
        调用 Satori 标准 API 或平台内部 API。
        """
        pass

    @abstractmethod
    async def get_capabilities(self) -> dict:
        """
        能力探测接口：返回适配器支持的元素、方法及限制。
        该注册表由适配器自行维护，核心仅用于查询。
        返回示例: 
        {
            "elements": ["text", "at", "img", "sb:poke"],
            "actions": ["message.create", "member.kick", "internal.qq.poke"],
            "limits": {"max_file_size": 10485760}
        }
        """
        pass

## 3. 能力发现机制 (Capability Discovery)

1. **注册表自主权**: 适配器拥有对其支持特性的唯一解释权。核心不预设任何平台的固定能力。
2. **动态适配**: 
    - **发送拦截**: 核心在 Egress 阶段可根据 `elements` 列表预检发送内容的兼容性。
    - **UI 联动**: Dashboard 通过此接口获取可用方法列表，为管理员生成动态调试面板。
    - **逻辑退避**: 插件可根据当前适配器的能力发现结果，自动切换备选执行路径。
```

## 3. Satori 标准 API 支持要求

驱动插件应尽可能对齐 Satori 的资源管理规范：
- **消息类**: `message.get`, `message.delete`, `message.list`
- **管理类**: `member.kick`, `member.mute`, `guild.get`
- **关系类**: `friend.list`, `friend.approve`
- **交互类**: `reaction.create`, `reaction.delete`

## 4. 内部扩展与透传 (Internal API)

当标准 API 无法覆盖平台特性时：
- 适配器应提供 `internal.{platform}.{action}` 命名空间的调用。
- 核心不理解这些调用的含义，仅负责将参数透传给适配器。

## 5. 官方补全策略与语义修复

官方适配器（Pure Satori 与 OB11 Bridge）除了负责翻译，还承担 **“语义修复层”** 的职责：

1. **深度解析**: 对于仅提供资源 ID 的复合消息（如聊天记录），适配器应在 Ingress 阶段自动拉取详情并构建完整的 AST。
2. **非标标准化**: 将各平台特有的交互（戳一戳、红包、小程序）统一映射为 `sb:` 命名空间的标准元素或 `internal` 规范 API。
3. **接口透传**: 严格遵循 `docs/design/11_internal_api_registry.md` 中的规范暴露平台特有能力。
