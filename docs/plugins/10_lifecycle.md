# 生命周期

本文档对应 `PluginManager` 的真实行为。

## 1. 核心阶段

### 加载（load）

顺序：

1. 导入模块
2. 调用 `setup(ctx)`
3. 调用可选 `on_enable(...)`
4. 标记插件为 `ACTIVE`

### 禁用（disable）

顺序：

1. 调用可选 `on_disable(...)`
2. 注销命令/事件/工具
3. 调用可选 `teardown()`
4. 保留插件元信息，状态改为 `DISABLED`

### 卸载（unload）

和禁用类似，但最终会从 `PluginManager` 中移除插件元信息。

### 重载（reload）

顺序：

1. 先执行一次停用链路
2. `importlib.reload` 模块
3. 再次执行 `setup(ctx)` + `on_enable(...)`

## 2. 钩子签名

### `setup`（必需）

```python
def setup(ctx: PluginContext) -> None:
    ...
```

也支持 `async def setup(ctx)`，框架会 await。

### `on_enable`（可选）

`PluginManager` 支持两种形式：

- `def on_enable()` 或 `async def on_enable()`
- `def on_enable(ctx)` 或 `async def on_enable(ctx)`

### `on_disable`（可选）

同上，支持 0 或 1 个参数（`ctx`）。

### `teardown`（可选）

当前实现按 **无参数** 调用：

```python
def teardown() -> None:
    ...
```

如果你写成 `teardown(ctx)`，会在卸载时触发参数错误并被记录日志。

## 3. 推荐模式

```python
import asyncio

_task: asyncio.Task | None = None


def setup(ctx):
    global _task
    _task = asyncio.create_task(_loop(ctx))


async def _loop(ctx):
    while True:
        await asyncio.sleep(5)
        ctx.logger.debug("tick")


async def on_disable(ctx):
    global _task
    if _task is None:
        return
    _task.cancel()
    try:
        await _task
    except asyncio.CancelledError:
        pass
    _task = None


def teardown():
    # 最终轻量清理
    pass
```

## 4. 异常处理语义

- `setup` / `on_enable` 异常会导致加载失败，并回滚已注册命令/事件/工具。
- `on_disable` / `teardown` 异常会被记录日志，但卸载流程继续进行。

## 5. 依赖加载顺序

当通过 `metadata.json` 目录扫描加载时，会根据 `dependencies` 做拓扑排序，尽量保证依赖先于被依赖插件。

下一步：阅读 [API 参考](./16_api_reference.md)。
