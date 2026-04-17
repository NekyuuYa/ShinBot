# 插件配置系统

ShinBot 当前的插件配置能力，核心是：

1. 用 `__plugin_config_class__` 暴露 Pydantic 模型。
2. 通过 API 读 schema、写配置。
3. 保存到主配置中的 `plugin_configs`。

## 1. 声明配置模型

```python
from pydantic import BaseModel, Field


class DemoConfig(BaseModel):
    api_key: str = Field(default="", description="API key")
    timeout: int = Field(default=10, ge=1, le=120, description="Timeout seconds")


__plugin_config_class__ = DemoConfig
```

只要模型有 `model_validate` / `model_json_schema` 能力，就会被识别。

## 2. API 行为

### 读取 schema

`GET /api/v1/plugins/{plugin_id}/schema`

- 读取 `__plugin_config_class__` 生成的 JSON Schema。
- 适配器角色插件会被拒绝（404）。

### 更新配置

`PATCH /api/v1/plugins/{plugin_id}/config`

请求体是 key-value 对象，支持扁平 key：

```json
{
  "api_key": "secret",
  "retry.timeout": 8
}
```

服务端会先把扁平 key 展开成嵌套对象，再用 `model_validate` 校验。

## 3. 配置存储位置

配置会保存到运行配置对象中的 `plugin_configs`：

```json
{
  "plugin_configs": {
    "demo-plugin": {
      "api_key": "secret",
      "retry": {
        "timeout": 8
      }
    }
  }
}
```

## 4. 现阶段限制

这是当前实现中最重要的一点：

- `PluginContext` 不会自动注入“已保存配置实例”。
- 也就是说，插件代码里目前没有统一的 `ctx.config` 可直接读取。

因此你需要自行决定在运行期如何读取配置（例如通过业务侧 API、自定义读取逻辑等）。

## 5. 配置翻译键

配置 schema 的 UI 文本可通过 locale 覆盖：

- `config.title`
- `config.description`
- `config.fields.<field_path>.label`
- `config.fields.<field_path>.description`

详见 [本地化](./07_localization.md)。

下一步：阅读 [本地化](./07_localization.md)。
