# Config Provider Schema Design

This document describes the proposed provider-owned configuration schema system
for the new bot-centered ShinBot configuration model.

The goal is to keep `data/config.toml` focused on application wiring while each
adapter, plugin, and large subsystem owns its own detailed config schema and
template.


## Goals

- Keep the main config small and readable.
- Avoid duplicating adapter/plugin-specific options in `config.example.toml`.
- Let WebUI render config forms from provider metadata instead of hard-coded
field lists.
- Give each provider a single place to define defaults, validation, examples,
secret fields, and documentation.
- Support future provider installation without editing the main config template.


## File Layout

Root templates:

```text
config.example.toml       # main config template, copied to data/config.toml
agent.example.toml        # full agent config template, copied to data/agents/*.toml
```

Runtime user files:

```text
data/
  config.toml
  agents/
    full-agent.toml
  prompts/
    zh-CN/
    en-US/
```

Provider-owned schemas/templates should live next to the provider code:

```text
shinbot/
  adapters/
    onebot_v11/
      config.schema.toml
      config.example.toml
    satori/
      config.schema.toml
      config.example.toml
  builtin_plugins/
    shinbot_plugin_search/
      config.schema.toml
      config.example.toml
```

The exact package paths can follow the current adapter/plugin layout. The
important rule is ownership: a provider owns its own config details.


## Main Config Boundary

`data/config.toml` should describe only wiring:

```toml
[[adapter_instances]]
id = "qq-main"
adapter = "onebot_v11"
enabled = true

[adapter_instances.config]
mode = "reverse"
reverse_port = 8082
reverse_path = "/onebot/v11"

[[plugins]]
id = "shinbot_plugin_search"
module = "shinbot.builtin_plugins.shinbot_plugin_search"
enabled = false

[[bots]]
id = "full-agent"
enabled = true

[bots.plugins]
enabled_plugins = ["*"]

[bots.agent]
mode = "full"
config = "agents/full-agent.toml"
```

The main config may contain small examples under `*.config`, but full field
documentation belongs to the provider schema/template.


## Provider Schema Model

A provider schema should expose:

- Provider identity: `id`, `kind`, `display_name`, `description`.
- Config version.
- Default values.
- Field definitions.
- Validation hints.
- Secret-field markers.
- WebUI presentation hints.
- Optional TOML example.

Provider kinds:

- `adapter`
- `plugin`
- `agent`
- Future: `model_provider`, `tool_provider`, `memory_provider`


## Schema TOML Shape

Example adapter schema:

```toml
[provider]
kind = "adapter"
id = "onebot_v11"
display_name = "OneBot v11"
description = "OneBot v11 WebSocket adapter."
config_version = "1.0.0"

[[fields]]
path = "mode"
type = "enum"
required = true
default = "reverse"
choices = ["forward", "reverse"]
description = "Connection mode."

[[fields]]
path = "reverse_port"
type = "integer"
required = true
default = 8082
min = 1
max = 65535
visible_when = "mode == 'reverse'"

[[fields]]
path = "access_token"
type = "string"
required = false
default = ""
secret = true
description = "Adapter access token."
```

Example plugin schema:

```toml
[provider]
kind = "plugin"
id = "shinbot_plugin_search"
display_name = "Search"
description = "Web search plugin."
config_version = "1.0.0"

[[fields]]
path = "tavily_api_key"
type = "string"
required = false
default = ""
secret = true
env = "TAVILY_API_KEY"

[[fields]]
path = "default_max_results"
type = "integer"
required = false
default = 5
min = 1
max = 20
```


## Field Types

Initial field types:

- `string`
- `integer`
- `float`
- `boolean`
- `enum`
- `string_list`
- `integer_list`
- `object`
- `array_object`
- `path`
- `duration`

Additional metadata:

- `required`
- `default`
- `choices`
- `min`
- `max`
- `secret`
- `env`
- `placeholder`
- `description`
- `visible_when`
- `advanced`
- `deprecated`

The parser should treat unknown metadata keys as forward-compatible UI hints.


## Template Files

Each provider may also ship `config.example.toml`.

The schema is canonical for validation and WebUI forms. The example TOML is for
humans and quick copy/paste.

Example:

```toml
mode = "reverse"
reverse_port = 8082
reverse_path = "/onebot/v11"
access_token = ""
```


## Runtime Registry

Introduce a config provider registry with entries like:

```python
ConfigProviderDefinition(
    kind="adapter",
    id="onebot_v11",
    schema=...,
    example_toml="...",
    owner_module="shinbot.adapters.onebot_v11",
)
```

Adapter providers register during adapter discovery.
Plugin providers register during plugin discovery.

Agent config can be registered by the agent module itself.


## WebUI Flow

Adapter config:

1. WebUI asks for provider catalog.
2. User chooses `adapter = "onebot_v11"`.
3. WebUI fetches schema/example for that provider.
4. WebUI renders a form and stores values under `adapter_instances.config`.

Plugin config:

1. WebUI lists installed plugins.
2. User opens one plugin.
3. WebUI fetches plugin config schema/example.
4. WebUI stores global plugin config under `plugins.config`, or bot-specific
   plugin overrides under the selected bot.

Agent config:

1. Main config references `data/agents/*.toml`.
2. WebUI edits the agent file using the agent config schema.
3. Prompt file edits use `data/prompts/{locale}/{prompt_id}.md` and call
   `AgentRuntime.reload_prompt_files()`.


## Validation Strategy

Validation should happen in layers:

1. TOML syntax parse.
2. Main config structure validation.
3. Provider existence validation.
4. Provider-specific config validation.
5. Runtime semantic validation.

Provider-specific validation should produce field-path errors:

```json
{
  "path": "adapter_instances[0].config.reverse_port",
  "message": "reverse_port must be between 1 and 65535"
}
```


## Secret Handling

Fields marked `secret = true` should:

- Be masked in WebUI by default.
- Avoid appearing in normal logs.
- Support env-var indirection.
- Preserve existing stored value when WebUI submits an unchanged masked value.


## Compatibility

The new config plan intentionally does not support automatic migration from the
old platform-centered config shape.

Runtime should read:

```text
data/config.toml
```

If missing, startup should fail with a message telling the user to copy
`config.example.toml` to `data/config.toml`.


## Open Questions

- Should provider schemas be TOML only, or should providers also be allowed to
  construct schemas in Python?
- Should plugin config support both global defaults and bot-specific overrides
  in the first implementation?
- Should adapter provider schemas live under adapter packages or in a central
  registry package for built-in adapters?
- Should WebUI write examples as TOML fragments or always submit normalized JSON
  to backend APIs?
