# ShinBot Design Principles — Distilled from docs/

Extracted from 15 project documents. Each principle below represents validated **user intent** (the "what and why"), separated from Gemini's implementation specifics (the "how").

---

## P1. Micro-Kernel + Full Plugin Architecture

> "系统核心仅负责逻辑编排，所有的平台接入与业务功能均以插件形式存在。" — 07_plugin_system_design.md

**Rule**: `shinbot/core/` is split by responsibility:
- `application/` for app assembly and boot lifecycle
- `dispatch/` for message ingress, route-table dispatch, route targets, and EventBus
- `message_routes/` for built-in message route subsystems such as commands and keywords
- `platform/` for adapter abstractions and instance management
- `plugins/` for plugin lifecycle and registration
- `security/` for permission and audit
- `state/` for session state

**Rule**: Platform adapters and business capabilities live outside those subpackages, typically under `shinbot/builtin_plugins/` or user plugin directories.

**Rationale**: Micro-kernel enables hot-reload, keeps the core testable without any platform SDK, and allows third-party extensibility.

---

## P2. Core Purity (Zero Upward Dependencies)

**Rule**: Core may depend on `models/`, `utils/`, and persistence abstractions. Core MUST NOT import from `builtin_plugins/` or any external plugin.

**Rule**: Plugins depend on Core, never the reverse. The dependency arrow is strictly downward.

**Corollary**: Web frameworks (`fastapi`, `uvicorn`, `starlette`) belong in `api/` and plugins, never in core.

---

## P3. Adapter as Protocol Translator

> "Core is completely decoupled from protocol implementations." — 09_adapter_interface_spec.md

**Rule**: Adapters are plugins that implement `BaseAdapter`. They translate between platform-native wire formats and ShinBot's internal `UnifiedEvent`/`MessageElement` AST.

**Rule**: Core calls adapters through the abstract `BaseAdapter` interface only. The core engine never contains platform-specific logic.

**Contract**: `start()`, `shutdown()`, `send()`, `call_api()`, `get_capabilities()`.

---

## P4. Satori-Aligned Message AST

> "Messages are sequences of MessageElement AST nodes, following the Satori protocol model." — 02_message_element_spec.md

**Rule**: The internal message representation is a tree of `MessageElement` nodes with Satori-compatible types (text, at, img, quote, ...).

**Extension**: Non-Satori types use the `sb:` namespace (e.g. `sb:poke`, `sb:ark`).

**Dual View**: `.elements` for programmatic access (AST array), `.text` for string access (Satori XML).

---

## P5. Three-Pillar Interaction Model

> "听 (Listen), 说 (Speak), 管 (Action)" — 00_core_philosophy.md

1. **Listen**: Adapter → `UnifiedEvent` → `MessageIngress` → `RouteTable`
2. **Speak**: Plugin → `MessageElement[]` → `bot.send()` → adapter (egress)
3. **Action**: Plugin → `bot.call_api(method, params)` → adapter (control)

**Rule**: The `bot` handle auto-binds to the originating adapter. Plugins never choose which adapter to use — routing is implicit.

---

## P6. Plugin Lifecycle via Plugin

> "Plugin entry point is setup(plg: Plugin) — declarative, not global code execution." — internals/04_plugin_lifecycle.md

**Rule**: Plugins register capabilities (commands, keywords, custom routes, event listeners, adapter factories) through `Plugin` methods, not by mutating global state.

**Rule**: On unload, the framework deregisters all stubs owned by the plugin. This prevents "duplicate commands after reload".

**Contract**: `setup(plg)` for initialization, optional `on_enable(plg)` after activation, `on_disable(plg)` for cleanup, and optional `teardown()` for final release.

---

## P7. Session as Processing Unit

> "Session is the minimum unit for logic processing, context maintenance, and permission binding." — 04_session_management.md

**Rule**: Session ID is a URN: `{instance_id}:{type}:{target_id}`.

**Rule**: Instance-level isolation: same group + different bot accounts = different sessions.

**Rule**: Plugin data is sandboxed per-session via `Session.plugin_data`.

---

## P8. RBAC Permission Model

> "Dot-separated permission tree with explicit-deny highest priority." — 05_permission_system.md

**Rule**: Permissions are hierarchical paths (`tools.weather.admin`).

**Rule**: Explicit deny (`-permission.node`) overrides all grants.

**Rule**: Two scopes — session-scoped and global-scoped.

---

## P9. Naming Convention Enforcement

> 07_plugin_system_design.md §2

| Type | Pattern | Example |
|------|---------|---------|
| Business plugin | `shinbot_plugin_{name}` | `shinbot_plugin_weather` |
| Adapter plugin | `shinbot_adapter_{platform}` | `shinbot_adapter_satori` |
| Debug plugin | `shinbot_debug_{name}` | `shinbot_debug_message` |

---

## P10. Data Isolation

**Rule**: Plugin code directory is read-only at runtime.

**Rule**: Plugin-owned file assets go to `data/plugin_data/{plugin_id}/`.

**Rule**: Framework injects path via `plg.data_dir`.

---

## Gemini-Specific Decisions (Documented but NOT Ratified as Principles)

The following items appear in docs but are Gemini implementation choices, not validated design intent. They should be treated as "current implementation" rather than "architectural constraints":

1. **P0 hard-fail**: Prefix-matched but unrecognized commands error instead of falling through to LLM. (03_command_system.md — strong opinion, needs user confirmation.)
2. **JSON file persistence** for sessions. (internals/05 — may not scale.)
3. **Specific FastAPI admin endpoints** (`/admin/plugins`, `/admin/plugins/{id}/reload`). (internals/07 — implementation detail.)
4. **TOML as config format**. (Never formally specified in a design doc, appeared in Gemini implementation.)
5. **Specific permission delimiter choices** (`:` in URN, `.` in permission paths). (Ambiguity with platform IDs unresolved.)
