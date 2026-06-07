import type {
  ConfigRecord,
  ConfigValue,
  NormalizedAdapterInstanceConfig,
  NormalizedBotBindingConfig,
} from '@/api/config'
import type { BotInstanceDraft, BotInstanceFormState } from '@/components/instances/botTypes'

export function createEmptyBotForm(): BotInstanceFormState {
  return {
    id: '',
    display_name: '',
    enabled: true,
    commands: {
      enabled: true,
      prefixes: ['/'],
    },
    plugins: {
      enabled: true,
      enabled_plugins: ['*'],
      disabled_plugins: [],
    },
    agent: {
      mode: 'none',
      config: '',
    },
    bindings: [],
  }
}

export function isConfigRecord(value: unknown): value is ConfigRecord {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

export function cloneConfigRecord(value: ConfigRecord = {}): ConfigRecord {
  return JSON.parse(JSON.stringify(value)) as ConfigRecord
}

export function normalizeAdapter(
  record: ConfigRecord,
  index: number
): NormalizedAdapterInstanceConfig {
  const id = normalizeString(record.id, `adapter-${index + 1}`)
  const adapter = normalizeString(record.adapter)
  return {
    id,
    name: normalizeString(record.name, id),
    adapter,
    enabled: normalizeBoolean(record.enabled, true),
    config: isConfigRecord(record.config) ? cloneConfigRecord(record.config) : {},
    createdAt: typeof record.createdAt === 'number' ? record.createdAt : 0,
    lastModified: typeof record.lastModified === 'number' ? record.lastModified : 0,
  }
}

export function normalizeBot(record: ConfigRecord, index: number): BotInstanceDraft {
  const id = normalizeString(record.id, `bot-${index + 1}`)
  const commands = isConfigRecord(record.commands) ? record.commands : {}
  const plugins = isConfigRecord(record.plugins) ? record.plugins : {}
  const agent = isConfigRecord(record.agent) ? record.agent : {}
  const rawBindings = Array.isArray(record.bindings) ? record.bindings : []

  return {
    id,
    display_name: normalizeString(record.display_name, id),
    enabled: normalizeBoolean(record.enabled, true),
    commands: {
      enabled: normalizeBoolean(commands.enabled, true),
      prefixes: normalizeStringList(commands.prefixes, ['/']),
    },
    plugins: {
      enabled: normalizeBoolean(plugins.enabled, true),
      enabled_plugins: normalizeStringList(plugins.enabled_plugins, ['*']),
      disabled_plugins: normalizeStringList(plugins.disabled_plugins, []),
    },
    agent: {
      mode: normalizeString(agent.mode, 'none'),
      config: normalizeString(agent.config),
    },
    bindings: rawBindings.map((binding, bindingIndex) =>
      normalizeBinding(binding, bindingIndex, id)
    ),
    platformBindings: [],
    platformStatusSummary: '',
  }
}

export function buildBotRecord(form: BotInstanceFormState): ConfigRecord {
  const id = form.id.trim()
  return {
    id,
    display_name: form.display_name.trim() || id,
    enabled: form.enabled,
    commands: {
      enabled: form.commands.enabled,
      prefixes: cleanStringList(form.commands.prefixes, ['/']),
    },
    plugins: {
      enabled: form.plugins.enabled,
      enabled_plugins: cleanStringList(form.plugins.enabled_plugins, ['*']),
      disabled_plugins: cleanStringList(form.plugins.disabled_plugins, []),
    },
    agent: {
      mode: form.agent.mode,
      config: form.agent.config.trim(),
    },
    bindings: form.bindings.map((binding) => ({
      id: binding.id.trim(),
      adapter_instance_id: binding.adapter_instance_id.trim(),
      session_patterns: cleanStringList(binding.session_patterns, ['group:*']),
      enabled: binding.enabled,
      priority: Number.isInteger(Number(binding.priority)) ? Number(binding.priority) : 0,
    })),
  }
}

function normalizeString(value: ConfigValue | undefined, fallback = ''): string {
  return typeof value === 'string' ? value.trim() : fallback
}

function normalizeBoolean(value: ConfigValue | undefined, fallback = true): boolean {
  return typeof value === 'boolean' ? value : fallback
}

function normalizeInteger(value: ConfigValue | undefined, fallback = 0): number {
  return typeof value === 'number' && Number.isInteger(value) ? value : fallback
}

function normalizeStringList(value: ConfigValue | undefined, fallback: string[] = []): string[] {
  if (!Array.isArray(value)) {
    return fallback
  }
  return value
    .filter((item): item is string => typeof item === 'string')
    .map((item) => item.trim())
    .filter(Boolean)
}

function normalizeBinding(
  value: ConfigValue | undefined,
  index: number,
  botId: string
): NormalizedBotBindingConfig {
  const record = isConfigRecord(value) ? value : {}
  return {
    id: normalizeString(record.id, `${botId}-binding-${index + 1}`),
    adapter_instance_id: normalizeString(record.adapter_instance_id),
    session_patterns: normalizeStringList(record.session_patterns, ['group:*']),
    enabled: normalizeBoolean(record.enabled, true),
    priority: normalizeInteger(record.priority, 0),
  }
}

function cleanStringList(value: string[], fallback: string[]): string[] {
  const result = value.map((item) => String(item).trim()).filter(Boolean)
  return result.length > 0 ? Array.from(new Set(result)) : fallback
}
