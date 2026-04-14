export type ModelRuntimeTab = 'routes' | 'chat' | 'embedding' | 'other'

export interface ProviderSourceTemplate {
  key: string
  label: string
  type: string
  defaultBaseUrl: string
  supportsToken: boolean
  supportsCatalog: boolean
  supportsThinking: boolean
  supportsFilters: boolean
  showApiVersion?: boolean
}

export const providerSourceTemplates: ProviderSourceTemplate[] = [
  {
    key: 'openai',
    label: 'OpenAI',
    type: 'openai',
    defaultBaseUrl: 'https://api.openai.com/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
  {
    key: 'openrouter',
    label: 'OpenRouter',
    type: 'openrouter',
    defaultBaseUrl: 'https://openrouter.ai/api/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'anthropic',
    label: 'Anthropic',
    type: 'anthropic',
    defaultBaseUrl: 'https://api.anthropic.com',
    supportsToken: true,
    supportsCatalog: false,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'gemini',
    label: 'Gemini',
    type: 'gemini',
    defaultBaseUrl: 'https://generativelanguage.googleapis.com',
    supportsToken: true,
    supportsCatalog: false,
    supportsThinking: true,
    supportsFilters: true,
  },
  {
    key: 'azure_openai',
    label: 'Azure OpenAI',
    type: 'azure_openai',
    defaultBaseUrl: 'https://your-resource.openai.azure.com/openai',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
    showApiVersion: true,
  },
  {
    key: 'ollama',
    label: 'Ollama',
    type: 'ollama',
    defaultBaseUrl: 'http://127.0.0.1:11434',
    supportsToken: false,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
  {
    key: 'custom_openai',
    label: 'Custom OpenAI Compatible',
    type: 'custom_openai',
    defaultBaseUrl: 'https://api.example.com/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
]

export function resolveProviderSource(type: string) {
  return providerSourceTemplates.find((item) => item.type === type || item.key === type) ?? null
}

export function isChatModel(capabilities: string[]) {
  const normalized = capabilities.map((item) => item.toLowerCase())
  return normalized.some((item) =>
    ['chat', 'vision', 'tool_calling', 'json_mode'].includes(item)
  )
}

export function isEmbeddingModel(capabilities: string[]) {
  return capabilities.map((item) => item.toLowerCase()).includes('embedding')
}

export function modelMatchesTab(capabilities: string[], tab: ModelRuntimeTab) {
  if (tab === 'routes') {
    return true
  }
  if (tab === 'chat') {
    return isChatModel(capabilities)
  }
  if (tab === 'embedding') {
    return isEmbeddingModel(capabilities)
  }
  return !isChatModel(capabilities) && !isEmbeddingModel(capabilities)
}

export function routeMatchesTab(
  metadata: Record<string, unknown> | undefined,
  tab: ModelRuntimeTab
) {
  if (tab === 'routes') {
    return true
  }
  const domain = typeof metadata?.domain === 'string' ? metadata.domain : ''
  return domain ? domain === tab : true
}

export function makeModelId(providerId: string, modelId: string) {
  const normalized = modelId
    .trim()
    .replace(/[^a-zA-Z0-9._/-]+/g, '-')
    .replace(/\/+/g, '/')
    .replace(/^-+|-+$/g, '')
  return `${providerId}/${normalized}`
}
