import type { ProviderTypeMetadata } from '@/api/modelRuntime'

export type ModelRuntimeTab = 'routes' | 'chat' | 'embedding' | 'rerank' | 'tts' | 'stt' | 'image' | 'video'
export type ProviderCapabilityType = 'completion' | 'embedding' | 'rerank' | 'tts' | 'stt' | 'image' | 'video'

export const PROVIDER_CAPABILITY_TYPES: ProviderCapabilityType[] = [
  'completion',
  'embedding',
  'rerank',
  'tts',
  'stt',
  'image',
  'video',
]

/** Capabilities stored on models under a given provider capability type. */
export const MODEL_CAPABILITIES_FOR_TYPE: Record<ProviderCapabilityType, string[]> = {
  completion: ['chat', 'vision', 'tool_calling', 'json_mode'],
  embedding: ['embedding'],
  rerank: ['rerank'],
  tts: ['tts'],
  stt: ['stt', 'audio_transcription'],
  image: ['image_generation'],
  video: ['video_generation'],
}

/** Default capabilities pre-set when adding a model to a provider. */
export const DEFAULT_CAPABILITIES_FOR_TYPE: Record<ProviderCapabilityType, string[]> = {
  completion: ['chat'],
  embedding: ['embedding'],
  rerank: ['rerank'],
  tts: ['tts'],
  stt: ['stt'],
  image: ['image_generation'],
  video: ['video_generation'],
}

export function tabToCapabilityType(tab: ModelRuntimeTab): ProviderCapabilityType {
  if (tab === 'embedding') return 'embedding'
  if (tab === 'rerank') return 'rerank'
  if (tab === 'tts') return 'tts'
  if (tab === 'stt') return 'stt'
  if (tab === 'image') return 'image'
  if (tab === 'video') return 'video'
  return 'completion'
}

export function capabilityTypeToTab(type: string): ModelRuntimeTab {
  if (type === 'embedding') return 'embedding'
  if (type === 'rerank') return 'rerank'
  if (type === 'tts') return 'tts'
  if (type === 'stt') return 'stt'
  if (type === 'image') return 'image'
  if (type === 'video') return 'video'
  return 'chat'
}

export interface ProviderSourceTemplate {
  key: string
  label: string
  type: string
  icon?: string
  description?: string
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
    icon: 'mdi-cloud-outline',
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
    icon: 'mdi-cloud-outline',
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
    icon: 'mdi-alpha-a-circle-outline',
    defaultBaseUrl: 'https://api.anthropic.com',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'gemini',
    label: 'Gemini',
    type: 'gemini',
    icon: 'mdi-google',
    defaultBaseUrl: 'https://generativelanguage.googleapis.com',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: true,
  },
  {
    key: 'azure_openai',
    label: 'Azure OpenAI',
    type: 'azure_openai',
    icon: 'mdi-microsoft-azure',
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
    icon: 'mdi-lan',
    defaultBaseUrl: 'http://127.0.0.1:11434',
    supportsToken: false,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
  {
    key: 'dashscope',
    label: 'DashScope (Qwen)',
    type: 'dashscope',
    icon: 'mdi-cloud-outline',
    defaultBaseUrl: 'https://dashscope-intl.aliyuncs.com/compatible-mode/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'custom_openai',
    label: 'Custom OpenAI Compatible',
    type: 'custom_openai',
    icon: 'mdi-api',
    defaultBaseUrl: 'https://api.example.com/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
  {
    key: 'deepseek',
    label: 'DeepSeek',
    type: 'deepseek',
    icon: 'mdi-fish',
    defaultBaseUrl: 'https://api.deepseek.com',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'xiaomi_mimo',
    label: 'Xiaomi MiMo',
    type: 'xiaomi_mimo',
    icon: 'mdi-cellphone',
    defaultBaseUrl: 'https://api.xiaomimimo.com/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'xiaomi_mimo_token_plan',
    label: 'Xiaomi MiMo Token Plan',
    type: 'xiaomi_mimo',
    icon: 'mdi-cellphone',
    defaultBaseUrl: 'https://token-plan-cn.xiaomimimo.com/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'siliconflow',
    label: 'SiliconFlow',
    type: 'siliconflow',
    icon: 'mdi-atom',
    defaultBaseUrl: 'https://api.siliconflow.cn/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
]

export function providerTypeMetadataToSources(
  metadata: ProviderTypeMetadata,
): ProviderSourceTemplate[] {
  const fields = metadata.configFields || []
  const hasField = (location: 'auth' | 'default_params', key: string) =>
    fields.some((field) => field.location === location && field.key === key)
  const buildSource = (
    key: string,
    label: string,
    defaultBaseUrl: string,
  ): ProviderSourceTemplate => ({
    key,
    label,
    type: metadata.type,
    icon: metadata.icon,
    description: metadata.description,
    defaultBaseUrl,
    supportsToken: fields.some((field) => field.location === 'auth' && field.secret),
    supportsCatalog: metadata.supportsCatalog,
    supportsThinking: hasField('default_params', 'thinking'),
    supportsFilters: hasField('default_params', 'filters'),
    showApiVersion: hasField('default_params', 'apiVersion'),
  })

  if (metadata.presets.length > 0) {
    return metadata.presets.map((preset) =>
      buildSource(
        preset.key,
        preset.label || metadata.displayName || metadata.type,
        preset.defaultBaseUrl || metadata.defaultBaseUrl,
      ),
    )
  }

  return [
    buildSource(
      metadata.type,
      metadata.displayName || metadata.type,
      metadata.defaultBaseUrl,
    ),
  ]
}

export function buildProviderSourceCatalog(
  providerTypes: ProviderTypeMetadata[],
): ProviderSourceTemplate[] {
  if (providerTypes.length === 0) {
    return providerSourceTemplates
  }
  return providerTypes.flatMap(providerTypeMetadataToSources)
}

export function resolveProviderSource(
  type: string,
  providerTypes: ProviderTypeMetadata[] = [],
) {
  const catalog = buildProviderSourceCatalog(providerTypes)
  return catalog.find((item) => item.type === type || item.key === type) ?? null
}

export function resolveProviderSourceKey(
  type: string,
  baseUrl?: string,
  providerTypes: ProviderTypeMetadata[] = [],
): string {
  const catalog = buildProviderSourceCatalog(providerTypes)
  if (baseUrl) {
    const exact = catalog.find(
      (item) => item.type === type && item.defaultBaseUrl === baseUrl
    )
    if (exact) return exact.key
  }
  return catalog.find((item) => item.type === type)?.key ?? type
}

export function providerSourceIcon(type: string) {
  const staticMatch = providerSourceTemplates.find((item) => item.key === type || item.type === type)
  if (staticMatch?.icon) {
    return staticMatch.icon
  }
  if (type === 'azure_openai') {
    return 'mdi-microsoft-azure'
  }
  if (type === 'ollama') {
    return 'mdi-lan'
  }
  if (type === 'custom_openai') {
    return 'mdi-api'
  }
  if (type === 'anthropic') {
    return 'mdi-alpha-a-circle-outline'
  }
  if (type === 'gemini') {
    return 'mdi-google'
  }
  if (type === 'deepseek') {
    return 'mdi-fish'
  }
  if (type === 'xiaomi_mimo' || type === 'xiaomi_mimo_token_plan') {
    return 'mdi-cellphone'
  }
  if (type === 'siliconflow') {
    return 'mdi-atom'
  }
  return 'mdi-cloud-outline'
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
