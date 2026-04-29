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
    supportsCatalog: true,
    supportsThinking: true,
    supportsFilters: false,
  },
  {
    key: 'gemini',
    label: 'Gemini',
    type: 'gemini',
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
    key: 'dashscope',
    label: 'DashScope (Qwen)',
    type: 'dashscope',
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
    defaultBaseUrl: 'https://api.siliconflow.cn/v1',
    supportsToken: true,
    supportsCatalog: true,
    supportsThinking: false,
    supportsFilters: false,
  },
]

export function resolveProviderSource(type: string) {
  return providerSourceTemplates.find((item) => item.type === type || item.key === type) ?? null
}

export function resolveProviderSourceKey(type: string, baseUrl?: string): string {
  if (baseUrl) {
    const exact = providerSourceTemplates.find(
      (item) => item.type === type && item.defaultBaseUrl === baseUrl
    )
    if (exact) return exact.key
  }
  return providerSourceTemplates.find((item) => item.type === type)?.key ?? type
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
