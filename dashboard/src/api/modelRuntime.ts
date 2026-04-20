import { apiClient } from './client'

const encodePathSegment = (value: string) => encodeURIComponent(value)

export interface ModelRuntimeProvider {
  id: string
  type: string
  displayName: string
  capabilityType: string
  baseUrl: string
  hasAuth: boolean
  defaultParams: Record<string, unknown>
  enabled: boolean
  createdAt: string
  lastModified: string
}

export interface ModelRuntimeModel {
  id: string
  providerId: string
  litellmModel: string
  displayName: string
  capabilities: string[]
  contextWindow: number | null
  defaultParams: Record<string, unknown>
  costMetadata: Record<string, unknown>
  enabled: boolean
  createdAt: string
  lastModified: string
}

export interface ModelRuntimeRouteMember {
  modelId: string
  priority: number
  weight: number
  conditions: Record<string, unknown>
  timeoutOverride: number | null
  enabled: boolean
}

export interface ModelRuntimeRoute {
  id: string
  purpose: string
  strategy: string
  enabled: boolean
  stickySessions: boolean
  metadata: Record<string, unknown>
  members: ModelRuntimeRouteMember[]
  createdAt: string
  lastModified: string
}

export interface ModelExecutionRecord {
  id: string
  routeId: string
  providerId: string
  modelId: string
  caller: string
  sessionId: string
  instanceId: string
  purpose: string
  startedAt: string
  firstTokenAt: string | null
  finishedAt: string | null
  latencyMs: number
  timeToFirstTokenMs: number | null
  inputTokens: number
  outputTokens: number
  cacheHit: boolean
  cacheReadTokens: number
  cacheWriteTokens: number
  success: boolean
  errorCode: string
  errorMessage: string
  fallbackFromModelId: string
  fallbackReason: string
  estimatedCost: number | null
  currency: string
  metadata: Record<string, unknown>
}

export interface ModelTokenSummaryModel {
  providerId: string
  modelId: string
  totalCalls: number
  inputTokens: number
  outputTokens: number
  totalTokens: number
  cacheReadTokens: number
  cacheWriteTokens: number
}

export interface ModelTokenSummary {
  windowDays: number
  since: string
  totalCalls: number
  successfulCalls: number
  inputTokens: number
  outputTokens: number
  totalTokens: number
  cacheReadTokens: number
  cacheWriteTokens: number
  estimatedCost: number
  currency: string
  topModels: ModelTokenSummaryModel[]
}

export interface ProviderCatalogItem {
  id: string
  displayName: string
  litellmModel: string
  contextWindow: number | null
}

export interface ProviderProbeResult {
  success: boolean
  providerId: string
  modelId?: string
  mode: string
  checkedAt: string
  executionId?: string
  catalogSize?: number
}

export interface ProviderPayload {
  id: string
  type: string
  displayName: string
  capabilityType: string
  baseUrl: string
  auth?: Record<string, unknown>
  defaultParams: Record<string, unknown>
  enabled: boolean
}

export interface ModelPayload {
  id: string
  providerId: string
  litellmModel: string
  displayName: string
  capabilities: string[]
  contextWindow: number | null
  defaultParams: Record<string, unknown>
  costMetadata: Record<string, unknown>
  enabled: boolean
}

export interface RoutePayload {
  id: string
  purpose: string
  strategy: string
  enabled: boolean
  stickySessions: boolean
  metadata: Record<string, unknown>
  members: ModelRuntimeRouteMember[]
}

export const modelRuntimeApi = {
  listProviders() {
    return apiClient.get<ModelRuntimeProvider[]>('/model-runtime/providers')
  },

  createProvider(data: ProviderPayload) {
    return apiClient.post<ModelRuntimeProvider>('/model-runtime/providers', data)
  },

  updateProvider(id: string, data: Partial<ProviderPayload>) {
    return apiClient.patch<ModelRuntimeProvider>(
      `/model-runtime/providers/${encodePathSegment(id)}`,
      data
    )
  },

  deleteProvider(id: string) {
    return apiClient.delete<void>(`/model-runtime/providers/${encodePathSegment(id)}`)
  },

  fetchProviderCatalog(id: string) {
    return apiClient.get<ProviderCatalogItem[]>(
      `/model-runtime/providers/${encodePathSegment(id)}/catalog`
    )
  },

  probeProvider(id: string, modelId?: string) {
    return apiClient.post<ProviderProbeResult>(
      `/model-runtime/providers/${encodePathSegment(id)}/probe`,
      {
      modelId,
      }
    )
  },

  listModels(providerId?: string) {
    return apiClient.get<ModelRuntimeModel[]>('/model-runtime/models', {
      params: providerId ? { providerId } : undefined,
    })
  },

  createModel(data: ModelPayload) {
    return apiClient.post<ModelRuntimeModel>('/model-runtime/models', data)
  },

  updateModel(id: string, data: Partial<ModelPayload>) {
    return apiClient.patch<ModelRuntimeModel>(
      `/model-runtime/models/${encodePathSegment(id)}`,
      data
    )
  },

  deleteModel(id: string) {
    return apiClient.delete<void>(`/model-runtime/models/${encodePathSegment(id)}`)
  },

  listRoutes() {
    return apiClient.get<ModelRuntimeRoute[]>('/model-runtime/routes')
  },

  createRoute(data: RoutePayload) {
    return apiClient.post<ModelRuntimeRoute>('/model-runtime/routes', data)
  },

  updateRoute(id: string, data: Partial<RoutePayload>) {
    return apiClient.patch<ModelRuntimeRoute>(
      `/model-runtime/routes/${encodePathSegment(id)}`,
      data
    )
  },

  deleteRoute(id: string) {
    return apiClient.delete<void>(`/model-runtime/routes/${encodePathSegment(id)}`)
  },

  listExecutions(limit = 50) {
    return apiClient.get<ModelExecutionRecord[]>('/model-runtime/executions', {
      params: { limit },
    })
  },

  getTokenSummary(days = 7) {
    return apiClient.get<ModelTokenSummary>('/model-runtime/token-summary', {
      params: { days },
    })
  },
}
