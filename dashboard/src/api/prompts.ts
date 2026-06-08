import { apiClient } from './client'

export interface PromptCatalogItem {
  id: string
  fileId: string
  layer: 'runtime' | 'custom' | string
  locale: string
  displayName: string
  description: string
  stage: string
  type: string
  version: string
  priority: number
  enabled: boolean
  resolverRef: string
  templateVars: string[]
  bundleRefs: string[]
  tags: string[]
  sourceType: string
  sourceId: string
  ownerPluginId: string
  ownerModule: string
  modulePath: string
  editable: boolean
  deletable: boolean
  resettable: boolean
  sourceStatus: string
  loadedFrom: string
  sourcePath: string
  runtimePath: string
  loadedPath: string
  metadata: Record<string, unknown>
}

export interface PromptFile extends PromptCatalogItem {
  promptId: string
  name: string
  content: string
  config: Record<string, unknown>
  createdAt: string
  lastModified: string
}

export interface PromptFilePayload {
  promptId?: string
  name?: string
  stage?: string
  type?: string
  priority?: number
  version?: string
  description?: string
  enabled?: boolean
  content?: string
  templateVars?: string[]
  resolverRef?: string
  bundleRefs?: string[]
  config?: Record<string, unknown>
  tags?: string[]
  metadata?: Record<string, unknown>
}

export interface CustomPromptCreatePayload extends PromptFilePayload {
  promptId: string
  name: string
  stage: string
  type: string
  sourceType?: string
  sourceId?: string
  ownerPluginId?: string
  ownerModule?: string
  modulePath?: string
}

const filePath = (fileId: string) => `/prompts/${encodeURIComponent(fileId)}`

export const promptsApi = {
  list() {
    return apiClient.get<PromptCatalogItem[]>('/prompts')
  },
  get(fileId: string) {
    return apiClient.get<PromptFile>(filePath(fileId))
  },
  create(payload: CustomPromptCreatePayload) {
    return apiClient.post<PromptFile>('/prompts/custom', payload)
  },
  update(fileId: string, payload: PromptFilePayload) {
    return apiClient.patch<PromptFile>(filePath(fileId), payload)
  },
  delete(fileId: string) {
    return apiClient.delete<{ deleted: boolean; fileId: string }>(filePath(fileId))
  },
  reset(fileId: string) {
    return apiClient.post<{ reset: boolean; fileId: string }>(`${filePath(fileId)}/reset`)
  },
}
