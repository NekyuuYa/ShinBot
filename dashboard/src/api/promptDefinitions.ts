import { apiClient } from './client'

export interface PromptDefinitionSource {
  sourceType: string
  sourceId: string
  ownerPluginId: string
  ownerModule: string
  modulePath: string
}

export interface PromptDefinition {
  uuid: string
  promptId: string
  name: string
  source: PromptDefinitionSource
  stage: string
  type: string
  priority: number
  version: string
  description: string
  enabled: boolean
  content: string
  templateVars: string[]
  resolverRef: string
  bundleRefs: string[]
  config: Record<string, unknown>
  tags: string[]
  metadata: Record<string, unknown>
  createdAt: string
  lastModified: string
}

export interface PromptDefinitionPayload {
  promptId: string
  name: string
  sourceType?: string
  sourceId?: string
  ownerPluginId?: string
  ownerModule?: string
  modulePath?: string
  stage: string
  type: string
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

export const promptDefinitionsApi = {
  list() {
    return apiClient.get<PromptDefinition[]>('/prompt-definitions')
  },
  get(uuid: string) {
    return apiClient.get<PromptDefinition>(`/prompt-definitions/${uuid}`)
  },
  create(payload: PromptDefinitionPayload) {
    return apiClient.post<PromptDefinition>('/prompt-definitions', payload)
  },
  update(uuid: string, payload: Partial<PromptDefinitionPayload>) {
    return apiClient.patch<PromptDefinition>(`/prompt-definitions/${uuid}`, payload)
  },
  delete(uuid: string) {
    return apiClient.delete<{ deleted: boolean; uuid: string }>(`/prompt-definitions/${uuid}`)
  },
}
