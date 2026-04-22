import { apiClient } from './client'

export interface PromptCatalogItem {
  id: string
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
  metadata: Record<string, unknown>
}

export const promptsApi = {
  list() {
    return apiClient.get<PromptCatalogItem[]>('/prompts')
  },
}
