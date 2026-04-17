import { apiClient } from './client'

export interface ContextStrategy {
  uuid: string
  name: string
  type: string
  resolverRef: string
  description: string
  config: Record<string, unknown>
  enabled: boolean
  createdAt: string
  lastModified: string
}

export const contextStrategiesApi = {
  list() {
    return apiClient.get<ContextStrategy[]>('/context-strategies')
  },
}
