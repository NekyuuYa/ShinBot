import { apiClient } from './client'

export interface AgentSummary {
  uuid: string
  agentId: string
  name: string
  personaUuid: string
  tags: string[]
}

export const agentsApi = {
  list() {
    return apiClient.get<AgentSummary[]>('/agents')
  },
}
