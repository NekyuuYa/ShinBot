import { apiClient } from './client'

export interface AgentContextStrategy {
  ref: string
  type: string
  params: Record<string, unknown>
}

export interface Agent {
  uuid: string
  agentId: string
  name: string
  personaUuid: string
  prompts: string[]
  tools: string[]
  contextStrategy: AgentContextStrategy
  config: Record<string, unknown>
  tags: string[]
  createdAt: string
  lastModified: string
}

export type AgentSummary = Agent

export interface AgentPayload {
  agentId: string
  name: string
  personaUuid: string
  prompts: string[]
  tools: string[]
  contextStrategy: AgentContextStrategy
  config: Record<string, unknown>
  tags: string[]
}

export const agentsApi = {
  list() {
    return apiClient.get<Agent[]>('/agents')
  },

  get(uuid: string) {
    return apiClient.get<Agent>(`/agents/${encodeURIComponent(uuid)}`)
  },

  create(payload: AgentPayload) {
    return apiClient.post<Agent>('/agents', payload)
  },

  update(uuid: string, payload: Partial<AgentPayload>) {
    return apiClient.patch<Agent>(`/agents/${encodeURIComponent(uuid)}`, payload)
  },

  delete(uuid: string) {
    return apiClient.delete<{ deleted: boolean; uuid: string }>(
      `/agents/${encodeURIComponent(uuid)}`
    )
  },
}
