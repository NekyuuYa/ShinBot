import { apiClient } from './client'

export interface Agent {
  uuid: string
  agentId: string
  name: string
  personaUuid: string
  prompts: string[]
  tools: string[]
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
  config: Record<string, unknown>
  tags: string[]
}

export interface AgentRuntimeSession {
  sessionId: string
  state: string
  reviewPlan: {
    sessionId: string
    nextReviewAt: number
    reason: string
    mentionSensitivity: string
    activeReplyThreshold: {
      atCount: number
      windowSeconds: number
    }
    updatedAt: number
  } | null
  activeChatState: {
    sessionId: string
    interestValue: number
    decayHalfLifeSeconds: number
    enteredAt: number
    updatedAt: number
    tickCount: number
    activeEpoch: number
    bootstrapApplied: boolean
    bootstrapDisposition: string | null
  } | null
  unreadCount: number
  highPriorityCount: number
  latestReviewRun: {
    id: string
    sessionId: string
    startedAt: number
    finishedAt: number | null
    batchSize: number
    replied: boolean
    responseSummary: string
    finishReason: string
  } | null
  latestReviewSummary: {
    id: number
    sessionId: string
    startMsgLogId: number | null
    endMsgLogId: number | null
    messageCount: number
    summary: string
    reason: string
    createdAt: number
  } | null
  latestAudit: {
    id: number
    timestamp: string
    entry_type: string
    command_name: string
    plugin_id: string
    user_id: string
    session_id: string
    instance_id: string
    permission_required: string
    permission_granted: boolean
    execution_time_ms: number
    success: boolean
    error: string
    metadata: Record<string, unknown>
  } | null
}

export interface AgentRuntimeProfile {
  botId: string
  botName: string
  enabled: boolean
  agentMode: string
  agentConfig: string
  bindings: {
    adapterInstanceId: string
    sessionPatterns: string[]
    enabled: boolean
    priority: number
  }[]
  sessions: AgentRuntimeSession[]
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

  runtimeOverview(config?: { suppressErrorNotify?: boolean }) {
    return apiClient.get<AgentRuntimeProfile[]>('/agent-runtime', config)
  },
}
