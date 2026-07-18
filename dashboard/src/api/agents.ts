import { apiClient } from './client'

export interface AgentRuntimePlatformState {
  running: boolean
  connected: boolean
  available: boolean
}

export interface AgentRuntimeIdleReviewPlanningModelResult {
  auditId: number
  recordedAt: string
  outcome: string
  reason: string
  failureCode: string
  modelExecutionId: string
  promptSignature: string
  requestedNextReviewAfterSeconds: number | null
  appliedNextReviewAfterSeconds: number | null
  proposedNextReviewAt: number | null
  proposedPlanReason: string
}

export interface AgentRuntimeIdleReviewPlanningApplication {
  auditId: number
  recordedAt: string
  outcome: string
  reason: string
  modelPlanSupplied: boolean
  modelPlanReason: string
  modelPlanNextReviewAt: number | null
  decisionSkippedReason: string
  appliedPlanReason: string
  appliedNextReviewAt: number | null
  schedulerState: string
}

export interface AgentRuntimeIdleReviewPlanningDecision {
  signalId: string
  trigger: string
  activeEpoch: number
  checkedAt: number
  latestAt: string
  modelResult: AgentRuntimeIdleReviewPlanningModelResult | null
  application: AgentRuntimeIdleReviewPlanningApplication | null
}

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
  adapterInstanceId: string
  platformState: AgentRuntimePlatformState
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
    entryType: string
    commandName: string
    pluginId: string
    success: boolean
  } | null
  idleReviewPlanningDecisions: AgentRuntimeIdleReviewPlanningDecision[]
}

export interface AgentRuntimeProfile {
  profileId: string
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
    platformState: AgentRuntimePlatformState
  }[]
  sessions: AgentRuntimeSession[]
}

export interface ManualActionResponse {
  sessionId: string
  success: boolean
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

  triggerReview(sessionId: string) {
    return apiClient.post<ManualActionResponse>(
      `/agent-runtime/sessions/${encodeURIComponent(sessionId)}/trigger-review`
    )
  },

  forceIdle(sessionId: string) {
    return apiClient.post<ManualActionResponse>(
      `/agent-runtime/sessions/${encodeURIComponent(sessionId)}/force-idle`
    )
  },
}
