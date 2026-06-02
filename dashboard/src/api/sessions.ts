import { apiClient } from './client'

export interface SessionConfig {
  prefixes: string[]
  llmEnabled: boolean
  isMuted: boolean
  auditEnabled: boolean
  updatedAt: number
}

export interface SessionListItem {
  id: string
  instanceId: string
  sessionType: string
  platform: string
  guildId: string | null
  channelId: string
  displayName: string
  permissionGroup: string
  createdAt: number
  lastActive: number
}

export interface SessionPlatformState {
  running: boolean
  connected: boolean
  available: boolean
}

export interface SessionMessage {
  id: number
  sessionId: string
  platformMsgId: string
  senderId: string
  senderName: string
  content: unknown[]
  rawText: string
  role: string
  isRead: boolean
  isMentioned: boolean
  createdAt: number
  routingStatus: string
  routedAt: number | null
  routingSkipReason: string | null
}

export interface SessionAuditLog {
  id: number
  timestamp: string
  entryType: string
  commandName: string
  pluginId: string
  userId: string
  sessionId: string
  instanceId: string
  permissionRequired: string
  permissionGranted: boolean
  executionTimeMs: number
  success: boolean
  error: string
  metadata: Record<string, unknown>
}

export interface SessionSummary {
  id: number
  sessionId: string
  summaryType: string
  startMsgLogId: number | null
  endMsgLogId: number | null
  messageCount: number
  summary: string
  reason: string
  createdAt: number
}

export interface SessionWorkflowRun {
  id: string
  sessionId: string
  instanceId: string
  responseProfile: string
  batchStartMsgId: number | null
  batchEndMsgId: number | null
  batchSize: number
  triggerAttention: number
  effectiveThreshold: number
  toolCalls: unknown[]
  replied: boolean
  responseSummary: string
  finishReason: string
  startedAt: number
  finishedAt: number | null
}

export interface SessionAgentReviewPlan {
  sessionId: string
  nextReviewAt: number
  reason: string
  mentionSensitivity: string
  activeReplyThreshold: {
    atCount: number
    windowSeconds: number
  }
  updatedAt: number
}

export interface SessionAgentActiveChatState {
  sessionId: string
  interestValue: number
  decayHalfLifeSeconds: number
  enteredAt: number
  updatedAt: number
  tickCount: number
  activeEpoch: number
  bootstrapApplied: boolean
  bootstrapDisposition: string | null
}

export interface SessionAgentState {
  state: string
  reviewPlan: SessionAgentReviewPlan | null
  activeChatState: SessionAgentActiveChatState | null
  unreadCount: number
  highPriorityCount: number
}

export interface SessionOverviewItem {
  session: SessionListItem
  platformState: SessionPlatformState
  config: SessionConfig | null
  history: SessionMessage[]
  latestMessage: SessionMessage | null
  latestAudit: SessionAuditLog | null
  latestReviewSummary: SessionSummary | null
  latestActiveChatSummary: SessionSummary | null
  latestOverflowSummary: SessionSummary | null
  latestWorkflowRun: SessionWorkflowRun | null
  agent: SessionAgentState | null
  messageCount: number
  auditCount: number
}

export interface SessionBatchActionResponse {
  action: string
  requestedCount: number
  processedCount: number
  processedSessionIds: string[]
  missingSessionIds: string[]
}

export const sessionsApi = {
  overview() {
    return apiClient.get<SessionOverviewItem[]>('/session-overview')
  },

  clearHistoryBatch(sessionIds: string[]) {
    return apiClient.post<SessionBatchActionResponse>('/session-overview/batch/history', {
      sessionIds,
    })
  },

  clearAuditLogsBatch(sessionIds: string[]) {
    return apiClient.post<SessionBatchActionResponse>('/session-overview/batch/audit-logs', {
      sessionIds,
    })
  },

  deleteBatch(sessionIds: string[]) {
    return apiClient.post<SessionBatchActionResponse>('/session-overview/batch/delete', {
      sessionIds,
    })
  },

  clearHistory(sessionId: string) {
    return apiClient.delete<{ cleared: boolean; scope: string; sessionId: string }>(
      `/session-overview/${encodeURIComponent(sessionId)}/history`
    )
  },

  clearAuditLogs(sessionId: string) {
    return apiClient.delete<{ cleared: boolean; scope: string; sessionId: string }>(
      `/session-overview/${encodeURIComponent(sessionId)}/audit-logs`
    )
  },

  delete(sessionId: string) {
    return apiClient.delete<{ deleted: boolean; sessionId: string }>(
      `/session-overview/${encodeURIComponent(sessionId)}`
    )
  },
}
