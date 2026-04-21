import { apiClient } from './client'

export interface BotConfig {
  uuid: string
  instanceId: string
  defaultAgentUuid: string
  mainLlm: string
  mediaInspectionLlm: string
  config: Record<string, unknown>
  tags: string[]
  createdAt: string
  lastModified: string
}

export interface BotConfigSummary {
  uuid: string
  defaultAgentUuid: string
  mainLlm: string
  mediaInspectionLlm: string
  tags: string[]
}

export interface CreateBotConfigRequest {
  instanceId: string
  defaultAgentUuid?: string
  mainLlm?: string
  mediaInspectionLlm?: string
  config?: Record<string, unknown>
  tags?: string[]
}

export interface UpdateBotConfigRequest {
  instanceId?: string
  defaultAgentUuid?: string
  mainLlm?: string
  mediaInspectionLlm?: string
  config?: Record<string, unknown>
  tags?: string[]
}

export const botConfigsApi = {
  list() {
    return apiClient.get<BotConfig[]>('/bot-configs')
  },

  create(data: CreateBotConfigRequest) {
    return apiClient.post<BotConfig>('/bot-configs', data)
  },

  update(id: string, data: UpdateBotConfigRequest) {
    return apiClient.patch<BotConfig>(`/bot-configs/${id}`, data)
  },
}
