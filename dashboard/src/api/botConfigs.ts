import { apiClient } from './client'

export interface BotConfig {
  uuid: string
  instanceId: string
  defaultAgentUuid: string
  mainLlm: string
  explicitPromptCacheEnabled: boolean
  mediaInspectionLlm: string
  mediaInspectionPrompt: string
  stickerSummaryLlm: string
  stickerSummaryPrompt: string
  contextCompressionLlm: string
  maxContextTokens: number | null
  contextEvictRatio: number | null
  contextCompressionMaxChars: number | null
  config: Record<string, unknown>
  tags: string[]
  createdAt: string
  lastModified: string
}

export interface BotConfigSummary {
  uuid: string
  defaultAgentUuid: string
  mainLlm: string
  explicitPromptCacheEnabled: boolean
  mediaInspectionLlm: string
  mediaInspectionPrompt: string
  stickerSummaryLlm: string
  stickerSummaryPrompt: string
  contextCompressionLlm: string
  maxContextTokens: number | null
  contextEvictRatio: number | null
  contextCompressionMaxChars: number | null
  tags: string[]
}

export interface CreateBotConfigRequest {
  instanceId: string
  defaultAgentUuid?: string
  mainLlm?: string
  explicitPromptCacheEnabled?: boolean | null
  mediaInspectionLlm?: string | null
  mediaInspectionPrompt?: string | null
  stickerSummaryLlm?: string | null
  stickerSummaryPrompt?: string | null
  contextCompressionLlm?: string | null
  maxContextTokens?: number | null
  contextEvictRatio?: number | null
  contextCompressionMaxChars?: number | null
  config?: Record<string, unknown>
  tags?: string[]
}

export interface UpdateBotConfigRequest {
  instanceId?: string
  defaultAgentUuid?: string
  mainLlm?: string
  explicitPromptCacheEnabled?: boolean | null
  mediaInspectionLlm?: string | null
  mediaInspectionPrompt?: string | null
  stickerSummaryLlm?: string | null
  stickerSummaryPrompt?: string | null
  contextCompressionLlm?: string | null
  maxContextTokens?: number | null
  contextEvictRatio?: number | null
  contextCompressionMaxChars?: number | null
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
