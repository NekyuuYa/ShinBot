import { apiClient } from './client'

export type ToolRiskLevel = 'low' | 'medium' | 'high'
export type ToolVisibility = 'private' | 'scoped' | 'public'
export type ToolOwnerType =
  | 'builtin_module'
  | 'plugin'
  | 'adapter_bridge'
  | 'skill_module'
  | 'external_bridge'

export interface ToolDefinition {
  id: string
  name: string
  displayName: string
  description: string
  inputSchema: Record<string, unknown>
  outputSchema: Record<string, unknown> | null
  ownerType: ToolOwnerType
  ownerId: string
  ownerModule: string
  permission: string
  enabled: boolean
  visibility: ToolVisibility
  timeoutSeconds: number
  riskLevel: ToolRiskLevel
  tags: string[]
  metadata: Record<string, unknown>
}

export const toolsApi = {
  list() {
    return apiClient.get<ToolDefinition[]>('/tools')
  },
}
