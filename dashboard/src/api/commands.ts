import { apiClient } from './client'

export type CommandMode = 'delegated' | 'managed'

export interface CommandDefinition {
  name: string
  aliases: string[]
  triggers: string[]
  description: string
  usage: string
  defaultPermission: string
  permission: string
  permissionOverridden: boolean
  mode: CommandMode
  priority: number
  priorityLabel: string
  pattern: string
  owner: string
  enabled: boolean
}

export const commandsApi = {
  list() {
    return apiClient.get<CommandDefinition[]>('/commands')
  },

  update(name: string, payload: Pick<CommandDefinition, 'enabled'>) {
    return apiClient.patch<CommandDefinition>(`/commands/${encodeURIComponent(name)}`, payload)
  },
}
