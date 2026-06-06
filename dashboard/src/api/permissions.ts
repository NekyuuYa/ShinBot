import { apiClient } from './client'

export interface PermissionGroup {
  id: string
  name: string
  description: string
  permissions: string[]
  orphanPermissions: string[]
  builtin: boolean
  system: boolean
  protected: boolean
}

export interface PermissionBinding {
  scopeKey: string
  groups: string[]
  groupIds?: string[]
}

export interface PermissionGroupPayload {
  name?: string
  description?: string
  permissions?: string[]
  protected?: boolean
}

export interface CreatePermissionGroupPayload extends PermissionGroupPayload {
  id: string
}

export interface SetPermissionBindingPayload {
  groups: string[]
}

export interface CommandPermissionPayload {
  permission: string
}

export interface PermissionBindingFilters {
  scopeKey?: string
  groupId?: string
}

export const permissionsApi = {
  listGroups() {
    return apiClient.get<PermissionGroup[]>('/permissions/groups')
  },

  createGroup(payload: CreatePermissionGroupPayload) {
    return apiClient.post<PermissionGroup>('/permissions/groups', payload)
  },

  getGroup(groupId: string) {
    return apiClient.get<PermissionGroup>(`/permissions/groups/${encodeURIComponent(groupId)}`)
  },

  updateGroup(groupId: string, payload: PermissionGroupPayload) {
    return apiClient.patch<PermissionGroup>(
      `/permissions/groups/${encodeURIComponent(groupId)}`,
      payload
    )
  },

  deleteGroup(groupId: string) {
    return apiClient.delete<unknown>(`/permissions/groups/${encodeURIComponent(groupId)}`)
  },

  listBindings(filters: PermissionBindingFilters = {}) {
    return apiClient.get<PermissionBinding[]>('/permissions/bindings', {
      params: {
        scopeKey: filters.scopeKey || undefined,
        groupId: filters.groupId || undefined,
      },
    })
  },

  setBinding(scopeKey: string, payload: SetPermissionBindingPayload) {
    return apiClient.put<PermissionBinding>(
      `/permissions/bindings/${encodeURIComponent(scopeKey)}`,
      payload
    )
  },

  deleteBinding(scopeKey: string, groupId?: string) {
    return apiClient.delete<unknown>(`/permissions/bindings/${encodeURIComponent(scopeKey)}`, {
      params: { groupId: groupId || undefined },
    })
  },

  updateCommandPermission(commandName: string, payload: CommandPermissionPayload) {
    return apiClient.patch<unknown>(
      `/commands/${encodeURIComponent(commandName)}/permission`,
      payload
    )
  },

  resetCommandPermission(commandName: string) {
    return apiClient.delete<unknown>(`/commands/${encodeURIComponent(commandName)}/permission`)
  },
}
