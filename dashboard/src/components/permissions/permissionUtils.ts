import type { CommandDefinition } from '@/api/commands'
import type { PermissionBinding, PermissionGroup, PermissionGroupPayload } from '@/api/permissions'

export interface PermissionCommand extends CommandDefinition {
  defaultPermission: string
  permissionOverridden: boolean
}

export interface PermissionGroupForm {
  name: string
  description: string
  permissions: string[]
  deniedPermissions: string[]
  protected: boolean
}

export interface PermissionSelectItem {
  title: string
  value: string
}

export function createEmptyPermissionGroupForm(): PermissionGroupForm {
  return {
    name: '',
    description: '',
    permissions: [],
    deniedPermissions: [],
    protected: false,
  }
}

export function formFromGroup(group: PermissionGroup): PermissionGroupForm {
  return {
    name: group.name,
    description: group.description,
    permissions: group.permissions.filter((permission) => !permission.startsWith('-')).sort(),
    deniedPermissions: group.permissions
      .filter((permission) => permission.startsWith('-'))
      .map(stripDenyPrefix)
      .sort(),
    protected: group.protected,
  }
}

export function groupPayloadFromForm(form: PermissionGroupForm): PermissionGroupPayload {
  return {
    name: form.name,
    description: form.description,
    protected: form.protected,
    permissions: [
      ...normalizePermissionList(form.permissions),
      ...normalizePermissionList(form.deniedPermissions).map((permission) => `-${permission}`),
    ],
  }
}

export function normalizePermissionList(values: string[]): string[] {
  return Array.from(new Set(values.map(stripDenyPrefix).map((value) => value.trim()).filter(Boolean))).sort()
}

export function stripDenyPrefix(value: string): string {
  return String(value).trim().replace(/^-+/, '')
}

export function normalizeGroup(group: PermissionGroup): PermissionGroup {
  const builtin = Boolean(group.builtin || group.system)
  return {
    id: String(group.id),
    name: group.name || '',
    description: group.description || '',
    permissions: Array.isArray(group.permissions) ? [...group.permissions].sort() : [],
    orphanPermissions: Array.isArray(group.orphanPermissions)
      ? [...group.orphanPermissions].sort()
      : [],
    builtin,
    system: builtin,
    protected: Boolean(group.protected),
  }
}

export function normalizeBinding(binding: PermissionBinding & { key?: string; groupIds?: string[] }): PermissionBinding {
  const groups = Array.isArray(binding.groups)
    ? [...binding.groups].sort()
    : Array.isArray(binding.groupIds)
      ? [...binding.groupIds].sort()
      : []
  return {
    scopeKey: String(binding.scopeKey ?? binding.key ?? ''),
    groups,
    groupIds: groups,
  }
}
