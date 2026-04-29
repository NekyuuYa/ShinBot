import type { ModelRuntimeRouteMember } from '@/api/modelRuntime'
import type { ModelRuntimeTab } from '@/utils/modelRuntimeSources'

export type RuntimeSelectionKind = 'provider' | 'route'

export interface RuntimeTabOption {
  value: ModelRuntimeTab
  label: string
  icon: string
}

export interface RuntimeDomainOption {
  label: string
  value: string
}

export interface RuntimeSidebarItem {
  id: string
  title: string
  subtitle?: string
  icon: string
  badge?: string | number
  badgeColor?: string
}

export interface KeyValueEntry {
  key: string
  value: string
}

export interface ProviderFormState {
  id: string
  displayName: string
  sourceType: string
  baseUrl: string
  token: string
  enabled: boolean
  proxyAddress: string
  thinkingJson: string
  filtersJson: string
  apiVersion: string
}

export interface RouteFormState {
  id: string
  purpose: string
  strategy: string
  enabled: boolean
  stickySessions: boolean
  domain: string
}

export type RouteMemberDraft = ModelRuntimeRouteMember

export interface ModelFormState {
  id: string
  displayName: string
  litellmModel: string
  capabilities: string[]
  contextWindow: number | null
  inputPrice: string
  outputPrice: string
  cacheWritePrice: string
  cacheReadPrice: string
  enabled: boolean
}
