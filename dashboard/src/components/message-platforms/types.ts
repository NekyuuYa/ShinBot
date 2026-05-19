import type { ConfigRecord } from '@/api/config'

export interface MessagePlatformFormState {
  id: string
  name: string
  adapter: string
  enabled: boolean
  config: ConfigRecord
}

export interface MessagePlatformDraft extends MessagePlatformFormState {
  createdAt?: number
  lastModified?: number
  running?: boolean
}

export interface MessagePlatformAdapterOption {
  title: string
  value: string
  props?: {
    subtitle?: string
  }
}
