import type {
  NormalizedBotAgentConfig,
  NormalizedBotBindingConfig,
  NormalizedBotCommandsConfig,
  NormalizedBotPluginsConfig,
} from '@/api/config'
import type { AgentRuntimePlatformState } from '@/api/agents'

export interface BotBindingRuntimeSummary {
  adapterInstanceId: string
  platformState: AgentRuntimePlatformState
}

export interface BotInstanceFormState {
  id: string
  display_name: string
  enabled: boolean
  administrators: string[]
  commands: NormalizedBotCommandsConfig
  plugins: NormalizedBotPluginsConfig
  agent: NormalizedBotAgentConfig
  bindings: NormalizedBotBindingConfig[]
}

export type BotInstanceDraft = BotInstanceFormState & {
  platformBindings?: BotBindingRuntimeSummary[]
  platformStatusSummary?: string
}

export interface SelectOption {
  title: string
  value: string
  props?: {
    subtitle?: string
  }
}
