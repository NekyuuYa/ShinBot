import type {
  NormalizedBotAgentConfig,
  NormalizedBotBindingConfig,
  NormalizedBotCommandsConfig,
  NormalizedBotPluginsConfig,
} from '@/api/config'

export interface BotInstanceFormState {
  id: string
  display_name: string
  enabled: boolean
  commands: NormalizedBotCommandsConfig
  plugins: NormalizedBotPluginsConfig
  agent: NormalizedBotAgentConfig
  bindings: NormalizedBotBindingConfig[]
}

export type BotInstanceDraft = BotInstanceFormState

export interface SelectOption {
  title: string
  value: string
  props?: {
    subtitle?: string
  }
}
