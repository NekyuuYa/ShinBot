export type BotConfigTargetKey =
  | 'mainLlm'
  | 'mediaInspectionLlm'
  | 'mediaInspectionPrompt'
  | 'stickerSummaryLlm'
  | 'stickerSummaryPrompt'
  | 'contextCompressionLlm'

export interface BotConfigTargetField {
  key: BotConfigTargetKey
  labelKey: string
  pickerType: 'model' | 'prompt'
}

export interface TargetSummary {
  title: string
  subtitle: string
  icon: string
  color: string
}

export interface InstanceFormState {
  name: string
  adapterType: string
  config: Record<string, unknown>
  botConfig: {
    uuid: string
    defaultAgentUuid: string
    mainLlm: string
    explicitPromptCacheEnabled: boolean
    mediaInspectionLlm: string
    mediaInspectionPrompt: string
    stickerSummaryLlm: string
    stickerSummaryPrompt: string
    contextCompressionLlm: string
    maxContextTokens: string
    contextEvictRatio: string
    contextCompressionMaxChars: string
    tags: string[]
  }
}

export interface KeyValueEntry {
  key: string
  value: string
}
