import axios from 'axios'

import { apiClient, type ApiRequestConfig, type ApiResponse } from './client'

export type ConfigValue =
  | string
  | number
  | boolean
  | null
  | ConfigValue[]
  | { [key: string]: ConfigValue }

export type ConfigDocument = Record<string, ConfigValue>
export type ConfigRecord = Record<string, ConfigValue>

export type ConfigProviderKind =
  | 'adapter'
  | 'plugin'
  | 'agent'
  | 'model_provider'
  | 'tool_provider'
  | 'memory_provider'

export type ConfigFieldType =
  | 'string'
  | 'integer'
  | 'float'
  | 'boolean'
  | 'enum'
  | 'string_list'
  | 'integer_list'
  | 'object'
  | 'array_object'
  | 'path'
  | 'duration'

export interface ConfigFieldDefinition {
  path: string
  type: ConfigFieldType
  required: boolean
  choices: ConfigValue[]
  secret: boolean
  env: string
  placeholder: string
  description: string
  visible_when: string
  advanced: boolean
  deprecated: boolean
  metadata: Record<string, ConfigValue>
  default?: ConfigValue
  min?: number
  max?: number
}

export interface ConfigProviderDefinition {
  kind: ConfigProviderKind
  id: string
  display_name: string
  description: string
  config_version: string
  fields: ConfigFieldDefinition[]
  example_toml: string
  owner_module: string
  source_path: string
  metadata: Record<string, ConfigValue>
}

export interface ConfigWorkspaceProvider extends ConfigProviderDefinition {
  defaults: ConfigRecord
  schemaRef: string
  defaultsRef: string
  validateRef: string
}

export interface ConfigValidationIssue {
  path: string
  message: string
  code: string
  source?: 'boot' | 'provider' | string
}

export interface NormalizedAdapterInstanceConfig {
  id: string
  name: string
  adapter: string
  enabled: boolean
  config: ConfigRecord
  createdAt: number
  lastModified: number
}

export interface NormalizedBotCommandsConfig {
  enabled: boolean
  prefixes: string[]
}

export interface NormalizedBotPluginsConfig {
  enabled: boolean
  enabled_plugins: string[]
  disabled_plugins: string[]
}

export interface NormalizedBotAgentConfig {
  mode: string
  config: string
}

export interface NormalizedBotBindingConfig {
  id: string
  adapter_instance_id: string
  session_patterns: string[]
  enabled: boolean
  priority: number
}

export interface NormalizedBotServiceConfig {
  id: string
  display_name: string
  enabled: boolean
  commands: NormalizedBotCommandsConfig
  plugins: NormalizedBotPluginsConfig
  agent: NormalizedBotAgentConfig
  bindings: NormalizedBotBindingConfig[]
}

export interface ConfigValidationResult {
  valid: boolean
  issues: ConfigValidationIssue[]
  normalized: {
    adapterInstances: NormalizedAdapterInstanceConfig[]
    bots: NormalizedBotServiceConfig[]
  }
}

export interface ConfigWorkspaceRuntime {
  modelMounted: boolean
  modelEnabled: boolean
  agentMounted: boolean
  adapterInstances: Array<{
    id: string
    running: boolean
  }>
  requiresRestartAfterSave: boolean
}

export interface ConfigWorkspaceProviders {
  adapters: ConfigWorkspaceProvider[]
  plugins: ConfigWorkspaceProvider[]
  agents: ConfigWorkspaceProvider[]
}

export interface ConfigWorkspaceOptions {
  agentModes: string[]
  adapterPlatforms: string[]
  pluginIds: string[]
}

export interface ConfigRuntimeTemplate {
  agent: boolean
}

export interface ConfigLoggingTemplate {
  level: string
  third_party_noise: string
  file: {
    enabled: boolean
    path: string
    when: string
    interval: number
    backup_count: number
  }
}

export interface ConfigDatabaseTemplate {
  url: string
  snapshot_ttl: number
}

export interface ConfigAdapterInstanceTemplate {
  id: string
  name: string
  adapter: string
  enabled: boolean
  config: ConfigRecord
}

export interface ConfigPluginTemplate {
  id: string
  module: string
  enabled: boolean
  config: ConfigRecord
}

export interface ConfigBotTemplate {
  id: string
  display_name: string
  enabled: boolean
  commands: NormalizedBotCommandsConfig
  plugins: NormalizedBotPluginsConfig
  agent: NormalizedBotAgentConfig
  bindings: NormalizedBotBindingConfig[]
}

export interface ConfigBotBindingTemplate {
  id: string
  adapter_instance_id: string
  session_patterns: string[]
  enabled: boolean
  priority: number
}

export interface ConfigWorkspaceTemplates {
  runtime: ConfigRuntimeTemplate
  logging: ConfigLoggingTemplate
  database: ConfigDatabaseTemplate
  adapterInstance: ConfigAdapterInstanceTemplate
  plugin: ConfigPluginTemplate
  bot: ConfigBotTemplate
  botBinding: ConfigBotBindingTemplate
}

export interface ConfigPluginCatalogItem {
  id: string
  name: string
  module: string
  role: string
  state: string
  configurable: boolean
  schemaRef: string
}

export interface ConfigWorkspace {
  version: number
  configPath: string
  dataDir: string
  config: ConfigDocument
  validation: ConfigValidationResult
  runtime: ConfigWorkspaceRuntime
  templates: ConfigWorkspaceTemplates
  options: ConfigWorkspaceOptions
  providers: ConfigWorkspaceProviders
  plugins: ConfigPluginCatalogItem[]
}

export interface ValidateConfigRequest {
  config: ConfigDocument
}

export interface SaveConfigRequest {
  config: ConfigDocument
  validateBeforeSave?: boolean
}

export interface SaveAdapterInstancesRequest {
  adapterInstances: ConfigRecord[]
  validateBeforeSave?: boolean
}

export interface SaveBotsRequest {
  bots: ConfigRecord[]
  validateBeforeSave?: boolean
}

export interface SaveConfigResult {
  saved: boolean
  requiresRestart: boolean
  validation: ConfigValidationResult
  workspace: ConfigWorkspace
}

export interface ValidateConfigProviderRequest {
  config: ConfigRecord
  pathPrefix?: string
  strict?: boolean
}

export interface ValidateConfigProviderResult {
  issues: ConfigValidationIssue[]
}

export interface ConfigValidationFailureData {
  issues?: ConfigValidationIssue[]
}

export const configApi = {
  getWorkspace(config?: ApiRequestConfig) {
    return apiClient.get<ConfigWorkspace>('/config', config)
  },

  validate(payload: ValidateConfigRequest, config?: ApiRequestConfig) {
    return apiClient.post<ConfigValidationResult>('/config/validate', payload, config)
  },

  save(payload: SaveConfigRequest, config?: ApiRequestConfig) {
    return apiClient.put<SaveConfigResult>('/config', payload, config)
  },

  saveAdapterInstances(payload: SaveAdapterInstancesRequest, config?: ApiRequestConfig) {
    return apiClient.put<SaveConfigResult>('/config/adapter-instances', payload, config)
  },

  saveBots(payload: SaveBotsRequest, config?: ApiRequestConfig) {
    return apiClient.put<SaveConfigResult>('/config/bots', payload, config)
  },
}

export const configProvidersApi = {
  list(kind?: ConfigProviderKind, config?: ApiRequestConfig) {
    return apiClient.get<ConfigProviderDefinition[]>('/config-providers', {
      ...config,
      params: { ...config?.params, ...(kind ? { kind } : {}) },
    })
  },

  get(kind: ConfigProviderKind, providerId: string, config?: ApiRequestConfig) {
    return apiClient.get<ConfigProviderDefinition>(
      `/config-providers/${kind}/${encodeURIComponent(providerId)}`,
      config
    )
  },

  getDefaults(kind: ConfigProviderKind, providerId: string, config?: ApiRequestConfig) {
    return apiClient.get<ConfigRecord>(
      `/config-providers/${kind}/${encodeURIComponent(providerId)}/defaults`,
      config
    )
  },

  validate(
    kind: ConfigProviderKind,
    providerId: string,
    payload: ValidateConfigProviderRequest,
    config?: ApiRequestConfig
  ) {
    return apiClient.post<ValidateConfigProviderResult>(
      `/config-providers/${kind}/${encodeURIComponent(providerId)}/validate`,
      payload,
      config
    )
  },
}

export function extractConfigValidationIssues(error: unknown): ConfigValidationIssue[] {
  if (!axios.isAxiosError<ApiResponse<ConfigValidationFailureData>>(error)) {
    return []
  }

  const issues = error.response?.data.data?.issues
  return Array.isArray(issues) ? issues : []
}
