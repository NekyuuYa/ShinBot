import { apiClient } from './client'

export interface ConfigSchemaField {
  label?: string
  type?: 'string' | 'number' | 'boolean' | 'password'
  description?: string
  default?: string | number | boolean
  required?: boolean
}

export interface Plugin {
  id: string
  name: string
  version: string
  role?: string
  state?: string
  status: 'enabled' | 'disabled'
  description?: string
  author?: string
  metadata?: PluginMetadata
}

export interface JsonSchemaProperty {
  type?: 'string' | 'number' | 'integer' | 'boolean' | 'object' | 'array'
  title?: string
  description?: string
  modes?: Array<'forward' | 'reverse' | string>
  enum?: Array<string | number | boolean>
  default?: string | number | boolean | null
  properties?: Record<string, JsonSchemaProperty>
  items?: JsonSchemaProperty
  required?: string[]
}

export interface PluginConfigSchema {
  type?: 'object'
  title?: string
  description?: string
  properties?: Record<string, JsonSchemaProperty>
  required?: string[]
}

export interface PluginMetadata {
  adapter_platform?: string
  config?: Record<string, unknown>
  configSchema?: Record<string, ConfigSchemaField>
  config_schema?: PluginConfigSchema
  dynamicForm?: Record<string, unknown>
  [key: string]: unknown
}

export const pluginsApi = {
  list() {
    return apiClient.get<Plugin[]>('/plugins')
  },

  getSchema(id: string) {
    return apiClient.get<PluginConfigSchema>(`/plugins/${id}/schema`, {
      suppressErrorNotify: true,
    })
  },

  reload() {
    return apiClient.post<void>('/plugins/reload')
  },

  rescan() {
    return apiClient.post<void>('/plugins/rescan')
  },

  updateConfig(id: string, config: Record<string, unknown>) {
    return apiClient.patch<Plugin>(`/plugins/${id}/config`, config)
  },

  enable(id: string) {
    return apiClient.post<Plugin>(`/plugins/${id}/enable`)
  },

  disable(id: string) {
    return apiClient.post<Plugin>(`/plugins/${id}/disable`)
  },
}
