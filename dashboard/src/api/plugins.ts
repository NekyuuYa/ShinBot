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
  ui_group?: 'basic' | 'advanced' | string
  ui_component?: string
  enum?: Array<string | number | boolean>
  enum_titles?: string[]
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
  builtin?: boolean
  config?: Record<string, unknown>
  configSchema?: Record<string, ConfigSchemaField>
  config_schema?: PluginConfigSchema
  dynamicForm?: Record<string, unknown>
  install_source?: PluginInstallSource
  source?: 'builtin' | 'local' | string
  [key: string]: unknown
}

export type PluginInstallSourceType = 'github' | 'archive' | 'marketplace'

export interface PluginInstallSource {
  plugin_id?: string
  source_type: PluginInstallSourceType
  source_url: string
  ref: string
  resolved_ref: string
  plugin_path: string
  installed_at?: number
  updated_at?: number
  installed_version: string
  managed_by_webui: boolean
  archive_sha256?: string
  installer_type?: string
  marketplace_source_id?: string
  can_update?: boolean
  can_uninstall?: boolean
}

export interface PluginInstallSourcesResponse {
  plugins: PluginInstallSource[]
}

export interface PluginInstallPreview {
  plugin_id: string
  name: string
  version: string
  description: string
  author: string
  role: string
  entry: string
  permissions: string[]
  required_dependencies: string[]
  optional_dependencies: string[]
  legacy_dependencies: string[]
  missing_required_dependencies: string[]
  missing_optional_dependencies: string[]
  source_type: PluginInstallSourceType
  source_url: string
  ref: string
  resolved_ref: string
  plugin_path: string
  archive_sha256: string
  target_exists: boolean
  target_managed_by_webui: boolean
  can_install: boolean
  warnings: string[]
}

export interface GithubPluginInstallPayload {
  url: string
  ref: string
  plugin_path?: string
  enable_after_install?: boolean
  allow_overwrite?: boolean
}

export interface PluginArchiveInstallOptions {
  filename?: string
  enable_after_install?: boolean
  allow_overwrite?: boolean
}

export interface PluginInstallTask {
  task_id: string
  status: 'queued' | 'running' | 'succeeded' | 'failed'
  stage: string
  message: string
  plugin_id?: string | null
  error?: {
    code: string
    message: string
  } | null
  created_at: number
  updated_at: number
}

export interface PluginMarketplaceSource {
  id: string
  name: string
  source_type: 'github_monorepo'
  repository_url: string
  repo_url?: string
  ref: string
  plugin_root: string
  installer_type?: string
  owner_plugin_id?: string
}

export interface PluginMarketplaceItem {
  id: string
  plugin_id: string
  name: string
  version: string
  description?: string
  author?: string
  role?: string
  entry: string
  permissions: string[]
  required_dependencies: string[]
  optional_dependencies: string[]
  legacy_dependencies: string[]
  missing_required_dependencies: string[]
  missing_optional_dependencies: string[]
  tags: string[]
  homepage?: string
  repository?: string
  repository_url: string
  ref: string
  plugin_path: string
  installed: boolean
  installed_version: string
  installed_source?: PluginInstallSource | null
  managed_by_webui: boolean
  can_install: boolean
  can_update: boolean
  update_available: boolean
  warnings: string[]
}

export interface PluginMarketplaceSourcesResponse {
  sources: PluginMarketplaceSource[]
}

export interface PluginMarketplaceResponse {
  source: PluginMarketplaceSource
  cache?: {
    cached: boolean
    cached_at: number
    expires_at: number
    ttl_seconds: number
  }
  plugins: PluginMarketplaceItem[]
}

export interface PluginMarketplaceItemResponse {
  source: PluginMarketplaceSource
  plugin: PluginMarketplaceItem
}

export interface PluginMarketplaceInstallPayload {
  source?: string
  refresh?: boolean
  enable_after_install?: boolean
  allow_overwrite?: boolean
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

  listInstallSources() {
    return apiClient.get<PluginInstallSourcesResponse>('/plugin-installs')
  },

  previewGithubInstall(payload: GithubPluginInstallPayload) {
    return apiClient.post<PluginInstallPreview>('/plugin-installs/github/preview', payload)
  },

  installGithub(payload: GithubPluginInstallPayload) {
    return apiClient.post<PluginInstallTask>('/plugin-installs/github', payload)
  },

  previewArchiveInstall(file: Blob, filename = '') {
    return apiClient.post<PluginInstallPreview>('/plugin-installs/archive/preview', file, {
      headers: {
        'Content-Type': 'application/zip',
      },
      params: {
        filename,
      },
    })
  },

  installArchive(file: Blob, options: PluginArchiveInstallOptions = {}) {
    return apiClient.post<PluginInstallTask>('/plugin-installs/archive', file, {
      headers: {
        'Content-Type': 'application/zip',
      },
      params: {
        filename: options.filename ?? '',
        enable_after_install: options.enable_after_install ?? true,
        allow_overwrite: options.allow_overwrite ?? false,
      },
    })
  },

  fetchInstallTask(taskId: string) {
    return apiClient.get<PluginInstallTask>(`/plugin-installs/tasks/${taskId}`)
  },

  updateInstalledPlugin(id: string, enableAfterInstall = true) {
    return apiClient.post<PluginInstallTask>(`/plugin-installs/${id}/update`, undefined, {
      params: {
        enable_after_install: enableAfterInstall,
      },
    })
  },

  uninstallInstalledPlugin(id: string) {
    return apiClient.delete<PluginInstallTask>(`/plugin-installs/${id}`)
  },

  listMarketplaceSources() {
    return apiClient.get<PluginMarketplaceSourcesResponse>('/plugin-marketplace/sources')
  },

  listMarketplace(source = 'official', refresh = false) {
    return apiClient.get<PluginMarketplaceResponse>('/plugin-marketplace', {
      params: { source, refresh },
    })
  },

  getMarketplacePlugin(id: string, source = 'official') {
    return apiClient.get<PluginMarketplaceItemResponse>(`/plugin-marketplace/${id}`, {
      params: { source },
    })
  },

  previewMarketplacePlugin(id: string, source = 'official') {
    return apiClient.post<PluginInstallPreview>(`/plugin-marketplace/${id}/preview`, {
      source,
    })
  },

  installMarketplacePlugin(id: string, payload: PluginMarketplaceInstallPayload = {}) {
    return apiClient.post<PluginInstallTask>(`/plugin-marketplace/${id}/install`, payload)
  },
}
