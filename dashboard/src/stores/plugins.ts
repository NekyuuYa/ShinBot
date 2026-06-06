import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { AxiosResponse } from 'axios'
import {
  pluginsApi,
  type Plugin,
  type PluginConfigSchema,
  type ConfigSchemaField,
  type JsonSchemaProperty,
  type GithubPluginInstallPayload,
  type PluginInstallPreview,
  type PluginInstallSource,
  type PluginInstallTask,
  type PluginMarketplaceItem,
  type PluginMarketplaceSource,
} from '@/api/plugins'
import type { ApiResponse } from '@/api/client'
import { translate } from '@/plugins/i18n'
import { getErrorMessage, isAxiosError } from '@/utils/error'
import { createCrudStore } from './crud'
import { useUiStore } from './ui'

const PLUGINS_LIST_STALE_TIME_MS = 30_000
const REQUEST_FAILED_CODE = 'REQUEST_FAILED'

type ApiCall<T> = Promise<AxiosResponse<ApiResponse<T>>>

interface PluginInstallErrorState {
  code: string
  message: string
}

export const usePluginsStore = defineStore('plugins', () => {
  const uiStore = useUiStore()
  const crud = createCrudStore<Plugin, never, Record<string, unknown>, string>({
    api: {
      list: pluginsApi.list,
      update: pluginsApi.updateConfig,
    },
    i18nKey: {
      loadFailed: 'pages.plugins.loadFailed',
      updateFailed: 'common.actions.message.operationFailed',
      updated: 'pages.plugins.configSaved',
    },
    idOf: (plugin) => plugin.id,
    listStaleTimeMs: PLUGINS_LIST_STALE_TIME_MS,
  })
  const plugins = crud.items
  const pluginSchemas = ref<Record<string, PluginConfigSchema>>({})
  const installSources = ref<PluginInstallSource[]>([])
  const marketplaceSources = ref<PluginMarketplaceSource[]>([])
  const marketplaceItems = ref<PluginMarketplaceItem[]>([])
  const marketplaceSource = ref<PluginMarketplaceSource | null>(null)
  const isMarketplaceLoading = ref(false)
  const marketplaceError = ref('')
  const installError = ref<PluginInstallErrorState | null>(null)

  const clearInstallError = () => {
    installError.value = null
  }

  const extractInstallError = (errorDetail: unknown, fallbackKey: string): PluginInstallErrorState => {
    const fallback = translate(fallbackKey)
    if (isAxiosError(errorDetail)) {
      const data = errorDetail.response?.data as {
        error?: { code?: string; message?: string }
        detail?: { code?: string; message?: string }
        message?: string
      } | undefined
      const envelopeError = data?.error ?? data?.detail
      return {
        code: envelopeError?.code ?? REQUEST_FAILED_CODE,
        message:
          envelopeError?.message
          ?? data?.message
          ?? getErrorMessage(errorDetail, fallback),
      }
    }

    return {
      code: REQUEST_FAILED_CODE,
      message: getErrorMessage(errorDetail, fallback),
    }
  }

  const refreshAfterInstallTask = async (task: PluginInstallTask | undefined) => {
    if (task?.status === 'succeeded') {
      await crud.fetchItems({ force: true })
      await fetchInstallSources()
    }
  }

  const runInstallRequest = async <T>(
    request: () => ApiCall<T>,
    options: {
      errorKey: string
      successKey?: string
      successColor?: 'success' | 'info'
      onSuccess?: (data: T | undefined) => void | Promise<void>
    }
  ) => {
    crud.isSaving.value = true
    crud.error.value = ''
    clearInstallError()
    try {
      const response = await request()
      if (response.data.success && response.data.data !== undefined) {
        crud.error.value = ''
        await options.onSuccess?.(response.data.data)
        if (options.successKey) {
          uiStore.showSnackbar(translate(options.successKey), options.successColor ?? 'success')
        }
        return response.data.data
      }

      const errorState = {
        code: response.data.error?.code ?? REQUEST_FAILED_CODE,
        message: response.data.error?.message ?? translate(options.errorKey),
      }
      installError.value = errorState
      crud.error.value = errorState.message
      return null
    } catch (errorDetail: unknown) {
      const errorState = extractInstallError(errorDetail, options.errorKey)
      installError.value = errorState
      crud.error.value = errorState.message
      return null
    } finally {
      crud.isSaving.value = false
    }
  }

  const toSchemaFromLegacyMap = (schema: Record<string, ConfigSchemaField>): PluginConfigSchema => {
    const properties: Record<string, JsonSchemaProperty> = {}

    for (const [key, field] of Object.entries(schema)) {
      properties[key] = {
        type:
          field.type === 'password'
            ? 'string'
            : field.type === 'number'
              ? 'number'
              : field.type === 'boolean'
                ? 'boolean'
                : 'string',
        title: field.label,
        description: field.description,
        default: field.default,
      }
    }

    return {
      type: 'object',
      properties,
      required: Object.entries(schema)
        .filter(([, field]) => field.required)
        .map(([key]) => key),
    }
  }

  const fetchPluginSchema = async (id: string) => {
    const plugin = plugins.value.find((item) => item.id === id)
    if (plugin?.role === 'adapter') {
      return null
    }

    try {
      const response = await pluginsApi.getSchema(id)
      if (response.data.success && response.data.data) {
        pluginSchemas.value[id] = response.data.data
        return response.data.data
      }
    } catch {
      // Fallback to metadata-backed schema if endpoint is unavailable.
    }

    const metadataSchema = plugin?.metadata?.config_schema
    if (metadataSchema) {
      pluginSchemas.value[id] = metadataSchema
      return metadataSchema
    }

    const legacyMap = plugin?.metadata?.configSchema
    if (legacyMap) {
      const schema = toSchemaFromLegacyMap(legacyMap)
      pluginSchemas.value[id] = schema
      return schema
    }

    return null
  }

  const reloadPlugins = async () => {
    const result = await crud.runRequest(() => pluginsApi.reload(), {
      expectData: false,
      successKey: 'pages.plugins.reloaded',
      onSuccess: async () => {
        await crud.fetchItems({ force: true })
      },
    })

    return result.ok
  }

  const rescanPlugins = async () => {
    const result = await crud.runRequest(() => pluginsApi.rescan(), {
      expectData: false,
      successKey: 'pages.plugins.rescanned',
      onSuccess: async () => {
        await crud.fetchItems({ force: true })
      },
    })

    return result.ok
  }

  const enablePlugin = async (id: string) => {
    const result = await crud.runRequest(() => pluginsApi.enable(id), {
      errorKey: 'common.actions.message.operationFailed',
      successKey: 'pages.plugins.enabled',
      successColor: 'success',
      onSuccess: (plugin) => {
        if (plugin) {
          crud.replaceItem(plugin)
        }
      },
    })

    return result.ok
  }

  const disablePlugin = async (id: string) => {
    const result = await crud.runRequest(() => pluginsApi.disable(id), {
      errorKey: 'common.actions.message.operationFailed',
      successKey: 'pages.plugins.disabled',
      successColor: 'info',
      onSuccess: (plugin) => {
        if (plugin) {
          crud.replaceItem(plugin)
        }
      },
    })

    return result.ok
  }

  const updatePluginConfig = async (id: string, config: Record<string, unknown>) => {
    const plugin = await crud.updateItem(id, config)
    return plugin !== null
  }

  const fetchInstallSources = async () => {
    const result = await crud.runRequest(() => pluginsApi.listInstallSources(), {
      errorKey: 'pages.plugins.install.sourcesLoadFailed',
      onSuccess: (data) => {
        installSources.value = data?.plugins ?? []
      },
    })

    return result.ok
  }

  const fetchMarketplaceSources = async () => {
    marketplaceError.value = ''
    try {
      const response = await pluginsApi.listMarketplaceSources()
      if (response.data.success && response.data.data) {
        marketplaceSources.value = response.data.data.sources
        return true
      }
      marketplaceError.value = response.data.error?.message ?? translate('pages.plugins.marketplace.loadFailed')
      return false
    } catch (errorDetail: unknown) {
      marketplaceError.value = getErrorMessage(errorDetail, translate('pages.plugins.marketplace.loadFailed'))
      return false
    }
  }

  const fetchMarketplace = async (source = 'official', options: { refresh?: boolean } = {}) => {
    isMarketplaceLoading.value = true
    marketplaceError.value = ''
    try {
      const response = await pluginsApi.listMarketplace(source, options.refresh ?? false)
      if (response.data.success && response.data.data) {
        marketplaceSource.value = response.data.data.source
        marketplaceItems.value = response.data.data.plugins
        return true
      }
      marketplaceError.value = response.data.error?.message ?? translate('pages.plugins.marketplace.loadFailed')
      return false
    } catch (errorDetail: unknown) {
      marketplaceError.value = getErrorMessage(errorDetail, translate('pages.plugins.marketplace.loadFailed'))
      return false
    } finally {
      isMarketplaceLoading.value = false
    }
  }

  const previewGithubInstall = async (
    payload: Pick<GithubPluginInstallPayload, 'url' | 'ref' | 'plugin_path'>
  ): Promise<PluginInstallPreview | null> => {
    return await runInstallRequest(
      () => pluginsApi.previewGithubInstall(payload),
      {
        errorKey: 'pages.plugins.install.previewFailed',
        onSuccess: () => {
          clearInstallError()
        },
      }
    )
  }

  const previewArchiveInstall = async (file: File): Promise<PluginInstallPreview | null> => {
    return await runInstallRequest(
      () => pluginsApi.previewArchiveInstall(file, file.name),
      {
        errorKey: 'pages.plugins.install.previewFailed',
        onSuccess: () => {
          clearInstallError()
        },
      }
    )
  }

  const installGithub = async (payload: GithubPluginInstallPayload): Promise<PluginInstallTask | null> => {
    return await runInstallRequest(
      () => pluginsApi.installGithub(payload),
      {
        errorKey: 'pages.plugins.install.installFailed',
        successKey: 'pages.plugins.install.installed',
        onSuccess: async (task) => {
          clearInstallError()
          await refreshAfterInstallTask(task)
        },
      }
    )
  }

  const installArchive = async (
    file: File,
    options: { enable_after_install: boolean; allow_overwrite: boolean }
  ): Promise<PluginInstallTask | null> => {
    return await runInstallRequest(
      () => pluginsApi.installArchive(file, {
        filename: file.name,
        enable_after_install: options.enable_after_install,
        allow_overwrite: options.allow_overwrite,
      }),
      {
        errorKey: 'pages.plugins.install.installFailed',
        successKey: 'pages.plugins.install.installed',
        onSuccess: async (task) => {
          clearInstallError()
          await refreshAfterInstallTask(task)
        },
      }
    )
  }

  const fetchInstallTask = async (taskId: string): Promise<PluginInstallTask | null> => {
    return await runInstallRequest(
      () => pluginsApi.fetchInstallTask(taskId),
      {
        errorKey: 'pages.plugins.install.taskLoadFailed',
      }
    )
  }

  const updateInstalledPlugin = async (
    id: string,
    enableAfterInstall = true
  ): Promise<PluginInstallTask | null> => {
    return await runInstallRequest(
      () => pluginsApi.updateInstalledPlugin(id, enableAfterInstall),
      {
        errorKey: 'pages.plugins.install.updateFailed',
        successKey: 'pages.plugins.install.updated',
        onSuccess: async (task) => {
          clearInstallError()
          await refreshAfterInstallTask(task)
        },
      }
    )
  }

  const uninstallInstalledPlugin = async (id: string): Promise<PluginInstallTask | null> => {
    return await runInstallRequest(
      () => pluginsApi.uninstallInstalledPlugin(id),
      {
        errorKey: 'pages.plugins.install.uninstallFailed',
        successKey: 'pages.plugins.install.uninstalled',
        successColor: 'info',
        onSuccess: async (task) => {
          clearInstallError()
          await refreshAfterInstallTask(task)
        },
      }
    )
  }

  const installMarketplacePlugin = async (
    id: string,
    options: {
      source?: string
      enable_after_install?: boolean
      allow_overwrite?: boolean
    } = {}
  ): Promise<PluginInstallTask | null> => {
    return await runInstallRequest(
      () => pluginsApi.installMarketplacePlugin(id, options),
      {
        errorKey: 'pages.plugins.install.installFailed',
        successKey: options.allow_overwrite
          ? 'pages.plugins.install.updated'
          : 'pages.plugins.install.installed',
        onSuccess: async (task) => {
          clearInstallError()
          await refreshAfterInstallTask(task)
          await fetchMarketplace(options.source ?? 'official')
        },
      }
    )
  }

  return {
    plugins,
    pluginSchemas,
    installSources,
    marketplaceSources,
    marketplaceItems,
    marketplaceSource,
    isMarketplaceLoading,
    marketplaceError,
    installError,
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    fetchPlugins: crud.fetchItems,
    fetchPluginSchema,
    reloadPlugins,
    rescanPlugins,
    enablePlugin,
    disablePlugin,
    updatePluginConfig,
    fetchInstallSources,
    fetchMarketplaceSources,
    fetchMarketplace,
    previewGithubInstall,
    previewArchiveInstall,
    installGithub,
    installArchive,
    fetchInstallTask,
    updateInstalledPlugin,
    uninstallInstalledPlugin,
    installMarketplacePlugin,
    clearInstallError,
  }
})
