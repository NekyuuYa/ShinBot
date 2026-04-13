import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  pluginsApi,
  type Plugin,
  type PluginConfigSchema,
  type ConfigSchemaField,
  type JsonSchemaProperty,
} from '@/api/plugins'
import { useUiStore } from './ui'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export const usePluginsStore = defineStore('plugins', () => {
  const plugins = ref<Plugin[]>([])
  const pluginSchemas = ref<Record<string, PluginConfigSchema>>({})
  const isLoading = ref(false)
  const error = ref<string>('')

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
    try {
      const response = await pluginsApi.getSchema(id)
      if (response.data.success && response.data.data) {
        pluginSchemas.value[id] = response.data.data
        return response.data.data
      }
    } catch {
      // Fallback to metadata-backed schema if endpoint is unavailable.
    }

    const plugin = plugins.value.find((item) => item.id === id)
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

  const fetchPlugins = async () => {
    isLoading.value = true
    error.value = ''

    try {
      const response = await pluginsApi.list()
      if (response.data.success && response.data.data) {
        plugins.value = response.data.data
      } else {
        error.value = response.data.error?.message || translate('pages.plugins.loadFailed')
      }
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
    } finally {
      isLoading.value = false
    }
  }

  const reloadPlugins = async () => {
    try {
      const response = await pluginsApi.reload()
      if (response.data.success) {
        // Re-fetch plugins after reload
        await fetchPlugins()
        useUiStore().showSnackbar(translate('pages.plugins.reloaded'), 'success')
        return true
      }
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const rescanPlugins = async () => {
    try {
      const response = await pluginsApi.rescan()
      if (response.data.success) {
        await fetchPlugins()
        useUiStore().showSnackbar(translate('pages.plugins.rescanned'), 'success')
        return true
      }
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const enablePlugin = async (id: string) => {
    try {
      const response = await pluginsApi.enable(id)
      if (response.data.success && response.data.data) {
        const index = plugins.value.findIndex((p) => p.id === id)
        if (index !== -1) {
          plugins.value[index] = response.data.data
        }
        useUiStore().showSnackbar(translate('pages.plugins.enabled'), 'success')
        return true
      }
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const disablePlugin = async (id: string) => {
    try {
      const response = await pluginsApi.disable(id)
      if (response.data.success && response.data.data) {
        const index = plugins.value.findIndex((p) => p.id === id)
        if (index !== -1) {
          plugins.value[index] = response.data.data
        }
        useUiStore().showSnackbar(translate('pages.plugins.disabled'), 'info')
        return true
      }
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const updatePluginConfig = async (id: string, config: Record<string, unknown>) => {
    try {
      const response = await pluginsApi.updateConfig(id, config)
      if (response.data.success && response.data.data) {
        const index = plugins.value.findIndex((plugin) => plugin.id === id)
        if (index !== -1) {
          plugins.value[index] = response.data.data
        }
        useUiStore().showSnackbar(translate('pages.plugins.configSaved'), 'success')
        return true
      }

      error.value = response.data.error?.message || translate('common.actions.message.operationFailed')
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  return {
    plugins,
    pluginSchemas,
    isLoading,
    error,
    fetchPlugins,
    fetchPluginSchema,
    reloadPlugins,
    rescanPlugins,
    enablePlugin,
    disablePlugin,
    updatePluginConfig,
  }
})
