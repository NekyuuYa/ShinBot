import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  pluginsApi,
  type Plugin,
  type PluginConfigSchema,
  type ConfigSchemaField,
  type JsonSchemaProperty,
} from '@/api/plugins'
import { createCrudStore } from './crud'

const PLUGINS_LIST_STALE_TIME_MS = 30_000

export const usePluginsStore = defineStore('plugins', () => {
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

  return {
    plugins,
    pluginSchemas,
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
  }
})
