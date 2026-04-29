import { computed, ref, watch, type Ref } from 'vue'

import { agentsApi, type AgentSummary } from '@/api/agents'
import { botConfigsApi, type BotConfig } from '@/api/botConfigs'
import { apiClient } from '@/api/client'
import type { JsonSchemaProperty, PluginConfigSchema } from '@/api/plugins'
import { promptsApi, type PromptCatalogItem } from '@/api/prompts'
import type { InstanceFormState } from '@/components/instances/types'
import { translate } from '@/plugins/i18n'
import { usePluginsStore } from '@/stores/plugins'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

export function useInstanceResources(form: Ref<InstanceFormState>) {
  const pluginsStore = usePluginsStore()
  const uiStore = useUiStore()

  const agents = ref<AgentSummary[]>([])
  const botConfigs = ref<BotConfig[]>([])
  const promptCatalog = ref<PromptCatalogItem[]>([])

  const notifyLoadFailure = (errorDetail: unknown, key: string) => {
    uiStore.showSnackbar(getErrorMessage(errorDetail, translate(key)), 'error')
  }

  const fetchAgents = async () => {
    try {
      agents.value = await apiClient.unwrap(agentsApi.list())
    } catch (errorDetail: unknown) {
      notifyLoadFailure(errorDetail, 'pages.instances.agentsLoadFailed')
    }
  }

  const fetchBotConfigs = async () => {
    try {
      botConfigs.value = await apiClient.unwrap(botConfigsApi.list())
    } catch (errorDetail: unknown) {
      notifyLoadFailure(errorDetail, 'pages.instances.botConfigLoadFailed')
    }
  }

  const fetchPrompts = async () => {
    try {
      promptCatalog.value = await apiClient.unwrap(promptsApi.list())
    } catch (errorDetail: unknown) {
      notifyLoadFailure(errorDetail, 'pages.instances.promptsLoadFailed')
    }
  }

  const fetchAllResources = () => Promise.all([
    pluginsStore.fetchPlugins(),
    fetchAgents(),
    fetchBotConfigs(),
    fetchPrompts(),
  ])

  const botConfigByInstanceId = computed(
    () => new Map(botConfigs.value.map((item) => [item.instanceId, item] as const))
  )

  const getBotConfigForInstance = (instanceId: string) => botConfigByInstanceId.value.get(instanceId)

  const adapterOptions = computed(() => {
    const adapters = pluginsStore.plugins
      .filter((plugin) => plugin.role === 'adapter')
      .map((plugin) => plugin.metadata?.adapter_platform)
      .filter((platform): platform is string => Boolean(platform))

    const uniqueAdapters = Array.from(new Set(adapters))
    return uniqueAdapters.length > 0 ? uniqueAdapters : ['satori', 'onebot_v11']
  })

  const adapterSchemaByPlatform = computed<Record<string, PluginConfigSchema>>(() => {
    const mapping: Record<string, PluginConfigSchema> = {}

    pluginsStore.plugins.forEach((plugin) => {
      if (plugin.role === 'adapter' && plugin.metadata?.adapter_platform && plugin.metadata?.config_schema) {
        mapping[plugin.metadata.adapter_platform] = plugin.metadata.config_schema
      }
    })

    return mapping
  })

  const activeAdapterSchema = computed(() => adapterSchemaByPlatform.value[form.value.adapterType] ?? null)

  watch(
    () => form.value.adapterType,
    (next, prev) => {
      if (!next || next === prev || Object.keys(form.value.config).length > 0) {
        return
      }

      const schema = adapterSchemaByPlatform.value[next]
      if (!schema?.properties) {
        return
      }

      const defaults: Record<string, string | number | boolean | null> = {}
      Object.entries(schema.properties).forEach(([key, property]: [string, JsonSchemaProperty]) => {
        if (property.default !== undefined) {
          defaults[key] = property.default
        }
      })

      form.value.config = defaults
    }
  )

  return {
    agents,
    promptCatalog,
    adapterOptions,
    activeAdapterSchema,
    fetchAllResources,
    getBotConfigForInstance,
  }
}