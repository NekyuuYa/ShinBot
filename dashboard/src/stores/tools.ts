import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { toolsApi, type ToolDefinition } from '@/api/tools'
import { createCrudStore } from './crud'

export type ToolLayoutMode = 'list' | 'card'

export const useToolsStore = defineStore(
  'tools',
  () => {
    const tools = ref<ToolDefinition[]>([])
    const crud = createCrudStore<ToolDefinition, never, never, string>({
      api: {
        list: toolsApi.list,
      },
      i18nKey: {
        loadFailed: 'pages.tools.loadFailed',
      },
      idOf: (tool) => tool.id,
      items: tools,
    })
    const layoutMode = ref<ToolLayoutMode>('list')

    const enabledCount = computed(() => tools.value.filter((item) => item.enabled).length)
    const publicCount = computed(() => tools.value.filter((item) => item.visibility === 'public').length)
    const highRiskCount = computed(() => tools.value.filter((item) => item.riskLevel === 'high').length)

    const setLayoutMode = (mode: ToolLayoutMode) => {
      layoutMode.value = mode
    }

    return {
      tools,
      isLoading: crud.isLoading,
      error: crud.error,
      layoutMode,
      enabledCount,
      publicCount,
      highRiskCount,
      fetchTools: crud.fetchItems,
      setLayoutMode,
    }
  },
  {
    persist: true,
  }
)
