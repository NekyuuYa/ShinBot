import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { toolsApi, type ToolDefinition } from '@/api/tools'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export type ToolLayoutMode = 'list' | 'card'

export const useToolsStore = defineStore(
  'tools',
  () => {
    const tools = ref<ToolDefinition[]>([])
    const isLoading = ref(false)
    const error = ref('')
    const layoutMode = ref<ToolLayoutMode>('list')

    const enabledCount = computed(() => tools.value.filter((item) => item.enabled).length)
    const publicCount = computed(() => tools.value.filter((item) => item.visibility === 'public').length)
    const highRiskCount = computed(() => tools.value.filter((item) => item.riskLevel === 'high').length)

    const fetchTools = async () => {
      isLoading.value = true
      error.value = ''

      try {
        const response = await toolsApi.list()
        if (response.data.success && response.data.data) {
          tools.value = response.data.data
        } else {
          error.value = response.data.error?.message || translate('pages.tools.loadFailed')
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

    const setLayoutMode = (mode: ToolLayoutMode) => {
      layoutMode.value = mode
    }

    return {
      tools,
      isLoading,
      error,
      layoutMode,
      enabledCount,
      publicCount,
      highRiskCount,
      fetchTools,
      setLayoutMode,
    }
  },
  {
    persist: true,
  }
)
