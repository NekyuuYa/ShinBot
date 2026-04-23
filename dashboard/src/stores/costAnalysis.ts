import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { modelRuntimeApi, type CostAnalysisResponse } from '@/api/modelRuntime'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export const COST_ANALYSIS_WINDOWS = [1, 7, 30, 90] as const
export type CostAnalysisWindow = (typeof COST_ANALYSIS_WINDOWS)[number]

export const useCostAnalysisStore = defineStore(
  'costAnalysis',
  () => {
    const analysis = ref<CostAnalysisResponse | null>(null)
    const selectedDays = ref<CostAnalysisWindow>(7)
    const isLoading = ref(false)
    const error = ref('')

    const hasData = computed(() => (analysis.value?.models.length ?? 0) > 0)

    const setSelectedDays = (days: CostAnalysisWindow) => {
      selectedDays.value = days
    }

    const fetchAnalysis = async (days = selectedDays.value) => {
      isLoading.value = true
      error.value = ''

      try {
        const response = await modelRuntimeApi.getCostAnalysis(days)
        analysis.value = response.data.data ?? null
        selectedDays.value = days
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.costAnalysis.messages.loadFailed')
        )
      } finally {
        isLoading.value = false
      }
    }

    return {
      analysis,
      selectedDays,
      isLoading,
      error,
      hasData,
      setSelectedDays,
      fetchAnalysis,
    }
  },
  {
    persist: {
      paths: ['selectedDays'],
    },
  }
)
