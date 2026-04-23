import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { modelRuntimeApi, type CostAnalysisResponse } from '@/api/modelRuntime'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export const COST_ANALYSIS_WINDOWS = [1, 7, 30] as const
export type CostAnalysisWindow = (typeof COST_ANALYSIS_WINDOWS)[number]

const DEFAULT_COST_ANALYSIS_WINDOW: CostAnalysisWindow = 7

const normalizeCostAnalysisWindow = (value: number): CostAnalysisWindow =>
  COST_ANALYSIS_WINDOWS.includes(value as CostAnalysisWindow)
    ? (value as CostAnalysisWindow)
    : DEFAULT_COST_ANALYSIS_WINDOW

export const useCostAnalysisStore = defineStore(
  'costAnalysis',
  () => {
    const analysis = ref<CostAnalysisResponse | null>(null)
    const selectedDays = ref<CostAnalysisWindow>(DEFAULT_COST_ANALYSIS_WINDOW)
    const isLoading = ref(false)
    const error = ref('')

    const hasData = computed(() => (analysis.value?.models.length ?? 0) > 0)

    const setSelectedDays = (days: number) => {
      selectedDays.value = normalizeCostAnalysisWindow(days)
    }

    const fetchAnalysis = async (days = selectedDays.value) => {
      const normalizedDays = normalizeCostAnalysisWindow(days)
      isLoading.value = true
      error.value = ''

      try {
        const response = await modelRuntimeApi.getCostAnalysis(normalizedDays)
        analysis.value = response.data.data ?? null
        selectedDays.value = normalizedDays
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
