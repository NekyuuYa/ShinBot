import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { modelRuntimeApi, type CostAnalysisResponse } from '@/api/modelRuntime'
import { createRequestStore } from './crud'

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
    const requests = createRequestStore()

    const hasData = computed(() => (analysis.value?.models.length ?? 0) > 0)

    const setSelectedDays = (days: number) => {
      selectedDays.value = normalizeCostAnalysisWindow(days)
    }

    const fetchAnalysis = async (days = selectedDays.value) => {
      const normalizedDays = normalizeCostAnalysisWindow(days)
      const result = await requests.runRequest(() => modelRuntimeApi.getCostAnalysis(normalizedDays), {
        mode: 'loading',
        errorKey: 'pages.costAnalysis.messages.loadFailed',
        onSuccess: (data) => {
          analysis.value = data ?? null
          selectedDays.value = normalizedDays
        },
      })

      return result.ok
    }

    return {
      analysis,
      selectedDays,
      isLoading: requests.isLoading,
      error: requests.error,
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
