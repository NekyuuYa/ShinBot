import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { modelRuntimeApi, type CostAnalysisResponse } from '@/api/modelRuntime'
import { createRequestStore } from './crud'

export const COST_ANALYSIS_WINDOWS = [1, 7, 30] as const
export type CostAnalysisWindow = (typeof COST_ANALYSIS_WINDOWS)[number]

const DEFAULT_COST_ANALYSIS_WINDOW: CostAnalysisWindow = 7
const COST_ANALYSIS_STALE_TIME_MS = 30_000

interface CostAnalysisCacheEntry {
  data: CostAnalysisResponse | null
  fetchedAt: number
}

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
    const cache = new Map<CostAnalysisWindow, CostAnalysisCacheEntry>()
    const inflightRequests = new Map<CostAnalysisWindow, Promise<boolean>>()

    const hasData = computed(() => (analysis.value?.models.length ?? 0) > 0)

    const applyAnalysis = (
      data: CostAnalysisResponse | null,
      days: CostAnalysisWindow
    ) => {
      analysis.value = data
      selectedDays.value = days
      cache.set(days, {
        data,
        fetchedAt: Date.now(),
      })
    }

    const setSelectedDays = (days: number) => {
      selectedDays.value = normalizeCostAnalysisWindow(days)
    }

    const fetchAnalysis = async (
      days = selectedDays.value,
      options: { force?: boolean } = {}
    ) => {
      const normalizedDays = normalizeCostAnalysisWindow(days)
      const existingRequest = inflightRequests.get(normalizedDays)
      if (existingRequest) {
        return existingRequest
      }

      const cached = cache.get(normalizedDays)
      const isCacheFresh =
        !options.force
        && cached !== undefined
        && Date.now() - cached.fetchedAt < COST_ANALYSIS_STALE_TIME_MS

      if (isCacheFresh) {
        analysis.value = cached.data
        selectedDays.value = normalizedDays
        return true
      }

      const request = requests.runRequest(() => modelRuntimeApi.getCostAnalysis(normalizedDays), {
        mode: 'loading',
        errorKey: 'pages.costAnalysis.messages.loadFailed',
        onSuccess: (data) => {
          applyAnalysis(data ?? null, normalizedDays)
        },
      })
        .then((result) => result.ok)
        .finally(() => {
          inflightRequests.delete(normalizedDays)
        })

      inflightRequests.set(normalizedDays, request)
      return request
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
