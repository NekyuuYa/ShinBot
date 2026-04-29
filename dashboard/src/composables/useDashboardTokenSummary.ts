import { modelRuntimeApi, type ModelTokenSummary } from '@/api/modelRuntime'
import { createCachedRequest, type CachedRequestOptions } from '@/utils/requestCache'

const DASHBOARD_TOKEN_SUMMARY_STALE_TIME_MS = 30_000

const loadDashboardTokenSummary = createCachedRequest(async (): Promise<ModelTokenSummary | null> => {
  const response = await modelRuntimeApi.getTokenSummary(7)
  return response.data.data ?? null
}, DASHBOARD_TOKEN_SUMMARY_STALE_TIME_MS)

export const fetchDashboardTokenSummary = (options: CachedRequestOptions = {}) =>
  loadDashboardTokenSummary(options)
