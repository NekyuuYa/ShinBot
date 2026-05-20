import { computed, type Ref } from 'vue'

import type { CostAnalysisBucket, CostAnalysisModel, CostAnalysisResponse } from '@/api/modelRuntime'
import { COST_ANALYSIS_WINDOWS } from '@/stores/costAnalysis'

type FocusGranularity = 'daily' | 'hourly'
type FocusMetric = 'tokens' | 'cost' | 'calls' | 'cache'
type SummaryTone = 'primary' | 'warning' | 'info' | 'success' | 'secondary'

interface TranslateFn {
  (key: string, params?: Record<string, unknown>): string
}

interface Formatters {
  formatNumber: (value: number) => string
  formatCompactNumber: (value: number) => string
  formatCurrency: (value: number) => string
  formatPercent: (value: number) => string
  formatDate: (value: string | null | undefined) => string
  formatHour: (value: string) => string
  formatShortDate: (value: string) => string
  formatDuration: (value: number | null | undefined) => string
}

export interface ProviderCostRow {
  key: string
  name: string
  modelCount: number
  totalCalls: number
  totalTokens: number
  cacheHits: number
  estimatedCost: number
  costShare: number
  cacheHitRate: number
}

export interface RankedCostModel extends CostAnalysisModel {
  costShare: number
}

export interface LineChartPoint {
  key: string
  x: number
  y: number
  bucket: CostAnalysisBucket
}

export interface LineChartSeries {
  points: LineChartPoint[]
  linePath: string
  areaPath: string
}

const LINE_CHART_TOP = 10
const LINE_CHART_BOTTOM = 88
export const lineChartGuideYs = [10, 29.5, 49, 68.5, 88]

const defaultSummary = () => ({
  totalCalls: 0,
  successfulCalls: 0,
  failedCalls: 0,
  successRate: 0,
  cacheHits: 0,
  cacheHitRate: 0,
  inputTokens: 0,
  outputTokens: 0,
  totalTokens: 0,
  cacheReadTokens: 0,
  cacheWriteTokens: 0,
  estimatedCost: 0,
  averageLatencyMs: null as number | null,
  averageTimeToFirstTokenMs: null as number | null,
})

export function useCostAnalysisViewModel(options: {
  analysis: Ref<CostAnalysisResponse | null>
  focusGranularity: Ref<FocusGranularity>
  focusMetric: Ref<FocusMetric>
  t: TranslateFn
  formatters: Formatters
}) {
  const {
    analysis,
    focusGranularity,
    focusMetric,
    t,
    formatters: {
      formatNumber,
      formatCompactNumber,
      formatCurrency,
      formatPercent,
      formatDate,
      formatHour,
      formatShortDate,
      formatDuration,
    },
  } = options

  const windowOptions = computed(() =>
    COST_ANALYSIS_WINDOWS.map((value) => ({
      value,
      label: t('pages.costAnalysis.windowOption', { days: value }),
    })),
  )

  const focusMetricOptions = computed(() => [
    { value: 'tokens' as const, label: t('pages.costAnalysis.metrics.tokens') },
    { value: 'cost' as const, label: t('pages.costAnalysis.metrics.cost') },
    { value: 'calls' as const, label: t('pages.costAnalysis.metrics.calls') },
    { value: 'cache' as const, label: t('pages.costAnalysis.metrics.cache') },
  ])

  const summary = computed(() => analysis.value?.summary ?? defaultSummary())

  const dailyBuckets = computed(() => analysis.value?.timeline.daily ?? [])
  const hourlyBuckets = computed(() => analysis.value?.timeline.hourly ?? [])
  const focusModels = computed(() => analysis.value?.focusModels ?? [])
  const allModels = computed(() => analysis.value?.models ?? [])

  const averageCostPerCall = computed(() =>
    summary.value.totalCalls > 0
      ? summary.value.estimatedCost / summary.value.totalCalls
      : 0,
  )
  const averageTokensPerCall = computed(() =>
    summary.value.totalCalls > 0
      ? summary.value.totalTokens / summary.value.totalCalls
      : 0,
  )

  const dailyCostMax = computed(() =>
    Math.max(...dailyBuckets.value.map((bucket) => bucket.estimatedCost), 0.01),
  )
  const hourlyTokenMax = computed(() =>
    Math.max(...hourlyBuckets.value.map((bucket) => bucket.totalTokens), 1),
  )

  const chartNumber = (value: number) => Number(value.toFixed(3))
  const clampChartValue = (value: number, min = 0, max = 100) =>
    Math.min(Math.max(value, min), max)
  const chartY = (value: number, max: number) => {
    if (value <= 0 || max <= 0) return LINE_CHART_BOTTOM

    const ratio = Math.min(value / max, 1)
    return chartNumber(
      LINE_CHART_BOTTOM - ratio * (LINE_CHART_BOTTOM - LINE_CHART_TOP),
    )
  }
  const createSmoothLinePath = (points: LineChartPoint[]) => {
    if (points.length === 0) return ''
    if (points.length === 1) {
      return `M 0 ${points[0].y} L 100 ${points[0].y}`
    }
    if (points.length === 2) {
      return `M ${points[0].x} ${points[0].y} L ${points[1].x} ${points[1].y}`
    }

    let path = `M ${points[0].x} ${points[0].y}`
    for (let i = 0; i < points.length - 1; i += 1) {
      const previous = points[i - 1] ?? points[i]
      const current = points[i]
      const next = points[i + 1]
      const afterNext = points[i + 2] ?? next
      const controlStartX = current.x + (next.x - previous.x) / 6
      const controlStartY = current.y + (next.y - previous.y) / 6
      const controlEndX = next.x - (afterNext.x - current.x) / 6
      const controlEndY = next.y - (afterNext.y - current.y) / 6

      path += ` C ${chartNumber(clampChartValue(controlStartX))} ${chartNumber(
        clampChartValue(controlStartY, LINE_CHART_TOP, LINE_CHART_BOTTOM),
      )}, ${chartNumber(clampChartValue(controlEndX))} ${chartNumber(
        clampChartValue(controlEndY, LINE_CHART_TOP, LINE_CHART_BOTTOM),
      )}, ${next.x} ${next.y}`
    }

    return path
  }
  const createLineChartSeries = (
    buckets: CostAnalysisBucket[],
    valueOf: (bucket: CostAnalysisBucket) => number,
    max: number,
  ): LineChartSeries => {
    const points = buckets.map((bucket, index) => ({
      key: bucket.bucketStart,
      x:
        buckets.length === 1
          ? 50
          : chartNumber((index / Math.max(buckets.length - 1, 1)) * 100),
      y: chartY(valueOf(bucket), max),
      bucket,
    }))
    const linePath = createSmoothLinePath(points)
    const firstX = points.length === 1 ? 0 : (points[0]?.x ?? 0)
    const lastX = points.length === 1 ? 100 : (points.at(-1)?.x ?? 100)

    return {
      points,
      linePath,
      areaPath: linePath
        ? `${linePath} L ${lastX} ${LINE_CHART_BOTTOM} L ${firstX} ${LINE_CHART_BOTTOM} Z`
        : '',
    }
  }

  const dailyCostChart = computed(() =>
    createLineChartSeries(
      dailyBuckets.value,
      (bucket) => bucket.estimatedCost,
      dailyCostMax.value,
    ),
  )
  const hourlyTokenChart = computed(() =>
    createLineChartSeries(
      hourlyBuckets.value,
      (bucket) => bucket.totalTokens,
      hourlyTokenMax.value,
    ),
  )
  const hourlyTokenTotal = computed(() =>
    hourlyBuckets.value.reduce((sum, bucket) => sum + bucket.totalTokens, 0),
  )

  const peakDailyBucket = computed(() =>
    dailyBuckets.value.reduce<CostAnalysisBucket | null>(
      (best, bucket) =>
        !best || bucket.estimatedCost > best.estimatedCost ? bucket : best,
      null,
    ),
  )

  const providerRows = computed<ProviderCostRow[]>(() => {
    const providers = new Map<
      string,
      Omit<ProviderCostRow, 'costShare' | 'cacheHitRate'>
    >()

    for (const model of allModels.value) {
      const key = model.providerId || 'unknown'
      const existing = providers.get(key) ?? {
        key,
        name: providerName(model),
        modelCount: 0,
        totalCalls: 0,
        totalTokens: 0,
        cacheHits: 0,
        estimatedCost: 0,
      }

      existing.modelCount += 1
      existing.totalCalls += model.totalCalls
      existing.totalTokens += model.totalTokens
      existing.cacheHits += model.cacheHits
      existing.estimatedCost += model.estimatedCost
      providers.set(key, existing)
    }

    const totalCost = summary.value.estimatedCost
    return Array.from(providers.values())
      .sort((a, b) => b.estimatedCost - a.estimatedCost)
      .map((row) => ({
        ...row,
        costShare: totalCost > 0 ? row.estimatedCost / totalCost : 0,
        cacheHitRate: row.totalCalls > 0 ? row.cacheHits / row.totalCalls : 0,
      }))
  })

  const topCostModels = computed<RankedCostModel[]>(() => {
    const totalCost = summary.value.estimatedCost
    return [...allModels.value]
      .sort((a, b) => b.estimatedCost - a.estimatedCost)
      .slice(0, 6)
      .map((model) => ({
        ...model,
        costShare: totalCost > 0 ? model.estimatedCost / totalCost : 0,
      }))
  })

  const topCostModelRows = computed(() =>
    topCostModels.value.map((model) => ({
      key: modelKey(model),
      name: modelName(model),
      detail: providerName(model),
      value: formatCurrency(model.estimatedCost),
      meta: t('pages.costAnalysis.labels.modelRankMeta', {
        calls: formatCompactNumber(model.totalCalls),
        tokens: formatCompactNumber(model.totalTokens),
        share: formatPercent(model.costShare),
      }),
      shareWidth: shareWidth(model.costShare),
    })),
  )

  const metricValue = (bucket: CostAnalysisBucket) => {
    if (focusMetric.value === 'cost') return bucket.estimatedCost
    if (focusMetric.value === 'calls') return bucket.totalCalls
    if (focusMetric.value === 'cache')
      return bucketRate(bucket.cacheHits, bucket.totalCalls)
    return bucket.totalTokens
  }

  const heatmapMetricMax = computed(() => {
    if (focusMetric.value === 'cache') return 1
    const values = focusModels.value.flatMap((model) =>
      model[focusGranularity.value].map(metricValue),
    )
    return Math.max(...values, 1)
  })

  const bucketIntensity = (bucket: CostAnalysisBucket) => {
    const value = metricValue(bucket)
    const max = heatmapMetricMax.value
    return !value || max <= 0 ? 0.06 : Math.min(0.16 + (value / max) * 0.74, 0.9)
  }

  const formatHeatmapValue = (bucket: CostAnalysisBucket) => {
    if (focusMetric.value === 'cost') return formatCurrency(bucket.estimatedCost)
    if (focusMetric.value === 'calls') return formatNumber(bucket.totalCalls)
    if (focusMetric.value === 'cache')
      return formatPercent(bucketRate(bucket.cacheHits, bucket.totalCalls))
    return formatCompactNumber(bucket.totalTokens)
  }

  const summaryStats = computed(() => [
    {
      key: 'cost',
      tone: 'warning' as SummaryTone,
      icon: 'mdi-cash-multiple',
      label: t('pages.costAnalysis.summary.totalCost'),
      value: formatCurrency(summary.value.estimatedCost),
      meta: t('pages.costAnalysis.summary.costPerCall', {
        value: formatCurrency(averageCostPerCall.value),
      }),
    },
    {
      key: 'calls',
      tone: 'info' as SummaryTone,
      icon: 'mdi-phone-in-talk-outline',
      label: t('pages.costAnalysis.summary.totalCalls'),
      value: formatCompactNumber(summary.value.totalCalls),
      meta: t('pages.costAnalysis.summary.successFailed', {
        rate: formatPercent(summary.value.successRate),
        failed: formatCompactNumber(summary.value.failedCalls),
      }),
    },
    {
      key: 'tokens',
      tone: 'primary' as SummaryTone,
      icon: 'mdi-counter',
      label: t('pages.costAnalysis.summary.totalTokens'),
      value: formatCompactNumber(summary.value.totalTokens),
      meta: t('pages.costAnalysis.summary.tokensPerCall', {
        value: formatCompactNumber(averageTokensPerCall.value),
      }),
    },
    {
      key: 'cache',
      tone: 'success' as SummaryTone,
      icon: 'mdi-database-sync-outline',
      label: t('pages.costAnalysis.summary.cacheHitRate'),
      value: formatPercent(summary.value.cacheHitRate),
      meta: t('pages.costAnalysis.summary.cacheReadWrite', {
        read: formatCompactNumber(summary.value.cacheReadTokens),
        write: formatCompactNumber(summary.value.cacheWriteTokens),
      }),
    },
    {
      key: 'latency',
      tone: 'secondary' as SummaryTone,
      icon: 'mdi-timer-sand',
      label: t('pages.costAnalysis.summary.avgLatency'),
      value: formatDuration(summary.value.averageLatencyMs),
      meta: t('pages.costAnalysis.summary.ttft', {
        value: formatDuration(summary.value.averageTimeToFirstTokenMs),
      }),
    },
  ])

  const overviewSignals = computed(() => [
    {
      key: 'models',
      label: t('pages.costAnalysis.labels.activeModels'),
      value: formatNumber(allModels.value.length),
      meta: t('pages.costAnalysis.labels.focusModels', {
        count: focusModels.value.length,
      }),
    },
    {
      key: 'providers',
      label: t('pages.costAnalysis.labels.providerCount'),
      value: formatNumber(providerRows.value.length),
      meta: providerRows.value[0]
        ? t('pages.costAnalysis.labels.topProvider', {
            name: providerRows.value[0].name,
            value: formatCurrency(providerRows.value[0].estimatedCost),
          })
        : t('pages.costAnalysis.labels.noData'),
    },
    {
      key: 'peak',
      label: t('pages.costAnalysis.labels.peakCostDay'),
      value: peakDailyBucket.value
        ? formatDate(peakDailyBucket.value.bucketStart)
        : t('pages.costAnalysis.labels.noData'),
      meta: peakDailyBucket.value
        ? formatCurrency(peakDailyBucket.value.estimatedCost)
        : t('pages.costAnalysis.labels.noData'),
    },
  ])

  const timelineLabel = (
    index: number,
    length: number,
    value: string,
    granularity: FocusGranularity,
  ) => {
    if (granularity === 'hourly') {
      return index % 4 !== 0 && index !== length - 1 ? '' : formatHour(value)
    }
    const divider = length > 45 ? 9 : length > 21 ? 5 : 1
    return index % divider !== 0 && index !== length - 1
      ? ''
      : formatShortDate(value)
  }

  const formatBucketHeader = (value: string, granularity: FocusGranularity) =>
    granularity === 'hourly' ? formatHour(value) : formatShortDate(value)

  return {
    windowOptions,
    focusMetricOptions,
    summary,
    dailyBuckets,
    hourlyBuckets,
    focusModels,
    allModels,
    dailyCostChart,
    hourlyTokenChart,
    hourlyTokenTotal,
    providerRows,
    topCostModelRows,
    heatmapMetricMax,
    summaryStats,
    overviewSignals,
    averageCostPerCall,
    averageTokensPerCall,
    peakDailyBucket,
    bucketRate,
    shareWidth,
    metricValue,
    bucketIntensity,
    formatHeatmapValue,
    modelKey,
    modelName,
    providerName,
    timelineLabel,
    formatBucketHeader,
  }
}

export const bucketRate = (hits: number, total: number) =>
  total > 0 ? hits / total : 0

export const shareWidth = (share: number) => {
  if (share <= 0) return '0%'
  return `${Math.min(Math.max(share * 100, 4), 100)}%`
}

export const modelKey = (model: Pick<CostAnalysisModel, 'providerId' | 'modelId'>) =>
  `${model.providerId}:${model.modelId}`
export const modelName = (
  model: Pick<CostAnalysisModel, 'modelDisplayName' | 'modelId'>,
) => model.modelDisplayName || model.modelId
export const providerName = (
  model: Pick<CostAnalysisModel, 'providerDisplayName' | 'providerId'>,
) => model.providerDisplayName || model.providerId
