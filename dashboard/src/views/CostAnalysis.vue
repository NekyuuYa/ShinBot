<template>
  <v-container fluid class="pa-0 cost-analysis-page">
    <app-page-header :title="$t('pages.costAnalysis.title')" :subtitle="$t('pages.costAnalysis.subtitle')"
      :kicker="$t('pages.costAnalysis.kicker')">
      <template #actions>
        <v-btn-toggle v-model="activeWindow" mandatory density="comfortable" class="window-toggle">
          <v-btn v-for="option in windowOptions" :key="option.value" :value="option.value" rounded="lg">
            {{ option.label }}
          </v-btn>
        </v-btn-toggle>

        <v-btn color="primary" variant="tonal" prepend-icon="mdi-refresh" rounded="lg" :loading="isLoading"
          @click="refreshPage">
          {{ $t('pages.costAnalysis.actions.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert v-if="error" type="error" variant="tonal" class="mb-6">
      {{ error }}
    </v-alert>

    <v-progress-linear v-if="isLoading" indeterminate color="primary" rounded class="mb-6" />

    <!-- Metric Summary Cards -->
    <v-row class="mb-8" dense>
      <v-col v-for="card in summaryCards" :key="card.key" cols="12" sm="6" md="4" lg="" class="d-flex">
        <metric-card :icon="card.icon" :label="card.label" :value="card.value" :meta="card.meta" :tone="card.tone" />
      </v-col>
    </v-row>

    <!-- Timeline & Load Analysis -->
    <section class="analysis-panel mb-8">
      <div class="panel-head">
        <div>
          <div class="panel-kicker">{{ $t('pages.costAnalysis.sections.timeline.kicker') }}</div>
          <h2 class="panel-title">{{ $t('pages.costAnalysis.sections.timeline.title') }}</h2>
          <p class="panel-subtitle">{{ $t('pages.costAnalysis.sections.timeline.subtitle') }}</p>
        </div>

        <div class="panel-meta">
          <v-chip variant="tonal" color="primary" size="small">
            {{ $t('pages.costAnalysis.labels.dailySince', { value: formatDateRangeStart(analysis?.since ?? null) }) }}
          </v-chip>
          <v-chip variant="tonal" color="info" size="small">
            {{ $t('pages.costAnalysis.labels.hourlySince', { value: formatDateTime(analysis?.hourlySince ?? null) }) }}
          </v-chip>
        </div>
      </div>

      <v-row class="ma-0" align="stretch">
        <v-col cols="12" lg="7" class="pa-0 pe-lg-4 pb-4 pb-lg-0">
          <div class="trend-panel">
            <div class="trend-panel__head">
              <div>
                <div class="trend-panel__title">{{ $t('pages.costAnalysis.charts.dailyCostTitle') }}</div>
                <div class="trend-panel__caption">{{ $t('pages.costAnalysis.charts.dailyCostSubtitle') }}</div>
              </div>
              <div class="trend-panel__stat">
                {{ formatCurrency(summary.estimatedCost) }}
              </div>
            </div>

            <div class="timeline-chart" :class="{ 'timeline-chart--sparse': dailyBuckets.length <= 7 }"
              :style="{ '--timeline-bucket-count': String(Math.max(dailyBuckets.length, 1)) }">
              <div v-for="(bucket, index) in dailyBuckets" :key="bucket.bucketStart" class="timeline-column">
                <v-tooltip location="top">
                  <template #activator="{ props }">
                    <div v-bind="props" class="timeline-hit-area">
                      <div class="timeline-bar timeline-bar--cost" :style="{
                        '--timeline-height': `${bucketHeight(bucket.estimatedCost, dailyCostMax)}%`,
                      }" />
                    </div>
                  </template>

                  <div class="tooltip-stack">
                    <div class="font-weight-medium">{{ formatDate(bucket.bucketStart) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(bucket.estimatedCost) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(bucket.totalCalls) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(bucket.totalTokens) }}
                    </div>
                    <div>{{ $t('pages.costAnalysis.table.cacheHitRate') }}: {{
                      formatPercent(bucketRate(bucket.cacheHits,
                      bucket.totalCalls)) }}</div>
                  </div>
                </v-tooltip>

                <div class="timeline-label">
                  {{ timelineLabel(index, dailyBuckets.length, bucket.bucketStart, 'daily') }}
                </div>
              </div>
            </div>
          </div>
        </v-col>

        <v-col cols="12" lg="5" class="pa-0">
          <div class="trend-panel">
            <div class="trend-panel__head">
              <div>
                <div class="trend-panel__title">{{ $t('pages.costAnalysis.charts.hourlyLoadTitle') }}</div>
                <div class="trend-panel__caption">{{ $t('pages.costAnalysis.charts.hourlyLoadSubtitle') }}</div>
              </div>
              <div class="trend-panel__stat">
                {{ formatCompactNumber(hourlyTokenTotal) }}
              </div>
            </div>

            <div class="timeline-chart timeline-chart--compact"
              :style="{ '--timeline-bucket-count': String(Math.max(hourlyBuckets.length, 1)) }">
              <div v-for="(bucket, index) in hourlyBuckets" :key="bucket.bucketStart" class="timeline-column">
                <v-tooltip location="top">
                  <template #activator="{ props }">
                    <div v-bind="props" class="timeline-hit-area">
                      <div class="timeline-bar timeline-bar--tokens" :style="{
                        '--timeline-height': `${bucketHeight(bucket.totalTokens, hourlyTokenMax)}%`,
                      }" />
                    </div>
                  </template>

                  <div class="tooltip-stack">
                    <div class="font-weight-medium">{{ formatDateTime(bucket.bucketStart) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(bucket.totalTokens) }}
                    </div>
                    <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(bucket.totalCalls) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.cacheWrites') }}: {{
                      formatCompactNumber(bucket.cacheWriteTokens)
                      }}</div>
                    <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(bucket.estimatedCost) }}</div>
                  </div>
                </v-tooltip>

                <div class="timeline-label">
                  {{ timelineLabel(index, hourlyBuckets.length, bucket.bucketStart, 'hourly') }}
                </div>
              </div>
            </div>
          </div>
        </v-col>
      </v-row>
    </section>

    <!-- Detailed Intensity Heatmap -->
    <section class="analysis-panel mb-8">
      <div class="panel-head panel-head--dense">
        <div>
          <div class="panel-kicker">{{ $t('pages.costAnalysis.sections.focus.kicker') }}</div>
          <h2 class="panel-title">{{ $t('pages.costAnalysis.sections.focus.title') }}</h2>
          <p class="panel-subtitle">{{ $t('pages.costAnalysis.sections.focus.subtitle') }}</p>
        </div>

        <div class="focus-toolbar">
          <v-btn-toggle v-model="focusGranularity" mandatory density="comfortable" class="focus-toggle">
            <v-btn value="daily" rounded="lg">
              {{ $t('pages.costAnalysis.actions.daily') }}
            </v-btn>
            <v-btn value="hourly" rounded="lg">
              {{ $t('pages.costAnalysis.actions.hourly') }}
            </v-btn>
          </v-btn-toggle>

          <v-btn-toggle v-model="focusMetric" mandatory density="comfortable" class="focus-toggle">
            <v-btn v-for="metric in focusMetricOptions" :key="metric.value" :value="metric.value" rounded="lg">
              {{ metric.label }}
            </v-btn>
          </v-btn-toggle>
        </div>
      </div>

      <div v-if="focusModels.length > 0" class="heatmap-shell">
        <div class="heatmap-grid" :style="{ '--heatmap-columns': String(Math.max(focusBuckets.length, 1)) }">
          <div class="heatmap-corner sticky-col">
            <div class="font-weight-medium">{{ $t('pages.costAnalysis.table.model') }}</div>
            <div class="text-caption text-medium-emphasis">
              {{ $t('pages.costAnalysis.labels.focusModels', { count: focusModels.length }) }}
            </div>
          </div>

          <div v-for="bucket in focusBuckets" :key="`header-${bucket.bucketStart}`" class="heatmap-header">
            {{ formatBucketHeader(bucket.bucketStart, focusGranularity) }}
          </div>

          <template v-for="model in focusModels" :key="model.modelId">
            <div class="heatmap-model sticky-col">
              <div class="heatmap-model__title">{{ model.modelDisplayName || model.modelId }}</div>
              <div class="heatmap-model__meta">
                {{ model.providerDisplayName || model.providerId }}
              </div>
            </div>

            <v-tooltip v-for="bucket in model[focusGranularity]" :key="`${model.modelId}-${bucket.bucketStart}`"
              location="top">
              <template #activator="{ props }">
                <div v-bind="props" class="heatmap-cell" :class="`heatmap-cell--${focusMetric}`"
                  :style="{ '--cell-intensity': String(bucketIntensity(bucket)) }">
                  <span>{{ formatHeatmapValue(bucket) }}</span>
                </div>
              </template>

              <div class="tooltip-stack">
                <div class="font-weight-medium">{{ model.modelDisplayName || model.modelId }}</div>
                <div>{{ formatDateTime(bucket.bucketStart) }}</div>
                <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(bucket.totalCalls) }}</div>
                <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(bucket.totalTokens) }}
                </div>
                <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(bucket.estimatedCost) }}</div>
                <div>{{ $t('pages.costAnalysis.table.cacheReads') }}: {{ formatCompactNumber(bucket.cacheReadTokens) }}
                </div>
                <div>{{ $t('pages.costAnalysis.table.cacheWrites') }}: {{ formatCompactNumber(bucket.cacheWriteTokens)
                  }}</div>
              </div>
            </v-tooltip>
          </template>
        </div>
      </div>

      <v-empty-state v-else icon="mdi-chart-box-outline" :title="$t('pages.costAnalysis.emptyState.title')"
        :text="$t('pages.costAnalysis.emptyState.subtitle')" variant="plain" />
    </section>

    <!-- Raw Data Table -->
    <section class="analysis-panel">
      <div class="panel-head panel-head--dense">
        <div>
          <div class="panel-kicker">{{ $t('pages.costAnalysis.sections.table.kicker') }}</div>
          <h2 class="panel-title">{{ $t('pages.costAnalysis.sections.table.title') }}</h2>
          <p class="panel-subtitle">{{ $t('pages.costAnalysis.sections.table.subtitle') }}</p>
        </div>

        <div class="panel-meta">
          <v-chip variant="tonal" color="secondary" size="small">
            {{ $t('pages.costAnalysis.labels.modelCount', { count: allModels.length }) }}
          </v-chip>
        </div>
      </div>

      <div v-if="allModels.length > 0" class="table-shell">
        <v-table fixed-header height="560" density="comfortable" class="analysis-table">
          <thead>
            <tr>
              <th>{{ $t('pages.costAnalysis.table.model') }}</th>
              <th>{{ $t('pages.costAnalysis.table.cost') }}</th>
              <th>{{ $t('pages.costAnalysis.table.calls') }}</th>
              <th>{{ $t('pages.costAnalysis.table.successRate') }}</th>
              <th>{{ $t('pages.costAnalysis.table.cacheHitRate') }}</th>
              <th>{{ $t('pages.costAnalysis.table.inputTokens') }}</th>
              <th>{{ $t('pages.costAnalysis.table.outputTokens') }}</th>
              <th>{{ $t('pages.costAnalysis.table.cacheReads') }}</th>
              <th>{{ $t('pages.costAnalysis.table.cacheWrites') }}</th>
              <th>{{ $t('pages.costAnalysis.table.avgLatency') }}</th>
              <th>{{ $t('pages.costAnalysis.table.lastSeen') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="model in allModels" :key="model.modelId">
              <td>
                <div class="model-cell">
                  <div class="model-cell__title">{{ model.modelDisplayName || model.modelId }}</div>
                  <div class="model-cell__meta">{{ model.providerDisplayName || model.providerId }}</div>
                </div>
              </td>
              <td>{{ formatCurrency(model.estimatedCost) }}</td>
              <td>{{ formatNumber(model.totalCalls) }}</td>
              <td>
                <v-chip size="small" variant="tonal" color="info">{{ formatPercent(model.successRate) }}</v-chip>
              </td>
              <td>
                <v-chip size="small" variant="tonal" color="success">{{ formatPercent(model.cacheHitRate) }}</v-chip>
              </td>
              <td>{{ formatCompactNumber(model.inputTokens) }}</td>
              <td>{{ formatCompactNumber(model.outputTokens) }}</td>
              <td>{{ formatCompactNumber(model.cacheReadTokens) }}</td>
              <td>{{ formatCompactNumber(model.cacheWriteTokens) }}</td>
              <td>{{ formatDuration(model.averageLatencyMs) }}</td>
              <td>{{ formatDateTime(model.lastSeenAt) }}</td>
            </tr>
          </tbody>
        </v-table>
      </div>
    </section>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useI18n } from 'vue-i18n'
import AppPageHeader from '@/components/AppPageHeader.vue'
import MetricCard from '@/components/analysis/MetricCard.vue'
import {
  COST_ANALYSIS_WINDOWS,
  type CostAnalysisWindow,
  useCostAnalysisStore,
} from '@/stores/costAnalysis'
import { useSystemSettingsStore } from '@/stores/systemSettings'
import type { CostAnalysisBucket } from '@/api/modelRuntime'

type FocusGranularity = 'daily' | 'hourly'
type FocusMetric = 'tokens' | 'cost' | 'calls' | 'cache'

const costAnalysisStore = useCostAnalysisStore()
const systemSettingsStore = useSystemSettingsStore()
const { analysis, error, hasData, isLoading, selectedDays } = storeToRefs(costAnalysisStore)
const { locale, t } = useI18n()

const focusGranularity = ref<FocusGranularity>('daily')
const focusMetric = ref<FocusMetric>('tokens')

const activeWindow = computed<CostAnalysisWindow>({
  get: () => selectedDays.value,
  set: (value) => {
    if (!value || value === selectedDays.value) return
    costAnalysisStore.setSelectedDays(value)
    void costAnalysisStore.fetchAnalysis(value)
  },
})

const windowOptions = computed(() =>
  COST_ANALYSIS_WINDOWS.map((value) => ({
    value,
    label: t('pages.costAnalysis.windowOption', { days: value }),
  }))
)

const focusMetricOptions = computed(() => [
  { value: 'tokens' as const, label: t('pages.costAnalysis.metrics.tokens') },
  { value: 'cost' as const, label: t('pages.costAnalysis.metrics.cost') },
  { value: 'calls' as const, label: t('pages.costAnalysis.metrics.calls') },
  { value: 'cache' as const, label: t('pages.costAnalysis.metrics.cache') },
])

const summary = computed(() => analysis.value?.summary ?? {
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
  averageLatencyMs: null,
  averageTimeToFirstTokenMs: null,
})

const dailyBuckets = computed(() => analysis.value?.timeline.daily ?? [])
const hourlyBuckets = computed(() => analysis.value?.timeline.hourly ?? [])
const focusModels = computed(() => analysis.value?.focusModels ?? [])
const allModels = computed(() => analysis.value?.models ?? [])

const focusBuckets = computed(() => {
  const firstModel = focusModels.value[0]
  return firstModel ? firstModel[focusGranularity.value] : []
})

const dailyCostMax = computed(() => Math.max(...dailyBuckets.value.map((b) => b.estimatedCost), 0.01))
const hourlyTokenMax = computed(() => Math.max(...hourlyBuckets.value.map((b) => b.totalTokens), 1))
const hourlyTokenTotal = computed(() => hourlyBuckets.value.reduce((s, b) => s + b.totalTokens, 0))

const heatmapMetricMax = computed(() => {
  if (focusMetric.value === 'cache') return 1
  const values = focusModels.value.flatMap((m) => m[focusGranularity.value].map(metricValue))
  return Math.max(...values, 1)
})

const summaryCards = computed(() => [
  {
    key: 'cost', tone: 'warning' as const, icon: 'mdi-currency-usd',
    label: t('pages.costAnalysis.summary.totalCost'), value: formatCurrency(summary.value.estimatedCost),
    meta: `${formatCompactNumber(summary.value.totalCalls)} ${t('pages.costAnalysis.summary.callsSuffix')}`,
  },
  {
    key: 'tokens', tone: 'primary' as const, icon: 'mdi-counter',
    label: t('pages.costAnalysis.summary.totalTokens'), value: formatCompactNumber(summary.value.totalTokens),
    meta: `${formatCompactNumber(summary.value.inputTokens)} / ${formatCompactNumber(summary.value.outputTokens)}`,
  },
  {
    key: 'success', tone: 'info' as const, icon: 'mdi-check-decagram-outline',
    label: t('pages.costAnalysis.summary.successRate'), value: formatPercent(summary.value.successRate),
    meta: t('pages.costAnalysis.summary.failedCalls', { count: summary.value.failedCalls }),
  },
  {
    key: 'cache', tone: 'success' as const, icon: 'mdi-database-sync-outline',
    label: t('pages.costAnalysis.summary.cacheHitRate'), value: formatPercent(summary.value.cacheHitRate),
    meta: `${formatCompactNumber(summary.value.cacheReadTokens)} / ${formatCompactNumber(summary.value.cacheWriteTokens)}`,
  },
  {
    key: 'latency', tone: 'secondary' as const, icon: 'mdi-timer-sand',
    label: t('pages.costAnalysis.summary.avgLatency'), value: formatDuration(summary.value.averageLatencyMs),
    meta: t('pages.costAnalysis.summary.ttft', { value: formatDuration(summary.value.averageTimeToFirstTokenMs) }),
  },
])

const refreshPage = () => void costAnalysisStore.fetchAnalysis(selectedDays.value)
const formatNumber = (v: number) => new Intl.NumberFormat(locale.value, { maximumFractionDigits: 0 }).format(v)
const formatCompactNumber = (v: number) => new Intl.NumberFormat(locale.value, { maximumFractionDigits: v >= 1000 ? 1 : 0, notation: v >= 1000 ? 'compact' : 'standard' }).format(v)
const formatCurrency = (v: number) => new Intl.NumberFormat(locale.value, { style: 'currency', currency: systemSettingsStore.pricingCurrency || analysis.value?.currency || 'CNY', minimumFractionDigits: v >= 100 ? 0 : 2, maximumFractionDigits: v >= 100 ? 0 : 2 }).format(v)
const formatPercent = (v: number) => new Intl.NumberFormat(locale.value, { style: 'percent', minimumFractionDigits: 0, maximumFractionDigits: 1 }).format(v)
const formatDate = (v: string | null) => v ? new Intl.DateTimeFormat(locale.value, { month: 'short', day: 'numeric' }).format(new Date(v)) : '—'
const formatDateRangeStart = (v: string | null) => v ? new Intl.DateTimeFormat(locale.value, { year: 'numeric', month: 'short', day: 'numeric' }).format(new Date(v)) : '—'
const formatDateTime = (v: string | null) => v ? new Intl.DateTimeFormat(locale.value, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }).format(new Date(v)) : '—'
const formatDuration = (v: number | null) => v === null || Number.isNaN(v) ? '—' : (v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${Math.round(v)}ms`)
const bucketRate = (h: number, t: number) => (t > 0 ? h / t : 0)
const bucketHeight = (v: number, m: number) => v <= 0 || m <= 0 ? 0 : Math.max((v / m) * 100, 12)
const metricValue = (b: CostAnalysisBucket) => {
  if (focusMetric.value === 'cost') return b.estimatedCost
  if (focusMetric.value === 'calls') return b.totalCalls
  if (focusMetric.value === 'cache') return bucketRate(b.cacheHits, b.totalCalls)
  return b.totalTokens
}
const bucketIntensity = (b: CostAnalysisBucket) => {
  const v = metricValue(b), m = heatmapMetricMax.value
  return !v || m <= 0 ? 0.08 : Math.min(0.18 + (v / m) * 0.72, 0.92)
}
const formatHeatmapValue = (b: CostAnalysisBucket) => {
  if (focusMetric.value === 'cost') return formatCurrency(b.estimatedCost)
  if (focusMetric.value === 'calls') return formatNumber(b.totalCalls)
  if (focusMetric.value === 'cache') return formatPercent(bucketRate(b.cacheHits, b.totalCalls))
  return formatCompactNumber(b.totalTokens)
}

const timelineLabel = (index: number, length: number, value: string, granularity: FocusGranularity) => {
  const date = new Date(value)
  if (granularity === 'hourly') {
    return (index % 4 !== 0 && index !== length - 1) ? '' : new Intl.DateTimeFormat(locale.value, { hour: '2-digit' }).format(date)
  }
  const divider = length > 45 ? 9 : length > 21 ? 5 : 1
  return (index % divider !== 0 && index !== length - 1) ? '' : new Intl.DateTimeFormat(locale.value, { month: 'short', day: 'numeric' }).format(date)
}

const formatBucketHeader = (value: string, granularity: FocusGranularity) => {
  const date = new Date(value)
  return new Intl.DateTimeFormat(locale.value, granularity === 'hourly' ? { hour: '2-digit' } : { month: 'short', day: 'numeric' }).format(date)
}

onMounted(() => { if (!hasData.value) void costAnalysisStore.fetchAnalysis(selectedDays.value) })
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.window-toggle,
.focus-toggle {
  padding: 2px;
  overflow: visible;
}

.window-toggle :deep(.v-btn),
.focus-toggle :deep(.v-btn) {
  border-radius: 14px;
  margin: 2px;
}

.analysis-panel {
  @include analysis-section-panel;
}

.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 24px;

  &--dense {
    margin-bottom: 18px;
  }
}

.panel-subtitle {
  margin: 10px 0 0;
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-size: 0.95rem;
  line-height: 1.6;
}

.panel-meta,
.focus-toolbar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12px;
  flex-wrap: wrap;
}

.trend-panel {
  display: flex;
  flex-direction: column;
  gap: 18px;
  min-height: 360px;
  padding: 20px;
  border-radius: 18px;
  background: rgba(var(--v-theme-on-surface), 0.02);
  border: 1px solid $border-color-soft;


  &__head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }

  &__title {
    color: rgba(var(--v-theme-on-surface), 0.92);
    font-size: 1rem;
    font-weight: 700;
  }

  &__caption {
    margin-top: 6px;
    color: rgba(var(--v-theme-on-surface), 0.64);
    font-size: 0.88rem;
  }

  &__stat {
    color: rgba(var(--v-theme-on-surface), 0.88);
    font-size: 1.1rem;
    font-weight: 700;
  }
}

.timeline-chart {
  @include timeline-chart-grid;
}

.timeline-column {
  display: grid;
  grid-template-rows: minmax(0, 1fr) 18px;
  gap: 8px;
  height: 100%;
  min-width: 0;
}

.timeline-hit-area {
  position: relative;
  width: 100%;
  height: 100%;
  border-radius: 999px;
  background: rgba(var(--v-theme-on-surface), 0.045);

  &:hover .timeline-bar {
    filter: brightness(1.08);
  }
}

.timeline-bar {
  position: absolute;
  bottom: 0;
  width: 100%;
  height: var(--timeline-height);
  border-radius: 999px;
  transition: height 0.2s ease;

  &--cost {
    background: linear-gradient(180deg, rgba(var(--v-theme-warning), 0.4) 0%, rgba(var(--v-theme-warning), 0.95) 100%);
  }

  &--tokens {
    background: linear-gradient(180deg, rgba(var(--v-theme-info), 0.38) 0%, rgba(var(--v-theme-primary), 0.92) 100%);
  }
}

.timeline-label {
  min-height: 16px;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: 0.7rem;
  text-align: center;
}

.heatmap-shell {
  overflow-x: auto;
  padding-bottom: 6px;
}

.heatmap-grid {
  @include heatmap-grid-layout;
}

.heatmap-header,
.heatmap-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 64px;
  border-radius: 14px;
  border: 1px solid $border-color-soft;
  text-align: center;
}

.heatmap-header {
  color: rgba(var(--v-theme-on-surface), 0.64);
  font-size: 0.76rem;
  font-weight: 700;
  background: rgba(var(--v-theme-on-surface), 0.02);
}

.heatmap-model {
  &__title {
    color: rgba(var(--v-theme-on-surface), 0.92);
    font-weight: 700;
  }

  &__meta {
    margin-top: 4px;
    color: rgba(var(--v-theme-on-surface), 0.6);
    font-size: 0.82rem;
  }
}

.heatmap-cell {
  padding: 8px;
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: 0.76rem;
  font-weight: 700;

  &--tokens {
    background: rgba(var(--v-theme-primary), var(--cell-intensity, 0.1));
  }

  &--cost {
    background: rgba(var(--v-theme-warning), var(--cell-intensity, 0.1));
  }

  &--calls {
    background: rgba(var(--v-theme-info), var(--cell-intensity, 0.1));
  }

  &--cache {
    background: rgba(var(--v-theme-success), var(--cell-intensity, 0.1));
  }
}

.table-shell {
  overflow: hidden;
  border-radius: 18px;
  border: 1px solid $border-color-soft;
}

.analysis-table {
  background: transparent;
}

.model-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 220px;

  &__title {
    color: rgba(var(--v-theme-on-surface), 0.94);
    font-weight: 700;
  }

  &__meta {
    color: rgba(var(--v-theme-on-surface), 0.62);
    font-size: 0.82rem;
  }
}

.tooltip-stack {
  display: grid;
  gap: 4px;
}

@include respond-to('tablet') {
  .analysis-panel {
    padding: 20px;
    border-radius: 20px;
  }

  .panel-head {
    flex-direction: column;
  }

  .panel-meta,
  .focus-toolbar {
    width: 100%;
    justify-content: flex-start;
  }

  .trend-panel {
    min-height: auto;
  }

  .heatmap-grid {
    grid-template-columns: 220px repeat(var(--heatmap-columns, 1), minmax(68px, 1fr));
  }
}
</style>
