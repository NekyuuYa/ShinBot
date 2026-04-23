<template>
  <v-container fluid class="pa-0 cost-analysis-page">
    <app-page-header
      :title="$t('pages.costAnalysis.title')"
      :subtitle="$t('pages.costAnalysis.subtitle')"
      :kicker="$t('pages.costAnalysis.kicker')"
    >
      <template #actions>
        <v-btn-toggle
          v-model="activeWindow"
          mandatory
          density="comfortable"
          class="window-toggle"
        >
          <v-btn
            v-for="option in windowOptions"
            :key="option.value"
            :value="option.value"
            rounded="lg"
          >
            {{ option.label }}
          </v-btn>
        </v-btn-toggle>

        <v-btn
          color="primary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          rounded="lg"
          :loading="isLoading"
          @click="refreshPage"
        >
          {{ $t('pages.costAnalysis.actions.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert
      v-if="error"
      type="error"
      variant="tonal"
      class="mb-6"
    >
      {{ error }}
    </v-alert>

    <v-progress-linear
      v-if="isLoading"
      indeterminate
      color="primary"
      rounded
      class="mb-6"
    />

    <v-row class="metric-grid mb-8">
      <v-col
        v-for="card in summaryCards"
        :key="card.key"
        cols="12"
        sm="6"
        xl="2"
        class="d-flex"
      >
        <article class="metric-card flex-grow-1">
          <div class="metric-card__icon" :class="`metric-card__icon--${card.tone}`">
            <v-icon :icon="card.icon" size="22" />
          </div>
          <div class="metric-card__label">{{ card.label }}</div>
          <div class="metric-card__value">{{ card.value }}</div>
          <div class="metric-card__meta">{{ card.meta }}</div>
        </article>
      </v-col>
    </v-row>

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

            <div
              class="timeline-chart"
              :class="{ 'timeline-chart--sparse': dailyBuckets.length <= 7 }"
              :style="{ '--timeline-bucket-count': String(Math.max(dailyBuckets.length, 1)) }"
            >
              <div
                v-for="(bucket, index) in dailyBuckets"
                :key="bucket.bucketStart"
                class="timeline-column"
              >
                <v-tooltip location="top">
                  <template #activator="{ props }">
                    <div
                      v-bind="props"
                      class="timeline-hit-area"
                    >
                      <div
                        class="timeline-bar timeline-bar--cost"
                        :style="{
                          '--timeline-height': `${bucketHeight(bucket.estimatedCost, dailyCostMax)}%`,
                        }"
                      />
                    </div>
                  </template>

                  <div class="tooltip-stack">
                    <div class="font-weight-medium">{{ formatDate(bucket.bucketStart) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(bucket.estimatedCost) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(bucket.totalCalls) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(bucket.totalTokens) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.cacheHitRate') }}: {{ formatPercent(bucketRate(bucket.cacheHits, bucket.totalCalls)) }}</div>
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

            <div
              class="timeline-chart timeline-chart--compact"
              :style="{ '--timeline-bucket-count': String(Math.max(hourlyBuckets.length, 1)) }"
            >
              <div
                v-for="(bucket, index) in hourlyBuckets"
                :key="bucket.bucketStart"
                class="timeline-column"
              >
                <v-tooltip location="top">
                  <template #activator="{ props }">
                    <div
                      v-bind="props"
                      class="timeline-hit-area"
                    >
                      <div
                        class="timeline-bar timeline-bar--tokens"
                        :style="{
                          '--timeline-height': `${bucketHeight(bucket.totalTokens, hourlyTokenMax)}%`,
                        }"
                      />
                    </div>
                  </template>

                  <div class="tooltip-stack">
                    <div class="font-weight-medium">{{ formatDateTime(bucket.bucketStart) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(bucket.totalTokens) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(bucket.totalCalls) }}</div>
                    <div>{{ $t('pages.costAnalysis.table.cacheWrites') }}: {{ formatCompactNumber(bucket.cacheWriteTokens) }}</div>
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

    <section class="analysis-panel mb-8">
      <div class="panel-head panel-head--dense">
        <div>
          <div class="panel-kicker">{{ $t('pages.costAnalysis.sections.focus.kicker') }}</div>
          <h2 class="panel-title">{{ $t('pages.costAnalysis.sections.focus.title') }}</h2>
          <p class="panel-subtitle">{{ $t('pages.costAnalysis.sections.focus.subtitle') }}</p>
        </div>

        <div class="focus-toolbar">
          <v-btn-toggle
            v-model="focusGranularity"
            mandatory
            density="comfortable"
            class="focus-toggle"
          >
            <v-btn value="daily" rounded="lg">
              {{ $t('pages.costAnalysis.actions.daily') }}
            </v-btn>
            <v-btn value="hourly" rounded="lg">
              {{ $t('pages.costAnalysis.actions.hourly') }}
            </v-btn>
          </v-btn-toggle>

          <v-btn-toggle
            v-model="focusMetric"
            mandatory
            density="comfortable"
            class="focus-toggle"
          >
            <v-btn
              v-for="metric in focusMetricOptions"
              :key="metric.value"
              :value="metric.value"
              rounded="lg"
            >
              {{ metric.label }}
            </v-btn>
          </v-btn-toggle>
        </div>
      </div>

      <div v-if="focusModels.length > 0" class="heatmap-shell">
        <div
          class="heatmap-grid"
          :style="{ '--heatmap-columns': String(Math.max(focusBuckets.length, 1)) }"
        >
          <div class="heatmap-corner">
            <div class="font-weight-medium">{{ $t('pages.costAnalysis.table.model') }}</div>
            <div class="text-caption text-medium-emphasis">
              {{ $t('pages.costAnalysis.labels.focusModels', { count: focusModels.length }) }}
            </div>
          </div>

          <div
            v-for="bucket in focusBuckets"
            :key="`header-${bucket.bucketStart}`"
            class="heatmap-header"
          >
            {{ formatBucketHeader(bucket.bucketStart, focusGranularity) }}
          </div>

          <template v-for="model in focusModels" :key="model.modelId">
            <div class="heatmap-model">
              <div class="heatmap-model__title">{{ model.modelDisplayName || model.modelId }}</div>
              <div class="heatmap-model__meta">
                {{ model.providerDisplayName || model.providerId }}
              </div>
            </div>

            <v-tooltip
              v-for="bucket in model[focusGranularity]"
              :key="`${model.modelId}-${bucket.bucketStart}`"
              location="top"
            >
              <template #activator="{ props }">
                <div
                  v-bind="props"
                  class="heatmap-cell"
                  :class="`heatmap-cell--${focusMetric}`"
                  :style="{ '--cell-intensity': String(bucketIntensity(bucket)) }"
                >
                  <span>{{ formatHeatmapValue(bucket) }}</span>
                </div>
              </template>

              <div class="tooltip-stack">
                <div class="font-weight-medium">
                  {{ model.modelDisplayName || model.modelId }}
                </div>
                <div>{{ formatDateTime(bucket.bucketStart) }}</div>
                <div>{{ $t('pages.costAnalysis.table.calls') }}: {{ formatNumber(bucket.totalCalls) }}</div>
                <div>{{ $t('pages.costAnalysis.table.totalTokens') }}: {{ formatCompactNumber(bucket.totalTokens) }}</div>
                <div>{{ $t('pages.costAnalysis.table.cost') }}: {{ formatCurrency(bucket.estimatedCost) }}</div>
                <div>{{ $t('pages.costAnalysis.table.cacheReads') }}: {{ formatCompactNumber(bucket.cacheReadTokens) }}</div>
                <div>{{ $t('pages.costAnalysis.table.cacheWrites') }}: {{ formatCompactNumber(bucket.cacheWriteTokens) }}</div>
              </div>
            </v-tooltip>
          </template>
        </div>
      </div>

      <v-empty-state
        v-else
        icon="mdi-chart-box-outline"
        :title="$t('pages.costAnalysis.emptyState.title')"
        :text="$t('pages.costAnalysis.emptyState.subtitle')"
        variant="plain"
      />
    </section>

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
                  <div class="model-cell__code">{{ model.modelId }}</div>
                </div>
              </td>
              <td>{{ formatCurrency(model.estimatedCost) }}</td>
              <td>{{ formatNumber(model.totalCalls) }}</td>
              <td>
                <v-chip size="small" variant="tonal" color="info">
                  {{ formatPercent(model.successRate) }}
                </v-chip>
              </td>
              <td>
                <v-chip size="small" variant="tonal" color="success">
                  {{ formatPercent(model.cacheHitRate) }}
                </v-chip>
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

      <v-empty-state
        v-else
        icon="mdi-currency-usd-off"
        :title="$t('pages.costAnalysis.emptyState.title')"
        :text="$t('pages.costAnalysis.emptyState.subtitle')"
        variant="plain"
      />
    </section>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useI18n } from 'vue-i18n'
import AppPageHeader from '@/components/AppPageHeader.vue'
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
    if (!value || value === selectedDays.value) {
      return
    }
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
  if (!firstModel) {
    return []
  }
  return firstModel[focusGranularity.value]
})

const dailyCostMax = computed(() =>
  Math.max(...dailyBuckets.value.map((bucket) => bucket.estimatedCost), 0.01)
)

const hourlyTokenMax = computed(() =>
  Math.max(...hourlyBuckets.value.map((bucket) => bucket.totalTokens), 1)
)

const hourlyTokenTotal = computed(() =>
  hourlyBuckets.value.reduce((total, bucket) => total + bucket.totalTokens, 0)
)

const heatmapMetricMax = computed(() => {
  if (focusMetric.value === 'cache') {
    return 1
  }

  const values = focusModels.value.flatMap((model) =>
    model[focusGranularity.value].map((bucket) => metricValue(bucket))
  )
  return Math.max(...values, 1)
})

const summaryCards = computed(() => [
  {
    key: 'cost',
    tone: 'warning',
    icon: 'mdi-currency-usd',
    label: t('pages.costAnalysis.summary.totalCost'),
    value: formatCurrency(summary.value.estimatedCost),
    meta: `${formatCompactNumber(summary.value.totalCalls)} ${t('pages.costAnalysis.summary.callsSuffix')}`,
  },
  {
    key: 'tokens',
    tone: 'primary',
    icon: 'mdi-counter',
    label: t('pages.costAnalysis.summary.totalTokens'),
    value: formatCompactNumber(summary.value.totalTokens),
    meta: `${formatCompactNumber(summary.value.inputTokens)} / ${formatCompactNumber(summary.value.outputTokens)}`,
  },
  {
    key: 'success',
    tone: 'info',
    icon: 'mdi-check-decagram-outline',
    label: t('pages.costAnalysis.summary.successRate'),
    value: formatPercent(summary.value.successRate),
    meta: t('pages.costAnalysis.summary.failedCalls', { count: summary.value.failedCalls }),
  },
  {
    key: 'cache',
    tone: 'success',
    icon: 'mdi-database-sync-outline',
    label: t('pages.costAnalysis.summary.cacheHitRate'),
    value: formatPercent(summary.value.cacheHitRate),
    meta: `${formatCompactNumber(summary.value.cacheReadTokens)} / ${formatCompactNumber(summary.value.cacheWriteTokens)}`,
  },
  {
    key: 'latency',
    tone: 'secondary',
    icon: 'mdi-timer-sand',
    label: t('pages.costAnalysis.summary.avgLatency'),
    value: formatDuration(summary.value.averageLatencyMs),
    meta: t('pages.costAnalysis.summary.ttft', {
      value: formatDuration(summary.value.averageTimeToFirstTokenMs),
    }),
  },
])

const refreshPage = () => {
  void costAnalysisStore.fetchAnalysis(selectedDays.value)
}

const formatNumber = (value: number) =>
  new Intl.NumberFormat(locale.value, { maximumFractionDigits: 0 }).format(value)

const formatCompactNumber = (value: number) =>
  new Intl.NumberFormat(locale.value, {
    maximumFractionDigits: value >= 1000 ? 1 : 0,
    notation: value >= 1000 ? 'compact' : 'standard',
  }).format(value)

const formatCurrency = (value: number) =>
  new Intl.NumberFormat(locale.value, {
    style: 'currency',
    currency: systemSettingsStore.pricingCurrency || analysis.value?.currency || 'CNY',
    minimumFractionDigits: value >= 100 ? 0 : 2,
    maximumFractionDigits: value >= 100 ? 0 : 2,
  }).format(value)

const formatPercent = (value: number) =>
  new Intl.NumberFormat(locale.value, {
    style: 'percent',
    minimumFractionDigits: 0,
    maximumFractionDigits: 1,
  }).format(value)

const formatDate = (value: string | null) => {
  if (!value) return '—'
  return new Intl.DateTimeFormat(locale.value, {
    month: 'short',
    day: 'numeric',
  }).format(new Date(value))
}

const formatDateRangeStart = (value: string | null) => {
  if (!value) return '—'
  return new Intl.DateTimeFormat(locale.value, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  }).format(new Date(value))
}

const formatDateTime = (value: string | null) => {
  if (!value) return '—'
  return new Intl.DateTimeFormat(locale.value, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value))
}

const formatDuration = (value: number | null) => {
  if (value === null || Number.isNaN(value)) return '—'
  if (value >= 1000) return `${(value / 1000).toFixed(2)}s`
  return `${Math.round(value)}ms`
}

const bucketRate = (hits: number, total: number) => (total > 0 ? hits / total : 0)

const bucketHeight = (value: number, max: number) => {
  if (value <= 0 || max <= 0) {
    return 0
  }
  return Math.max((value / max) * 100, 12)
}

const timelineLabel = (
  index: number,
  length: number,
  value: string,
  granularity: FocusGranularity
) => {
  const date = new Date(value)
  if (granularity === 'hourly') {
    if (index % 4 !== 0 && index !== length - 1) {
      return ''
    }
    return new Intl.DateTimeFormat(locale.value, {
      hour: '2-digit',
    }).format(date)
  }

  const divider = length > 45 ? 9 : length > 21 ? 5 : 1
  if (index % divider !== 0 && index !== length - 1) {
    return ''
  }
  return new Intl.DateTimeFormat(locale.value, {
    month: 'short',
    day: 'numeric',
  }).format(date)
}

const formatBucketHeader = (value: string, granularity: FocusGranularity) => {
  const date = new Date(value)
  return new Intl.DateTimeFormat(locale.value, granularity === 'hourly'
    ? { hour: '2-digit' }
    : { month: 'short', day: 'numeric' }).format(date)
}

const metricValue = (bucket: CostAnalysisBucket) => {
  if (focusMetric.value === 'cost') return bucket.estimatedCost
  if (focusMetric.value === 'calls') return bucket.totalCalls
  if (focusMetric.value === 'cache') return bucketRate(bucket.cacheHits, bucket.totalCalls)
  return bucket.totalTokens
}

const bucketIntensity = (bucket: CostAnalysisBucket) => {
  const value = metricValue(bucket)
  const maxValue = heatmapMetricMax.value
  if (!value || maxValue <= 0) {
    return 0.08
  }
  return Math.min(0.18 + (value / maxValue) * 0.72, 0.92)
}

const formatHeatmapValue = (bucket: CostAnalysisBucket) => {
  if (focusMetric.value === 'cost') return formatCurrency(bucket.estimatedCost)
  if (focusMetric.value === 'calls') return formatNumber(bucket.totalCalls)
  if (focusMetric.value === 'cache') {
    return formatPercent(bucketRate(bucket.cacheHits, bucket.totalCalls))
  }
  return formatCompactNumber(bucket.totalTokens)
}

onMounted(() => {
  if (!hasData.value) {
    void costAnalysisStore.fetchAnalysis(selectedDays.value)
  }
})
</script>

<style scoped>
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

.metric-grid {
  align-items: stretch;
}

.metric-card {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 180px;
  padding: 22px;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.08);
  border-radius: 18px;
  background:
    linear-gradient(180deg, rgba(var(--v-theme-surface), 0.98) 0%, rgba(var(--v-theme-background), 0.96) 100%);
  box-shadow: 0 14px 30px rgba(var(--v-theme-on-surface), 0.04);
}

.metric-card__icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 42px;
  height: 42px;
  border-radius: 12px;
}

.metric-card__icon--primary {
  background: rgba(var(--v-theme-primary), 0.14);
  color: rgb(var(--v-theme-primary));
}

.metric-card__icon--warning {
  background: rgba(var(--v-theme-warning), 0.14);
  color: rgb(var(--v-theme-warning));
}

.metric-card__icon--info {
  background: rgba(var(--v-theme-info), 0.14);
  color: rgb(var(--v-theme-info));
}

.metric-card__icon--success {
  background: rgba(var(--v-theme-success), 0.14);
  color: rgb(var(--v-theme-success));
}

.metric-card__icon--secondary {
  background: rgba(var(--v-theme-secondary), 0.14);
  color: rgb(var(--v-theme-secondary));
}

.metric-card__label {
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: 0.82rem;
  font-weight: 600;
  text-transform: uppercase;
}

.metric-card__value {
  color: rgba(var(--v-theme-on-surface), 0.96);
  font-size: clamp(1.55rem, 2vw, 2.2rem);
  font-weight: 800;
  line-height: 1.15;
}

.metric-card__meta {
  margin-top: auto;
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-size: 0.92rem;
}

.analysis-panel {
  border: 1px solid rgba(var(--v-theme-on-surface), 0.08);
  border-radius: 24px;
  background:
    linear-gradient(180deg, rgba(var(--v-theme-surface), 0.98) 0%, rgba(var(--v-theme-background), 0.96) 100%);
  box-shadow: 0 16px 36px rgba(var(--v-theme-on-surface), 0.04);
  padding: 24px;
}

.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 24px;
}

.panel-head--dense {
  margin-bottom: 18px;
}

.panel-kicker {
  margin-bottom: 8px;
  color: rgba(var(--v-theme-primary), 0.92);
  font-size: 0.74rem;
  font-weight: 700;
  text-transform: uppercase;
}

.panel-title {
  margin: 0;
  color: rgba(var(--v-theme-on-surface), 0.94);
  font-size: 1.4rem;
  font-weight: 800;
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
  border: 1px solid rgba(var(--v-theme-on-surface), 0.06);
}

.trend-panel__head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.trend-panel__title {
  color: rgba(var(--v-theme-on-surface), 0.92);
  font-size: 1rem;
  font-weight: 700;
}

.trend-panel__caption {
  margin-top: 6px;
  color: rgba(var(--v-theme-on-surface), 0.64);
  font-size: 0.88rem;
}

.trend-panel__stat {
  color: rgba(var(--v-theme-on-surface), 0.88);
  font-size: 1.1rem;
  font-weight: 700;
}

.timeline-chart {
  display: grid;
  grid-template-columns: repeat(var(--timeline-bucket-count, 1), minmax(14px, 1fr));
  align-items: stretch;
  gap: 8px;
  height: 260px;
  min-height: 260px;
}

.timeline-chart--sparse {
  grid-template-columns: repeat(var(--timeline-bucket-count, 1), minmax(38px, 56px));
  justify-content: start;
}

.timeline-chart--compact {
  grid-template-columns: repeat(var(--timeline-bucket-count, 24), minmax(0, 1fr));
  gap: 6px;
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
  min-height: 0;
  border-radius: 999px;
  background: rgba(var(--v-theme-on-surface), 0.045);
  cursor: default;
}

.timeline-hit-area:hover .timeline-bar {
  filter: brightness(1.08);
}

.timeline-bar {
  position: absolute;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  min-height: 0;
  height: var(--timeline-height);
  border-radius: 999px;
  transition: height 0.2s ease, filter 0.18s ease;
  pointer-events: none;
}

.timeline-bar--cost {
  background: linear-gradient(180deg, rgba(var(--v-theme-warning), 0.4) 0%, rgba(var(--v-theme-warning), 0.95) 100%);
}

.timeline-bar--tokens {
  background: linear-gradient(180deg, rgba(var(--v-theme-info), 0.38) 0%, rgba(var(--v-theme-primary), 0.92) 100%);
}

.timeline-label {
  min-height: 16px;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: 0.7rem;
  text-align: center;
  white-space: nowrap;
}

.heatmap-shell {
  overflow-x: auto;
  padding-bottom: 6px;
}

.heatmap-grid {
  display: grid;
  grid-template-columns: 260px repeat(var(--heatmap-columns, 1), minmax(72px, 1fr));
  gap: 8px;
  min-width: fit-content;
}

.heatmap-corner,
.heatmap-model {
  position: sticky;
  left: 0;
  z-index: 1;
  padding: 14px 16px;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.08);
  border-radius: 14px;
  background: rgba(var(--v-theme-surface), 0.98);
}

.heatmap-header,
.heatmap-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 64px;
  border-radius: 14px;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.06);
  text-align: center;
}

.heatmap-header {
  color: rgba(var(--v-theme-on-surface), 0.64);
  font-size: 0.76rem;
  font-weight: 700;
  background: rgba(var(--v-theme-on-surface), 0.02);
}

.heatmap-model__title {
  color: rgba(var(--v-theme-on-surface), 0.92);
  font-weight: 700;
}

.heatmap-model__meta {
  margin-top: 4px;
  color: rgba(var(--v-theme-on-surface), 0.6);
  font-size: 0.82rem;
}

.heatmap-cell {
  padding: 8px;
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: 0.76rem;
  font-weight: 700;
}

.heatmap-cell--tokens {
  background: rgba(var(--v-theme-primary), var(--cell-intensity, 0.1));
}

.heatmap-cell--cost {
  background: rgba(var(--v-theme-warning), var(--cell-intensity, 0.1));
}

.heatmap-cell--calls {
  background: rgba(var(--v-theme-info), var(--cell-intensity, 0.1));
}

.heatmap-cell--cache {
  background: rgba(var(--v-theme-success), var(--cell-intensity, 0.1));
}

.table-shell {
  overflow: hidden;
  border-radius: 18px;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.08);
}

.analysis-table {
  background: transparent;
}

.model-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 220px;
}

.model-cell__title {
  color: rgba(var(--v-theme-on-surface), 0.94);
  font-weight: 700;
}

.model-cell__meta,
.model-cell__code {
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: 0.82rem;
}

.tooltip-stack {
  display: grid;
  gap: 4px;
}

@media (max-width: 1280px) {
  .metric-card {
    min-height: 160px;
  }
}

@media (max-width: 960px) {
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
