<template>
  <section class="analysis-panel mb-6">
    <div class="panel-head panel-head--dense">
      <div>
        <div class="panel-kicker">{{ kicker }}</div>
        <h2 class="panel-title">{{ title }}</h2>
        <p class="panel-subtitle">{{ subtitle }}</p>
      </div>

      <div class="focus-toolbar">
        <v-btn-toggle
          v-model="granularity"
          mandatory
          density="comfortable"
          class="focus-toggle"
        >
          <v-btn value="daily" rounded="lg">
            {{ dailyLabel }}
          </v-btn>
          <v-btn value="hourly" rounded="lg">
            {{ hourlyLabel }}
          </v-btn>
        </v-btn-toggle>

        <v-btn-toggle
          v-model="metric"
          mandatory
          density="comfortable"
          class="focus-toggle"
        >
          <v-btn v-for="item in metricOptions" :key="item.value" :value="item.value" rounded="lg">
            {{ item.label }}
          </v-btn>
        </v-btn-toggle>
      </div>
    </div>

    <div v-if="models.length > 0" class="heatmap-shell">
      <div class="heatmap-grid" :style="{ '--heatmap-columns': String(Math.max(buckets.length, 1)) }">
        <div class="heatmap-corner sticky-col">
          <div class="font-weight-medium">{{ modelLabel }}</div>
          <div class="text-caption text-medium-emphasis">{{ modelCountLabel }}</div>
        </div>

        <div v-for="bucket in buckets" :key="`header-${bucket.bucketStart}`" class="heatmap-header">
          {{ formatBucketHeader(bucket.bucketStart, granularity) }}
        </div>

        <template v-for="model in models" :key="modelKey(model)">
          <div class="heatmap-model sticky-col">
            <div class="heatmap-model__title">{{ modelName(model) }}</div>
            <div class="heatmap-model__meta">{{ providerName(model) }}</div>
          </div>

          <v-tooltip
            v-for="bucket in model[granularity]"
            :key="`${modelKey(model)}-${bucket.bucketStart}`"
            location="top"
          >
            <template #activator="{ props }">
              <div
                v-bind="props"
                class="heatmap-cell"
                :class="`heatmap-cell--${metric}`"
                :style="{ '--cell-intensity': String(bucketIntensity(bucket)) }"
              >
                <span>{{ formatHeatmapValue(bucket) }}</span>
              </div>
            </template>

            <div class="tooltip-stack">
              <div class="font-weight-medium">{{ modelName(model) }}</div>
              <div>{{ formatDateTime(bucket.bucketStart) }}</div>
              <div>{{ callsLabel }}: {{ formatNumber(bucket.totalCalls) }}</div>
              <div>{{ totalTokensLabel }}: {{ formatCompactNumber(bucket.totalTokens) }}</div>
              <div>{{ costLabel }}: {{ formatCurrency(bucket.estimatedCost) }}</div>
              <div>{{ cacheReadsLabel }}: {{ formatCompactNumber(bucket.cacheReadTokens) }}</div>
              <div>{{ cacheWritesLabel }}: {{ formatCompactNumber(bucket.cacheWriteTokens) }}</div>
            </div>
          </v-tooltip>
        </template>
      </div>
    </div>

    <v-empty-state
      v-else
      icon="mdi-chart-box-outline"
      :title="emptyTitle"
      :text="emptyText"
      variant="plain"
    />
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { CostAnalysisBucket, CostAnalysisFocusModel } from '@/api/modelRuntime'

type FocusGranularity = 'daily' | 'hourly'
type FocusMetric = 'tokens' | 'cost' | 'calls' | 'cache'

const props = defineProps<{
  kicker: string
  title: string
  subtitle: string
  dailyLabel: string
  hourlyLabel: string
  modelLabel: string
  modelCountLabel: string
  callsLabel: string
  totalTokensLabel: string
  costLabel: string
  cacheReadsLabel: string
  cacheWritesLabel: string
  emptyTitle: string
  emptyText: string
  models: CostAnalysisFocusModel[]
  metricOptions: Array<{ value: FocusMetric; label: string }>
  formatBucketHeader: (value: string, granularity: FocusGranularity) => string
  formatDateTime: (value: string) => string
  formatNumber: (value: number) => string
  formatCompactNumber: (value: number) => string
  formatCurrency: (value: number) => string
  modelKey: (model: CostAnalysisFocusModel) => string
  modelName: (model: CostAnalysisFocusModel) => string
  providerName: (model: CostAnalysisFocusModel) => string
  bucketIntensity: (bucket: CostAnalysisBucket) => number
  formatHeatmapValue: (bucket: CostAnalysisBucket) => string
}>()

const granularity = defineModel<FocusGranularity>({ default: 'daily' })
const metric = defineModel<FocusMetric>('metric', { default: 'tokens' })

const buckets = computed(() => {
  const first = props.models[0]
  return first ? first[granularity.value] : []
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.analysis-panel {
  @include analysis-section-panel;
  min-width: 0;
}

.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 18px;

  &--dense {
    margin-bottom: 18px;
  }
}

.panel-kicker {
  margin-bottom: 8px;
  color: rgb(var(--v-theme-primary));
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}

.panel-title {
  margin: 0;
  color: rgba(var(--v-theme-on-surface), 0.94);
  font-size: $font-size-lg;
  font-weight: 800;
  line-height: 1.2;
}

.panel-subtitle {
  margin: 8px 0 0;
  max-width: 720px;
  color: rgba(var(--v-theme-on-surface), 0.66);
  font-size: 0.92rem;
  line-height: 1.55;
}

.focus-toolbar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.focus-toggle {
  padding: 2px;
  overflow: visible;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.72);
}

.focus-toggle :deep(.v-btn) {
  min-width: 64px;
  border-radius: 14px;
  margin: 2px;
}

.heatmap-shell {
  overflow-x: auto;
  padding-bottom: 4px;
}

.heatmap-grid {
  @include heatmap-grid-layout(230px, 64px);
  gap: 6px;
}

.heatmap-header,
.heatmap-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 44px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  text-align: center;
}

.heatmap-header {
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: 0.7rem;
  font-weight: 800;
  background: rgba(var(--v-theme-on-surface), 0.02);
}

.heatmap-model {
  display: grid;
  align-content: center;
  min-height: 48px;
  padding: 8px 10px;
  border-radius: $radius-xs;

  &__title,
  &__meta {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &__title {
    color: rgba(var(--v-theme-on-surface), 0.92);
    font-size: 0.86rem;
    font-weight: 800;
  }

  &__meta {
    margin-top: 2px;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: 0.74rem;
  }
}

.heatmap-corner {
  display: grid;
  align-content: center;
  min-height: 44px;
  padding: 8px 10px;
  border-radius: $radius-xs;
}

.heatmap-cell {
  padding: 6px;
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: 0.68rem;
  font-weight: 800;

  span {
    overflow: hidden;
    max-width: 100%;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

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

@media (max-width: 1280px) {
  .panel-head {
    flex-direction: column;
  }

  .focus-toolbar {
    width: 100%;
    justify-content: flex-start;
  }

  .heatmap-grid {
    grid-template-columns: 220px repeat(var(--heatmap-columns, 1), minmax(62px, 1fr));
  }
}
</style>
