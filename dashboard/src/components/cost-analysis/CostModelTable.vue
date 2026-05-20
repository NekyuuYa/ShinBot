<template>
  <section class="analysis-panel">
    <div class="panel-head panel-head--dense">
      <div>
        <div class="panel-kicker">{{ kicker }}</div>
        <h2 class="panel-title">{{ title }}</h2>
        <p class="panel-subtitle">{{ subtitle }}</p>
      </div>

      <div class="panel-meta">
        <v-chip variant="tonal" color="secondary" size="small">
          {{ modelCountLabel }}
        </v-chip>
      </div>
    </div>

    <div v-if="items.length > 0" class="table-shell">
      <v-table fixed-header height="480" density="compact" class="analysis-table">
        <thead>
          <tr>
            <th>{{ modelLabel }}</th>
            <th>{{ costLabel }}</th>
            <th>{{ callsLabel }}</th>
            <th>{{ successRateLabel }}</th>
            <th>{{ cacheHitRateLabel }}</th>
            <th>{{ inputTokensLabel }}</th>
            <th>{{ outputTokensLabel }}</th>
            <th>{{ cacheReadsLabel }}</th>
            <th>{{ cacheWritesLabel }}</th>
            <th>{{ avgLatencyLabel }}</th>
            <th>{{ lastSeenLabel }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="model in items" :key="modelKey(model)">
            <td>
              <div class="model-cell">
                <div class="model-cell__title">{{ modelName(model) }}</div>
                <div class="model-cell__meta">{{ providerName(model) }}</div>
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
      icon="mdi-table-search"
      :title="emptyTitle"
      :text="emptyText"
      variant="plain"
    />
  </section>
</template>

<script setup lang="ts">
import type { CostAnalysisModel } from '@/api/modelRuntime'

defineProps<{
  kicker: string
  title: string
  subtitle: string
  modelCountLabel: string
  modelLabel: string
  costLabel: string
  callsLabel: string
  successRateLabel: string
  cacheHitRateLabel: string
  inputTokensLabel: string
  outputTokensLabel: string
  cacheReadsLabel: string
  cacheWritesLabel: string
  avgLatencyLabel: string
  lastSeenLabel: string
  emptyTitle: string
  emptyText: string
  items: CostAnalysisModel[]
  formatNumber: (value: number) => string
  formatCompactNumber: (value: number) => string
  formatCurrency: (value: number) => string
  formatPercent: (value: number) => string
  formatDuration: (value: number | null | undefined) => string
  formatDateTime: (value: string | null | undefined) => string
  modelKey: (model: CostAnalysisModel) => string
  modelName: (model: CostAnalysisModel) => string
  providerName: (model: CostAnalysisModel) => string
}>()
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

.panel-meta {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.table-shell {
  overflow: auto;
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
}

.analysis-table {
  min-width: 1120px;
  background: transparent;
}

.analysis-table :deep(th),
.analysis-table :deep(td) {
  white-space: nowrap;
}

.analysis-table :deep(th:not(:first-child)),
.analysis-table :deep(td:not(:first-child)) {
  text-align: right;
}

.analysis-table :deep(thead th) {
  background: rgb(var(--v-theme-surface));
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: 0.76rem;
  font-weight: 800;
}

.analysis-table :deep(tbody tr:hover) {
  background: rgba(var(--v-theme-primary), 0.035);
}

.model-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 220px;
  max-width: 320px;

  &__title,
  &__meta {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &__title {
    color: rgba(var(--v-theme-on-surface), 0.94);
    font-weight: 800;
  }

  &__meta {
    color: rgba(var(--v-theme-on-surface), 0.62);
    font-size: 0.8rem;
  }
}
</style>
