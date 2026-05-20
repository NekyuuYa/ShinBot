<template>
  <section class="analysis-panel">
    <div class="panel-head panel-head--compact">
      <div>
        <div class="panel-kicker">{{ kicker }}</div>
        <h2 class="panel-title">{{ title }}</h2>
        <p class="panel-subtitle">{{ subtitle }}</p>
      </div>
    </div>

    <div v-if="items.length > 0" class="provider-list">
      <article v-for="provider in items" :key="provider.key" class="provider-row">
        <div class="provider-row__head">
          <div class="provider-row__name">
            <strong>{{ provider.name }}</strong>
            <span>
              {{
                providerMetaLabel({
                  models: provider.modelCount,
                  calls: formatCompactNumber(provider.totalCalls),
                })
              }}
            </span>
          </div>
          <div class="provider-row__value">
            {{ formatCurrency(provider.estimatedCost) }}
          </div>
        </div>
        <div class="provider-row__metrics">
          <span>
            {{ totalTokensLabel }} {{ formatCompactNumber(provider.totalTokens) }}
          </span>
          <span>
            {{ cacheHitRateLabel }} {{ formatPercent(provider.cacheHitRate) }}
          </span>
          <span>{{ formatPercent(provider.costShare) }}</span>
        </div>
        <div class="share-track share-track--provider">
          <span :style="{ width: shareWidth(provider.costShare) }" />
        </div>
      </article>
    </div>

    <v-empty-state
      v-else
      icon="mdi-cloud-outline"
      :title="emptyTitle"
      :text="emptyText"
      variant="plain"
    />
  </section>
</template>

<script setup lang="ts">
import type { ProviderCostRow } from '@/composables/useCostAnalysisViewModel'

defineProps<{
  kicker: string
  title: string
  subtitle: string
  emptyTitle: string
  emptyText: string
  totalTokensLabel: string
  cacheHitRateLabel: string
  items: ProviderCostRow[]
  formatCompactNumber: (value: number) => string
  formatCurrency: (value: number) => string
  formatPercent: (value: number) => string
  shareWidth: (share: number) => string
  providerMetaLabel: (args: { models: number; calls: string }) => string
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
  margin-bottom: 14px;

  &--compact {
    margin-bottom: 14px;
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

.provider-list {
  display: grid;
}

.provider-row {
  padding: 14px 0;
  border-bottom: 1px solid $border-color-soft;

  &:last-child {
    border-bottom: 0;
  }

  &__head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }

  &__name {
    display: grid;
    gap: 3px;
    min-width: 0;

    strong,
    span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    strong {
      color: rgba(var(--v-theme-on-surface), 0.92);
      font-size: 0.94rem;
    }

    span {
      color: rgba(var(--v-theme-on-surface), 0.58);
      font-size: $font-size-xs;
    }
  }

  &__value {
    flex: 0 0 auto;
    color: rgba(var(--v-theme-on-surface), 0.9);
    font-weight: 800;
    white-space: nowrap;
  }

  &__metrics {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-top: 10px;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
  }
}

.share-track {
  width: 100%;
  height: 6px;
  overflow: hidden;
  margin-top: 10px;
  border-radius: 999px;
  background: rgba(var(--v-theme-on-surface), 0.06);

  span {
    display: block;
    height: 100%;
    border-radius: inherit;
    background: rgb(var(--v-theme-warning));
  }

  &--provider span {
    background: rgb(var(--v-theme-primary));
  }
}
</style>
