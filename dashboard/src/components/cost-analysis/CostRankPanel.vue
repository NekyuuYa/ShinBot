<template>
  <section class="analysis-panel analysis-panel--rank">
    <div class="panel-head panel-head--compact">
      <div>
        <div class="panel-kicker">{{ kicker }}</div>
        <h2 class="panel-title">{{ title }}</h2>
        <p class="panel-subtitle">{{ subtitle }}</p>
      </div>
    </div>

    <div v-if="items.length > 0" class="rank-list">
      <article v-for="(item, index) in items" :key="item.key" class="rank-row">
        <div class="rank-row__index">{{ index + 1 }}</div>
        <div class="rank-row__body">
          <div class="rank-row__top">
            <div class="rank-row__name">
              <strong>{{ item.name }}</strong>
              <span>{{ item.detail }}</span>
            </div>
            <div class="rank-row__value">{{ item.value }}</div>
          </div>
          <div class="rank-row__meta">{{ item.meta }}</div>
          <div class="share-track">
            <span :style="{ width: item.shareWidth }" />
          </div>
        </div>
      </article>
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
export interface CostRankItem {
  key: string
  name: string
  detail: string
  value: string
  meta: string
  shareWidth: string
}

defineProps<{
  kicker: string
  title: string
  subtitle: string
  emptyTitle: string
  emptyText: string
  items: CostRankItem[]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.analysis-panel {
  @include analysis-section-panel;
  min-width: 0;
}

.analysis-panel--rank {
  min-height: 390px;
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

.rank-list {
  display: grid;
}

.rank-row {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr);
  gap: 12px;
  padding: 14px 0;
  border-bottom: 1px solid $border-color-soft;

  &:last-child {
    border-bottom: 0;
  }

  &__index {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 999px;
    color: rgb(var(--v-theme-primary));
    background: rgba(var(--v-theme-primary), 0.1);
    font-size: $font-size-xs;
    font-weight: 800;
  }

  &__body {
    min-width: 0;
  }

  &__top {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    min-width: 0;
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
      font-size: 0.92rem;
    }

    span {
      color: rgba(var(--v-theme-on-surface), 0.58);
      font-size: $font-size-xs;
    }
  }

  &__value {
    flex: 0 0 auto;
    color: rgba(var(--v-theme-on-surface), 0.9);
    font-size: 0.92rem;
    font-weight: 800;
    white-space: nowrap;
  }

  &__meta {
    margin-top: 8px;
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
}
</style>
