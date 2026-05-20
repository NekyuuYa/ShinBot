<template>
  <section class="overview-band">
    <div class="overview-head">
      <div class="overview-head__copy">
        <div class="panel-kicker">{{ kicker }}</div>
        <h2 class="panel-title">{{ title }}</h2>
        <p class="panel-subtitle">{{ subtitle }}</p>
      </div>

      <div class="panel-meta">
        <v-chip variant="tonal" color="primary" size="small">
          {{ dailyLabel }}
        </v-chip>
        <v-chip variant="tonal" color="info" size="small">
          {{ hourlyLabel }}
        </v-chip>
      </div>
    </div>

    <div class="summary-strip">
      <article
        v-for="item in summaryStats"
        :key="item.key"
        class="summary-cell"
        :class="`summary-cell--${item.tone}`"
      >
        <div class="summary-cell__label">
          <v-icon :icon="item.icon" size="18" />
          <span>{{ item.label }}</span>
        </div>
        <div class="summary-cell__value">{{ item.value }}</div>
        <div class="summary-cell__meta">{{ item.meta }}</div>
      </article>
    </div>

    <div class="overview-context">
      <div v-for="item in overviewSignals" :key="item.key" class="context-item">
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
        <small>{{ item.meta }}</small>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
interface OverviewStatItem {
  key: string
  tone: 'primary' | 'warning' | 'info' | 'success' | 'secondary'
  icon: string
  label: string
  value: string
  meta: string
}

interface OverviewSignalItem {
  key: string
  label: string
  value: string
  meta: string
}

defineProps<{
  kicker: string
  title: string
  subtitle: string
  dailyLabel: string
  hourlyLabel: string
  summaryStats: OverviewStatItem[]
  overviewSignals: OverviewSignalItem[]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.overview-band {
  @include surface-card;
  padding: 24px;
}

.overview-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 22px;
}

.overview-head__copy {
  min-width: 0;
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

.summary-strip {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  overflow: hidden;
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.summary-cell {
  min-width: 0;
  padding: 16px;
  border-inline-end: 1px solid $border-color-soft;

  &:last-child {
    border-inline-end: 0;
  }

  &__label {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    color: rgba(var(--v-theme-on-surface), 0.62);
    font-size: $font-size-xs;
    font-weight: 800;
    letter-spacing: 0;
    text-transform: uppercase;

    span {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }

  &__value {
    margin-top: 12px;
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.94);
    font-size: 1.45rem;
    font-weight: 850;
    line-height: 1.05;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  &__meta {
    margin-top: 8px;
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

.summary-cell--primary .v-icon {
  color: rgb(var(--v-theme-primary));
}
.summary-cell--warning .v-icon {
  color: rgb(var(--v-theme-warning));
}
.summary-cell--info .v-icon {
  color: rgb(var(--v-theme-info));
}
.summary-cell--success .v-icon {
  color: rgb(var(--v-theme-success));
}
.summary-cell--secondary .v-icon {
  color: rgb(var(--v-theme-secondary));
}

.overview-context {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 18px;
  margin-top: 18px;
  padding-top: 18px;
  border-top: 1px solid $border-color-soft;
}

.context-item {
  display: grid;
  gap: 4px;
  min-width: 0;

  span,
  small {
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  strong {
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.9);
    font-size: 1rem;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

@media (max-width: 1280px) {
  .summary-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    border: 0;
    gap: 10px;
    background: transparent;
  }

  .summary-cell {
    border: 1px solid $border-color-soft;
    border-radius: $radius-sm;
    background: rgba(var(--v-theme-on-surface), 0.018);
  }

  .overview-context {
    grid-template-columns: 1fr;
    gap: 12px;
  }
}
</style>
