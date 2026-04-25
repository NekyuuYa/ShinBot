<template>
  <article class="metric-card h-100">
    <div class="metric-card__icon" :class="`metric-card__icon--${tone}`">
      <v-icon :icon="icon" size="20" />
    </div>
    <div class="metric-card__label">{{ label }}</div>
    <div class="metric-card__value">{{ value }}</div>
    <div v-if="meta" class="metric-card__meta text-truncate">{{ meta }}</div>
  </article>
</template>

<script setup lang="ts">
interface Props {
  icon: string
  label: string
  value: string | number
  meta?: string
  tone?: 'primary' | 'warning' | 'info' | 'success' | 'secondary'
}

withDefaults(defineProps<Props>(), {
  tone: 'primary',
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.metric-card {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 160px;
  padding: 20px;
  @include surface-card($radius: $radius-sm);
  @include hover-lift;
}

.metric-card__icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 40px;
  border-radius: $radius-xs;
  margin-bottom: 4px;
}

.metric-card__icon--primary { background: rgba(var(--v-theme-primary), 0.12); color: rgb(var(--v-theme-primary)); }
.metric-card__icon--warning { background: rgba(var(--v-theme-warning), 0.12); color: rgb(var(--v-theme-warning)); }
.metric-card__icon--info { background: rgba(var(--v-theme-info), 0.12); color: rgb(var(--v-theme-info)); }
.metric-card__icon--success { background: rgba(var(--v-theme-success), 0.12); color: rgb(var(--v-theme-success)); }
.metric-card__icon--secondary { background: rgba(var(--v-theme-secondary), 0.12); color: rgb(var(--v-theme-secondary)); }

.metric-card__label {
  color: rgba(var(--v-theme-on-surface), 0.6);
  font-size: $font-size-xs;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.metric-card__value {
  color: rgba(var(--v-theme-on-surface), 0.94);
  font-size: $font-size-lg;
  font-weight: 800;
  line-height: 1.1;
}

.metric-card__meta {
  margin-top: auto;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: $font-size-xs;
}

@include respond-to('tablet') {
  .metric-card {
    min-height: 140px;
    padding: 16px;
  }
}
</style>
