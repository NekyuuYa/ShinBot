<template>
  <section class="audit-detail-section" :class="toneClass">
    <div class="audit-detail-section__title">
      <v-icon :icon="icon" size="18" />
      <span>{{ title }}</span>
    </div>
    <div class="audit-detail-section__body">
      <slot />
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    title: string
    icon: string
    tone?: 'default' | 'error'
  }>(),
  {
    tone: 'default',
  }
)

const toneClass = computed(() =>
  props.tone === 'error' ? 'audit-detail-section--error' : ''
)
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.audit-detail-section {
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.audit-detail-section--error {
  border-color: rgba(var(--v-theme-error), 0.24);
  background: rgba(var(--v-theme-error), 0.05);
}

.audit-detail-section__title {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 14px 8px;
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-size: $font-size-sm;
  font-weight: 800;
}

.audit-detail-section__body {
  padding: 0 14px 12px;
}
</style>
