<template>
  <div class="audit-detail-row">
    <span>{{ label }}</span>
    <strong>{{ displayValue }}</strong>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    label: string
    value?: string | number | boolean | null
    emptyValue?: string
  }>(),
  {
    value: '',
    emptyValue: '—',
  }
)

const displayValue = computed(() => {
  if (props.value === null || props.value === undefined || props.value === '') {
    return props.emptyValue
  }
  return String(props.value)
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.audit-detail-row {
  display: grid;
  grid-template-columns: minmax(92px, 0.75fr) minmax(0, 1.25fr);
  gap: 12px;
  padding: 6px 0;
  font-size: $font-size-xs;
}

.audit-detail-row span {
  color: rgba(var(--v-theme-on-surface), 0.52);
}

.audit-detail-row strong {
  overflow-wrap: anywhere;
  color: rgba(var(--v-theme-on-surface), 0.86);
  font-weight: 700;
}
</style>
