<template>
  <div class="resource-meta-row">
    <span class="resource-meta-row__label">{{ label }}</span>
    <strong :class="{ 'resource-meta-row__value--truncate': truncateValue }">
      {{ value }}
    </strong>
  </div>
</template>

<script setup lang="ts">
interface Props {
  label: string
  value: string | number
  truncateValue?: boolean
}

withDefaults(defineProps<Props>(), {
  truncateValue: false,
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.resource-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 0;
  border-top: 1px solid $border-color-soft;
  font-size: $font-size-sm;
}

.resource-meta-row__label {
  color: rgb(var(--v-theme-on-surface));
  opacity: 0.66;
}

.resource-meta-row strong {
  color: rgb(var(--v-theme-on-surface));
  font-weight: 700;
  opacity: 0.9;
}

.resource-meta-row__value--truncate {
  max-width: 62%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
