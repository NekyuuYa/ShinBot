<template>
  <div class="json-tree-shell">
    <json-tree-node :value="value" :open="open" />
  </div>
</template>

<script setup lang="ts">
import JsonTreeNode from './JsonTreeNode.vue'

withDefaults(
  defineProps<{
    value: unknown
    open?: boolean
  }>(),
  {
    open: false,
  }
)
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.json-tree-shell {
  max-height: 360px;
  padding: 14px 16px;
  overflow: auto;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.08);
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-surface), 0.36);
  color: rgba(var(--v-theme-on-surface), 0.78);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: $font-size-xs;
  line-height: 1.55;
}

:deep(.json-node) {
  min-height: 22px;
}

:deep(.json-node__summary) {
  display: flex;
  align-items: baseline;
  gap: 8px;
  min-height: 22px;
  padding: 1px 0;
  cursor: pointer;
}

:deep(.json-node__summary::marker) {
  color: rgba(var(--v-theme-on-surface), 0.46);
}

:deep(.json-node__body) {
  margin-left: 8px;
  border-left: 1px solid rgba(var(--v-theme-on-surface), 0.08);
}

:deep(.json-node__key) {
  color: rgba(var(--v-theme-on-surface), 0.86);
  font-weight: 700;
  overflow-wrap: anywhere;
}

:deep(.json-node__meta) {
  color: rgba(var(--v-theme-on-surface), 0.46);
  font-size: $font-size-xs;
}

:deep(.json-node--scalar) {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 1px 0;
}

:deep(.json-node__scalar),
:deep(.json-node__empty) {
  color: rgba(var(--v-theme-on-surface), 0.72);
  overflow-wrap: anywhere;
}
</style>
