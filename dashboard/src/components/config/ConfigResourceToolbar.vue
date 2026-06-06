<template>
  <div class="config-resource-toolbar mb-6">
    <v-text-field
      :model-value="search"
      :label="searchLabel"
      prepend-inner-icon="mdi-magnify"
      single-line
      hide-details
      density="comfortable"
      variant="outlined"
      bg-color="surface"
      class="config-resource-toolbar__search"
      @update:model-value="emit('update:search', String($event ?? ''))"
    />
    <v-spacer />
    <layout-mode-button
      :model-value="viewMode"
      :list-label="listLabel"
      :card-label="cardLabel"
      @update:model-value="emit('update:viewMode', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import LayoutModeButton from '@/components/LayoutModeButton.vue'

export type ConfigResourceViewMode = 'card' | 'list'

interface Props {
  search: string
  searchLabel: string
  viewMode: ConfigResourceViewMode
  listLabel: string
  cardLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  'update:search': [value: string]
  'update:viewMode': [value: ConfigResourceViewMode]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.config-resource-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px;
  @include surface-card;
}

.config-resource-toolbar__search {
  flex: 0 1 420px;
}

@include respond-to('tablet') {
  .config-resource-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .config-resource-toolbar__search {
    flex: 1 1 auto;
    width: 100%;
  }
}
</style>
