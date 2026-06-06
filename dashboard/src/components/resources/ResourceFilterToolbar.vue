<template>
  <v-card rounded="xl" elevation="0" class="filter-card mb-6">
    <v-card-text>
      <v-row class="mx-0" align="center">
        <v-col cols="12" md="4" class="pa-2">
          <v-text-field
            :model-value="search"
            :label="searchLabel"
            prepend-inner-icon="mdi-magnify"
            variant="outlined"
            density="comfortable"
            hide-details
            rounded="lg"
            bg-color="surface"
            @update:model-value="emit('update:search', String($event ?? ''))"
          />
        </v-col>
        <v-col
          v-for="filter in filters"
          :key="filter.key"
          cols="12"
          sm="6"
          md="3"
          class="pa-2"
        >
          <v-select
            :model-value="filter.value"
            :label="filter.label"
            :items="filter.items"
            item-title="title"
            item-value="value"
            variant="outlined"
            density="comfortable"
            hide-details
            rounded="lg"
            bg-color="surface"
            @update:model-value="emit('update:filter', filter.key, String($event ?? ''))"
          />
        </v-col>
        <v-col cols="12" md="2" class="pa-2 d-flex justify-end">
          <layout-mode-button
            :model-value="layoutMode"
            :list-label="listLabel"
            :card-label="cardLabel"
            @update:model-value="emit('update:layoutMode', $event)"
          />
        </v-col>
      </v-row>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import LayoutModeButton from '@/components/LayoutModeButton.vue'

export type ResourceLayoutMode = 'list' | 'card'

export interface ResourceFilterItem {
  title: string
  value: string
}

export interface ResourceFilter {
  key: string
  label: string
  value: string
  items: readonly ResourceFilterItem[]
}

interface Props {
  search: string
  searchLabel: string
  filters: readonly ResourceFilter[]
  layoutMode: ResourceLayoutMode
  listLabel: string
  cardLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  'update:search': [value: string]
  'update:filter': [key: string, value: string]
  'update:layoutMode': [value: ResourceLayoutMode]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.filter-card {
  @include surface-card;
  @include hover-lift;
}
</style>
