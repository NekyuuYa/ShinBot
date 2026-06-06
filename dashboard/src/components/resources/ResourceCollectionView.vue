<template>
  <v-row v-if="showLoadingState" class="mx-0">
    <v-col cols="12" class="pa-0">
      <v-skeleton-loader :type="skeletonType" />
    </v-col>
  </v-row>

  <v-row v-else-if="showEmptyState" justify="center" class="mx-0 py-12">
    <v-col cols="12" md="6" class="text-center pa-0">
      <v-icon size="120" color="grey-lighten-1" :icon="emptyConfig.icon" />
      <h3 class="text-h6 my-4">{{ emptyConfig.title }}</h3>
      <p class="text-body-2 text-medium-emphasis">{{ emptyConfig.subtitle }}</p>
    </v-col>
  </v-row>

  <template v-else-if="layoutMode === 'card'">
    <v-row class="mx-n3">
      <v-col
        v-for="(item, index) in items"
        :key="getKey(item, index)"
        cols="12"
        md="6"
        xl="4"
        class="pa-3"
      >
        <slot name="card" :item="item" :index="index" />
      </v-col>
    </v-row>
  </template>

  <div v-else class="d-grid ga-4">
    <slot
      v-for="(item, index) in items"
      name="row"
      :item="item"
      :index="index"
    />
  </div>
</template>

<script setup lang="ts" generic="T">
import { computed } from 'vue'

export interface ResourceEmptyConfig {
  icon: string
  title: string
  subtitle: string
}

interface Props {
  items: readonly T[]
  loading: boolean
  loaded: boolean
  layoutMode: 'list' | 'card'
  emptyConfig: ResourceEmptyConfig
  skeletonType?: string
  getItemKey?: (item: T, index: number) => string | number
}

const props = withDefaults(defineProps<Props>(), {
  skeletonType: 'list-item-two-line, list-item-two-line, list-item-two-line',
  getItemKey: undefined,
})

const showLoadingState = computed(() => props.loading)
const showEmptyState = computed(() => props.loaded && props.items.length === 0)

const getKey = (item: T, index: number) => props.getItemKey?.(item, index) ?? index
</script>
