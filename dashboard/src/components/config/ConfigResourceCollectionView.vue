<template>
  <v-row v-if="loading">
    <v-col cols="12">
      <v-skeleton-loader type="card" :count="skeletonCount" />
    </v-col>
  </v-row>

  <v-row v-else-if="shouldShowEmptyState" justify="center" class="py-12">
    <v-col cols="12" sm="8" md="6" class="text-center">
      <v-icon size="112" color="grey-lighten-1" :icon="emptyIcon" />
      <h3 class="text-h6 my-4">{{ emptyTitle }}</h3>
      <slot name="empty-action" />
    </v-col>
  </v-row>

  <v-row v-else-if="viewMode === 'card'" class="ma-0">
    <v-col
      v-for="(item, index) in items"
      :key="getKey(item, index)"
      cols="12"
      sm="6"
      md="4"
      lg="3"
    >
      <slot name="card" :item="item" :index="index" />
    </v-col>
  </v-row>

  <v-row v-else>
    <v-col cols="12">
      <slot name="table" />
    </v-col>
  </v-row>
</template>

<script setup lang="ts" generic="T">
import { computed } from 'vue'

interface Props {
  items: readonly T[]
  loading: boolean
  viewMode: 'card' | 'list'
  emptyIcon: string
  emptyTitle: string
  showEmptyState?: boolean
  skeletonCount?: number
  getItemKey?: (item: T, index: number) => string | number
}

const props = withDefaults(defineProps<Props>(), {
  showEmptyState: undefined,
  skeletonCount: 3,
  getItemKey: undefined,
})

const shouldShowEmptyState = computed(() => props.showEmptyState ?? props.items.length === 0)
const getKey = (item: T, index: number) => props.getItemKey?.(item, index) ?? index
</script>
