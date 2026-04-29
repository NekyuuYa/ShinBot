<template>
  <div class="dual-pane-list-view" :style="layoutStyle">
    <div class="dual-pane-list-view__sidebar">
      <slot name="sidebar" />
    </div>

    <div :class="['dual-pane-list-view__content', contentClass]">
      <slot v-if="hasContentSlot" name="content" />

      <template v-else>
        <v-row v-if="showLoadingState" class="mx-0">
          <v-col cols="12" class="pa-0">
            <v-skeleton-loader :type="skeletonType" :count="skeletonCount" />
          </v-col>
        </v-row>

        <v-row v-else-if="showEmptyState" justify="center" class="mx-0 py-12">
          <v-col cols="12" md="8" class="text-center pa-0">
            <v-icon size="96" color="grey-lighten-1" :icon="emptyConfig!.icon" />
            <h3 class="text-h6 my-4">{{ emptyConfig!.title }}</h3>
            <p class="text-body-2 text-medium-emphasis">{{ emptyConfig!.subtitle }}</p>
          </v-col>
        </v-row>

        <v-row v-else class="mx-n4">
          <v-col
            v-for="(item, index) in items"
            :key="getKey(item, index)"
            cols="12"
            sm="6"
            md="6"
            lg="4"
            class="pa-4"
          >
            <slot name="card" :item="item" :index="index" />
          </v-col>
        </v-row>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts" generic="T">
import { computed, useSlots } from 'vue'

export interface DualPaneEmptyConfig {
  icon: string
  title: string
  subtitle: string
}

interface Props {
  items: readonly T[]
  loading?: boolean
  showSkeleton?: boolean
  emptyConfig?: DualPaneEmptyConfig | null
  sidebarWidth?: string
  contentClass?: string
  skeletonType?: string
  skeletonCount?: number
  getItemKey?: (item: T, index: number) => string | number
}

const props = withDefaults(defineProps<Props>(), {
  loading: false,
  showSkeleton: undefined,
  emptyConfig: null,
  sidebarWidth: '300px',
  contentClass: '',
  skeletonType: 'card',
  skeletonCount: 3,
  getItemKey: undefined,
})

const slots = useSlots()

const hasContentSlot = computed(() => Boolean(slots.content))

const layoutStyle = computed(() => ({
  '--dual-pane-sidebar-width': props.sidebarWidth,
}))

const showLoadingState = computed(() => props.showSkeleton ?? (props.loading && props.items.length === 0))

const showEmptyState = computed(
  () => !hasContentSlot.value && !showLoadingState.value && props.items.length === 0 && Boolean(props.emptyConfig)
)

const getKey = (item: T, index: number) => props.getItemKey?.(item, index) ?? index
</script>

<style scoped lang="scss">
.dual-pane-list-view {
  display: flex;
  align-items: flex-start;
  gap: 16px;
}

.dual-pane-list-view__sidebar {
  flex: 0 0 var(--dual-pane-sidebar-width, 300px);
  width: var(--dual-pane-sidebar-width, 300px);
  max-width: var(--dual-pane-sidebar-width, 300px);
}

.dual-pane-list-view__content {
  flex: 1 1 auto;
  min-width: 0;
}

@media (max-width: 960px) {
  .dual-pane-list-view {
    flex-direction: column;
  }

  .dual-pane-list-view__sidebar {
    flex: 1 1 auto;
    width: 100%;
    max-width: none;
  }
}
</style>