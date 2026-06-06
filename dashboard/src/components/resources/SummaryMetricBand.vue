<template>
  <v-row class="mx-0 mb-6" align="stretch">
    <v-col
      v-for="metric in metrics"
      :key="metric.key ?? metric.label"
      cols="12"
      :md="columnMd"
      class="pa-2"
    >
      <v-card rounded="xl" elevation="0" class="summary-card">
        <v-card-text>
          <div class="text-caption text-medium-emphasis">{{ metric.label }}</div>
          <div class="text-h4 font-weight-black mt-2">{{ metric.value }}</div>
        </v-card-text>
      </v-card>
    </v-col>
  </v-row>
</template>

<script setup lang="ts">
import { computed } from 'vue'

export interface SummaryMetric {
  key?: string
  label: string
  value: string | number
}

interface Props {
  metrics: readonly SummaryMetric[]
}

const props = defineProps<Props>()

const columnMd = computed(() => {
  if (props.metrics.length <= 0) {
    return 12
  }
  return Math.max(3, Math.floor(12 / props.metrics.length))
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.summary-card {
  @include surface-card;
  @include hover-lift;
}
</style>
