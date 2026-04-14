<template>
  <v-row class="mb-8" align="center">
    <v-col cols="12" :md="hasActions ? 8 : 12">
      <div v-if="kicker" class="page-kicker">{{ kicker }}</div>
      <h1 class="text-h4 font-weight-bold">{{ title }}</h1>
      <p v-if="subtitle" class="text-body-2 text-medium-emphasis mt-2">
        {{ subtitle }}
      </p>
    </v-col>

    <v-col v-if="hasActions" cols="12" md="4" class="text-end">
      <slot name="actions" />
    </v-col>
  </v-row>
</template>

<script setup lang="ts">
import { computed, useSlots } from 'vue'

interface Props {
  title: string
  subtitle?: string
  kicker?: string
}

defineProps<Props>()

const slots = useSlots()
const hasActions = computed(() => Boolean(slots.actions))
</script>

<style scoped>
.page-kicker {
  margin-bottom: 10px;
  color: rgba(120, 86, 0, 0.88);
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}
</style>
