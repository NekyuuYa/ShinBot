<template>
  <v-btn
    :prepend-icon="buttonIcon"
    variant="outlined"
    rounded="lg"
    class="layout-mode-btn"
    @click="toggleMode"
  >
    {{ buttonLabel }}
  </v-btn>
</template>

<script setup lang="ts">
import { computed } from 'vue'

type LayoutMode = 'list' | 'card'

interface Props {
  modelValue: LayoutMode
  listLabel: string
  cardLabel: string
}

const props = defineProps<Props>()

const emit = defineEmits<{
  'update:modelValue': [value: LayoutMode]
}>()

const nextMode = computed<LayoutMode>(() => (props.modelValue === 'list' ? 'card' : 'list'))

const buttonIcon = computed(() =>
  nextMode.value === 'card' ? 'mdi-view-grid-outline' : 'mdi-format-list-bulleted'
)

const buttonLabel = computed(() =>
  nextMode.value === 'card' ? props.cardLabel : props.listLabel
)

const toggleMode = () => {
  emit('update:modelValue', nextMode.value)
}
</script>

<style scoped>
.layout-mode-btn {
  min-width: 148px;
}
</style>
