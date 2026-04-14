<template>
  <div class="d-flex flex-column ga-3">
    <div v-for="(entry, index) in rows" :key="entry.key + index" class="d-flex ga-3 align-start">
      <v-text-field
        v-model="entry.key"
        :label="$t('pages.modelRuntime.fields.key')"
        density="compact"
        variant="outlined"
        hide-details
        class="flex-grow-1"
      />
      <v-text-field
        v-model="entry.value"
        :label="$t('pages.modelRuntime.fields.value')"
        density="compact"
        variant="outlined"
        hide-details
        class="flex-grow-1"
      />
      <v-btn
        icon="mdi-delete-outline"
        variant="text"
        color="error"
        @click="removeRow(index)"
      />
    </div>

    <v-btn
      variant="tonal"
      color="primary"
      prepend-icon="mdi-plus"
      size="small"
      class="align-self-start"
      @click="addRow"
    >
      {{ $t('pages.modelRuntime.actions.addEntry') }}
    </v-btn>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface KeyValueRow {
  key: string
  value: string
}

interface Props {
  modelValue: KeyValueRow[]
}

const props = defineProps<Props>()
const emit = defineEmits<{
  'update:modelValue': [value: KeyValueRow[]]
}>()

const rows = computed({
  get: () => props.modelValue,
  set: (value: KeyValueRow[]) => emit('update:modelValue', value),
})

const addRow = () => {
  rows.value = [...rows.value, { key: '', value: '' }]
}

const removeRow = (index: number) => {
  rows.value = rows.value.filter((_, rowIndex) => rowIndex !== index)
}
</script>
