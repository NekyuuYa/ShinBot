<template>
  <div class="key-value-editor d-flex flex-column ga-3">
    <div v-for="(entry, index) in rows" :key="entry.key + index" class="key-value-row d-flex ga-3 align-start">
      <v-text-field
        v-model="entry.key"
        :label="$t('pages.modelRuntime.fields.key')"
        density="comfortable"
        variant="outlined"
        hide-details
        rounded="lg"
        bg-color="surface"
        class="flex-grow-1"
      />
      <v-text-field
        v-model="entry.value"
        :label="$t('pages.modelRuntime.fields.value')"
        density="comfortable"
        variant="outlined"
        hide-details
        rounded="lg"
        bg-color="surface"
        class="flex-grow-1"
      />
      <v-btn
        icon="mdi-delete-outline"
        variant="outlined"
        rounded="xl"
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

<style scoped>
.key-value-editor {
  padding: 14px;
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  border-radius: 20px;
  background: rgba(var(--v-theme-surface), 0.72);
}

.key-value-row {
  padding: 10px;
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
  border-radius: 18px;
  background: rgba(var(--v-theme-surface), 0.82);
}
</style>
