<template>
  <div class="schedule-table">
    <div class="d-flex align-center mb-2">
      <span class="text-body-2 text-medium-emphasis">{{ label }}</span>
      <v-spacer />
      <v-btn
        size="x-small"
        variant="tonal"
        color="primary"
        prepend-icon="mdi-plus"
        @click="addRow"
      >
        {{ $t('common.action.add') }}
      </v-btn>
    </div>

    <v-table density="compact" class="schedule-table__table rounded-lg" :hover="true">
      <thead>
        <tr>
          <th
            v-for="col in columns"
            :key="col.key"
            class="text-caption text-medium-emphasis"
            :style="col.width ? { width: col.width } : {}"
          >
            {{ col.label }}
          </th>
          <th style="width: 40px" />
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in rows" :key="index">
          <td v-for="col in columns" :key="col.key" class="py-1">
            <v-switch
              v-if="col.type === 'boolean'"
              :model-value="Boolean(row[col.key])"
              color="primary"
              density="compact"
              hide-details
              @update:model-value="(v) => updateCell(index, col.key, v)"
            />
            <v-text-field
              v-else
              :model-value="String(row[col.key] ?? '')"
              :type="col.type === 'number' ? 'number' : 'text'"
              density="compact"
              variant="plain"
              hide-details
              class="schedule-table__cell-input"
              @update:model-value="(v) => updateCell(index, col.key, col.type === 'number' ? Number(v) : v)"
            />
          </td>
          <td class="py-1 text-right">
            <v-btn
              icon="mdi-delete-outline"
              size="x-small"
              variant="text"
              color="error"
              @click="removeRow(index)"
            />
          </td>
        </tr>
        <tr v-if="rows.length === 0">
          <td :colspan="columns.length + 1" class="text-center text-medium-emphasis text-caption py-3">
            —
          </td>
        </tr>
      </tbody>
    </v-table>

    <div v-if="description" class="text-caption text-medium-emphasis mt-1 px-1">
      {{ description }}
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { JsonSchemaProperty } from '@/api/plugins'

interface Column {
  key: string
  label: string
  type: 'string' | 'number' | 'boolean'
  width?: string
  default: string | number | boolean
}

interface Props {
  fieldKey: string
  label: string
  description?: string
  itemsSchema?: JsonSchemaProperty
  modelValue: Array<Record<string, unknown>>
}

const props = defineProps<Props>()
const emit = defineEmits<{ 'update:modelValue': [value: Array<Record<string, unknown>>] }>()

const columns = computed<Column[]>(() => {
  const props_ = props.itemsSchema?.properties
  if (!props_) return []
  return Object.entries(props_).map(([key, schema]) => {
    const type =
      schema.type === 'boolean' ? 'boolean' : schema.type === 'number' || schema.type === 'integer' ? 'number' : 'string'
    const defaultVal =
      schema.default !== undefined && schema.default !== null
        ? schema.default
        : type === 'boolean'
          ? true
          : type === 'number'
            ? 0
            : ''
    return {
      key,
      label: schema.title ?? key,
      type,
      width: key === 'name' ? '120px' : key === 'threshold_delta' ? '80px' : key === 'enabled' ? '60px' : undefined,
      default: defaultVal as string | number | boolean,
    }
  })
})

const rows = computed<Array<Record<string, unknown>>>(() =>
  Array.isArray(props.modelValue) ? props.modelValue : [],
)

function makeDefaultRow(): Record<string, unknown> {
  return Object.fromEntries(columns.value.map((c) => [c.key, c.default]))
}

function addRow() {
  emit('update:modelValue', [...rows.value, makeDefaultRow()])
}

function removeRow(index: number) {
  const next = rows.value.filter((_, i) => i !== index)
  emit('update:modelValue', next)
}

function updateCell(rowIndex: number, key: string, value: unknown) {
  const next = rows.value.map((row, i) =>
    i === rowIndex ? { ...row, [key]: value } : row,
  )
  emit('update:modelValue', next)
}
</script>

<style scoped>
.schedule-table__table {
  border: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
}

.schedule-table__cell-input :deep(.v-field__input) {
  padding-top: 2px;
  padding-bottom: 2px;
  min-height: unset;
  font-size: 0.8125rem;
}
</style>
