<template>
  <v-row>
    <v-col v-for="field in flatFields" :key="field.key" cols="12" md="6">
      <v-select
        v-if="field.component === 'select'"
        :model-value="selectValue(field.key, field.multiple)"
        :items="field.options"
        :label="field.label"
        :hint="field.description"
        :multiple="field.multiple"
        chips
        persistent-hint
        @update:model-value="(value) => updateField(field.key, value)"
      />

      <v-switch
        v-else-if="field.component === 'switch'"
        :model-value="Boolean(fieldValue(field.key))"
        :label="field.label"
        :hint="field.description"
        color="primary"
        inset
        persistent-hint
        @update:model-value="(value) => updateField(field.key, value)"
      />

      <v-text-field
        v-else
        :model-value="stringValue(field.key)"
        :label="field.label"
        :hint="field.description"
        :type="field.inputType"
        persistent-hint
        @update:model-value="(value) => updateField(field.key, normalizeInput(value, field.kind))"
      />
    </v-col>
  </v-row>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { PluginConfigSchema, JsonSchemaProperty } from '@/api/plugins'

interface FlatField {
  key: string
  label: string
  description: string
  component: 'input' | 'switch' | 'select'
  inputType: 'text' | 'number' | 'password'
  kind: 'string' | 'number' | 'integer' | 'boolean' | 'array'
  options?: Array<string | number | boolean>
  multiple?: boolean
}

interface Props {
  schema: PluginConfigSchema | null
  modelValue: Record<string, unknown>
}

const props = defineProps<Props>()

const emit = defineEmits<{
  'update:modelValue': [value: Record<string, unknown>]
}>()

function flattenProperties(
  properties: Record<string, JsonSchemaProperty> | undefined,
  parent = ''
): FlatField[] {
  if (!properties) {
    return []
  }

  const result: FlatField[] = []

  for (const [key, property] of Object.entries(properties)) {
    const fieldKey = parent ? `${parent}.${key}` : key

    if (property.type === 'object' && property.properties) {
      result.push(...flattenProperties(property.properties, fieldKey))
      continue
    }

    const kind = property.type === 'integer' ? 'integer' : property.type ?? 'string'
    const label = property.title ?? fieldKey
    const description = property.description ?? ''

    if (kind === 'array') {
      const arrayOptions = property.items?.enum
      result.push({
        key: fieldKey,
        label,
        description,
        component: 'select',
        inputType: 'text',
        kind: 'array',
        options: arrayOptions,
        multiple: true,
      })
      continue
    }

    if (property.enum && property.enum.length > 0) {
      result.push({
        key: fieldKey,
        label,
        description,
        component: 'select',
        inputType: 'text',
        kind: kind === 'number' || kind === 'integer' ? kind : 'string',
        options: property.enum,
        multiple: false,
      })
      continue
    }

    if (kind === 'boolean') {
      result.push({
        key: fieldKey,
        label,
        description,
        component: 'switch',
        inputType: 'text',
        kind: 'boolean',
      })
      continue
    }

    result.push({
      key: fieldKey,
      label,
      description,
      component: 'input',
      inputType: kind === 'number' || kind === 'integer' ? 'number' : 'text',
      kind: kind === 'number' || kind === 'integer' ? kind : 'string',
    })
  }

  return result
}

const flatFields = computed(() => flattenProperties(props.schema?.properties))

const fieldValue = (key: string) => props.modelValue[key]

const selectValue = (
  key: string,
  multiple?: boolean,
): string | number | boolean | Array<string | number | boolean> | null | undefined => {
  const value = props.modelValue[key]
  if (multiple) {
    return Array.isArray(value) ? value : []
  }
  if (Array.isArray(value)) {
    return value[0]
  }
  return value as string | number | boolean | null | undefined
}

const stringValue = (key: string) => {
  const value = props.modelValue[key]
  return value === undefined ? '' : String(value)
}

function normalizeInput(value: unknown, kind: FlatField['kind']): unknown {
  if (kind === 'boolean') {
    return Boolean(value)
  }
  if (kind === 'number' || kind === 'integer') {
    const parsed = Number(value)
    return Number.isNaN(parsed) ? 0 : parsed
  }
  if (kind === 'array') {
    return Array.isArray(value) ? value : value === null || value === undefined ? [] : [value]
  }
  return String(value ?? '')
}

function updateField(key: string, value: unknown) {
  const nextValue = value === null || value === undefined ? '' : value
  emit('update:modelValue', {
    ...props.modelValue,
    [key]: nextValue,
  })
}
</script>
