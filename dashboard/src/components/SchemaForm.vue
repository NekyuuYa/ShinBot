<template>
  <div class="d-flex flex-column ga-4">
    <v-row>
      <v-col v-for="field in basicFields" :key="field.key" cols="12" md="6">
        <v-select
          v-if="field.component === 'select'"
          :model-value="selectValue(field.key, field.multiple)"
          :items="field.options"
          :label="field.label"
          :hint="field.description"
          :multiple="field.multiple"
          chips
          persistent-hint
          item-title="title"
          item-value="value"
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

    <v-expansion-panels v-if="advancedFields.length > 0" variant="accordion">
      <v-expansion-panel rounded="xl">
        <v-expansion-panel-title>
          {{ $t('pages.instances.form.advancedConfig') }}
        </v-expansion-panel-title>
        <v-expansion-panel-text>
          <v-row>
            <v-col v-for="field in advancedFields" :key="field.key" cols="12" md="6">
              <v-select
                v-if="field.component === 'select'"
                :model-value="selectValue(field.key, field.multiple)"
                :items="field.options"
                :label="field.label"
                :hint="field.description"
                :multiple="field.multiple"
                chips
                persistent-hint
                item-title="title"
                item-value="value"
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
        </v-expansion-panel-text>
      </v-expansion-panel>
    </v-expansion-panels>
  </div>
</template>
<script setup lang="ts">
import { computed } from 'vue'
import type { PluginConfigSchema, JsonSchemaProperty } from '@/api/plugins'

interface FlatField {
  key: string
  label: string
  description: string
  group: 'basic' | 'advanced'
  component: 'input' | 'switch' | 'select'
  inputType: 'text' | 'number' | 'password'
  kind: 'string' | 'number' | 'integer' | 'boolean' | 'array'
  options?: Array<string | number | boolean | { title: string; value: string | number | boolean }>
  multiple?: boolean
}

interface Props {
  schema: PluginConfigSchema | null
  modelValue: Record<string, unknown>
  mode?: string
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

    if (property.modes?.length && props.mode && !property.modes.includes(props.mode)) {
      continue
    }

    if (property.type === 'object' && property.properties) {
      result.push(...flattenProperties(property.properties, fieldKey))
      continue
    }

    const kind = property.type === 'integer' ? 'integer' : property.type ?? 'string'
    const label = property.title ?? fieldKey
    const description = property.description ?? ''
    const group = property.ui_group === 'advanced' ? 'advanced' : 'basic'

    if (kind === 'array') {
      const arrayOptions = property.items?.enum?.map((value) => ({
        title: String(value),
        value,
      }))
      result.push({
        key: fieldKey,
        label,
        description,
        group,
        component: 'select',
        inputType: 'text',
        kind: 'array',
        options: arrayOptions,
        multiple: true,
      })
      continue
    }

    if (property.enum && property.enum.length > 0) {
      const options =
        Array.isArray(property.enum_titles) && property.enum_titles.length === property.enum.length
          ? property.enum.map((value, index) => ({
              title: property.enum_titles?.[index] ?? String(value),
              value,
            }))
          : property.enum.map((value) => ({ title: String(value), value }))
      result.push({
        key: fieldKey,
        label,
        description,
        group,
        component: 'select',
        inputType: 'text',
        kind: kind === 'number' || kind === 'integer' ? kind : 'string',
        options,
        multiple: false,
      })
      continue
    }

    if (kind === 'boolean') {
      result.push({
        key: fieldKey,
        label,
        description,
        group,
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
      group,
      component: 'input',
      inputType: kind === 'number' || kind === 'integer' ? 'number' : 'text',
      kind: kind === 'number' || kind === 'integer' ? kind : 'string',
    })
  }

  return result
}

const flatFields = computed(() => flattenProperties(props.schema?.properties))
const basicFields = computed(() => flatFields.value.filter((field) => field.group === 'basic'))
const advancedFields = computed(() =>
  flatFields.value.filter((field) => field.group === 'advanced')
)

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
