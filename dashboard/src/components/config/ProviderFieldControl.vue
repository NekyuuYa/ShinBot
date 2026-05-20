<template>
  <v-select
    v-if="field.component === 'select'"
    :model-value="modelValue"
    :items="field.options"
    :label="field.label"
    :hint="field.description"
    :error-messages="errorMessages"
    :disabled="disabled"
    :density="density"
    item-title="title"
    item-value="value"
    variant="outlined"
    persistent-hint
    clearable
    @update:model-value="(value) => emitValue(value)"
  />

  <v-switch
    v-else-if="field.component === 'switch'"
    :model-value="Boolean(modelValue)"
    :label="field.label"
    :hint="field.description"
    :error-messages="errorMessages"
    :disabled="disabled"
    :density="density"
    color="primary"
    inset
    persistent-hint
    @update:model-value="(value) => emitValue(Boolean(value))"
  />

  <v-combobox
    v-else-if="field.component === 'string-list' || field.component === 'integer-list'"
    :model-value="listValue"
    :label="field.label"
    :hint="field.description"
    :error-messages="errorMessages"
    :disabled="disabled"
    :density="density"
    variant="outlined"
    multiple
    chips
    closable-chips
    clearable
    persistent-hint
    @update:model-value="updateListValue"
  />

  <v-textarea
    v-else-if="field.component === 'json' || field.component === 'array-object'"
    v-model="jsonText"
    :label="field.label"
    :hint="field.description"
    :error-messages="jsonErrorMessages"
    :disabled="disabled"
    :density="density"
    :rows="jsonRows"
    variant="outlined"
    auto-grow
    persistent-hint
    @update:model-value="updateJsonValue"
  />

  <v-text-field
    v-else-if="field.component === 'model-ref'"
    :model-value="scalarText"
    :label="field.label"
    :hint="field.description"
    :placeholder="field.placeholder"
    :error-messages="errorMessages"
    :disabled="disabled"
    :density="density"
    append-inner-icon="mdi-cube-scan"
    variant="outlined"
    persistent-hint
    clearable
    @click:append-inner="modelPickerVisible = true"
    @update:model-value="updateScalarValue"
  />

  <v-text-field
    v-else
    :model-value="scalarText"
    :label="field.label"
    :hint="field.description"
    :placeholder="field.placeholder"
    :type="resolvedInputType"
    :min="field.min"
    :max="field.max"
    :error-messages="errorMessages"
    :disabled="disabled"
    :density="density"
    :append-inner-icon="secretIcon"
    variant="outlined"
    persistent-hint
    @click:append-inner="toggleSecretVisibility"
    @update:model-value="updateScalarValue"
  />

  <model-id-picker-dialog
    v-if="field.component === 'model-ref'"
    v-model="modelPickerVisible"
    :current-value="scalarText"
    :route-options="modelRefRouteOptions"
    :provider-groups="modelRefProviderGroups"
    @select="selectModelRef"
  />
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'

import type { ConfigValue } from '@/api/config'
import type { ConfigFormField } from '@/config'
import ModelIdPickerDialog from '@/components/model-runtime/ModelIdPickerDialog.vue'

type FieldDensity = 'default' | 'comfortable' | 'compact'

interface ModelRefRouteOption {
  id: string
  title: string
  subtitle: string
  enabled: boolean
}

interface ModelRefProviderGroupItem {
  value: string
  title: string
  subtitle: string
  kind: 'catalog' | 'configured'
}

interface ModelRefProviderGroup {
  providerId: string
  providerName: string
  providerType: string
  items: ModelRefProviderGroupItem[]
}

interface Props {
  field: ConfigFormField
  modelValue?: ConfigValue
  disabled?: boolean
  density?: FieldDensity
  modelRefRouteOptions?: ModelRefRouteOption[]
  modelRefProviderGroups?: ModelRefProviderGroup[]
  jsonErrorText?: string
}

const props = withDefaults(defineProps<Props>(), {
  disabled: false,
  density: 'comfortable',
  modelRefRouteOptions: () => [],
  modelRefProviderGroups: () => [],
  jsonErrorText: 'Invalid JSON.',
})

const emit = defineEmits<{
  'update:modelValue': [value: ConfigValue]
}>()

const secretVisible = ref(false)
const modelPickerVisible = ref(false)
const jsonText = ref('')
const localJsonError = ref('')

const errorMessages = computed(() => props.field.issues.map((issue) => issue.message))
const jsonErrorMessages = computed(() => [
  ...errorMessages.value,
  ...(localJsonError.value ? [localJsonError.value] : []),
])

const scalarText = computed(() => {
  if (props.modelValue === undefined || props.modelValue === null) {
    return ''
  }
  return String(props.modelValue)
})

const listValue = computed<Array<string | number>>(() => {
  if (!Array.isArray(props.modelValue)) {
    return []
  }
  return props.modelValue
    .filter((value): value is string | number =>
      typeof value === 'string' || typeof value === 'number'
    )
})

const jsonRows = computed(() => (props.field.component === 'array-object' ? 6 : 5))
const resolvedInputType = computed(() => {
  if (props.field.secret) {
    return secretVisible.value ? 'text' : 'password'
  }
  return props.field.inputType
})
const secretIcon = computed(() => {
  if (!props.field.secret) {
    return undefined
  }
  return secretVisible.value ? 'mdi-eye-off-outline' : 'mdi-eye-outline'
})

function formatJsonValue(value: ConfigValue | undefined): string {
  if (value === undefined || value === null || value === '') {
    return props.field.component === 'array-object' ? '[]' : '{}'
  }
  return JSON.stringify(value, null, 2)
}

watch(
  () => props.modelValue,
  (value) => {
    if (props.field.component !== 'json' && props.field.component !== 'array-object') {
      return
    }
    jsonText.value = formatJsonValue(value)
    localJsonError.value = ''
  },
  { immediate: true }
)

function emitValue(value: ConfigValue) {
  emit('update:modelValue', value)
}

function toggleSecretVisibility() {
  if (props.field.secret) {
    secretVisible.value = !secretVisible.value
  }
}

function updateScalarValue(value: unknown) {
  const text = String(value ?? '')
  if (
    props.field.valueType === 'integer'
    || props.field.valueType === 'float'
    || props.field.valueType === 'duration'
  ) {
    if (!text.trim()) {
      emitValue(null)
      return
    }
    const parsed = props.field.valueType === 'integer'
      ? Number.parseInt(text, 10)
      : Number.parseFloat(text)
    emitValue(Number.isFinite(parsed) ? parsed : null)
    return
  }
  emitValue(text)
}

function selectModelRef(value: string) {
  modelPickerVisible.value = false
  emitValue(value)
}

function updateListValue(value: unknown) {
  const items = Array.isArray(value) ? value : []
  if (props.field.component === 'integer-list') {
    emitValue(
      items
        .map((item) => Number.parseInt(String(item), 10))
        .filter((item) => Number.isFinite(item))
    )
    return
  }

  emitValue(
    items
      .map((item) => String(item).trim())
      .filter(Boolean)
  )
}

function updateJsonValue(value: string) {
  jsonText.value = value
  const trimmed = value.trim()
  if (!trimmed) {
    const fallback = props.field.component === 'array-object' ? [] : {}
    localJsonError.value = ''
    emitValue(fallback)
    return
  }

  try {
    const parsed = JSON.parse(trimmed) as ConfigValue
    if (props.field.component === 'array-object' && !Array.isArray(parsed)) {
      localJsonError.value = props.jsonErrorText
      return
    }
    if (props.field.component === 'json' && (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed))) {
      localJsonError.value = props.jsonErrorText
      return
    }
    localJsonError.value = ''
    emitValue(parsed)
  } catch {
    localJsonError.value = props.jsonErrorText
  }
}
</script>
