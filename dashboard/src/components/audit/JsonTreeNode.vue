<template>
  <details
    v-if="arrayValue"
    class="json-node"
    :open="open"
    :style="indentStyle"
  >
    <summary class="json-node__summary">
      <span class="json-node__key">{{ label || '[ ]' }}</span>
      <span class="json-node__meta">Array({{ arrayValue.length }})</span>
    </summary>
    <div class="json-node__body">
      <json-tree-node
        v-for="(item, index) in arrayValue"
        :key="`${depth}-${index}`"
        :value="item"
        :label="`[${index}]`"
        :depth="depth + 1"
        :open="depth < 1"
      />
      <div v-if="arrayValue.length === 0" class="json-node__empty">[]</div>
    </div>
  </details>

  <details
    v-else-if="objectEntries"
    class="json-node"
    :open="open || depth < 1"
    :style="indentStyle"
  >
    <summary class="json-node__summary">
      <span class="json-node__key">{{ label || '{ }' }}</span>
      <span class="json-node__meta">Object({{ objectEntries.length }})</span>
    </summary>
    <div class="json-node__body">
      <json-tree-node
        v-for="[key, child] in objectEntries"
        :key="`${depth}-${key}`"
        :value="child"
        :label="key"
        :depth="depth + 1"
        :open="depth < 1"
      />
      <div v-if="objectEntries.length === 0" class="json-node__empty">{}</div>
    </div>
  </details>

  <div v-else class="json-node json-node--scalar" :style="indentStyle">
    <span v-if="label" class="json-node__key">{{ label }}</span>
    <span class="json-node__scalar">{{ scalarValue }}</span>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

defineOptions({
  name: 'JsonTreeNode',
})

const props = withDefaults(
  defineProps<{
    value: unknown
    label?: string
    depth?: number
    open?: boolean
  }>(),
  {
    label: '',
    depth: 0,
    open: false,
  }
)

const indentStyle = computed(() => ({
  paddingLeft: `${props.depth * 16}px`,
}))

const arrayValue = computed(() =>
  Array.isArray(props.value) ? (props.value as unknown[]) : null
)

const objectEntries = computed(() => {
  if (!props.value || typeof props.value !== 'object' || Array.isArray(props.value)) {
    return null
  }
  return Object.entries(props.value as Record<string, unknown>)
})

const scalarValue = computed(() => {
  if (props.value === null) return 'null'
  if (props.value === undefined) return 'undefined'
  if (typeof props.value === 'string') return JSON.stringify(props.value)
  if (typeof props.value === 'number' || typeof props.value === 'boolean') {
    return String(props.value)
  }
  return JSON.stringify(props.value, null, 2)
})
</script>
