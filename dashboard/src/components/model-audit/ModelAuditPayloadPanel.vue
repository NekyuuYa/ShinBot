<template>
  <section class="payload-block">
    <div class="payload-block__head">
      <div class="payload-block__title">
        <v-icon icon="mdi-file-document-outline" size="18" />
        <strong>{{ title }}</strong>
      </div>
      <v-btn
        size="small"
        variant="tonal"
        color="primary"
        :loading="loading"
        :disabled="!available"
        @click="$emit('load')"
      >
        {{ viewLabel }}
      </v-btn>
    </div>

    <v-alert v-if="error" type="warning" variant="tonal" class="mx-4 mb-3">
      {{ error }}
    </v-alert>

    <div v-if="payload" class="payload-tabs">
      <v-tabs
        :model-value="activeTab"
        density="comfortable"
        @update:model-value="updateTab"
      >
        <v-tab value="request">{{ labels.request }}</v-tab>
        <v-tab value="response">{{ labels.response }}</v-tab>
        <v-tab value="return">{{ labels.return }}</v-tab>
        <v-tab value="error">{{ labels.error }}</v-tab>
        <v-tab value="meta">{{ labels.meta }}</v-tab>
      </v-tabs>
      <v-window
        :model-value="activeTab"
        class="payload-window"
        @update:model-value="updateTab"
      >
        <v-window-item value="request">
          <json-tree :value="payload.request" />
        </v-window-item>
        <v-window-item value="response">
          <json-tree :value="payload.response" />
        </v-window-item>
        <v-window-item value="return">
          <json-tree :value="payload.return" />
        </v-window-item>
        <v-window-item value="error">
          <json-tree :value="payload.error" />
        </v-window-item>
        <v-window-item value="meta">
          <json-tree :value="payload.meta" />
        </v-window-item>
      </v-window>
      <div class="payload-footnote">
        <span>{{ payloadRefLabel }}: {{ payloadRef || emptyValue }}</span>
        <span>{{ payloadExpiresAtLabel }}: {{ payloadExpiresAt || emptyValue }}</span>
      </div>
    </div>

    <div v-else class="payload-empty">
      {{ available ? notLoadedLabel : unavailableLabel }}
    </div>
  </section>
</template>

<script setup lang="ts">
import JsonTree from '@/components/audit/JsonTree.vue'
import type { ModelExecutionAuditPayloadResponse } from '@/api/modelRuntime'

withDefaults(
  defineProps<{
    title: string
    viewLabel: string
    notLoadedLabel: string
    unavailableLabel: string
    payloadRefLabel: string
    payloadExpiresAtLabel: string
    labels: Record<'request' | 'response' | 'return' | 'error' | 'meta', string>
    activeTab: string
    payload?: ModelExecutionAuditPayloadResponse | null
    error?: string
    loading?: boolean
    available?: boolean
    payloadRef?: string
    payloadExpiresAt?: string
    emptyValue?: string
  }>(),
  {
    payload: null,
    error: '',
    loading: false,
    available: false,
    payloadRef: '',
    payloadExpiresAt: '',
    emptyValue: '—',
  }
)

const emit = defineEmits<{
  load: []
  'update:activeTab': [value: string]
}>()

const updateTab = (value: unknown) => {
  if (typeof value === 'string') {
    emit('update:activeTab', value)
  }
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.payload-block {
  margin-top: 16px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.payload-block__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px 10px;
}

.payload-block__title {
  display: flex;
  align-items: center;
  gap: 8px;
  color: rgba(var(--v-theme-on-surface), 0.82);
}

.payload-tabs {
  padding: 0 16px 16px;
}

.payload-window {
  margin-top: 14px;
}

.payload-footnote {
  display: flex;
  flex-wrap: wrap;
  gap: 12px 20px;
  margin-top: 10px;
  padding: 0 2px 2px;
  color: rgba(var(--v-theme-on-surface), 0.56);
  font-size: $font-size-xs;
}

.payload-empty {
  padding: 0 16px 16px;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: $font-size-sm;
}
</style>
