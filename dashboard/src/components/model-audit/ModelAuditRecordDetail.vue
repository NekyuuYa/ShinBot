<template>
  <div class="detail-grid">
    <audit-detail-section :title="detailsLabel" icon="mdi-identifier">
      <audit-detail-row :label="executionIdLabel" :value="record.id" />
      <audit-detail-row :label="promptSnapshotIdLabel" :value="record.promptSnapshotId" />
      <audit-detail-row :label="purposeLabel" :value="record.purpose" />
    </audit-detail-section>

    <audit-detail-section :title="routingLabel" icon="mdi-routes">
      <audit-detail-row :label="providerIdLabel" :value="record.providerId" />
      <audit-detail-row :label="modelIdLabel" :value="record.modelId" />
      <audit-detail-row :label="routeIdLabel" :value="record.routeId" />
      <audit-detail-row :label="callerLabel" :value="record.caller" />
      <audit-detail-row :label="sessionIdLabel" :value="record.sessionId" />
      <audit-detail-row :label="instanceIdLabel" :value="record.instanceId" />
    </audit-detail-section>

    <audit-detail-section :title="timingLabel" icon="mdi-timer-outline">
      <audit-detail-row :label="startedAtLabel" :value="formatDateTime(record.startedAt)" />
      <audit-detail-row :label="firstTokenAtLabel" :value="formatDateTime(record.firstTokenAt)" />
      <audit-detail-row :label="finishedAtLabel" :value="formatDateTime(record.finishedAt)" />
      <audit-detail-row :label="latencyLabel" :value="formatDuration(record.latencyMs)" />
      <audit-detail-row :label="ttftLabel" :value="formatDuration(record.timeToFirstTokenMs)" />
    </audit-detail-section>

    <audit-detail-section :title="usageLabel" icon="mdi-counter">
      <audit-detail-row :label="inputTokensLabel" :value="formatNumber(record.inputTokens)" />
      <audit-detail-row :label="outputTokensLabel" :value="formatNumber(record.outputTokens)" />
      <audit-detail-row :label="cacheReadLabel" :value="formatNumber(record.cacheReadTokens)" />
      <audit-detail-row :label="cacheWriteLabel" :value="formatNumber(record.cacheWriteTokens)" />
      <audit-detail-row :label="cacheHitLabel" :value="boolLabel(record.cacheHit)" />
      <audit-detail-row :label="costLabel" :value="formatCost(record)" />
    </audit-detail-section>

    <audit-detail-section
      v-if="record.fallbackFromModelId || record.fallbackReason"
      :title="fallbackLabel"
      icon="mdi-call-split"
    >
      <audit-detail-row :label="fallbackFromLabel" :value="record.fallbackFromModelId" />
      <audit-detail-row :label="fallbackReasonLabel" :value="record.fallbackReason" />
    </audit-detail-section>

    <audit-detail-section
      v-if="!record.success || record.errorCode || record.errorMessage"
      :title="errorLabel"
      icon="mdi-alert-outline"
      tone="error"
    >
      <audit-detail-row :label="errorCodeLabel" :value="record.errorCode" />
      <audit-detail-row :label="errorMessageLabel" :value="record.errorMessage" />
    </audit-detail-section>
  </div>

  <section class="metadata-block">
    <div class="metadata-block__head">
      <v-icon icon="mdi-code-json" size="18" />
      <strong>{{ metadataLabel }}</strong>
    </div>
    <div v-if="hasMetadata" class="metadata-json">
      <json-tree :value="record.metadata" />
    </div>
    <div v-else class="metadata-empty">{{ metadataEmptyLabel }}</div>
  </section>

  <model-audit-payload-panel
    :active-tab="activeTab"
    :title="payloadLabel"
    :view-label="viewPayloadLabel"
    :not-loaded-label="notLoadedLabel"
    :unavailable-label="unavailableLabel"
    :payload-ref-label="payloadRefLabel"
    :payload-expires-at-label="payloadExpiresAtLabel"
    :labels="labels"
    :payload="payload"
    :error="payloadError"
    :loading="loading"
    :available="available"
    :payload-ref="payloadRef"
    :payload-expires-at="payloadExpiresAt"
    :empty-value="emptyValue"
    @load="$emit('load-payload')"
    @update:active-tab="(value) => $emit('update:active-tab', value)"
  />
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { ModelExecutionAuditPayloadResponse, ModelExecutionRecord } from '@/api/modelRuntime'
import AuditDetailRow from '@/components/audit/AuditDetailRow.vue'
import AuditDetailSection from '@/components/audit/AuditDetailSection.vue'
import JsonTree from '@/components/audit/JsonTree.vue'
import ModelAuditPayloadPanel from '@/components/model-audit/ModelAuditPayloadPanel.vue'

const props = defineProps<{
  record: ModelExecutionRecord
  activeTab: string
  payload?: ModelExecutionAuditPayloadResponse | null
  payloadError?: string
  loading?: boolean
  available?: boolean
  payloadRef?: string
  payloadExpiresAt?: string
  labels: Record<'request' | 'response' | 'return' | 'error' | 'meta', string>
  detailsLabel: string
  routingLabel: string
  timingLabel: string
  usageLabel: string
  fallbackLabel: string
  errorLabel: string
  metadataLabel: string
  metadataEmptyLabel: string
  payloadLabel: string
  viewPayloadLabel: string
  notLoadedLabel: string
  unavailableLabel: string
  payloadRefLabel: string
  payloadExpiresAtLabel: string
  emptyValue: string
  executionIdLabel: string
  promptSnapshotIdLabel: string
  purposeLabel: string
  providerIdLabel: string
  modelIdLabel: string
  routeIdLabel: string
  callerLabel: string
  sessionIdLabel: string
  instanceIdLabel: string
  startedAtLabel: string
  firstTokenAtLabel: string
  finishedAtLabel: string
  latencyLabel: string
  ttftLabel: string
  inputTokensLabel: string
  outputTokensLabel: string
  cacheReadLabel: string
  cacheWriteLabel: string
  cacheHitLabel: string
  costLabel: string
  fallbackFromLabel: string
  fallbackReasonLabel: string
  errorCodeLabel: string
  errorMessageLabel: string
  formatNumber: (value: number) => string
  formatDuration: (value: number | null | undefined) => string
  formatDateTime: (value: string | null | undefined) => string
  formatCost: (record: ModelExecutionRecord) => string
  boolLabel: (value: boolean) => string
}>()

defineEmits<{
  'load-payload': []
  'update:active-tab': [value: string]
}>()

const hasMetadata = computed(() => Object.keys(props.record.metadata || {}).length > 0)
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.detail-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(220px, 1fr));
  gap: 14px;
}

.metadata-block {
  margin-top: 16px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.metadata-block__head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 14px 8px;
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-size: $font-size-sm;
  font-weight: 800;
}

.metadata-json {
  margin: 6px 16px 16px;
}

.metadata-empty {
  padding: 0 16px 16px;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: $font-size-sm;
}

@media (max-width: 1280px) {
  .detail-grid {
    grid-template-columns: repeat(2, minmax(220px, 1fr));
  }
}

@media (max-width: 760px) {
  .detail-grid {
    grid-template-columns: 1fr;
  }
}
</style>
