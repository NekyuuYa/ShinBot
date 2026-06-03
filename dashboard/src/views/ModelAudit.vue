<template>
  <v-container fluid class="pa-0 model-audit-page">
    <app-page-header
      :title="$t('pages.modelAudit.title')"
      :subtitle="$t('pages.modelAudit.subtitle')"
      :kicker="$t('pages.modelAudit.kicker')"
    >
      <template #actions>
        <v-btn
          color="primary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          rounded="lg"
          :loading="loading"
          @click="refresh"
        >
          {{ $t('pages.modelAudit.actions.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert v-if="error" type="error" variant="tonal" class="mb-6">
      {{ error }}
    </v-alert>

    <model-audit-filter-panel
      class="mb-6"
      v-model:query="filters.query"
      v-model:provider-id="filters.providerId"
      v-model:model-id="filters.modelId"
      v-model:route-id="filters.routeId"
      v-model:caller="filters.caller"
      v-model:session-id="filters.sessionId"
      v-model:instance-id="filters.instanceId"
      v-model:status="statusFilter"
      :title="$t('pages.modelAudit.filters.title')"
      :total-label="$t('pages.modelAudit.labels.total', { total })"
      :clear-label="$t('pages.modelAudit.actions.clear')"
      :search-label="$t('pages.modelAudit.filters.search')"
      :provider-label="$t('pages.modelAudit.filters.provider')"
      :model-label="$t('pages.modelAudit.filters.model')"
      :route-label="$t('pages.modelAudit.filters.route')"
      :caller-label="$t('pages.modelAudit.filters.caller')"
      :session-label="$t('pages.modelAudit.filters.session')"
      :instance-label="$t('pages.modelAudit.filters.instance')"
      :status-label="$t('pages.modelAudit.filters.status')"
      :provider-options="providerOptions"
      :model-options="modelOptions"
      :route-options="routeOptions"
      :status-options="statusOptions"
      @clear="clearFilters"
    />

    <model-audit-record-list
      :records="records"
      :loading="loading"
      :offset="offset"
      :has-next-page="hasNextPage"
      :selected-record-id="selectedRecordId"
      :page-label="$t('pages.modelAudit.labels.page', pageRange)"
      :records-label="$t('pages.modelAudit.sections.records')"
      :empty-label="$t('pages.modelAudit.labels.empty')"
      :success-label="$t('pages.modelAudit.labels.success')"
      :failed-label="$t('pages.modelAudit.labels.failed')"
      :none-label="$t('pages.modelAudit.labels.none')"
      :previous-label="$t('pages.modelAudit.actions.previous')"
      :next-label="$t('pages.modelAudit.actions.next')"
      :format-date-time="formatDateTime"
      :format-compact-number="formatCompactNumber"
      :format-duration="formatDuration"
      @previous="previousPage"
      @next="nextPage"
      @toggle="toggleRecord"
    >
      <template #detail="{ record }">
        <model-audit-record-detail
          :record="record"
          :active-tab="payloadTabById[record.id]"
          :payload="payloadById[record.id]"
          :payload-error="payloadErrorById[record.id]"
          :loading="payloadLoadingId === record.id"
          :available="record.auditPayloadAvailable"
          :payload-ref="record.auditPayloadRef"
          :payload-expires-at="record.auditPayloadExpiresAt"
          :labels="payloadLabels"
          :details-label="$t('pages.modelAudit.sections.details')"
          :routing-label="$t('pages.modelAudit.sections.routing')"
          :timing-label="$t('pages.modelAudit.sections.timing')"
          :usage-label="$t('pages.modelAudit.sections.usage')"
          :fallback-label="$t('pages.modelAudit.labels.fallback')"
          :error-label="$t('pages.modelAudit.sections.error')"
          :metadata-label="$t('pages.modelAudit.sections.metadata')"
          :metadata-empty-label="$t('pages.modelAudit.labels.metadataEmpty')"
          :payload-label="$t('pages.modelAudit.sections.payload')"
          :view-payload-label="$t('pages.modelAudit.actions.viewPayload')"
          :not-loaded-label="$t('pages.modelAudit.payload.notLoaded')"
          :unavailable-label="$t('pages.modelAudit.payload.unavailable')"
          :payload-ref-label="$t('pages.modelAudit.labels.auditPayloadRef')"
          :payload-expires-at-label="$t('pages.modelAudit.labels.auditPayloadExpiresAt')"
          :empty-value="NONE_VALUE"
          :execution-id-label="$t('pages.modelAudit.labels.executionId')"
          :prompt-snapshot-id-label="$t('pages.modelAudit.labels.promptSnapshotId')"
          :purpose-label="$t('pages.modelAudit.labels.purpose')"
          :provider-id-label="$t('pages.modelAudit.labels.providerId')"
          :model-id-label="$t('pages.modelAudit.labels.modelId')"
          :route-id-label="$t('pages.modelAudit.labels.routeId')"
          :caller-label="$t('pages.modelAudit.labels.caller')"
          :session-id-label="$t('pages.modelAudit.labels.sessionId')"
          :instance-id-label="$t('pages.modelAudit.labels.instanceId')"
          :started-at-label="$t('pages.modelAudit.labels.startedAt')"
          :first-token-at-label="$t('pages.modelAudit.labels.firstTokenAt')"
          :finished-at-label="$t('pages.modelAudit.labels.finishedAt')"
          :latency-label="$t('pages.modelAudit.labels.latency')"
          :ttft-label="$t('pages.modelAudit.labels.ttft')"
          :input-tokens-label="$t('pages.modelAudit.labels.inputTokens')"
          :output-tokens-label="$t('pages.modelAudit.labels.outputTokens')"
          :cache-read-label="$t('pages.modelAudit.labels.cacheRead')"
          :cache-write-label="$t('pages.modelAudit.labels.cacheWrite')"
          :cache-hit-label="$t('pages.modelAudit.labels.cacheHit')"
          :cost-label="$t('pages.modelAudit.labels.cost')"
          :fallback-from-label="$t('pages.modelAudit.labels.fallbackFrom')"
          :fallback-reason-label="$t('pages.modelAudit.labels.fallbackReason')"
          :error-code-label="$t('pages.modelAudit.labels.errorCode')"
          :error-message-label="$t('pages.modelAudit.labels.errorMessage')"
          :format-number="formatNumber"
          :format-duration="formatDuration"
          :format-date-time="formatDateTime"
          :format-cost="formatCost"
          :bool-label="boolLabel"
          @load-payload="loadPayload(record.id)"
          @update:active-tab="(value) => { payloadTabById[record.id] = value }"
        />
      </template>
    </model-audit-record-list>
  </v-container>
</template>

<script setup lang="ts">
import {
  computed,
  onBeforeUnmount,
  onMounted,
  reactive,
  ref,
  watch,
} from 'vue'
import { storeToRefs } from 'pinia'
import { useI18n } from 'vue-i18n'

import AppPageHeader from '@/components/AppPageHeader.vue'
import ModelAuditFilterPanel from '@/components/model-audit/ModelAuditFilterPanel.vue'
import ModelAuditRecordList from '@/components/model-audit/ModelAuditRecordList.vue'
import ModelAuditRecordDetail from '@/components/model-audit/ModelAuditRecordDetail.vue'
import {
  modelRuntimeApi,
  type ModelExecutionAuditPayloadResponse,
  type ModelExecutionAuditQuery,
  type ModelExecutionRecord,
  type ModelRuntimeModel,
  type ModelRuntimeProvider,
  type ModelRuntimeRoute,
} from '@/api/modelRuntime'
import { useFormatters } from '@/composables/useFormatters'
import { useSystemSettingsStore } from '@/stores/systemSettings'

const PAGE_SIZE = 30
const NONE_VALUE = '—'

type StatusFilter = 'all' | 'success' | 'failed'

const systemSettingsStore = useSystemSettingsStore()
const { locale, t } = useI18n()
const { pricingCurrency } = storeToRefs(systemSettingsStore)
const { formatNumber, formatCompactNumber, formatCurrency, formatDateTime, formatDuration } =
  useFormatters(locale, pricingCurrency)

const loading = ref(false)
const error = ref('')
const records = ref<ModelExecutionRecord[]>([])
const payloadById = reactive<Record<string, ModelExecutionAuditPayloadResponse | null | undefined>>({})
const payloadTabById = reactive<Record<string, string>>({})
const payloadErrorById = reactive<Record<string, string>>({})
const payloadLoadingId = ref('')
const total = ref(0)
const offset = ref(0)
const selectedRecordId = ref('')
const statusFilter = ref<StatusFilter>('all')
const providers = ref<ModelRuntimeProvider[]>([])
const models = ref<ModelRuntimeModel[]>([])
const routes = ref<ModelRuntimeRoute[]>([])
const filters = reactive({
  query: '',
  providerId: '',
  modelId: '',
  routeId: '',
  caller: '',
  sessionId: '',
  instanceId: '',
})

let refreshTimer: number | undefined

const statusOptions = computed(() => [
  { title: t('pages.modelAudit.filters.all'), value: 'all' },
  { title: t('pages.modelAudit.filters.success'), value: 'success' },
  { title: t('pages.modelAudit.filters.failed'), value: 'failed' },
])

const payloadLabels = computed(() => ({
  request: t('pages.modelAudit.payload.request'),
  response: t('pages.modelAudit.payload.response'),
  return: t('pages.modelAudit.payload.return'),
  error: t('pages.modelAudit.payload.error'),
  meta: t('pages.modelAudit.payload.meta'),
}))

const providerOptions = computed(() =>
  providers.value.map((provider) => ({
    title: provider.displayName ? `${provider.displayName} (${provider.id})` : provider.id,
    value: provider.id,
  }))
)

const modelOptions = computed(() =>
  models.value
    .filter((model) => !filters.providerId || model.providerId === filters.providerId)
    .map((model) => ({
      title: model.displayName ? `${model.displayName} (${model.id})` : model.id,
      value: model.id,
    }))
)

const routeOptions = computed(() =>
  routes.value.map((route) => ({
    title: route.purpose ? `${route.id} · ${route.purpose}` : route.id,
    value: route.id,
  }))
)

const pageRange = computed(() => {
  if (total.value === 0) {
    return { start: 0, end: 0, total: 0 }
  }
  return {
    start: offset.value + 1,
    end: Math.min(offset.value + records.value.length, total.value),
    total: total.value,
  }
})

const hasNextPage = computed(() => offset.value + PAGE_SIZE < total.value)

const cleanString = (value: string) => {
  const trimmed = value.trim()
  return trimmed || undefined
}

const buildQuery = (): ModelExecutionAuditQuery => ({
  limit: PAGE_SIZE,
  offset: offset.value,
  providerId: cleanString(filters.providerId),
  modelId: cleanString(filters.modelId),
  routeId: cleanString(filters.routeId),
  caller: cleanString(filters.caller),
  sessionId: cleanString(filters.sessionId),
  instanceId: cleanString(filters.instanceId),
  query: cleanString(filters.query),
  success:
    statusFilter.value === 'all'
      ? null
      : statusFilter.value === 'success',
})

const refresh = async () => {
  loading.value = true
  error.value = ''
  try {
    const response = await modelRuntimeApi.listExecutionAuditRecords(buildQuery())
    const payload = response.data.data
    records.value = payload?.items || []
    total.value = payload?.total || 0
    if (!records.value.some((record) => record.id === selectedRecordId.value)) {
      selectedRecordId.value = records.value[0]?.id || ''
    }
  } catch (err) {
    error.value = err instanceof Error ? err.message : t('pages.modelAudit.messages.loadFailed')
  } finally {
    loading.value = false
  }
}

const loadCatalog = async () => {
  const [providerResp, modelResp, routeResp] = await Promise.all([
    modelRuntimeApi.listProviders(),
    modelRuntimeApi.listModels(),
    modelRuntimeApi.listRoutes(),
  ])
  providers.value = providerResp.data.data || []
  models.value = modelResp.data.data || []
  routes.value = routeResp.data.data || []
}

const scheduleRefresh = () => {
  window.clearTimeout(refreshTimer)
  refreshTimer = window.setTimeout(() => {
    offset.value = 0
    void refresh()
  }, 250)
}

const clearFilters = () => {
  filters.query = ''
  filters.providerId = ''
  filters.modelId = ''
  filters.routeId = ''
  filters.caller = ''
  filters.sessionId = ''
  filters.instanceId = ''
  statusFilter.value = 'all'
}

const previousPage = () => {
  offset.value = Math.max(0, offset.value - PAGE_SIZE)
  void refresh()
}

const nextPage = () => {
  if (!hasNextPage.value) return
  offset.value += PAGE_SIZE
  void refresh()
}

const toggleRecord = (id: string) => {
  selectedRecordId.value = selectedRecordId.value === id ? '' : id
  if (selectedRecordId.value === id && !payloadTabById[id]) {
    payloadTabById[id] = 'request'
  }
}

const loadPayload = async (executionId: string) => {
  payloadLoadingId.value = executionId
  payloadErrorById[executionId] = ''
  try {
    const response = await modelRuntimeApi.getExecutionAuditPayload(executionId)
    payloadById[executionId] = response.data.data || null
    payloadTabById[executionId] = payloadTabById[executionId] || 'request'
    if (!response.data.data?.available) {
      payloadErrorById[executionId] = t('pages.modelAudit.payload.unavailable')
    }
  } catch (err) {
    payloadErrorById[executionId] = err instanceof Error ? err.message : t('pages.modelAudit.messages.loadFailed')
  } finally {
    payloadLoadingId.value = ''
  }
}

const boolLabel = (value: boolean) =>
  value ? t('pages.modelAudit.labels.yes') : t('pages.modelAudit.labels.no')

const formatCost = (record: ModelExecutionRecord) => {
  if (record.estimatedCost === null || Number.isNaN(record.estimatedCost)) {
    return NONE_VALUE
  }
  if (record.currency && record.currency !== pricingCurrency.value) {
    return `${record.currency} ${record.estimatedCost.toFixed(6)}`
  }
  return formatCurrency(record.estimatedCost)
}

watch(
  () => [
    filters.query,
    filters.providerId,
    filters.modelId,
    filters.routeId,
    filters.caller,
    filters.sessionId,
    filters.instanceId,
    statusFilter.value,
  ],
  scheduleRefresh,
)

watch(
  () => filters.providerId,
  () => {
    if (filters.modelId && !models.value.some((model) => model.id === filters.modelId && model.providerId === filters.providerId)) {
      filters.modelId = ''
    }
  }
)

onMounted(async () => {
  try {
    await loadCatalog()
  } catch {
    providers.value = []
    models.value = []
    routes.value = []
  }
  await refresh()
})

onBeforeUnmount(() => {
  window.clearTimeout(refreshTimer)
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.detail-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(220px, 1fr));
  gap: 14px;
}

.metadata-block {
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

.metadata-block {
  margin-top: 16px;
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
