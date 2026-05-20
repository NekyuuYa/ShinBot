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

    <section class="audit-filter-panel mb-6">
      <div class="filter-head">
        <div>
          <div class="panel-kicker">{{ $t('pages.modelAudit.filters.title') }}</div>
          <div class="panel-total">{{ $t('pages.modelAudit.labels.total', { total }) }}</div>
        </div>
        <v-btn
          variant="text"
          color="secondary"
          prepend-icon="mdi-filter-off-outline"
          rounded="lg"
          @click="clearFilters"
        >
          {{ $t('pages.modelAudit.actions.clear') }}
        </v-btn>
      </div>

      <div class="filter-grid">
        <v-text-field
          v-model="filters.query"
          :label="$t('pages.modelAudit.filters.search')"
          prepend-inner-icon="mdi-magnify"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-select
          v-model="filters.providerId"
          :label="$t('pages.modelAudit.filters.provider')"
          :items="providerOptions"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-select
          v-model="filters.modelId"
          :label="$t('pages.modelAudit.filters.model')"
          :items="modelOptions"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-select
          v-model="filters.routeId"
          :label="$t('pages.modelAudit.filters.route')"
          :items="routeOptions"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-text-field
          v-model="filters.caller"
          :label="$t('pages.modelAudit.filters.caller')"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-text-field
          v-model="filters.sessionId"
          :label="$t('pages.modelAudit.filters.session')"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-text-field
          v-model="filters.instanceId"
          :label="$t('pages.modelAudit.filters.instance')"
          density="comfortable"
          variant="outlined"
          hide-details
          clearable
        />
        <v-select
          v-model="statusFilter"
          :label="$t('pages.modelAudit.filters.status')"
          :items="statusOptions"
          density="comfortable"
          variant="outlined"
          hide-details
        />
      </div>
    </section>

    <section class="audit-record-panel">
      <div class="record-panel-head">
        <div>
          <div class="panel-kicker">{{ $t('pages.modelAudit.sections.records') }}</div>
          <h2>{{ $t('pages.modelAudit.labels.page', pageRange) }}</h2>
        </div>
        <div class="pagination-actions">
          <v-btn
            icon="mdi-chevron-left"
            variant="tonal"
            :disabled="offset === 0 || loading"
            :aria-label="$t('pages.modelAudit.actions.previous')"
            @click="previousPage"
          />
          <v-btn
            icon="mdi-chevron-right"
            variant="tonal"
            :disabled="!hasNextPage || loading"
            :aria-label="$t('pages.modelAudit.actions.next')"
            @click="nextPage"
          />
        </div>
      </div>

      <v-progress-linear
        v-if="loading"
        indeterminate
        color="primary"
        rounded
        class="mb-4"
      />

      <v-empty-state
        v-if="!loading && records.length === 0"
        icon="mdi-clipboard-search-outline"
        :title="$t('pages.modelAudit.labels.empty')"
        variant="plain"
      />

      <div v-else class="record-list">
        <article
          v-for="record in records"
          :key="record.id"
          class="record-item"
          :class="{ 'record-item--active': selectedRecordId === record.id }"
        >
          <button type="button" class="record-summary" @click="toggleRecord(record.id)">
            <span class="record-status" :class="record.success ? 'record-status--success' : 'record-status--failed'">
              <v-icon :icon="record.success ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline'" size="18" />
              {{ record.success ? $t('pages.modelAudit.labels.success') : $t('pages.modelAudit.labels.failed') }}
            </span>

            <span class="record-main">
              <strong>{{ record.modelId || $t('pages.modelAudit.labels.none') }}</strong>
              <small>{{ formatDateTime(record.startedAt) }} · {{ record.providerId || $t('pages.modelAudit.labels.none') }}</small>
            </span>

            <span class="record-meta">
              <span>{{ record.caller || $t('pages.modelAudit.labels.none') }}</span>
              <span>{{ record.sessionId || record.instanceId || $t('pages.modelAudit.labels.none') }}</span>
            </span>

            <span class="record-usage">
              <strong>{{ formatDuration(record.latencyMs) }}</strong>
              <small>{{ formatCompactNumber(record.inputTokens + record.outputTokens) }} tokens</small>
            </span>

            <v-icon
              :icon="selectedRecordId === record.id ? 'mdi-chevron-up' : 'mdi-chevron-down'"
              size="20"
              class="record-chevron"
            />
          </button>

          <v-expand-transition>
            <div v-if="selectedRecordId === record.id" class="record-detail">
              <div class="detail-grid">
                <detail-section :title="$t('pages.modelAudit.sections.details')" icon="mdi-identifier">
                  <detail-row :label="$t('pages.modelAudit.labels.executionId')" :value="record.id" />
                  <detail-row :label="$t('pages.modelAudit.labels.promptSnapshotId')" :value="record.promptSnapshotId" />
                  <detail-row :label="$t('pages.modelAudit.labels.purpose')" :value="record.purpose" />
                </detail-section>

                <detail-section :title="$t('pages.modelAudit.sections.routing')" icon="mdi-routes">
                  <detail-row :label="$t('pages.modelAudit.labels.providerId')" :value="record.providerId" />
                  <detail-row :label="$t('pages.modelAudit.labels.modelId')" :value="record.modelId" />
                  <detail-row :label="$t('pages.modelAudit.labels.routeId')" :value="record.routeId" />
                  <detail-row :label="$t('pages.modelAudit.labels.caller')" :value="record.caller" />
                  <detail-row :label="$t('pages.modelAudit.labels.sessionId')" :value="record.sessionId" />
                  <detail-row :label="$t('pages.modelAudit.labels.instanceId')" :value="record.instanceId" />
                </detail-section>

                <detail-section :title="$t('pages.modelAudit.sections.timing')" icon="mdi-timer-outline">
                  <detail-row :label="$t('pages.modelAudit.labels.startedAt')" :value="formatDateTime(record.startedAt)" />
                  <detail-row :label="$t('pages.modelAudit.labels.firstTokenAt')" :value="formatDateTime(record.firstTokenAt)" />
                  <detail-row :label="$t('pages.modelAudit.labels.finishedAt')" :value="formatDateTime(record.finishedAt)" />
                  <detail-row :label="$t('pages.modelAudit.labels.latency')" :value="formatDuration(record.latencyMs)" />
                  <detail-row :label="$t('pages.modelAudit.labels.ttft')" :value="formatDuration(record.timeToFirstTokenMs)" />
                </detail-section>

                <detail-section :title="$t('pages.modelAudit.sections.usage')" icon="mdi-counter">
                  <detail-row :label="$t('pages.modelAudit.labels.inputTokens')" :value="formatNumber(record.inputTokens)" />
                  <detail-row :label="$t('pages.modelAudit.labels.outputTokens')" :value="formatNumber(record.outputTokens)" />
                  <detail-row :label="$t('pages.modelAudit.labels.cacheRead')" :value="formatNumber(record.cacheReadTokens)" />
                  <detail-row :label="$t('pages.modelAudit.labels.cacheWrite')" :value="formatNumber(record.cacheWriteTokens)" />
                  <detail-row :label="$t('pages.modelAudit.labels.cacheHit')" :value="boolLabel(record.cacheHit)" />
                  <detail-row :label="$t('pages.modelAudit.labels.cost')" :value="formatCost(record)" />
                </detail-section>

                <detail-section
                  v-if="record.fallbackFromModelId || record.fallbackReason"
                  :title="$t('pages.modelAudit.labels.fallback')"
                  icon="mdi-call-split"
                >
                  <detail-row :label="$t('pages.modelAudit.labels.fallbackFrom')" :value="record.fallbackFromModelId" />
                  <detail-row :label="$t('pages.modelAudit.labels.fallbackReason')" :value="record.fallbackReason" />
                </detail-section>

                <detail-section
                  v-if="!record.success || record.errorCode || record.errorMessage"
                  :title="$t('pages.modelAudit.sections.error')"
                  icon="mdi-alert-outline"
                  tone="error"
                >
                  <detail-row :label="$t('pages.modelAudit.labels.errorCode')" :value="record.errorCode" />
                  <detail-row :label="$t('pages.modelAudit.labels.errorMessage')" :value="record.errorMessage" />
                </detail-section>
              </div>

              <section class="metadata-block">
                <div class="metadata-block__head">
                  <v-icon icon="mdi-code-json" size="18" />
                  <strong>{{ $t('pages.modelAudit.sections.metadata') }}</strong>
                </div>
                <div v-if="hasMetadata(record)" class="json-tree-shell">
                  <json-tree-node :value="record.metadata" />
                </div>
                <div v-else class="metadata-empty">{{ $t('pages.modelAudit.labels.metadataEmpty') }}</div>
              </section>

              <section class="payload-block">
                <div class="payload-block__head">
                  <div class="payload-block__title">
                    <v-icon icon="mdi-file-document-outline" size="18" />
                    <strong>{{ $t('pages.modelAudit.sections.payload') }}</strong>
                  </div>
                  <v-btn
                    size="small"
                    variant="tonal"
                    color="primary"
                    :loading="payloadLoadingId === record.id"
                    :disabled="!record.auditPayloadAvailable"
                    @click="loadPayload(record.id)"
                  >
                    {{ $t('pages.modelAudit.actions.viewPayload') }}
                  </v-btn>
                </div>
                <v-alert
                  v-if="payloadErrorById[record.id]"
                  type="warning"
                  variant="tonal"
                  class="mb-3"
                >
                  {{ payloadErrorById[record.id] }}
                </v-alert>
                <div v-if="payloadById[record.id]" class="payload-tabs">
                  <v-tabs v-model="payloadTabById[record.id]" density="comfortable">
                    <v-tab value="request">{{ $t('pages.modelAudit.payload.request') }}</v-tab>
                    <v-tab value="response">{{ $t('pages.modelAudit.payload.response') }}</v-tab>
                    <v-tab value="return">{{ $t('pages.modelAudit.payload.return') }}</v-tab>
                    <v-tab value="error">{{ $t('pages.modelAudit.payload.error') }}</v-tab>
                    <v-tab value="meta">{{ $t('pages.modelAudit.payload.meta') }}</v-tab>
                  </v-tabs>
                  <v-window v-model="payloadTabById[record.id]" class="payload-window">
                    <v-window-item value="request">
                      <div class="json-tree-shell">
                        <json-tree-node :value="payloadById[record.id]?.request" />
                      </div>
                    </v-window-item>
                    <v-window-item value="response">
                      <div class="json-tree-shell">
                        <json-tree-node :value="payloadById[record.id]?.response" />
                      </div>
                    </v-window-item>
                    <v-window-item value="return">
                      <div class="json-tree-shell">
                        <json-tree-node :value="payloadById[record.id]?.['return']" />
                      </div>
                    </v-window-item>
                    <v-window-item value="error">
                      <div class="json-tree-shell">
                        <json-tree-node :value="payloadById[record.id]?.error" />
                      </div>
                    </v-window-item>
                    <v-window-item value="meta">
                      <div class="json-tree-shell">
                        <json-tree-node :value="payloadById[record.id]?.meta" />
                      </div>
                    </v-window-item>
                  </v-window>
                  <div class="payload-footnote">
                    <span>{{ $t('pages.modelAudit.labels.auditPayloadRef') }}: {{ record.auditPayloadRef || NONE_VALUE }}</span>
                    <span>{{ $t('pages.modelAudit.labels.auditPayloadExpiresAt') }}: {{ record.auditPayloadExpiresAt || NONE_VALUE }}</span>
                  </div>
                </div>
                <div v-else class="payload-empty">
                  {{ record.auditPayloadAvailable ? $t('pages.modelAudit.payload.notLoaded') : $t('pages.modelAudit.payload.unavailable') }}
                </div>
              </section>
            </div>
          </v-expand-transition>
        </article>
      </div>
    </section>
  </v-container>
</template>

<script setup lang="ts">
import {
  computed,
  defineComponent,
  h,
  onBeforeUnmount,
  onMounted,
  reactive,
  ref,
  watch,
  type Component,
  type VNode,
} from 'vue'
import { storeToRefs } from 'pinia'
import { useI18n } from 'vue-i18n'

import AppPageHeader from '@/components/AppPageHeader.vue'
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

const DetailRow = defineComponent({
  name: 'DetailRow',
  props: {
    label: { type: String, required: true },
    value: { type: [String, Number, Boolean], default: '' },
  },
  setup(props) {
    return () =>
      h('div', { class: 'detail-row' }, [
        h('span', props.label),
        h('strong', String(props.value || NONE_VALUE)),
      ])
  },
})

const DetailSection = defineComponent({
  name: 'DetailSection',
  props: {
    title: { type: String, required: true },
    icon: { type: String, required: true },
    tone: { type: String, default: '' },
  },
  setup(props, { slots }) {
    return () =>
      h(
        'section',
        { class: ['detail-section', props.tone ? `detail-section--${props.tone}` : ''] },
        [
          h('div', { class: 'detail-section__title' }, [
            h('i', { class: ['mdi', props.icon] }),
            h('span', props.title),
          ]),
          h('div', { class: 'detail-section__body' }, slots.default?.()),
        ]
      )
  },
})

const JsonTreeNode: Component = defineComponent({
  name: 'JsonTreeNode',
  props: {
    value: { type: null, default: null },
    label: { type: String, default: '' },
    depth: { type: Number, default: 0 },
    open: { type: Boolean, default: false },
  },
  setup(props) {
    const renderScalar = (value: unknown): string => {
      if (value === null) return 'null'
      if (value === undefined) return 'undefined'
      if (typeof value === 'string') return JSON.stringify(value)
      if (typeof value === 'number' || typeof value === 'boolean') return String(value)
      return JSON.stringify(value, null, 2)
    }

    return (): VNode => {
      const value = props.value as unknown
      const indentStyle = { paddingLeft: `${props.depth * 16}px` }
      if (Array.isArray(value)) {
        return h(
          'details',
          { class: 'json-node', open: props.open, style: indentStyle },
          [
            h('summary', { class: 'json-node__summary' }, [
              h('span', { class: 'json-node__key' }, props.label || '[ ]'),
              h('span', { class: 'json-node__meta' }, `Array(${value.length})`),
            ]),
            h('div', { class: 'json-node__body' }, [
              value.length
                ? value.map((item, index) =>
                    h(JsonTreeNode, {
                      key: `${props.depth}-${index}`,
                      value: item,
                      label: `[${index}]`,
                      depth: props.depth + 1,
                      open: props.depth < 1,
                    })
                  )
                : h('div', { class: 'json-node__empty' }, '[]'),
            ]),
          ]
        )
      }
      if (value && typeof value === 'object') {
        const entries = Object.entries(value as Record<string, unknown>)
        return h(
          'details',
          { class: 'json-node', open: props.open || props.depth < 1, style: indentStyle },
          [
            h('summary', { class: 'json-node__summary' }, [
              h('span', { class: 'json-node__key' }, props.label || '{ }'),
              h('span', { class: 'json-node__meta' }, `Object(${entries.length})`),
            ]),
            h('div', { class: 'json-node__body' }, [
              entries.length
                ? entries.map(([key, child]) =>
                    h(JsonTreeNode, {
                      key: `${props.depth}-${key}`,
                      value: child,
                      label: key,
                      depth: props.depth + 1,
                      open: props.depth < 1,
                    })
                  )
                : h('div', { class: 'json-node__empty' }, '{}'),
            ]),
          ]
        )
      }
      return h(
        'div',
        { class: 'json-node json-node--scalar', style: indentStyle },
        [
          props.label
            ? h('span', { class: 'json-node__key' }, props.label)
            : null,
          h('span', { class: 'json-node__scalar' }, renderScalar(value)),
        ]
      )
    }
  },
})

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

const hasMetadata = (record: ModelExecutionRecord) => Object.keys(record.metadata || {}).length > 0

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
  scheduleRefresh
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

.audit-filter-panel,
.audit-record-panel {
  @include surface-card;
  padding: 20px;
}

.filter-head,
.record-panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}

.panel-kicker {
  color: rgb(var(--v-theme-primary));
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.panel-total {
  margin-top: 4px;
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-size: $font-size-sm;
}

.filter-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(180px, 1fr));
  gap: 12px;
}

.record-panel-head h2 {
  margin: 4px 0 0;
  color: rgba(var(--v-theme-on-surface), 0.88);
  font-size: $font-size-md;
  font-weight: 800;
}

.pagination-actions {
  display: flex;
  gap: 8px;
}

.record-list {
  display: grid;
  gap: 10px;
}

.record-item {
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
  background: rgba(var(--v-theme-surface), 0.74);
  overflow: hidden;
  transition:
    border-color $transition-fast,
    background-color $transition-fast,
    box-shadow $transition-fast;
}

.record-item:hover,
.record-item--active {
  border-color: $border-color-primary;
  background: rgba(var(--v-theme-surface), 0.9);
}

.record-item--active {
  box-shadow: inset 0 0 0 1px rgba(var(--v-theme-primary), 0.08);
}

.record-summary {
  width: 100%;
  min-height: 72px;
  display: grid;
  grid-template-columns: minmax(96px, 0.7fr) minmax(220px, 1.6fr) minmax(180px, 1.4fr) minmax(120px, 0.8fr) 32px;
  align-items: center;
  gap: 16px;
  padding: 12px 16px;
  border: 0;
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;
}

.record-status,
.record-main,
.record-meta,
.record-usage {
  min-width: 0;
}

.record-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: $font-size-sm;
  font-weight: 700;
}

.record-status--success {
  color: rgb(var(--v-theme-success));
}

.record-status--failed {
  color: rgb(var(--v-theme-error));
}

.record-main,
.record-meta,
.record-usage {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.record-main strong,
.record-usage strong {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.9);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.record-main small,
.record-meta,
.record-usage small {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.58);
  font-size: $font-size-xs;
  text-overflow: ellipsis;
}

.record-meta span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.record-chevron {
  justify-self: end;
  color: rgba(var(--v-theme-on-surface), 0.54);
}

.record-detail {
  padding: 12px 18px 20px;
}

.detail-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(220px, 1fr));
  gap: 14px;
}

.detail-section,
.metadata-block {
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.detail-section--error {
  border-color: rgba(var(--v-theme-error), 0.24);
  background: rgba(var(--v-theme-error), 0.05);
}

.detail-section__title,
.metadata-block__head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 14px 8px;
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-size: $font-size-sm;
  font-weight: 800;
}

.detail-section__body {
  padding: 0 14px 12px;
}

:deep(.detail-row) {
  display: grid;
  grid-template-columns: minmax(92px, 0.75fr) minmax(0, 1.25fr);
  gap: 12px;
  padding: 6px 0;
  font-size: $font-size-xs;
}

:deep(.detail-row span) {
  color: rgba(var(--v-theme-on-surface), 0.52);
}

:deep(.detail-row strong) {
  overflow-wrap: anywhere;
  color: rgba(var(--v-theme-on-surface), 0.86);
  font-weight: 700;
}

.metadata-block {
  margin-top: 16px;
}

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

.json-tree-shell {
  max-height: 360px;
  margin: 6px 16px 16px;
  padding: 14px 16px;
  overflow: auto;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.08);
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-surface), 0.36);
  color: rgba(var(--v-theme-on-surface), 0.78);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: $font-size-xs;
  line-height: 1.55;
}

.payload-tabs .json-tree-shell {
  margin: 0;
}

:deep(.json-node) {
  min-height: 22px;
}

:deep(.json-node__summary) {
  display: flex;
  align-items: baseline;
  gap: 8px;
  min-height: 22px;
  padding: 1px 0;
  cursor: pointer;
}

:deep(.json-node__summary::marker) {
  color: rgba(var(--v-theme-on-surface), 0.46);
}

:deep(.json-node__body) {
  margin-left: 8px;
  border-left: 1px solid rgba(var(--v-theme-on-surface), 0.08);
}

:deep(.json-node__key) {
  color: rgba(var(--v-theme-on-surface), 0.86);
  font-weight: 700;
  overflow-wrap: anywhere;
}

:deep(.json-node__meta) {
  color: rgba(var(--v-theme-on-surface), 0.46);
  font-size: $font-size-xs;
}

:deep(.json-node--scalar) {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 1px 0;
}

:deep(.json-node__scalar),
:deep(.json-node__empty) {
  color: rgba(var(--v-theme-on-surface), 0.72);
  overflow-wrap: anywhere;
}

.metadata-empty {
  padding: 0 16px 16px;
  color: rgba(var(--v-theme-on-surface), 0.54);
  font-size: $font-size-sm;
}

@media (max-width: 1280px) {
  .filter-grid {
    grid-template-columns: repeat(2, minmax(180px, 1fr));
  }

  .record-summary {
    grid-template-columns: minmax(92px, 0.7fr) minmax(200px, 1.6fr) minmax(140px, 1fr) 32px;
  }

  .record-usage {
    display: none;
  }

  .detail-grid {
    grid-template-columns: repeat(2, minmax(220px, 1fr));
  }
}

@media (max-width: 760px) {
  .filter-head,
  .record-panel-head {
    align-items: flex-start;
    flex-direction: column;
  }

  .filter-grid,
  .detail-grid {
    grid-template-columns: 1fr;
  }

  .record-summary {
    grid-template-columns: 1fr 28px;
    gap: 8px;
  }

  .record-status,
  .record-main,
  .record-meta {
    grid-column: 1;
  }

  .record-chevron {
    grid-column: 2;
    grid-row: 1;
  }
}
</style>
