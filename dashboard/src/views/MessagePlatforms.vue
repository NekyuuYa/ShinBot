<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.messagePlatforms.title')"
      :subtitle="$t('pages.messagePlatforms.subtitle')"
      :kicker="$t('pages.messagePlatforms.kicker')"
    >
      <template #actions>
        <config-workspace-actions
          :loading="configStore.isLoading"
          :dirty="configStore.isDirty"
          :saving="configStore.isSaving"
          :validating="configStore.isValidating"
          :refresh-label="$t('common.actions.action.refresh')"
          :reset-label="$t('common.actions.action.reset')"
          :validate-label="$t('pages.messagePlatforms.actions.validate')"
          :create-label="$t('pages.messagePlatforms.create')"
          @refresh="refreshWorkspace"
          @reset="configStore.resetDraft"
          @validate="validateDraft"
          @create="openCreate"
        />
      </template>
    </app-page-header>

    <config-validation-alerts
      :error="configStore.error"
      :issues="validationIssues"
      :title="$t('pages.messagePlatforms.validation.title')"
      :format-issue="issueMessage"
      :more-label="(count) => $t('pages.messagePlatforms.validation.more', { count })"
    />

    <config-resource-toolbar
      v-model:search="searchQuery"
      v-model:view-mode="viewMode"
      :search-label="$t('common.actions.action.search')"
      :list-label="t('pages.messagePlatforms.views.list')"
      :card-label="t('pages.messagePlatforms.views.card')"
    />

    <config-resource-collection-view
      :items="filteredPlatforms"
      :loading="showInitialSkeleton"
      :show-empty-state="!initialSkeletonRequested && filteredPlatforms.length === 0"
      :view-mode="viewMode"
      empty-icon="mdi-message-processing-outline"
      :empty-title="$t('pages.messagePlatforms.noData')"
      :get-item-key="(platform) => platform.id"
    >
      <template #empty-action>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreate">
          {{ $t('pages.messagePlatforms.create') }}
        </v-btn>
      </template>

      <template #card="{ item: platform }">
        <message-platform-card
          :platform="platform"
          :display-name="platformDisplayName(platform)"
          :adapter-label="adapterLabel(platform.adapter)"
          :config-field-count="configFieldCount(platform.config)"
          :updated-at="formatTimestamp(platform.lastModified)"
          :enabled-label="$t('common.actions.status.enabled')"
          :disabled-label="$t('common.actions.status.disabled')"
          :config-label="$t('pages.messagePlatforms.table.config')"
          :updated-label="$t('pages.messagePlatforms.table.updated')"
          :configure-label="$t('pages.messagePlatforms.actions.configure')"
          :edit-label="$t('common.actions.action.edit')"
          :delete-label="$t('common.actions.action.delete')"
          :connection="connectionStatus(platform)"
          @edit="openEdit"
          @delete="deletePlatform"
        />
      </template>

      <template #table>
        <message-platform-table
          :headers="tableHeaders"
          :items="filteredPlatforms"
          :loading="configStore.isLoading"
          :display-name="platformDisplayName"
          :adapter-label="adapterLabel"
          :config-field-count="configFieldCount"
          :format-timestamp="formatTimestamp"
          :connection-status="connectionStatus"
          :enabled-label="$t('common.actions.status.enabled')"
          :disabled-label="$t('common.actions.status.disabled')"
          @edit="openEdit"
          @delete="deletePlatform"
        />
      </template>
    </config-resource-collection-view>

    <message-platform-form-dialog
      v-model:visible="dialogVisible"
      v-model:form="editorForm"
      :title="dialogTitle"
      :adapter-options="adapterOptions"
      :active-provider="activeEditorProvider"
      :provider-issues="validationIssues"
      :path-prefix="editorPathPrefix"
      :editing="editingIndex >= 0"
      :saving="configStore.isSaving"
      :error-text="editorError"
      @adapter-change="applyProviderDefaults"
      @close="closeDialog"
      @save="applyDialog"
    />
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type {
  ConfigRecord,
  ConfigValidationIssue,
  ConfigValue,
  ConfigWorkspaceProvider,
} from '@/api/config'
import AppPageHeader from '@/components/AppPageHeader.vue'
import ConfigResourceCollectionView from '@/components/config/ConfigResourceCollectionView.vue'
import ConfigResourceToolbar from '@/components/config/ConfigResourceToolbar.vue'
import ConfigValidationAlerts from '@/components/config/ConfigValidationAlerts.vue'
import ConfigWorkspaceActions from '@/components/config/ConfigWorkspaceActions.vue'
import MessagePlatformCard from '@/components/message-platforms/MessagePlatformCard.vue'
import MessagePlatformFormDialog from '@/components/message-platforms/MessagePlatformFormDialog.vue'
import MessagePlatformTable from '@/components/message-platforms/MessagePlatformTable.vue'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import type {
  MessagePlatformAdapterOption,
  MessagePlatformDraft,
  MessagePlatformFormState,
} from '@/components/message-platforms/types'
import {
  createProviderConfigDraft,
  localizedConfigIssueMessage,
  providerDescription,
  providerDisplayName,
} from '@/config'
import { useConfigWorkspaceStore } from '@/stores/configWorkspace'

const { locale, t } = useI18n()
const configStore = useConfigWorkspaceStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
const dialogVisible = ref(false)
const editingIndex = ref(-1)
const editorError = ref('')
const editorForm = ref<MessagePlatformFormState>(createEmptyForm())
const hasLoadedWorkspace = ref(false)

const adapterOptions = computed<MessagePlatformAdapterOption[]>(() =>
  (configStore.workspace?.providers.adapters ?? []).map((provider) => ({
    title: providerDisplayName(provider, locale.value),
    value: provider.id,
    props: {
      subtitle: providerDescription(provider, locale.value) || provider.id,
    },
  }))
)

const adapterProviderById = computed(() => configStore.adapterProvidersById)

const adapterInstanceRecords = computed<ConfigRecord[]>(() => {
  const value = configStore.draft.adapter_instances
  if (!Array.isArray(value)) {
    return []
  }
  return value.filter(isConfigRecord)
})

const platforms = computed<MessagePlatformDraft[]>(() =>
  adapterInstanceRecords.value.map((record, index) => normalizePlatformDraft(record, index))
)

const platformRuntimeById = computed(() => {
  const rows = configStore.workspace?.runtime.adapterInstances ?? []
  return rows.reduce<Record<string, { running: boolean; connected: boolean; available: boolean }>>((result, item) => {
    result[item.id] = {
      running: item.running,
      connected: item.connected,
      available: item.available,
    }
    return result
  }, {})
})

const initialSkeletonRequested = computed(
  () => configStore.isLoading && !hasLoadedWorkspace.value && platforms.value.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const filteredPlatforms = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()
  if (!query) {
    return platforms.value
  }

  return platforms.value.filter((platform) => {
    const haystack = [
      platform.id,
      platform.name,
      platform.adapter,
      adapterLabel(platform.adapter),
    ].join(' ').toLowerCase()
    return haystack.includes(query)
  })
})

const validationIssues = computed(() => configStore.validationIssues)

const tableHeaders = computed(() => [
  { title: t('pages.messagePlatforms.table.name'), value: 'name', width: '24%' },
  { title: t('pages.messagePlatforms.table.adapter'), value: 'adapter', width: '16%' },
  { title: t('pages.messagePlatforms.table.status'), value: 'enabled', width: '14%' },
  { title: t('pages.messagePlatforms.table.connection'), value: 'connection', width: '14%' },
  { title: t('pages.messagePlatforms.table.config'), value: 'config', width: '12%' },
  { title: t('pages.messagePlatforms.table.updated'), value: 'lastModified', width: '14%' },
  { title: t('pages.messagePlatforms.table.actions'), value: 'actions', width: '10%', sortable: false },
])

const dialogTitle = computed(() =>
  editingIndex.value >= 0
    ? t('pages.messagePlatforms.dialog.editTitle')
    : t('pages.messagePlatforms.dialog.createTitle')
)

const activeEditorProvider = computed<ConfigWorkspaceProvider | null>(() =>
  adapterProviderById.value[editorForm.value.adapter] ?? null
)

const editorPathPrefix = computed(() =>
  editingIndex.value >= 0 ? `adapter_instances[${editingIndex.value}].config` : ''
)

function createEmptyForm(): MessagePlatformFormState {
  return {
    id: '',
    name: '',
    adapter: '',
    enabled: true,
    config: {},
  }
}

function isConfigRecord(value: unknown): value is ConfigRecord {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function cloneConfigRecord(value: ConfigRecord = {}): ConfigRecord {
  return JSON.parse(JSON.stringify(value)) as ConfigRecord
}

function cloneConfigValue<T extends ConfigValue>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function normalizeTimestamp(value: ConfigValue | undefined): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function normalizePlatformDraft(record: ConfigRecord, index: number): MessagePlatformDraft {
  const rawId = typeof record.id === 'string' ? record.id.trim() : ''
  const adapter = typeof record.adapter === 'string' ? record.adapter.trim() : ''
  const id = rawId || `${adapter || 'platform'}-${index + 1}`
  const name = typeof record.name === 'string' && record.name.trim() ? record.name.trim() : id
  const config = isConfigRecord(record.config) ? cloneConfigRecord(record.config) : {}
  const enabled = typeof record.enabled === 'boolean' ? record.enabled : true

  return {
    id,
    name,
    adapter,
    enabled,
    config,
    createdAt: normalizeTimestamp(record.createdAt),
    lastModified: normalizeTimestamp(record.lastModified),
    running: platformRuntimeById.value[id]?.running ?? false,
    connected: platformRuntimeById.value[id]?.connected ?? false,
    available: platformRuntimeById.value[id]?.available ?? false,
  }
}

function adapterLabel(adapter: string): string {
  const provider = adapterProviderById.value[adapter]
  return provider
    ? providerDisplayName(provider, locale.value)
    : adapter || t('pages.messagePlatforms.unknownAdapter')
}

function platformDisplayName(platform: MessagePlatformDraft): string {
  return platform.name || platform.id
}

function configFieldCount(config: ConfigRecord): string {
  return t('pages.messagePlatforms.configFieldCount', {
    count: Object.keys(config).length,
  })
}

function connectionStatus(platform: MessagePlatformDraft): {
  color: string
  icon: string
  label: string
} {
  if (!platform.enabled) {
    return {
      color: 'grey',
      icon: 'mdi-power-plug-off-outline',
      label: t('pages.messagePlatforms.connection.disabled'),
    }
  }
  if (platform.connected) {
    return {
      color: 'success',
      icon: 'mdi-lan-connect',
      label: t('pages.messagePlatforms.connection.connected'),
    }
  }
  if (platform.available) {
    return {
      color: 'info',
      icon: 'mdi-lan-pending',
      label: t('pages.messagePlatforms.connection.gracePeriod'),
    }
  }
  if (platform.running) {
    return {
      color: 'warning',
      icon: 'mdi-lan-disconnect',
      label: t('pages.messagePlatforms.connection.disconnected'),
    }
  }
  return {
    color: 'grey',
    icon: 'mdi-stop-circle-outline',
    label: t('pages.messagePlatforms.connection.stopped'),
  }
}

function issueMessage(issue: ConfigValidationIssue): string {
  return localizedConfigIssueMessage(issue, locale.value)
}

function formatTimestamp(value?: number): string {
  if (!value) {
    return t('pages.messagePlatforms.never')
  }

  const milliseconds = value > 1_000_000_000_000 ? value : value * 1000
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(milliseconds))
}

function makeDefaultId(adapter: string): string {
  const base = (adapter || 'platform')
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'platform'
  const usedIds = new Set(platforms.value.map((platform) => platform.id))
  let candidate = `${base}-main`
  let counter = 2

  while (usedIds.has(candidate)) {
    candidate = `${base}-${counter}`
    counter += 1
  }

  return candidate
}

function currentUnixTime(): number {
  return Math.floor(Date.now() / 1000)
}

function openCreate() {
  const provider = configStore.workspace?.providers.adapters[0] ?? null
  const adapter = provider?.id ?? ''
  const template = configStore.workspace?.templates.adapterInstance

  editorForm.value = {
    id: template?.id || makeDefaultId(adapter),
    name: template?.name || adapterLabel(adapter),
    adapter: template?.adapter || adapter,
    enabled: template?.enabled ?? true,
    config: provider ? createProviderConfigDraft(provider, template?.config ?? {}) : {},
  }
  editorError.value = ''
  editingIndex.value = -1
  dialogVisible.value = true
}

function openEdit(platform: MessagePlatformDraft) {
  const index = platforms.value.findIndex((item) => item.id === platform.id)
  editingIndex.value = index
  editorForm.value = {
    id: platform.id,
    name: platform.name,
    adapter: platform.adapter,
    enabled: platform.enabled,
    config: cloneConfigRecord(platform.config),
  }
  editorError.value = ''
  dialogVisible.value = true
}

function closeDialog() {
  dialogVisible.value = false
}

function applyProviderDefaults(adapter: string, previousAdapter = '') {
  const provider = adapterProviderById.value[adapter]
  const previousDefaultId = makeDefaultId(previousAdapter)
  const shouldRefreshIdentity = editingIndex.value < 0 && (
    !editorForm.value.id || editorForm.value.id === previousDefaultId
  )
  const shouldRefreshName = editingIndex.value < 0 && (
    !editorForm.value.name || editorForm.value.name === adapterLabel(previousAdapter)
  )

  editorForm.value = {
    ...editorForm.value,
    id: shouldRefreshIdentity ? makeDefaultId(adapter) : editorForm.value.id,
    name: shouldRefreshName ? adapterLabel(adapter) : editorForm.value.name,
    config: provider ? createProviderConfigDraft(provider, {}) : {},
  }
}

function validateEditorForm(): string {
  const id = editorForm.value.id.trim()
  const adapter = editorForm.value.adapter.trim()

  if (!id) {
    return t('pages.messagePlatforms.validation.requiredId')
  }
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(id)) {
    return t('pages.messagePlatforms.validation.idFormat')
  }
  if (!adapter) {
    return t('pages.messagePlatforms.validation.requiredAdapter')
  }
  if (!adapterProviderById.value[adapter]) {
    return t('pages.messagePlatforms.validation.unknownAdapter')
  }

  const duplicate = platforms.value.some((platform, index) =>
    platform.id === id && index !== editingIndex.value
  )
  if (duplicate) {
    return t('pages.messagePlatforms.validation.duplicateId')
  }

  return ''
}

function buildPlatformRecord(
  form: MessagePlatformFormState,
  previous?: ConfigRecord
): ConfigRecord {
  const id = form.id.trim()
  const now = currentUnixTime()

  return {
    ...(previous ? cloneConfigRecord(previous) : {}),
    id,
    name: form.name.trim() || id,
    adapter: form.adapter.trim(),
    enabled: form.enabled,
    config: cloneConfigRecord(form.config),
    createdAt: normalizeTimestamp(previous?.createdAt) ?? now,
    lastModified: now,
  }
}

function updateAdapterInstances(records: ConfigRecord[]) {
  configStore.setDraftPath(
    'adapter_instances',
    records.map((record) => cloneConfigValue(record))
  )
}

async function saveAdapterInstances(records: ConfigRecord[]) {
  const previousRecords = adapterInstanceRecords.value.map((record) => cloneConfigRecord(record))
  updateAdapterInstances(records)
  const result = await configStore.saveAdapterInstances({
    adapterInstances: records,
    validateBeforeSave: true,
  })
  if (!result) {
    updateAdapterInstances(previousRecords)
    return false
  }
  return true
}

async function applyDialog() {
  const error = validateEditorForm()
  if (error) {
    editorError.value = error
    return
  }

  const records = adapterInstanceRecords.value.map((record) => cloneConfigRecord(record))
  const nextRecord = buildPlatformRecord(
    editorForm.value,
    editingIndex.value >= 0 ? records[editingIndex.value] : undefined
  )

  if (editingIndex.value >= 0) {
    records[editingIndex.value] = nextRecord
  } else {
    records.push(nextRecord)
  }

  const saved = await saveAdapterInstances(records)
  if (saved) {
    dialogVisible.value = false
    return
  }
  editorError.value = configStore.error
}

async function deletePlatform(platform: MessagePlatformDraft) {
  if (!window.confirm(t('pages.messagePlatforms.deleteConfirm', { name: platformDisplayName(platform) }))) {
    return
  }

  await saveAdapterInstances(
    adapterInstanceRecords.value
      .filter((_, index) => platforms.value[index]?.id !== platform.id)
      .map((record) => cloneConfigRecord(record))
  )
}

async function refreshWorkspace() {
  await configStore.loadWorkspace({ preserveDraft: configStore.isDirty })
}

async function loadInitialWorkspace() {
  try {
    await configStore.loadWorkspace({ preserveDraft: configStore.isDirty })
  } finally {
    hasLoadedWorkspace.value = true
  }
}

async function validateDraft() {
  await configStore.validateDraft()
}

onMounted(() => {
  void loadInitialWorkspace()
})
</script>
