<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.messagePlatforms.title')"
      :subtitle="$t('pages.messagePlatforms.subtitle')"
      :kicker="$t('pages.messagePlatforms.kicker')"
    >
      <template #actions>
        <v-btn
          variant="tonal"
          color="secondary"
          prepend-icon="mdi-refresh"
          :loading="configStore.isLoading"
          rounded="lg"
          @click="refreshWorkspace"
        >
          {{ $t('common.actions.action.refresh') }}
        </v-btn>
        <v-btn
          variant="outlined"
          prepend-icon="mdi-restore"
          :disabled="!configStore.isDirty || configStore.isSaving"
          rounded="lg"
          @click="configStore.resetDraft"
        >
          {{ $t('common.actions.action.reset') }}
        </v-btn>
        <v-btn
          variant="outlined"
          prepend-icon="mdi-check-decagram-outline"
          :loading="configStore.isValidating"
          rounded="lg"
          @click="validateDraft"
        >
          {{ $t('pages.messagePlatforms.actions.validate') }}
        </v-btn>
        <v-btn
          color="primary"
          prepend-icon="mdi-content-save-outline"
          :disabled="!configStore.isDirty"
          :loading="configStore.isSaving"
          rounded="lg"
          @click="saveDraft"
        >
          {{ $t('common.actions.action.save') }}
        </v-btn>
        <v-btn
          color="primary"
          prepend-icon="mdi-plus"
          rounded="lg"
          @click="openCreate"
        >
          {{ $t('pages.messagePlatforms.create') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert
      v-if="configStore.error"
      type="error"
      variant="tonal"
      density="comfortable"
      class="mb-6"
    >
      {{ configStore.error }}
    </v-alert>

    <v-alert
      v-if="validationIssues.length > 0"
      type="warning"
      variant="tonal"
      density="comfortable"
      class="mb-6"
    >
      <div class="font-weight-medium mb-2">
        {{ $t('pages.messagePlatforms.validation.title') }}
      </div>
      <div
        v-for="issue in visibleValidationIssues"
        :key="`${issue.path}:${issue.code}:${issue.message}`"
        class="text-body-2 validation-issue-line"
      >
        <span class="font-weight-medium">{{ issue.path }}</span>
        <span>{{ issueMessage(issue) }}</span>
      </div>
      <div v-if="hiddenValidationIssueCount > 0" class="text-body-2 mt-1 text-medium-emphasis">
        {{ $t('pages.messagePlatforms.validation.more', { count: hiddenValidationIssueCount }) }}
      </div>
    </v-alert>

    <div class="platform-toolbar mb-6">
      <v-text-field
        v-model="searchQuery"
        :label="$t('common.actions.action.search')"
        prepend-inner-icon="mdi-magnify"
        single-line
        hide-details
        density="comfortable"
        variant="outlined"
        bg-color="surface"
        class="platform-search"
      />
      <v-spacer />
      <v-chip
        :color="configStore.isDirty ? 'warning' : 'success'"
        variant="tonal"
        size="small"
        class="platform-dirty-chip"
      >
        {{ configStore.isDirty ? $t('pages.messagePlatforms.status.unsaved') : $t('pages.messagePlatforms.status.saved') }}
      </v-chip>
      <layout-mode-button
        v-model="viewMode"
        :list-label="t('pages.messagePlatforms.views.list')"
        :card-label="t('pages.messagePlatforms.views.card')"
      />
    </div>

    <v-row v-if="showInitialSkeleton">
      <v-col cols="12">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row v-else-if="!initialSkeletonRequested && filteredPlatforms.length === 0" justify="center" class="py-12">
      <v-col cols="12" sm="8" md="6" class="text-center">
        <v-icon size="112" color="grey-lighten-1" icon="mdi-message-processing-outline" />
        <h3 class="text-h6 my-4">{{ $t('pages.messagePlatforms.noData') }}</h3>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreate">
          {{ $t('pages.messagePlatforms.create') }}
        </v-btn>
      </v-col>
    </v-row>

    <v-row v-else-if="viewMode === 'card'" class="ma-0">
      <v-col
        v-for="platform in filteredPlatforms"
        :key="platform.id"
        cols="12"
        sm="6"
        md="4"
        lg="3"
      >
        <v-card class="platform-card h-100" hover>
          <v-card-item class="pb-2">
            <template #prepend>
              <v-avatar color="primary" variant="tonal" icon="mdi-message-processing-outline" />
            </template>
            <v-card-title class="text-break">
              {{ platformDisplayName(platform) }}
            </v-card-title>
            <v-card-subtitle class="text-truncate">
              {{ platform.id }}
            </v-card-subtitle>
            <template #append>
              <v-menu>
                <template #activator="{ props }">
                  <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
                </template>
                <v-list>
                  <v-list-item @click="openEdit(platform)">
                    <v-list-item-title>{{ $t('common.actions.action.edit') }}</v-list-item-title>
                  </v-list-item>
                  <v-list-item @click="deletePlatform(platform)">
                    <v-list-item-title>{{ $t('common.actions.action.delete') }}</v-list-item-title>
                  </v-list-item>
                </v-list>
              </v-menu>
            </template>
          </v-card-item>

          <v-card-text class="pt-2">
            <div class="platform-card-chips">
              <v-chip :color="platform.enabled ? 'success' : 'grey'" size="small" variant="tonal">
                {{ platform.enabled ? $t('common.actions.status.enabled') : $t('common.actions.status.disabled') }}
              </v-chip>
              <v-chip color="info" size="small" variant="tonal">
                {{ adapterLabel(platform.adapter) }}
              </v-chip>
            </div>

            <div class="platform-meta-row">
              <span>{{ $t('pages.messagePlatforms.table.config') }}</span>
              <strong>{{ configFieldCount(platform.config) }}</strong>
            </div>
            <div class="platform-meta-row">
              <span>{{ $t('pages.messagePlatforms.table.updated') }}</span>
              <strong>{{ formatTimestamp(platform.lastModified) }}</strong>
            </div>
          </v-card-text>

          <v-card-actions>
            <v-btn
              color="primary"
              variant="text"
              size="small"
              prepend-icon="mdi-pencil"
              @click="openEdit(platform)"
            >
              {{ $t('pages.messagePlatforms.actions.configure') }}
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>
    </v-row>

    <v-row v-else>
      <v-col cols="12">
        <v-data-table
          :headers="tableHeaders"
          :items="filteredPlatforms"
          :loading="configStore.isLoading"
          hide-default-footer
          class="platform-table"
        >
          <template #item.name="{ item }">
            <div class="platform-name-cell">
              <span class="font-weight-medium">{{ platformDisplayName(tableRow(item)) }}</span>
              <span class="text-caption text-medium-emphasis">{{ tableRow(item).id }}</span>
            </div>
          </template>

          <template #item.adapter="{ item }">
            <v-chip size="small" color="info" variant="tonal">
              {{ adapterLabel(tableRow(item).adapter) }}
            </v-chip>
          </template>

          <template #item.enabled="{ item }">
            <v-chip
              :color="tableRow(item).enabled ? 'success' : 'grey'"
              size="small"
              variant="tonal"
            >
              {{ tableRow(item).enabled ? $t('common.actions.status.enabled') : $t('common.actions.status.disabled') }}
            </v-chip>
          </template>

          <template #item.config="{ item }">
            {{ configFieldCount(tableRow(item).config) }}
          </template>

          <template #item.lastModified="{ item }">
            {{ formatTimestamp(tableRow(item).lastModified) }}
          </template>

          <template #item.actions="{ item }">
            <v-btn
              icon="mdi-pencil"
              size="small"
              variant="text"
              @click="openEdit(tableRow(item))"
            />
            <v-btn
              icon="mdi-delete"
              size="small"
              variant="text"
              color="error"
              @click="deletePlatform(tableRow(item))"
            />
          </template>
        </v-data-table>
      </v-col>
    </v-row>

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
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import MessagePlatformFormDialog from '@/components/message-platforms/MessagePlatformFormDialog.vue'
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

type TableRowItem = MessagePlatformDraft | { raw: MessagePlatformDraft }

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
const visibleValidationIssues = computed(() => validationIssues.value.slice(0, 5))
const hiddenValidationIssueCount = computed(() =>
  Math.max(validationIssues.value.length - visibleValidationIssues.value.length, 0)
)

const tableHeaders = computed(() => [
  { title: t('pages.messagePlatforms.table.name'), value: 'name', width: '28%' },
  { title: t('pages.messagePlatforms.table.adapter'), value: 'adapter', width: '18%' },
  { title: t('pages.messagePlatforms.table.status'), value: 'enabled', width: '14%' },
  { title: t('pages.messagePlatforms.table.config'), value: 'config', width: '14%' },
  { title: t('pages.messagePlatforms.table.updated'), value: 'lastModified', width: '16%' },
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
  }
}

function tableRow(item: TableRowItem): MessagePlatformDraft {
  return 'raw' in item ? item.raw : item
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

function applyDialog() {
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

  updateAdapterInstances(records)
  dialogVisible.value = false
}

function deletePlatform(platform: MessagePlatformDraft) {
  if (!window.confirm(t('pages.messagePlatforms.deleteConfirm', { name: platformDisplayName(platform) }))) {
    return
  }

  updateAdapterInstances(
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

async function saveDraft() {
  await configStore.saveDraft({ validateBeforeSave: true })
}

onMounted(() => {
  void loadInitialWorkspace()
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.platform-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px;
  @include surface-card;
}

.platform-search {
  flex: 0 1 420px;
}

.platform-dirty-chip {
  min-width: 92px;
  justify-content: center;
}

.validation-issue-line {
  display: flex;
  gap: 8px;
  align-items: baseline;
  min-width: 0;
}

.platform-card {
  @include surface-card;
  @include hover-lift;
}

.platform-card-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 14px;
}

.platform-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 0;
  border-top: 1px solid $border-color-soft;
  color: rgba(var(--v-theme-on-surface), 0.66);
  font-size: $font-size-sm;
}

.platform-meta-row strong {
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-weight: 700;
}

.platform-table {
  @include surface-card;
}

.platform-name-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

@include respond-to('tablet') {
  .platform-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .platform-search {
    flex: 1 1 auto;
    width: 100%;
  }
}
</style>
