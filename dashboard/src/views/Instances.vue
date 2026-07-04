<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.instances.title')"
      :subtitle="$t('pages.instances.subtitle')"
      :kicker="$t('pages.instances.kicker')"
    >
      <template #actions>
        <config-workspace-actions
          :loading="configStore.isLoading"
          :dirty="configStore.isDirty"
          :saving="configStore.isSaving"
          :validating="configStore.isValidating"
          :refresh-label="$t('common.actions.action.refresh')"
          :reset-label="$t('common.actions.action.reset')"
          :validate-label="$t('pages.instances.actions.validate')"
          :create-label="$t('pages.instances.create')"
          @refresh="refreshWorkspace"
          @reset="configStore.resetDraft"
          @validate="validateDraft"
          @create="openCreate"
        />
      </template>
    </app-page-header>

    <config-validation-alerts
      :error="configStore.error"
      :issues="botValidationIssues"
      :title="$t('pages.instances.validation.title')"
      :format-issue="issueMessage"
      :more-label="(count) => $t('pages.instances.validation.more', { count })"
    />

    <config-resource-toolbar
      v-model:search="searchQuery"
      v-model:view-mode="viewMode"
      :search-label="$t('common.actions.action.search')"
      :list-label="t('pages.instances.views.list')"
      :card-label="t('pages.instances.views.card')"
    />

    <config-resource-collection-view
      :items="filteredBots"
      :loading="showInitialSkeleton"
      :show-empty-state="!initialSkeletonRequested && filteredBots.length === 0"
      :view-mode="viewMode"
      empty-icon="mdi-robot-confused-outline"
      :empty-title="$t('pages.instances.noData')"
      :get-item-key="(bot) => bot.id"
    >
      <template #empty-action>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreate">
          {{ $t('pages.instances.create') }}
        </v-btn>
      </template>

      <template #card="{ item: bot }">
        <bot-instance-card
          :bot="bot"
          :display-name="botDisplayName(bot)"
          :platform-summary="botPlatformSummary(bot)"
          :platform-health-label="$t('pages.instances.table.platformHealth')"
          :platform-health-summary="botPlatformHealthSummary(bot)"
          :agent-mode-label="agentModeLabel(bot.agent.mode)"
          :bindings-label="$t('pages.instances.table.bindings')"
          :platforms-label="$t('pages.instances.table.platforms')"
          :commands-label="$t('pages.instances.table.commands')"
          :configure-label="$t('pages.instances.actions.configure')"
          :edit-label="$t('common.actions.action.edit')"
          :delete-label="$t('common.actions.action.delete')"
          :enabled-label="$t('common.actions.status.enabled')"
          :disabled-label="$t('common.actions.status.disabled')"
          @edit="openEdit"
          @delete="deleteBot"
        />
      </template>

      <template #table>
        <bot-instance-table
          :headers="tableHeaders"
          :items="filteredBots"
          :loading="configStore.isLoading"
          :display-name="botDisplayName"
          :platform-summary="botPlatformSummary"
          :platform-health-summary="botPlatformHealthSummary"
          :agent-mode-label="agentModeLabel"
          :enabled-label="$t('common.actions.status.enabled')"
          :disabled-label="$t('common.actions.status.disabled')"
          @edit="openEdit"
          @delete="deleteBot"
        />
      </template>
    </config-resource-collection-view>

    <bot-instance-form-dialog
      v-model:visible="dialogVisible"
      v-model:form="editorForm"
      :title="dialogTitle"
      :adapter-options="adapterOptions"
      :plugin-options="pluginOptions"
      :agent-mode-options="agentModeOptions"
      :agent-config-options="agentConfigOptions"
      :editing="editingIndex >= 0"
      :saving="configStore.isSaving"
      :error-text="editorError"
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
  NormalizedAdapterInstanceConfig,
  NormalizedBotBindingConfig,
} from '@/api/config'
import AppPageHeader from '@/components/AppPageHeader.vue'
import ConfigResourceCollectionView from '@/components/config/ConfigResourceCollectionView.vue'
import ConfigResourceToolbar from '@/components/config/ConfigResourceToolbar.vue'
import ConfigValidationAlerts from '@/components/config/ConfigValidationAlerts.vue'
import ConfigWorkspaceActions from '@/components/config/ConfigWorkspaceActions.vue'
import BotInstanceCard from '@/components/instances/BotInstanceCard.vue'
import BotInstanceFormDialog from '@/components/instances/BotInstanceFormDialog.vue'
import BotInstanceTable from '@/components/instances/BotInstanceTable.vue'
import type {
  BotInstanceDraft,
  BotBindingRuntimeSummary,
  BotInstanceFormState,
  SelectOption,
} from '@/components/instances/botTypes'
import {
  buildBotRecord,
  cloneConfigRecord,
  createEmptyBotForm,
  isConfigRecord,
  normalizeAdapter,
  normalizeBot,
} from '@/components/instances/botConfigTransforms'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { localizedConfigIssueMessage } from '@/config'
import { useConfigWorkspaceStore } from '@/stores/configWorkspace'
import { agentConfigsApi, type AgentConfigProfile } from '@/api/agentConfigs'
import { apiClient } from '@/api/client'

const { locale, t } = useI18n()
const configStore = useConfigWorkspaceStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
const dialogVisible = ref(false)
const editingIndex = ref(-1)
const editorError = ref('')
const editorForm = ref<BotInstanceFormState>(createEmptyBotForm())
const hasLoadedWorkspace = ref(false)
const agentConfigProfiles = ref<AgentConfigProfile[]>([])

async function loadAgentConfigs() {
  try {
    const data = await apiClient.unwrap(
      agentConfigsApi.list({ suppressErrorNotify: true }),
    )
    agentConfigProfiles.value = data ?? []
  } catch {
    agentConfigProfiles.value = []
  }
}

onMounted(() => {
  loadAgentConfigs()
})

const botRecords = computed<ConfigRecord[]>(() => {
  const value = configStore.draft.bots
  return Array.isArray(value) ? value.filter(isConfigRecord) : []
})

const adapterRecords = computed<ConfigRecord[]>(() => {
  const value = configStore.draft.adapter_instances
  return Array.isArray(value) ? value.filter(isConfigRecord) : []
})

const adapters = computed<NormalizedAdapterInstanceConfig[]>(() =>
  adapterRecords.value.map((record, index) => normalizeAdapter(record, index))
)

const bots = computed<BotInstanceDraft[]>(() =>
  botRecords.value.map((record, index) => normalizeBot(record, index))
)

const initialSkeletonRequested = computed(
  () => configStore.isLoading && !hasLoadedWorkspace.value && bots.value.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const adapterOptions = computed<SelectOption[]>(() =>
  adapters.value.map((adapter) => ({
    title: adapter.name || adapter.id,
    value: adapter.id,
    props: {
      subtitle: adapter.adapter,
    },
  }))
)

const pluginOptions = computed<SelectOption[]>(() => [
  {
    title: t('pages.instances.form.allPlugins'),
    value: '*',
  },
  ...(configStore.workspace?.plugins ?? []).map((plugin) => ({
    title: plugin.name || plugin.id,
    value: plugin.id,
    props: {
      subtitle: plugin.module || plugin.id,
    },
  })),
])

const agentModeOptions = computed<SelectOption[]>(() =>
  (configStore.workspace?.options.agentModes ?? ['none', 'simple', 'full']).map((mode) => ({
    title: agentModeLabel(mode),
    value: mode,
  }))
)

const agentConfigOptions = computed<SelectOption[]>(() =>
  agentConfigProfiles.value.map((profile) => ({
    title: profile.agentId || profile.fileName,
    value: profile.fileName,
    props: {
      subtitle: `${profile.mode} - ${profile.fileName}`,
    },
  }))
)

const filteredBots = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()
  if (!query) {
    return bots.value
  }

  return bots.value.filter((bot) => {
    const haystack = [
      bot.id,
      bot.display_name,
      bot.agent.mode,
      bot.bindings.map((binding) => binding.adapter_instance_id).join(' '),
    ].join(' ').toLowerCase()
    return haystack.includes(query)
  })
})

const botValidationIssues = computed(() =>
  configStore.validationIssues.filter((issue) => issue.path === 'bots' || issue.path.startsWith('bots['))
)

const tableHeaders = computed(() => [
  { title: t('pages.instances.table.name'), value: 'display_name', width: '26%' },
  { title: t('pages.instances.table.status'), value: 'enabled', width: '14%' },
  { title: t('pages.instances.table.agent'), value: 'agent', width: '14%' },
  { title: t('pages.instances.table.bindings'), value: 'bindings', width: '12%' },
  { title: t('pages.instances.table.platforms'), value: 'platforms', width: '24%' },
  { title: t('pages.instances.table.actions'), value: 'actions', width: '10%', sortable: false },
])

const dialogTitle = computed(() =>
  editingIndex.value >= 0
    ? t('pages.instances.dialog.editTitle')
    : t('pages.instances.dialog.createTitle')
)

function botDisplayName(bot: BotInstanceDraft): string {
  return bot.display_name || bot.id
}

function agentModeLabel(mode: string): string {
  const key = `pages.instances.agentModes.${mode}`
  const translated = t(key)
  return translated === key ? mode : translated
}

function adapterName(adapterId: string): string {
  const adapter = adapters.value.find((item) => item.id === adapterId)
  return adapter?.name || adapterId
}

function adapterRuntimeState(adapterId: string): BotBindingRuntimeSummary {
  const runtime = configStore.workspace?.runtime.adapterInstances.find((item) => item.id === adapterId)
  return {
    adapterInstanceId: adapterId,
    platformState: {
      running: runtime?.running ?? false,
      connected: runtime?.connected ?? false,
      available: runtime?.available ?? false,
    },
  }
}

function summarizePlatformHealth(bindings: BotBindingRuntimeSummary[]): string {
  if (bindings.length === 0) {
    return t('pages.instances.empty.noPlatform')
  }

  let connected = 0
  let grace = 0
  let disconnected = 0
  let stopped = 0

  for (const binding of bindings) {
    if (binding.platformState.connected) {
      connected += 1
      continue
    }
    if (binding.platformState.available) {
      grace += 1
      continue
    }
    if (binding.platformState.running) {
      disconnected += 1
      continue
    }
    stopped += 1
  }

  const parts: string[] = []
  if (connected > 0) {
    parts.push(t('pages.instances.health.connectedCount', { count: connected }))
  }
  if (grace > 0) {
    parts.push(t('pages.instances.health.graceCount', { count: grace }))
  }
  if (disconnected > 0) {
    parts.push(t('pages.instances.health.disconnectedCount', { count: disconnected }))
  }
  if (stopped > 0) {
    parts.push(t('pages.instances.health.stoppedCount', { count: stopped }))
  }
  return parts.join(' / ')
}

function botPlatformSummary(bot: BotInstanceDraft): string {
  const names = Array.from(new Set(bot.bindings.map((binding) => binding.adapter_instance_id)))
    .filter(Boolean)
    .map(adapterName)
  return names.length > 0 ? names.join(', ') : t('pages.instances.empty.noPlatform')
}

function botPlatformHealthSummary(bot: BotInstanceDraft): string {
  const bindings = Array.from(
    new Set(bot.bindings.map((binding) => binding.adapter_instance_id).filter(Boolean))
  ).map(adapterRuntimeState)
  return summarizePlatformHealth(bindings)
}

function issueMessage(issue: ConfigValidationIssue): string {
  return localizedConfigIssueMessage(issue, locale.value)
}

function makeDefaultBotId(): string {
  const usedIds = new Set(bots.value.map((bot) => bot.id))
  let candidate = 'bot-main'
  let counter = 2
  while (usedIds.has(candidate)) {
    candidate = `bot-${counter}`
    counter += 1
  }
  return candidate
}

function makeDefaultBinding(botId: string): NormalizedBotBindingConfig | null {
  const adapterId = adapters.value[0]?.id
  if (!adapterId) {
    return null
  }
  return {
    id: `${botId}-main`,
    adapter_instance_id: adapterId,
    session_patterns: ['group:*'],
    enabled: true,
    priority: 0,
  }
}

function openCreate() {
  const template = configStore.workspace?.templates.bot
  const id = template?.id || makeDefaultBotId()
  const defaultBinding = makeDefaultBinding(id)
  editorForm.value = {
    id,
    display_name: template?.display_name || id,
    enabled: template?.enabled ?? true,
    commands: {
      enabled: template?.commands.enabled ?? true,
      prefixes: [...(template?.commands.prefixes ?? ['/'])],
    },
    plugins: {
      enabled: template?.plugins.enabled ?? true,
      enabled_plugins: [...(template?.plugins.enabled_plugins ?? ['*'])],
      disabled_plugins: [...(template?.plugins.disabled_plugins ?? [])],
    },
    agent: {
      mode: template?.agent.mode ?? 'none',
      config: template?.agent.config ?? '',
    },
    bindings: defaultBinding ? [defaultBinding] : [],
  }
  editorError.value = ''
  editingIndex.value = -1
  dialogVisible.value = true
}

function openEdit(bot: BotInstanceDraft) {
  const index = bots.value.findIndex((item) => item.id === bot.id)
  editingIndex.value = index
  editorForm.value = {
    id: bot.id,
    display_name: bot.display_name,
    enabled: bot.enabled,
    commands: {
      enabled: bot.commands.enabled,
      prefixes: [...bot.commands.prefixes],
    },
    plugins: {
      enabled: bot.plugins.enabled,
      enabled_plugins: [...bot.plugins.enabled_plugins],
      disabled_plugins: [...bot.plugins.disabled_plugins],
    },
    agent: {
      mode: bot.agent.mode,
      config: bot.agent.config,
    },
    bindings: bot.bindings.map((binding) => ({
      id: binding.id,
      adapter_instance_id: binding.adapter_instance_id,
      session_patterns: [...binding.session_patterns],
      enabled: binding.enabled,
      priority: binding.priority,
    })),
  }
  editorError.value = ''
  dialogVisible.value = true
}

function closeDialog() {
  dialogVisible.value = false
}

function validateEditorForm(): string {
  const id = editorForm.value.id.trim()
  if (!id) {
    return t('pages.instances.validation.requiredId')
  }
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(id)) {
    return t('pages.instances.validation.idFormat')
  }
  const duplicate = bots.value.some((bot, index) => bot.id === id && index !== editingIndex.value)
  if (duplicate) {
    return t('pages.instances.validation.duplicateId')
  }
  if (editorForm.value.agent.mode === 'full' && !editorForm.value.agent.config.trim()) {
    return t('pages.instances.validation.requiredAgentConfig')
  }
  const otherBindingIds = new Set(
    bots.value
      .filter((_, index) => index !== editingIndex.value)
      .flatMap((bot) => bot.bindings.map((binding) => binding.id))
  )
  const currentBindingIds = new Set<string>()
  for (const binding of editorForm.value.bindings) {
    const bindingId = binding.id.trim()
    if (!bindingId) {
      return t('pages.instances.validation.requiredBindingId')
    }
    if (otherBindingIds.has(bindingId) || currentBindingIds.has(bindingId)) {
      return t('pages.instances.validation.duplicateBindingId')
    }
    currentBindingIds.add(bindingId)
    if (!binding.adapter_instance_id.trim()) {
      return t('pages.instances.validation.requiredAdapterInstance')
    }
    if (binding.session_patterns.length === 0) {
      return t('pages.instances.validation.requiredSessionPatterns')
    }
  }
  return ''
}

function updateBots(records: ConfigRecord[]) {
  configStore.setDraftPath('bots', records.map((record) => cloneConfigRecord(record)))
}

async function saveBots(records: ConfigRecord[]) {
  const previousRecords = botRecords.value.map((record) => cloneConfigRecord(record))
  updateBots(records)
  const result = await configStore.saveBots({
    bots: records,
    validateBeforeSave: true,
  })
  if (!result) {
    updateBots(previousRecords)
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

  const records = botRecords.value.map((record) => cloneConfigRecord(record))
  const nextRecord = buildBotRecord(editorForm.value)
  if (editingIndex.value >= 0) {
    records[editingIndex.value] = nextRecord
  } else {
    records.push(nextRecord)
  }
  const saved = await saveBots(records)
  if (saved) {
    dialogVisible.value = false
    return
  }
  editorError.value = configStore.error
}

async function deleteBot(bot: BotInstanceDraft) {
  if (!window.confirm(t('pages.instances.deleteConfirm', { name: botDisplayName(bot) }))) {
    return
  }

  await saveBots(
    botRecords.value
      .filter((_, index) => bots.value[index]?.id !== bot.id)
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
