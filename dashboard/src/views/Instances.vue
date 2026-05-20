<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.instances.title')"
      :subtitle="$t('pages.instances.subtitle')"
      :kicker="$t('pages.instances.kicker')"
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
          {{ $t('pages.instances.actions.validate') }}
        </v-btn>
        <v-btn
          color="primary"
          prepend-icon="mdi-plus"
          rounded="lg"
          @click="openCreate"
        >
          {{ $t('pages.instances.create') }}
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
      v-if="botValidationIssues.length > 0"
      type="warning"
      variant="tonal"
      density="comfortable"
      class="mb-6"
    >
      <div class="font-weight-medium mb-2">
        {{ $t('pages.instances.validation.title') }}
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
        {{ $t('pages.instances.validation.more', { count: hiddenValidationIssueCount }) }}
      </div>
    </v-alert>

    <div class="instances-toolbar mb-6">
      <v-text-field
        v-model="searchQuery"
        :label="$t('common.actions.action.search')"
        prepend-inner-icon="mdi-magnify"
        single-line
        hide-details
        density="comfortable"
        variant="outlined"
        bg-color="surface"
        class="instances-search"
      />
      <v-spacer />
      <layout-mode-button
        v-model="viewMode"
        :list-label="t('pages.instances.views.list')"
        :card-label="t('pages.instances.views.card')"
      />
    </div>

    <v-row v-if="showInitialSkeleton">
      <v-col cols="12">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row v-else-if="!initialSkeletonRequested && filteredBots.length === 0" justify="center" class="py-12">
      <v-col cols="12" sm="8" md="6" class="text-center">
        <v-icon size="112" color="grey-lighten-1" icon="mdi-robot-confused-outline" />
        <h3 class="text-h6 my-4">{{ $t('pages.instances.noData') }}</h3>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreate">
          {{ $t('pages.instances.create') }}
        </v-btn>
      </v-col>
    </v-row>

    <v-row v-else-if="viewMode === 'card'" class="ma-0">
      <v-col v-for="bot in filteredBots" :key="bot.id" cols="12" sm="6" md="4" lg="3">
        <bot-instance-card
          :bot="bot"
          :display-name="botDisplayName(bot)"
          :platform-summary="botPlatformSummary(bot)"
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
      </v-col>
    </v-row>

    <v-row v-else>
      <v-col cols="12">
        <bot-instance-table
          :headers="tableHeaders"
          :items="filteredBots"
          :loading="configStore.isLoading"
          :display-name="botDisplayName"
          :platform-summary="botPlatformSummary"
          :agent-mode-label="agentModeLabel"
          :enabled-label="$t('common.actions.status.enabled')"
          :disabled-label="$t('common.actions.status.disabled')"
          @edit="openEdit"
          @delete="deleteBot"
        />
      </v-col>
    </v-row>

    <bot-instance-form-dialog
      v-model:visible="dialogVisible"
      v-model:form="editorForm"
      :title="dialogTitle"
      :adapter-options="adapterOptions"
      :plugin-options="pluginOptions"
      :agent-mode-options="agentModeOptions"
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
  ConfigValue,
  NormalizedAdapterInstanceConfig,
  NormalizedBotBindingConfig,
} from '@/api/config'
import AppPageHeader from '@/components/AppPageHeader.vue'
import BotInstanceCard from '@/components/instances/BotInstanceCard.vue'
import BotInstanceFormDialog from '@/components/instances/BotInstanceFormDialog.vue'
import BotInstanceTable from '@/components/instances/BotInstanceTable.vue'
import type {
  BotInstanceDraft,
  BotInstanceFormState,
  SelectOption,
} from '@/components/instances/botTypes'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { localizedConfigIssueMessage } from '@/config'
import { useConfigWorkspaceStore } from '@/stores/configWorkspace'

const { locale, t } = useI18n()
const configStore = useConfigWorkspaceStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
const dialogVisible = ref(false)
const editingIndex = ref(-1)
const editorError = ref('')
const editorForm = ref<BotInstanceFormState>(createEmptyForm())
const hasLoadedWorkspace = ref(false)

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
const visibleValidationIssues = computed(() => botValidationIssues.value.slice(0, 5))
const hiddenValidationIssueCount = computed(() =>
  Math.max(botValidationIssues.value.length - visibleValidationIssues.value.length, 0)
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

function createEmptyForm(): BotInstanceFormState {
  return {
    id: '',
    display_name: '',
    enabled: true,
    commands: {
      enabled: true,
      prefixes: ['/'],
    },
    plugins: {
      enabled: true,
      enabled_plugins: ['*'],
      disabled_plugins: [],
    },
    agent: {
      mode: 'none',
      config: '',
    },
    bindings: [],
  }
}

function isConfigRecord(value: unknown): value is ConfigRecord {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function cloneConfigRecord(value: ConfigRecord = {}): ConfigRecord {
  return JSON.parse(JSON.stringify(value)) as ConfigRecord
}

function normalizeString(value: ConfigValue | undefined, fallback = ''): string {
  return typeof value === 'string' ? value.trim() : fallback
}

function normalizeBoolean(value: ConfigValue | undefined, fallback = true): boolean {
  return typeof value === 'boolean' ? value : fallback
}

function normalizeInteger(value: ConfigValue | undefined, fallback = 0): number {
  return typeof value === 'number' && Number.isInteger(value) ? value : fallback
}

function normalizeStringList(value: ConfigValue | undefined, fallback: string[] = []): string[] {
  if (!Array.isArray(value)) {
    return fallback
  }
  return value
    .filter((item): item is string => typeof item === 'string')
    .map((item) => item.trim())
    .filter(Boolean)
}

function normalizeAdapter(record: ConfigRecord, index: number): NormalizedAdapterInstanceConfig {
  const id = normalizeString(record.id, `adapter-${index + 1}`)
  const adapter = normalizeString(record.adapter)
  return {
    id,
    name: normalizeString(record.name, id),
    adapter,
    enabled: normalizeBoolean(record.enabled, true),
    config: isConfigRecord(record.config) ? cloneConfigRecord(record.config) : {},
    createdAt: typeof record.createdAt === 'number' ? record.createdAt : 0,
    lastModified: typeof record.lastModified === 'number' ? record.lastModified : 0,
  }
}

function normalizeBinding(
  value: ConfigValue | undefined,
  index: number,
  botId: string
): NormalizedBotBindingConfig {
  const record = isConfigRecord(value) ? value : {}
  return {
    id: normalizeString(record.id, `${botId}-binding-${index + 1}`),
    adapter_instance_id: normalizeString(record.adapter_instance_id),
    session_patterns: normalizeStringList(record.session_patterns, ['group:*']),
    enabled: normalizeBoolean(record.enabled, true),
    priority: normalizeInteger(record.priority, 0),
  }
}

function normalizeBot(record: ConfigRecord, index: number): BotInstanceDraft {
  const id = normalizeString(record.id, `bot-${index + 1}`)
  const commands = isConfigRecord(record.commands) ? record.commands : {}
  const plugins = isConfigRecord(record.plugins) ? record.plugins : {}
  const agent = isConfigRecord(record.agent) ? record.agent : {}
  const rawBindings = Array.isArray(record.bindings) ? record.bindings : []

  return {
    id,
    display_name: normalizeString(record.display_name, id),
    enabled: normalizeBoolean(record.enabled, true),
    commands: {
      enabled: normalizeBoolean(commands.enabled, true),
      prefixes: normalizeStringList(commands.prefixes, ['/']),
    },
    plugins: {
      enabled: normalizeBoolean(plugins.enabled, true),
      enabled_plugins: normalizeStringList(plugins.enabled_plugins, ['*']),
      disabled_plugins: normalizeStringList(plugins.disabled_plugins, []),
    },
    agent: {
      mode: normalizeString(agent.mode, 'none'),
      config: normalizeString(agent.config),
    },
    bindings: rawBindings.map((binding, bindingIndex) =>
      normalizeBinding(binding, bindingIndex, id)
    ),
  }
}

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

function botPlatformSummary(bot: BotInstanceDraft): string {
  const names = Array.from(new Set(bot.bindings.map((binding) => binding.adapter_instance_id)))
    .filter(Boolean)
    .map(adapterName)
  return names.length > 0 ? names.join(', ') : t('pages.instances.empty.noPlatform')
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

function cleanStringList(value: string[], fallback: string[]): string[] {
  const result = value.map((item) => String(item).trim()).filter(Boolean)
  return result.length > 0 ? Array.from(new Set(result)) : fallback
}

function buildBotRecord(form: BotInstanceFormState): ConfigRecord {
  const id = form.id.trim()
  return {
    id,
    display_name: form.display_name.trim() || id,
    enabled: form.enabled,
    commands: {
      enabled: form.commands.enabled,
      prefixes: cleanStringList(form.commands.prefixes, ['/']),
    },
    plugins: {
      enabled: form.plugins.enabled,
      enabled_plugins: cleanStringList(form.plugins.enabled_plugins, ['*']),
      disabled_plugins: cleanStringList(form.plugins.disabled_plugins, []),
    },
    agent: {
      mode: form.agent.mode,
      config: form.agent.config.trim(),
    },
    bindings: form.bindings.map((binding) => ({
      id: binding.id.trim(),
      adapter_instance_id: binding.adapter_instance_id.trim(),
      session_patterns: cleanStringList(binding.session_patterns, ['group:*']),
      enabled: binding.enabled,
      priority: Number.isInteger(Number(binding.priority)) ? Number(binding.priority) : 0,
    })),
  }
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

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.instances-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px;
  @include surface-card;
}

.instances-search {
  flex: 0 1 420px;
}

.validation-issue-line {
  display: flex;
  gap: 8px;
  align-items: baseline;
  min-width: 0;
}

@include respond-to('tablet') {
  .instances-toolbar {
    align-items: stretch;
    flex-direction: column;
  }

  .instances-search {
    flex: 1 1 auto;
    width: 100%;
  }
}
</style>
