<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.instances.title')"
      :subtitle="$t('pages.instances.subtitle')"
      :kicker="$t('pages.instances.kicker')"
    >
      <template #actions>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="showCreateDialog">
          {{ $t('pages.instances.create') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-row class="mb-6 mx-0" align="center">
      <v-col cols="12" sm="8" md="4" class="pa-0">
        <v-text-field
          v-model="searchQuery"
          :label="$t('common.actions.action.search')"
          prepend-inner-icon="mdi-magnify"
          single-line
          hide-details
          density="comfortable"
          variant="outlined"
          bg-color="surface"
          rounded="lg"
        />
      </v-col>
      <v-spacer />
      <v-col cols="auto">
        <layout-mode-button
          v-model="viewMode"
          :list-label="t('pages.instances.views.list')"
          :card-label="t('pages.instances.views.card')"
        />
      </v-col>
      <v-col cols="auto">
        <v-btn
          icon="mdi-refresh"
          variant="outlined"
          @click="handleRefresh"
          :loading="instancesStore.isLoading"
        />
      </v-col>
    </v-row>

    <v-row v-if="instancesStore.isLoading && instancesStore.instances.length === 0">
      <v-col cols="12">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row v-else-if="filteredInstances.length === 0" justify="center" class="py-12">
      <v-col cols="12" sm="8" md="6" class="text-center">
        <v-icon size="120" color="grey-lighten-1" icon="mdi-robot-confused" />
        <h3 class="text-h6 my-4">{{ $t('pages.instances.noData') }}</h3>
        <v-btn color="primary" @click="showCreateDialog">
          {{ $t('pages.instances.create') }}
        </v-btn>
      </v-col>
    </v-row>

    <v-row v-else-if="viewMode === 'card'" class="ma-0">
      <v-col
        v-for="instance in filteredInstances"
        :key="instance.id"
        cols="12"
        sm="6"
        md="4"
        lg="3"
      >
        <instance-card :instance="instance" @edit="editInstance" @delete="deleteInstance" />
      </v-col>
    </v-row>

    <v-row v-else>
      <v-col cols="12">
        <v-data-table :headers="tableHeaders" :items="filteredInstances" hide-default-footer>
          <template #item.status="{ item }">
            <v-chip :color="instanceStatusColor(item)" size="small">
              {{ instanceStatusLabel(item) }}
            </v-chip>
          </template>
          <template #item.actions="{ item }">
            <v-btn
              v-if="tableRow(item).status === 'stopped'"
              icon="mdi-play"
              size="small"
              color="success"
              :loading="instancePendingAction(item) === 'start'"
              @click="startInstance(tableRow(item))"
            />
            <v-btn
              v-else
              icon="mdi-stop"
              size="small"
              color="warning"
              :loading="instancePendingAction(item) === 'stop'"
              @click="stopInstance(tableRow(item))"
            />
            <v-btn icon="mdi-pencil" size="small" @click="editInstance(tableRow(item))" />
            <v-btn icon="mdi-delete" size="small" @click="deleteInstance(tableRow(item))" />
          </template>
        </v-data-table>
      </v-col>
    </v-row>

    <v-alert v-if="instancesStore.error" type="error" class="mt-4">
      {{ instancesStore.error }}
    </v-alert>

    <instance-form-dialog
      v-model:visible="dialogVisible"
      v-model:form="form"
      v-model:bot-config-entries="botConfigEntries"
      :title-key="dialogTitleKey"
      :adapter-options="adapterOptions"
      :active-adapter-schema="activeAdapterSchema"
      :agents="agents"
      :prompt-catalog="promptCatalog"
      @close="closeDialog"
      @save="saveInstance"
    />
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { agentsApi, type AgentSummary } from '@/api/agents'
import {
  botConfigsApi,
  type BotConfig,
  type CreateBotConfigRequest,
  type UpdateBotConfigRequest,
} from '@/api/botConfigs'
import { promptsApi, type PromptCatalogItem } from '@/api/prompts'
import { useInstancesStore } from '@/stores/instances'
import { usePluginsStore } from '@/stores/plugins'
import InstanceCard from '@/components/InstanceCard.vue'
import InstanceFormDialog from '@/components/instances/InstanceFormDialog.vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import type { Instance, InstanceConfig, UpdateInstanceRequest } from '@/api/instances'
import type { PluginConfigSchema } from '@/api/plugins'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'
import type { InstanceFormState, KeyValueEntry } from '@/components/instances/types'

const { t } = useI18n()
const instancesStore = useInstancesStore()
const pluginsStore = usePluginsStore()
const uiStore = useUiStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
const dialogVisible = ref(false)
const dialogTitleKey = ref('pages.instances.dialog.createTitle')
const editingId = ref('')
const agents = ref<AgentSummary[]>([])
const botConfigs = ref<BotConfig[]>([])
const promptCatalog = ref<PromptCatalogItem[]>([])
const botConfigEntries = ref<KeyValueEntry[]>([])

const createEmptyBotConfig = (): InstanceFormState['botConfig'] => ({
  uuid: '',
  defaultAgentUuid: '',
  mainLlm: '',
  explicitPromptCacheEnabled: false,
  mediaInspectionLlm: '',
  mediaInspectionPrompt: '',
  stickerSummaryLlm: '',
  stickerSummaryPrompt: '',
  contextCompressionLlm: '',
  maxContextTokens: '',
  contextEvictRatio: '',
  contextCompressionMaxChars: '',
  tags: [],
})

const createFormState = (adapterType = ''): InstanceFormState => ({
  name: '',
  adapterType,
  config: {},
  botConfig: createEmptyBotConfig(),
})

const form = ref<InstanceFormState>(createFormState())

const adapterOptions = computed(() => {
  const adapters = pluginsStore.plugins
    .filter((plugin) => plugin.role === 'adapter')
    .map((plugin) => plugin.metadata?.adapter_platform)
    .filter((platform): platform is string => Boolean(platform))

  const unique = Array.from(new Set(adapters))
  return unique.length > 0 ? unique : ['satori', 'onebot_v11']
})

const adapterSchemaByPlatform = computed<Record<string, PluginConfigSchema>>(() => {
  const mapping: Record<string, PluginConfigSchema> = {}
  for (const plugin of pluginsStore.plugins) {
    if (plugin.role !== 'adapter') {
      continue
    }
    const platform = plugin.metadata?.adapter_platform
    const schema = plugin.metadata?.config_schema
    if (platform && schema) {
      mapping[platform] = schema
    }
  }
  return mapping
})

const activeAdapterSchema = computed<PluginConfigSchema | null>(
  () => adapterSchemaByPlatform.value[form.value.adapterType] ?? null,
)

const tableHeaders = computed(() => [
  { title: t('pages.instances.table.name'), value: 'name', width: '20%' },
  {
    title: t('pages.instances.table.adapterType'),
    value: 'adapterType',
    width: '25%',
  },
  { title: t('pages.instances.table.status'), value: 'status', width: '15%' },
  {
    title: t('pages.instances.table.created'),
    value: 'createdAt',
    width: '20%',
  },
  {
    title: t('pages.instances.table.actions'),
    value: 'actions',
    width: '20%',
    sortable: false,
  },
])

const tableRow = (item: unknown): Instance => {
  const row = item as { raw?: Instance }
  return row.raw ?? (item as Instance)
}

const instancePendingAction = (item: unknown) => instancesStore.pendingActions[tableRow(item).id]

const instanceStatusColor = (item: unknown) => {
  if (instancePendingAction(item)) {
    return 'warning'
  }
  return tableRow(item).status === 'running' ? 'success' : 'error'
}

const instanceStatusLabel = (item: unknown) => {
  if (instancePendingAction(item)) {
    return t('common.actions.status.loading')
  }
  return tableRow(item).status === 'running'
    ? t('pages.instances.card.isRunning')
    : t('pages.instances.card.isStopped')
}

const filteredInstances = computed(() =>
  instancesStore.instances.filter((instance: (typeof instancesStore.instances)[number]) =>
    instance.name.toLowerCase().includes(searchQuery.value.toLowerCase()),
  ),
)

onMounted(() => {
  void Promise.all([
    instancesStore.fetchInstances(),
    pluginsStore.fetchPlugins(),
    fetchAgents(),
    fetchBotConfigs(),
    fetchPrompts(),
  ])
})

watch(
  () => form.value.adapterType,
  (nextPlatform, prevPlatform) => {
    if (!nextPlatform || nextPlatform === prevPlatform) {
      return
    }
    if (Object.keys(form.value.config).length > 0) {
      return
    }
    const schema = adapterSchemaByPlatform.value[nextPlatform]
    if (!schema?.properties) {
      return
    }

    const defaults: Record<string, unknown> = {}
    for (const [key, property] of Object.entries(schema.properties)) {
      if (property.default !== undefined) {
        defaults[key] = property.default
      }
    }
    form.value.config = defaults
  },
)

const handleRefresh = async () => {
  await Promise.all([
    instancesStore.fetchInstances(),
    fetchAgents(),
    fetchBotConfigs(),
    fetchPrompts(),
  ])
}

const showCreateDialog = () => {
  editingId.value = ''
  dialogTitleKey.value = 'pages.instances.dialog.createTitle'
  const defaultAdapter = adapterOptions.value[0] ?? 'satori'
  form.value = createFormState(defaultAdapter)
  botConfigEntries.value = []
  dialogVisible.value = true
}

const editInstance = (instance: Instance) => {
  editingId.value = instance.id
  dialogTitleKey.value = 'pages.instances.dialog.editTitle'
  const config = instance.config as InstanceConfig
  const currentBotConfig = botConfigs.value.find((item) => item.instanceId === instance.id)
  form.value = {
    name: instance.name,
    adapterType: instance.adapterType,
    config: { ...config },
    botConfig: {
      uuid: currentBotConfig?.uuid ?? '',
      defaultAgentUuid:
        currentBotConfig?.defaultAgentUuid ?? instance.botConfig?.defaultAgentUuid ?? '',
      mainLlm: currentBotConfig?.mainLlm ?? instance.botConfig?.mainLlm ?? '',
      explicitPromptCacheEnabled:
        currentBotConfig?.explicitPromptCacheEnabled ??
        instance.botConfig?.explicitPromptCacheEnabled ??
        false,
      mediaInspectionLlm:
        currentBotConfig?.mediaInspectionLlm ?? instance.botConfig?.mediaInspectionLlm ?? '',
      mediaInspectionPrompt:
        currentBotConfig?.mediaInspectionPrompt ?? instance.botConfig?.mediaInspectionPrompt ?? '',
      stickerSummaryLlm:
        currentBotConfig?.stickerSummaryLlm ?? instance.botConfig?.stickerSummaryLlm ?? '',
      stickerSummaryPrompt:
        currentBotConfig?.stickerSummaryPrompt ?? instance.botConfig?.stickerSummaryPrompt ?? '',
      contextCompressionLlm:
        currentBotConfig?.contextCompressionLlm ?? instance.botConfig?.contextCompressionLlm ?? '',
      maxContextTokens: formatOptionalNumber(
        currentBotConfig?.maxContextTokens ?? instance.botConfig?.maxContextTokens,
      ),
      contextEvictRatio: formatOptionalNumber(
        currentBotConfig?.contextEvictRatio ?? instance.botConfig?.contextEvictRatio,
      ),
      contextCompressionMaxChars: formatOptionalNumber(
        currentBotConfig?.contextCompressionMaxChars ??
          instance.botConfig?.contextCompressionMaxChars,
      ),
      tags: [...(currentBotConfig?.tags ?? instance.botConfig?.tags ?? [])],
    },
  }
  botConfigEntries.value = objectToEntries(currentBotConfig?.config ?? {})
  dialogVisible.value = true
}

const closeDialog = () => {
  dialogVisible.value = false
  botConfigEntries.value = []
}

const saveInstance = async () => {
  const config: InstanceConfig = { ...form.value.config }

  const payload: UpdateInstanceRequest = {
    name: form.value.name,
    config,
  }

  const instance = editingId.value
    ? await instancesStore.updateInstance(editingId.value, payload)
    : await instancesStore.createInstance({
        name: form.value.name,
        adapterType: form.value.adapterType,
        config,
      })

  if (instance) {
    const botConfigSaved = await saveBotConfig(instance.id)
    if (!botConfigSaved) {
      return
    }
    await fetchBotConfigs()
    await instancesStore.fetchInstances()
    uiStore.showSnackbar(t('pages.instances.saved'), 'success')
    closeDialog()
  }
}

const deleteInstance = async (instance: Instance) => {
  await instancesStore.deleteInstance(instance.id)
}

const startInstance = async (instance: Instance) => {
  await instancesStore.startInstance(instance.id)
}

const stopInstance = async (instance: Instance) => {
  await instancesStore.stopInstance(instance.id)
}

const fetchAgents = async () => {
  try {
    const response = await agentsApi.list()
    if (response.data.success && response.data.data) {
      agents.value = response.data.data
    }
  } catch (error) {
    uiStore.showSnackbar(getErrorMessage(error, t('pages.instances.agentsLoadFailed')), 'error')
  }
}

const fetchBotConfigs = async () => {
  try {
    const response = await botConfigsApi.list()
    if (response.data.success && response.data.data) {
      botConfigs.value = response.data.data
    }
  } catch (error) {
    uiStore.showSnackbar(getErrorMessage(error, t('pages.instances.botConfigLoadFailed')), 'error')
  }
}

const fetchPrompts = async () => {
  try {
    const response = await promptsApi.list()
    if (response.data.success && response.data.data) {
      promptCatalog.value = response.data.data
    }
  } catch (error) {
    uiStore.showSnackbar(getErrorMessage(error, t('pages.instances.promptsLoadFailed')), 'error')
  }
}

const saveBotConfig = async (instanceId: string) => {
  let payloadBase: {
    instanceId: string
    defaultAgentUuid: string
    mainLlm: string
    explicitPromptCacheEnabled: boolean
    mediaInspectionLlm: string | null
    mediaInspectionPrompt: string | null
    stickerSummaryLlm: string | null
    stickerSummaryPrompt: string | null
    contextCompressionLlm: string | null
    maxContextTokens: number | null
    contextEvictRatio: number | null
    contextCompressionMaxChars: number | null
    config: Record<string, unknown>
    tags: string[]
  }
  try {
    payloadBase = {
      instanceId,
      defaultAgentUuid: form.value.botConfig.defaultAgentUuid,
      mainLlm: form.value.botConfig.mainLlm.trim(),
      explicitPromptCacheEnabled: form.value.botConfig.explicitPromptCacheEnabled,
      mediaInspectionLlm: normalizeNullableString(form.value.botConfig.mediaInspectionLlm),
      mediaInspectionPrompt: normalizeNullableString(form.value.botConfig.mediaInspectionPrompt),
      stickerSummaryLlm: normalizeNullableString(form.value.botConfig.stickerSummaryLlm),
      stickerSummaryPrompt: normalizeNullableString(form.value.botConfig.stickerSummaryPrompt),
      contextCompressionLlm: normalizeNullableString(form.value.botConfig.contextCompressionLlm),
      maxContextTokens: parseOptionalInteger(
        form.value.botConfig.maxContextTokens,
        'pages.instances.form.maxContextTokens',
      ),
      contextEvictRatio: parseOptionalFloat(
        form.value.botConfig.contextEvictRatio,
        'pages.instances.form.contextEvictRatio',
      ),
      contextCompressionMaxChars: parseOptionalInteger(
        form.value.botConfig.contextCompressionMaxChars,
        'pages.instances.form.contextCompressionMaxChars',
      ),
      config: entriesToObject(botConfigEntries.value),
      tags: form.value.botConfig.tags.map((tag) => tag.trim()).filter(Boolean),
    }
  } catch (error) {
    uiStore.showSnackbar(getErrorMessage(error, t('pages.instances.botConfigSaveFailed')), 'error')
    return false
  }
  const hasMeaningfulBotConfig =
    Boolean(payloadBase.defaultAgentUuid) ||
    Boolean(payloadBase.mainLlm) ||
    payloadBase.explicitPromptCacheEnabled ||
    Boolean(payloadBase.mediaInspectionLlm) ||
    Boolean(payloadBase.mediaInspectionPrompt) ||
    Boolean(payloadBase.stickerSummaryLlm) ||
    Boolean(payloadBase.stickerSummaryPrompt) ||
    Boolean(payloadBase.contextCompressionLlm) ||
    payloadBase.maxContextTokens !== null ||
    payloadBase.contextEvictRatio !== null ||
    payloadBase.contextCompressionMaxChars !== null ||
    payloadBase.tags.length > 0 ||
    Object.keys(payloadBase.config).length > 0

  if (!hasMeaningfulBotConfig && !form.value.botConfig.uuid) {
    return true
  }

  try {
    if (form.value.botConfig.uuid) {
      const payload: UpdateBotConfigRequest = payloadBase
      const response = await botConfigsApi.update(form.value.botConfig.uuid, payload)
      return response.data.success
    }
    const payload: CreateBotConfigRequest = payloadBase
    const response = await botConfigsApi.create(payload)
    return response.data.success
  } catch (error) {
    uiStore.showSnackbar(getErrorMessage(error, t('pages.instances.botConfigSaveFailed')), 'error')
    return false
  }
}

const objectToEntries = (value: Record<string, unknown>) =>
  Object.entries(value).map(([key, entryValue]) => ({
    key,
    value: typeof entryValue === 'string' ? entryValue : JSON.stringify(entryValue),
  }))

const entriesToObject = (rows: Array<{ key: string; value: string }>) => {
  const output: Record<string, unknown> = {}
  for (const row of rows) {
    const key = row.key.trim()
    if (!key) {
      continue
    }
    const rawValue = row.value.trim()
    if (!rawValue) {
      output[key] = ''
      continue
    }
    try {
      output[key] = JSON.parse(rawValue)
    } catch {
      output[key] = rawValue
    }
  }
  return output
}

const formatOptionalNumber = (value: number | null | undefined) =>
  value === null || value === undefined ? '' : String(value)

const normalizeNullableString = (value: string) => {
  const normalized = value.trim()
  return normalized || null
}

const parseOptionalInteger = (value: string, labelKey: string) => {
  const normalized = value.trim()
  if (!normalized) {
    return null
  }
  const parsed = Number.parseInt(normalized, 10)
  if (!Number.isFinite(parsed)) {
    throw new Error(t('pages.instances.form.invalidNumericValue', { field: t(labelKey) }))
  }
  return parsed
}

const parseOptionalFloat = (value: string, labelKey: string) => {
  const normalized = value.trim()
  if (!normalized) {
    return null
  }
  const parsed = Number.parseFloat(normalized)
  if (!Number.isFinite(parsed)) {
    throw new Error(t('pages.instances.form.invalidNumericValue', { field: t(labelKey) }))
  }
  return parsed
}
</script>
