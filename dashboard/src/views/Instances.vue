<template>
  <v-container fluid class="pa-0">
    <app-page-header :title="$t('pages.instances.title')" :subtitle="$t('pages.instances.subtitle')"
      :kicker="$t('pages.instances.kicker')">
      <template #actions>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreate">
          {{ $t('pages.instances.create') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-row class="mb-6 mx-0" align="center">
      <v-col cols="12" sm="8" md="4" class="pa-0">
        <v-text-field v-model="searchQuery" :label="$t('common.actions.action.search')" prepend-inner-icon="mdi-magnify"
          single-line hide-details density="comfortable" variant="outlined" bg-color="surface" rounded="lg" />
      </v-col>
      <v-spacer />
      <v-col cols="auto">
        <layout-mode-button v-model="viewMode" :list-label="t('pages.instances.views.list')"
          :card-label="t('pages.instances.views.card')" />
      </v-col>
      <v-col cols="auto">
        <v-btn icon="mdi-refresh" variant="outlined" @click="handleRefresh" :loading="instancesStore.isLoading" />
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
        <v-btn color="primary" @click="openCreate">
          {{ $t('pages.instances.create') }}
        </v-btn>
      </v-col>
    </v-row>

    <v-row v-else-if="viewMode === 'card'" class="ma-0">
      <v-col v-for="instance in filteredInstances" :key="instance.id" cols="12" sm="6" md="4" lg="3">
        <instance-card :instance="instance" @edit="openEdit" @delete="deleteInstance" />
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
            <v-btn v-if="tableRow(item).status === 'stopped'" icon="mdi-play" size="small" color="success"
              :loading="instancePendingAction(item) === 'start'" @click="startInstance(tableRow(item))" />
            <v-btn v-else icon="mdi-stop" size="small" color="warning" :loading="instancePendingAction(item) === 'stop'"
              @click="stopInstance(tableRow(item))" />
            <v-btn icon="mdi-pencil" size="small" @click="openEdit(tableRow(item))" />
            <v-btn icon="mdi-delete" size="small" @click="deleteInstance(tableRow(item))" />
          </template>
        </v-data-table>
      </v-col>
    </v-row>

    <v-alert v-if="instancesStore.error" type="error" class="mt-4">
      {{ instancesStore.error }}
    </v-alert>

    <instance-form-dialog v-model:visible="dialogVisible" v-model:form="form"
      v-model:bot-config-entries="botConfigEntries"
      :title-key="editingId ? 'pages.instances.dialog.editTitle' : 'pages.instances.dialog.createTitle'"
      :adapter-options="adapterOptions" :active-adapter-schema="activeAdapterSchema" :agents="agents"
      :prompt-catalog="promptCatalog" @close="dialogVisible = false" @save="submit" />
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { agentsApi, type AgentSummary } from '@/api/agents'
import { botConfigsApi, type BotConfig } from '@/api/botConfigs'
import { promptsApi, type PromptCatalogItem } from '@/api/prompts'
import { useInstancesStore } from '@/stores/instances'
import { usePluginsStore } from '@/stores/plugins'
import { useUiStore } from '@/stores/ui'
import { useCrudDialog } from '@/composables/useCrudDialog'
import { getErrorMessage } from '@/utils/error'
import {
  formatOptionalNumber,
  normalizeNullableString,
  parseOptionalInteger,
  parseOptionalFloat,
  objectToEntries,
  entriesToObject
} from '@/utils/form'
import InstanceCard from '@/components/InstanceCard.vue'
import InstanceFormDialog from '@/components/instances/InstanceFormDialog.vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import type { Instance, InstanceConfig } from '@/api/instances'
import type { PluginConfigSchema } from '@/api/plugins'
import type { InstanceFormState, KeyValueEntry } from '@/components/instances/types'

const { t } = useI18n()
const instancesStore = useInstancesStore()
const pluginsStore = usePluginsStore()
const uiStore = useUiStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
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

const { visible: dialogVisible, editingId, openCreate: openCreateBase, openEdit: openEditBase, submit } = useCrudDialog<Instance, any>({
  resetForm: () => {
    form.value = createFormState(adapterOptions.value[0] || 'satori')
    botConfigEntries.value = []
  },
  populateForm: (instance) => {
    const config = instance.config as InstanceConfig
    const bc = botConfigs.value.find((item) => item.instanceId === instance.id)
    form.value = {
      name: instance.name,
      adapterType: instance.adapterType,
      config: { ...config },
      botConfig: {
        uuid: bc?.uuid ?? '',
        defaultAgentUuid: bc?.defaultAgentUuid ?? instance.botConfig?.defaultAgentUuid ?? '',
        mainLlm: bc?.mainLlm ?? instance.botConfig?.mainLlm ?? '',
        explicitPromptCacheEnabled: bc?.explicitPromptCacheEnabled ?? instance.botConfig?.explicitPromptCacheEnabled ?? false,
        mediaInspectionLlm: bc?.mediaInspectionLlm ?? instance.botConfig?.mediaInspectionLlm ?? '',
        mediaInspectionPrompt: bc?.mediaInspectionPrompt ?? instance.botConfig?.mediaInspectionPrompt ?? '',
        stickerSummaryLlm: bc?.stickerSummaryLlm ?? instance.botConfig?.stickerSummaryLlm ?? '',
        stickerSummaryPrompt: bc?.stickerSummaryPrompt ?? instance.botConfig?.stickerSummaryPrompt ?? '',
        contextCompressionLlm: bc?.contextCompressionLlm ?? instance.botConfig?.contextCompressionLlm ?? '',
        maxContextTokens: formatOptionalNumber(bc?.maxContextTokens ?? instance.botConfig?.maxContextTokens),
        contextEvictRatio: formatOptionalNumber(bc?.contextEvictRatio ?? instance.botConfig?.contextEvictRatio),
        contextCompressionMaxChars: formatOptionalNumber(bc?.contextCompressionMaxChars ?? instance.botConfig?.contextCompressionMaxChars),
        tags: [...(bc?.tags ?? instance.botConfig?.tags ?? [])],
      },
    }
    botConfigEntries.value = objectToEntries(bc?.config ?? {})
  },
  buildPayload: () => form.value, // Dummy, handled in save
  save: async () => {
    const instance = editingId.value
      ? await instancesStore.updateInstance(editingId.value, { name: form.value.name, config: form.value.config })
      : await instancesStore.createInstance({ name: form.value.name, adapterType: form.value.adapterType, config: form.value.config })

    if (instance) {
      const bcSaved = await saveBotConfig(instance.id)
      if (bcSaved) {
        await handleRefresh()
        uiStore.showSnackbar(t('pages.instances.saved'), 'success')
        return true
      }
    }
    return false
  }
})

const openCreate = () => openCreateBase()
const openEdit = (item: Instance) => openEditBase(item)

const adapterOptions = computed(() => {
  const adapters = pluginsStore.plugins
    .filter((p) => p.role === 'adapter')
    .map((p) => p.metadata?.adapter_platform)
    .filter((p): p is string => Boolean(p))
  return Array.from(new Set(adapters)).length > 0 ? Array.from(new Set(adapters)) : ['satori', 'onebot_v11']
})

const adapterSchemaByPlatform = computed<Record<string, PluginConfigSchema>>(() => {
  const mapping: Record<string, PluginConfigSchema> = {}
  pluginsStore.plugins.forEach(p => {
    if (p.role === 'adapter' && p.metadata?.adapter_platform && p.metadata?.config_schema) {
      mapping[p.metadata.adapter_platform] = p.metadata.config_schema
    }
  })
  return mapping
})

const activeAdapterSchema = computed(() => adapterSchemaByPlatform.value[form.value.adapterType] ?? null)

const tableHeaders = computed(() => [
  { title: t('pages.instances.table.name'), value: 'name', width: '20%' },
  { title: t('pages.instances.table.adapterType'), value: 'adapterType', width: '25%' },
  { title: t('pages.instances.table.status'), value: 'status', width: '15%' },
  { title: t('pages.instances.table.created'), value: 'createdAt', width: '20%' },
  { title: t('pages.instances.table.actions'), value: 'actions', width: '20%', sortable: false },
])

const tableRow = (item: any): Instance => item.raw ?? item
const instancePendingAction = (item: any) => instancesStore.pendingActions[tableRow(item).id]
const instanceStatusColor = (item: any) => instancePendingAction(item) ? 'warning' : tableRow(item).status === 'running' ? 'success' : 'error'
const instanceStatusLabel = (item: any) => instancePendingAction(item) ? t('common.actions.status.loading') : (tableRow(item).status === 'running' ? t('pages.instances.card.isRunning') : t('pages.instances.card.isStopped'))

const filteredInstances = computed(() =>
  instancesStore.instances.filter((i) => i.name.toLowerCase().includes(searchQuery.value.toLowerCase()))
)

const handleRefresh = () => Promise.all([instancesStore.fetchInstances(), pluginsStore.fetchPlugins(), fetchAgents(), fetchBotConfigs(), fetchPrompts()])

const deleteInstance = (instance: Instance) => instancesStore.deleteInstance(instance.id)
const startInstance = (instance: Instance) => instancesStore.startInstance(instance.id)
const stopInstance = (instance: Instance) => instancesStore.stopInstance(instance.id)

const fetchAgents = async () => {
  try {
    const res = await agentsApi.list()
    if (res.data.success) agents.value = res.data.data || []
  } catch (e) {
    uiStore.showSnackbar(getErrorMessage(e, t('pages.instances.agentsLoadFailed')), 'error')
  }
}

const fetchBotConfigs = async () => {
  try {
    const res = await botConfigsApi.list()
    if (res.data.success) botConfigs.value = res.data.data || []
  } catch (e) {
    uiStore.showSnackbar(getErrorMessage(e, t('pages.instances.botConfigLoadFailed')), 'error')
  }
}

const fetchPrompts = async () => {
  try {
    const res = await promptsApi.list()
    if (res.data.success) promptCatalog.value = res.data.data || []
  } catch (e) {
    uiStore.showSnackbar(getErrorMessage(e, t('pages.instances.promptsLoadFailed')), 'error')
  }
}

const saveBotConfig = async (instanceId: string) => {
  try {
    const payload = {
      instanceId,
      defaultAgentUuid: form.value.botConfig.defaultAgentUuid,
      mainLlm: form.value.botConfig.mainLlm.trim(),
      explicitPromptCacheEnabled: form.value.botConfig.explicitPromptCacheEnabled,
      mediaInspectionLlm: normalizeNullableString(form.value.botConfig.mediaInspectionLlm),
      mediaInspectionPrompt: normalizeNullableString(form.value.botConfig.mediaInspectionPrompt),
      stickerSummaryLlm: normalizeNullableString(form.value.botConfig.stickerSummaryLlm),
      stickerSummaryPrompt: normalizeNullableString(form.value.botConfig.stickerSummaryPrompt),
      contextCompressionLlm: normalizeNullableString(form.value.botConfig.contextCompressionLlm),
      maxContextTokens: parseOptionalInteger(form.value.botConfig.maxContextTokens, 'pages.instances.form.maxContextTokens'),
      contextEvictRatio: parseOptionalFloat(form.value.botConfig.contextEvictRatio, 'pages.instances.form.contextEvictRatio'),
      contextCompressionMaxChars: parseOptionalInteger(form.value.botConfig.contextCompressionMaxChars, 'pages.instances.form.contextCompressionMaxChars'),
      config: entriesToObject(botConfigEntries.value),
      tags: form.value.botConfig.tags.map(t => t.trim()).filter(Boolean),
    }

    const hasData = payload.defaultAgentUuid || payload.mainLlm || payload.explicitPromptCacheEnabled ||
      payload.mediaInspectionLlm || payload.maxContextTokens !== null || payload.tags.length > 0 || Object.keys(payload.config).length > 0

    if (!hasData && !form.value.botConfig.uuid) return true

    const res = form.value.botConfig.uuid
      ? await botConfigsApi.update(form.value.botConfig.uuid, payload)
      : await botConfigsApi.create(payload)
    return res.data.success
  } catch (e) {
    uiStore.showSnackbar(getErrorMessage(e, t('pages.instances.botConfigSaveFailed')), 'error')
    return false
  }
}

watch(() => form.value.adapterType, (next, prev) => {
  if (!next || next === prev || Object.keys(form.value.config).length > 0) return
  const schema = adapterSchemaByPlatform.value[next]
  if (!schema?.properties) return
  const defaults: any = {}
  Object.entries(schema.properties).forEach(([k, v]: [string, any]) => {
    if (v.default !== undefined) defaults[k] = v.default
  })
  form.value.config = defaults
})

onMounted(handleRefresh)
</script>
