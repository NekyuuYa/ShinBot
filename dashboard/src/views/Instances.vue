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
            <v-chip
              :color="instancesStore.pendingActions[tableRow(item).id] ? 'warning' : tableRow(item).status === 'running' ? 'success' : 'error'"
              size="small"
            >
              {{
                instancesStore.pendingActions[tableRow(item).id]
                  ? $t('common.actions.status.loading')
                  : tableRow(item).status === 'running'
                    ? $t('pages.instances.card.isRunning')
                    : $t('pages.instances.card.isStopped')
              }}
            </v-chip>
          </template>
          <template #item.actions="{ item }">
            <v-btn
              v-if="tableRow(item).status === 'stopped'"
              icon="mdi-play"
              size="small"
              color="success"
              :loading="instancesStore.pendingActions[tableRow(item).id] === 'start'"
              @click="startInstance(tableRow(item))"
            />
            <v-btn
              v-else
              icon="mdi-stop"
              size="small"
              color="warning"
              :loading="instancesStore.pendingActions[tableRow(item).id] === 'stop'"
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

    <v-dialog v-model="dialogVisible" max-width="720">
      <v-card>
        <v-card-title>{{ t(dialogTitleKey) }}</v-card-title>
        <v-card-text>
          <v-row>
            <v-col cols="12" md="6">
              <v-text-field v-model="form.name" :label="$t('pages.instances.form.name')" />
            </v-col>
            <v-col cols="12" md="6">
              <v-select
                v-model="form.adapterType"
                :label="$t('pages.instances.form.adapterType')"
                :items="adapterOptions"
              />
            </v-col>
          </v-row>

          <schema-form
            v-if="activeAdapterSchema"
            v-model="form.config"
            :schema="activeAdapterSchema"
            :mode="String(form.config.mode ?? '')"
          />

          <v-alert v-else type="warning" variant="tonal" class="mt-3">
            {{ $t('pages.instances.form.noSchema') }}
          </v-alert>

          <v-divider class="my-5" />

          <div class="text-subtitle1 font-weight-medium mb-3">
            {{ $t('pages.instances.form.botConfigTitle') }}
          </div>
          <v-row>
            <v-col cols="12" md="6">
              <v-select
                v-model="form.botConfig.defaultAgentUuid"
                :label="$t('pages.instances.form.defaultAgent')"
                :items="agentOptions"
                item-title="title"
                item-value="value"
                clearable
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.botConfig.mainLlm"
                :label="$t('pages.instances.form.mainLlm')"
                placeholder="openai-main/gpt-fast"
                append-inner-icon="mdi-database-search-outline"
                @click:append-inner="showMainLlmPicker = true"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.botConfig.mediaInspectionLlm"
                :label="$t('pages.instances.form.mediaInspectionLlm')"
                placeholder="openai-main/gpt-4o"
                append-inner-icon="mdi-database-search-outline"
                @click:append-inner="showMediaInspectionLlmPicker = true"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.botConfig.mediaInspectionPrompt"
                :label="$t('pages.instances.form.mediaInspectionPrompt')"
                placeholder="builtin.prompt.media_inspection"
                append-inner-icon="mdi-text-box-search-outline"
                @click:append-inner="showMediaInspectionPromptPicker = true"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.botConfig.stickerSummaryLlm"
                :label="$t('pages.instances.form.stickerSummaryLlm')"
                placeholder="openai-main/gpt-4o-mini"
                append-inner-icon="mdi-database-search-outline"
                @click:append-inner="showStickerSummaryLlmPicker = true"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.botConfig.stickerSummaryPrompt"
                :label="$t('pages.instances.form.stickerSummaryPrompt')"
                placeholder="builtin.prompt.sticker_summary"
                append-inner-icon="mdi-text-box-search-outline"
                @click:append-inner="showStickerSummaryPromptPicker = true"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.botConfig.contextCompressionLlm"
                :label="$t('pages.instances.form.contextCompressionLlm')"
                placeholder="openai-main/gpt-4.1-mini"
                append-inner-icon="mdi-database-search-outline"
                @click:append-inner="showContextCompressionLlmPicker = true"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-switch
                v-model="form.botConfig.explicitPromptCacheEnabled"
                :label="$t('pages.instances.form.explicitPromptCacheEnabled')"
                :hint="$t('pages.instances.form.explicitPromptCacheEnabledHint')"
                color="primary"
                persistent-hint
                inset
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                v-model="form.botConfig.maxContextTokens"
                :label="$t('pages.instances.form.maxContextTokens')"
                placeholder="32000"
                type="number"
                min="1"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                v-model="form.botConfig.contextEvictRatio"
                :label="$t('pages.instances.form.contextEvictRatio')"
                placeholder="0.6"
                type="number"
                min="0"
                max="1"
                step="0.05"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                v-model="form.botConfig.contextCompressionMaxChars"
                :label="$t('pages.instances.form.contextCompressionMaxChars')"
                placeholder="240"
                type="number"
                min="1"
              />
            </v-col>
            <v-col cols="12">
              <v-combobox
                v-model="form.botConfig.tags"
                :label="$t('pages.instances.form.botTags')"
                multiple
                chips
                closable-chips
                clearable
              />
            </v-col>
            <v-col cols="12">
              <div class="text-body-2 text-medium-emphasis mb-2">
                {{ $t('pages.instances.form.botConfigFields') }}
              </div>
              <key-value-editor v-model="botConfigEntries" />
            </v-col>
          </v-row>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="closeDialog">{{ $t('common.actions.action.cancel') }}</v-btn>
          <v-btn color="primary" @click="saveInstance">{{ $t('common.actions.action.save') }}</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <generic-picker-dialog
      v-model="showMainLlmPicker"
      :title="$t('pages.instances.form.mainLlm')"
      :sections="mainLlmPickerSections"
      :selected="form.botConfig.mainLlm ? [form.botConfig.mainLlm] : []"
      :empty-text="$t('pages.modelRuntime.hints.modelIdPickerEmpty')"
      :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
      @update:selected="(vals) => { form.botConfig.mainLlm = vals[0] ?? '' }"
    />
    <generic-picker-dialog
      v-model="showMediaInspectionLlmPicker"
      :title="$t('pages.instances.form.mediaInspectionLlm')"
      :sections="mainLlmPickerSections"
      :selected="form.botConfig.mediaInspectionLlm ? [form.botConfig.mediaInspectionLlm] : []"
      :empty-text="$t('pages.modelRuntime.hints.modelIdPickerEmpty')"
      :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
      @update:selected="(vals) => { form.botConfig.mediaInspectionLlm = vals[0] ?? '' }"
    />
    <generic-picker-dialog
      v-model="showMediaInspectionPromptPicker"
      :title="$t('pages.instances.form.mediaInspectionPrompt')"
      :sections="summaryPromptPickerSections"
      :selected="form.botConfig.mediaInspectionPrompt ? [form.botConfig.mediaInspectionPrompt] : []"
      :empty-text="$t('pages.instances.form.summaryPromptPickerEmpty')"
      :no-results-text="$t('pages.instances.form.summaryPromptPickerNoMatches')"
      @update:selected="(vals) => { form.botConfig.mediaInspectionPrompt = vals[0] ?? '' }"
    />
    <generic-picker-dialog
      v-model="showStickerSummaryLlmPicker"
      :title="$t('pages.instances.form.stickerSummaryLlm')"
      :sections="mainLlmPickerSections"
      :selected="form.botConfig.stickerSummaryLlm ? [form.botConfig.stickerSummaryLlm] : []"
      :empty-text="$t('pages.modelRuntime.hints.modelIdPickerEmpty')"
      :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
      @update:selected="(vals) => { form.botConfig.stickerSummaryLlm = vals[0] ?? '' }"
    />
    <generic-picker-dialog
      v-model="showStickerSummaryPromptPicker"
      :title="$t('pages.instances.form.stickerSummaryPrompt')"
      :sections="summaryPromptPickerSections"
      :selected="form.botConfig.stickerSummaryPrompt ? [form.botConfig.stickerSummaryPrompt] : []"
      :empty-text="$t('pages.instances.form.summaryPromptPickerEmpty')"
      :no-results-text="$t('pages.instances.form.summaryPromptPickerNoMatches')"
      @update:selected="(vals) => { form.botConfig.stickerSummaryPrompt = vals[0] ?? '' }"
    />
    <generic-picker-dialog
      v-model="showContextCompressionLlmPicker"
      :title="$t('pages.instances.form.contextCompressionLlm')"
      :sections="mainLlmPickerSections"
      :selected="form.botConfig.contextCompressionLlm ? [form.botConfig.contextCompressionLlm] : []"
      :empty-text="$t('pages.modelRuntime.hints.modelIdPickerEmpty')"
      :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
      @update:selected="(vals) => { form.botConfig.contextCompressionLlm = vals[0] ?? '' }"
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
import { useModelRuntimeStore } from '@/stores/modelRuntime'
import InstanceCard from '@/components/InstanceCard.vue'
import SchemaForm from '@/components/SchemaForm.vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import KeyValueEditor from '@/components/model-runtime/KeyValueEditor.vue'
import GenericPickerDialog, {
  type GenericPickerSection,
} from '@/components/model-runtime/GenericPickerDialog.vue'
import { resolveProviderSource } from '@/utils/modelRuntimeSources'
import type { Instance, InstanceConfig, UpdateInstanceRequest } from '@/api/instances'
import type { PluginConfigSchema } from '@/api/plugins'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

const { t } = useI18n()
const instancesStore = useInstancesStore()
const pluginsStore = usePluginsStore()
const uiStore = useUiStore()
const modelRuntimeStore = useModelRuntimeStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
const dialogVisible = ref(false)
const dialogTitleKey = ref('pages.instances.dialog.createTitle')
const editingId = ref('')
const agents = ref<AgentSummary[]>([])
const botConfigs = ref<BotConfig[]>([])
const promptCatalog = ref<PromptCatalogItem[]>([])
const botConfigEntries = ref<Array<{ key: string; value: string }>>([])
const showMainLlmPicker = ref(false)
const showMediaInspectionLlmPicker = ref(false)
const showMediaInspectionPromptPicker = ref(false)
const showStickerSummaryLlmPicker = ref(false)
const showStickerSummaryPromptPicker = ref(false)
const showContextCompressionLlmPicker = ref(false)

const mainLlmPickerSections = computed<GenericPickerSection[]>(() => {
  const result: GenericPickerSection[] = []

  const routes = modelRuntimeStore.routes
  if (routes.length > 0) {
    result.push({
      id: 'routes',
      label: t('pages.modelRuntime.labels.routeTargets'),
      items: [...routes]
        .sort((a, b) => {
          if (a.enabled !== b.enabled) {
            return a.enabled ? -1 : 1
          }
          return a.id.localeCompare(b.id)
        })
        .map((route) => ({
          value: route.id,
          title: route.id,
          subtitle: route.purpose || route.strategy,
          icon: 'mdi-transit-connection-variant',
          iconColor: route.enabled ? 'primary' : 'surface-variant',
          tag: route.enabled
            ? t('pages.modelRuntime.labels.enabled')
            : t('pages.modelRuntime.labels.disabled'),
          tagColor: route.enabled ? 'primary' : 'default',
        })),
    })
  }

  const providerGroups = modelRuntimeStore.providers
    .map((provider) => {
      const items = new Map<string, { value: string; title: string; subtitle: string; kind: 'catalog' | 'configured' }>()

      for (const item of modelRuntimeStore.catalogItems[provider.id] || []) {
        const value = item.litellmModel.trim()
        if (!value) continue
        items.set(value, {
          value,
          title: item.displayName || item.id || value,
          subtitle: item.id && item.id !== value ? `${item.id} · ${value}` : value,
          kind: 'catalog',
        })
      }

      for (const model of modelRuntimeStore.modelsByProvider[provider.id] || []) {
        const value = model.litellmModel.trim()
        if (!value || items.has(value)) continue
        items.set(value, {
          value,
          title: model.displayName || model.id,
          subtitle: model.id !== value ? `${model.id} · ${value}` : value,
          kind: 'configured',
        })
      }

      return {
        id: provider.id,
        title: provider.displayName || provider.id,
        subtitle: resolveProviderSource(provider.type)?.label || provider.type,
        items: [...items.values()]
          .sort((a, b) => a.title.localeCompare(b.title))
          .map((item) => ({
            value: item.value,
            title: item.title,
            subtitle: item.subtitle,
            icon: 'mdi-cube-outline',
            iconColor: 'secondary',
            tag:
              item.kind === 'catalog'
                ? t('pages.modelRuntime.labels.catalog')
                : t('pages.modelRuntime.labels.configured'),
            tagColor: item.kind === 'catalog' ? 'info' : 'primary',
          })),
      }
    })
    .filter((group) => group.items.length > 0)
    .sort((a, b) => a.title.localeCompare(b.title))

  if (providerGroups.length > 0) {
    result.push({
      id: 'providers',
      label: t('pages.modelRuntime.sidebar.providers'),
      groups: providerGroups,
    })
  }

  return result
})

const summaryPromptPickerSections = computed<GenericPickerSection[]>(() => {
  const eligible = [...promptCatalog.value]
    .filter(
      (item) =>
        item.enabled &&
        (item.stage === 'system_base' || item.stage === 'identity' || item.type === 'bundle'),
    )
    .sort((a, b) => a.displayName.localeCompare(b.displayName))

  const builtinItems = eligible
    .filter((item) => item.sourceType === 'builtin_system')
    .map((item) => ({
      value: item.id,
      title: item.displayName || item.id,
      subtitle: item.description || item.id,
      icon: 'mdi-shield-star-outline',
      iconColor: 'primary',
      tag: t('pages.instances.form.promptBuiltinTag'),
      tagColor: 'primary',
    }))
  const customItems = eligible
    .filter((item) => item.sourceType !== 'builtin_system')
    .map((item) => ({
      value: item.id,
      title: item.displayName || item.id,
      subtitle: item.description || item.id,
      icon: 'mdi-text-box-outline',
      iconColor: 'secondary',
      tag: item.stage,
      tagColor: 'info',
    }))

  const sections: GenericPickerSection[] = []
  if (builtinItems.length > 0) {
    sections.push({
      id: 'builtin-prompts',
      label: t('pages.instances.form.builtinSummaryPrompts'),
      items: builtinItems,
    })
  }
  if (customItems.length > 0) {
    sections.push({
      id: 'custom-prompts',
      label: t('pages.instances.form.customSummaryPrompts'),
      items: customItems,
    })
  }
  return sections
})

const form = ref({
  name: '',
  adapterType: '',
  config: {} as Record<string, unknown>,
  botConfig: {
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
    tags: [] as string[],
  },
})

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
  () => adapterSchemaByPlatform.value[form.value.adapterType] ?? null
)

const agentOptions = computed(() => [
  { title: t('pages.instances.form.noDefaultAgent'), value: '' },
  ...agents.value.map((agent) => ({
    title: `${agent.name} (${agent.agentId})`,
    value: agent.uuid,
  })),
])

const tableHeaders = computed(() => [
  { title: t('pages.instances.table.name'), value: 'name', width: '20%' },
  { title: t('pages.instances.table.adapterType'), value: 'adapterType', width: '25%' },
  { title: t('pages.instances.table.status'), value: 'status', width: '15%' },
  { title: t('pages.instances.table.created'), value: 'createdAt', width: '20%' },
  { title: t('pages.instances.table.actions'), value: 'actions', width: '20%', sortable: false },
])

const tableRow = (item: unknown): Instance => {
  const row = item as { raw?: Instance }
  return row.raw ?? (item as Instance)
}

const filteredInstances = computed(() =>
  instancesStore.instances.filter((instance: (typeof instancesStore.instances)[number]) =>
    instance.name.toLowerCase().includes(searchQuery.value.toLowerCase())
  )
)

onMounted(() => {
  void Promise.all([
    instancesStore.fetchInstances(),
    pluginsStore.fetchPlugins(),
    fetchAgents(),
    fetchBotConfigs(),
    fetchPrompts(),
    modelRuntimeStore.routes.length === 0 ? modelRuntimeStore.fetchAll() : Promise.resolve(),
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
  }
)

const handleRefresh = async () => {
  await Promise.all([instancesStore.fetchInstances(), fetchAgents(), fetchBotConfigs(), fetchPrompts()])
}

const showCreateDialog = () => {
  editingId.value = ''
  dialogTitleKey.value = 'pages.instances.dialog.createTitle'
  const defaultAdapter = adapterOptions.value[0] ?? 'satori'
  form.value = {
    name: '',
    adapterType: defaultAdapter,
    config: {},
    botConfig: {
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
    },
  }
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
      defaultAgentUuid: currentBotConfig?.defaultAgentUuid ?? instance.botConfig?.defaultAgentUuid ?? '',
      mainLlm: currentBotConfig?.mainLlm ?? instance.botConfig?.mainLlm ?? '',
      explicitPromptCacheEnabled:
        currentBotConfig?.explicitPromptCacheEnabled ??
        instance.botConfig?.explicitPromptCacheEnabled ??
        false,
      mediaInspectionLlm: currentBotConfig?.mediaInspectionLlm ?? instance.botConfig?.mediaInspectionLlm ?? '',
      mediaInspectionPrompt:
        currentBotConfig?.mediaInspectionPrompt ?? instance.botConfig?.mediaInspectionPrompt ?? '',
      stickerSummaryLlm: currentBotConfig?.stickerSummaryLlm ?? instance.botConfig?.stickerSummaryLlm ?? '',
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
        currentBotConfig?.contextCompressionMaxChars ?? instance.botConfig?.contextCompressionMaxChars,
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
