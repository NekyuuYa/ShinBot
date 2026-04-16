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
          bg-color="white"
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
              text-color="white"
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
import { useInstancesStore } from '@/stores/instances'
import { usePluginsStore } from '@/stores/plugins'
import InstanceCard from '@/components/InstanceCard.vue'
import SchemaForm from '@/components/SchemaForm.vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import KeyValueEditor from '@/components/model-runtime/KeyValueEditor.vue'
import type { Instance, InstanceConfig, UpdateInstanceRequest } from '@/api/instances'
import type { PluginConfigSchema } from '@/api/plugins'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

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
const botConfigEntries = ref<Array<{ key: string; value: string }>>([])

const form = ref({
  name: '',
  adapterType: '',
  config: {} as Record<string, unknown>,
  botConfig: {
    uuid: '',
    defaultAgentUuid: '',
    mainLlm: '',
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
  await Promise.all([instancesStore.fetchInstances(), fetchAgents(), fetchBotConfigs()])
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

const saveBotConfig = async (instanceId: string) => {
  const payloadBase = {
    instanceId,
    defaultAgentUuid: form.value.botConfig.defaultAgentUuid,
    mainLlm: form.value.botConfig.mainLlm.trim(),
    config: entriesToObject(botConfigEntries.value),
    tags: form.value.botConfig.tags.map((tag) => tag.trim()).filter(Boolean),
  }
  const hasMeaningfulBotConfig =
    Boolean(payloadBase.defaultAgentUuid) ||
    Boolean(payloadBase.mainLlm) ||
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
</script>
