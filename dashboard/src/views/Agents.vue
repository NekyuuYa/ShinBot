<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.agents.title')"
      :subtitle="$t('pages.agents.subtitle')"
      :kicker="$t('pages.agents.kicker')"
    >
      <template #actions>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="agentsStore.isLoading"
          class="me-2"
          @click="refreshAgents"
        >
          {{ $t('pages.agents.actions.refresh') }}
        </v-btn>
        <v-btn color="primary" prepend-icon="mdi-account-plus" @click="openCreateAgent">
          {{ $t('pages.agents.actions.addAgent') }}
        </v-btn>
      </template>
    </app-page-header>

    <div class="agents-layout">
      <div class="agents-tag-pane">
        <sidebar-list-card
          :title="$t('pages.agents.tags.title')"
          :empty-text="$t('pages.agents.tags.empty')"
          :items="tagSidebarItems"
          :active-id="activeTag"
          :show-add-button="false"
          @select="selectTag"
        />
      </div>

      <div class="agents-content-pane">
        <v-row v-if="agentsStore.isLoading && agentsStore.agents.length === 0" class="mx-0">
          <v-col cols="12" class="pa-0">
            <v-skeleton-loader type="card, card, card" />
          </v-col>
        </v-row>

        <v-row v-else-if="filteredAgents.length === 0" justify="center" class="mx-0 py-12">
          <v-col cols="12" md="8" class="text-center pa-0">
            <v-icon size="96" color="grey-lighten-1" icon="mdi-account-search-outline" />
            <h3 class="text-h6 my-4">{{ $t('pages.agents.empty.title') }}</h3>
            <p class="text-body-2 text-medium-emphasis">{{ $t('pages.agents.empty.subtitle') }}</p>
          </v-col>
        </v-row>

        <v-row v-else class="mx-n4">
          <v-col
            v-for="agent in filteredAgents"
            :key="agent.uuid"
            cols="12"
            sm="6"
            md="6"
            lg="4"
            class="pa-4"
          >
            <v-card class="agent-card h-100 d-flex flex-column" elevation="0">
              <v-card-item>
                <template #prepend>
                  <v-avatar color="primary" variant="tonal" icon="mdi-account-cog-outline" />
                </template>
                <v-card-title class="text-break">
                  {{ agent.name }}
                </v-card-title>
                <v-card-subtitle>{{ agent.agentId }}</v-card-subtitle>
              </v-card-item>

              <v-card-text class="pt-1 flex-grow-1">
                <div class="text-caption text-medium-emphasis mb-2">
                  {{ $t('pages.agents.fields.personaUuid') }}: {{ agent.personaUuid }}
                </div>

                <div class="d-flex flex-wrap ga-2">
                  <v-chip
                    v-for="tag in agent.tags"
                    :key="`${agent.uuid}-${tag}`"
                    size="small"
                    color="secondary"
                    variant="tonal"
                  >
                    {{ tag }}
                  </v-chip>
                  <v-chip
                    v-if="agent.tags.length === 0"
                    size="small"
                    color="grey"
                    variant="tonal"
                  >
                    {{ $t('pages.agents.tags.untagged') }}
                  </v-chip>
                </div>
              </v-card-text>

              <v-card-actions>
                <v-btn variant="text" prepend-icon="mdi-pencil" @click="openEditAgent(agent)">
                  {{ $t('common.actions.action.edit') }}
                </v-btn>
                <v-spacer />
                <v-btn
                  color="error"
                  variant="text"
                  prepend-icon="mdi-delete-outline"
                  @click="removeAgent(agent.uuid, agent.name)"
                >
                  {{ $t('common.actions.action.delete') }}
                </v-btn>
              </v-card-actions>
            </v-card>
          </v-col>
        </v-row>
      </div>
    </div>

    <v-alert v-if="agentsStore.error" type="error" class="mt-4">
      {{ agentsStore.error }}
    </v-alert>

    <v-dialog v-model="dialogVisible" max-width="860">
      <v-card>
        <v-card-title>
          {{ editingAgentUuid ? $t('pages.agents.overlay.editTitle') : $t('pages.agents.overlay.createTitle') }}
        </v-card-title>
        <v-card-text>
          <v-row>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.agentId"
                :label="$t('pages.agents.fields.agentId')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.name"
                :label="$t('pages.agents.fields.name')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12">
              <v-select
                v-model="form.personaUuid"
                :label="$t('pages.agents.fields.persona')"
                :items="personaOptions"
                item-title="title"
                item-value="value"
                variant="outlined"
                density="comfortable"
                clearable
                :placeholder="$t('pages.agents.fields.personaPlaceholder')"
                :no-data-text="$t('pages.agents.fields.personaEmpty')"
              />
            </v-col>

            <v-col cols="12" md="4">
              <v-combobox
                v-model="form.tags"
                multiple
                chips
                closable-chips
                hide-selected
                clearable
                :label="$t('pages.agents.fields.tags')"
                :items="tagOptions"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                :model-value="promptSummary"
                :label="$t('pages.agents.fields.prompts')"
                :placeholder="$t('pages.agents.fields.promptsEmpty')"
                :loading="isLoadingPrompts"
                :clearable="form.prompts.length > 0"
                readonly
                variant="outlined"
                density="comfortable"
                append-inner-icon="mdi-menu-open"
                @click="showPromptPicker = true"
                @click:append-inner="showPromptPicker = true"
                @click:clear.stop="form.prompts = []"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                :model-value="toolSummary"
                :label="$t('pages.agents.fields.tools')"
                :placeholder="$t('pages.agents.fields.toolsEmpty')"
                :loading="isLoadingTools"
                :clearable="form.tools.length > 0"
                readonly
                variant="outlined"
                density="comfortable"
                append-inner-icon="mdi-menu-open"
                @click="showToolPicker = true"
                @click:append-inner="showToolPicker = true"
                @click:clear.stop="form.tools = []"
              />
            </v-col>

            <v-col cols="12" md="12">
              <v-select
                :model-value="form.contextStrategyRef"
                :label="$t('pages.agents.fields.contextStrategyRef')"
                :items="contextStrategyOptions"
                item-title="title"
                item-value="value"
                variant="outlined"
                density="comfortable"
                clearable
                :loading="isLoadingContextStrategies"
                :placeholder="$t('pages.agents.fields.contextStrategyPlaceholder')"
                :no-data-text="$t('pages.agents.fields.contextStrategyEmpty')"
                @update:model-value="handleContextStrategyChange"
              />
            </v-col>
            <v-col cols="12">
              <v-textarea
                v-model="form.contextStrategyParamsJson"
                :label="$t('pages.agents.fields.contextStrategyParams')"
                :hint="$t('pages.agents.hints.contextStrategyParams')"
                persistent-hint
                rows="3"
                variant="outlined"
              />
            </v-col>

            <v-col cols="12">
              <v-textarea
                v-model="form.configJson"
                :label="$t('pages.agents.fields.config')"
                :hint="$t('pages.agents.hints.config')"
                persistent-hint
                rows="4"
                variant="outlined"
              />
            </v-col>
          </v-row>

          <v-alert v-if="localError || agentsStore.error" type="error" class="mt-2">
            {{ localError || agentsStore.error }}
          </v-alert>
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="dialogVisible = false">
            {{ $t('common.actions.action.cancel') }}
          </v-btn>
          <v-btn color="primary" :loading="agentsStore.isSaving" @click="saveAgent">
            {{ editingAgentUuid ? $t('common.actions.action.save') : $t('common.actions.action.create') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <generic-picker-dialog
      v-model="showPromptPicker"
      :title="$t('pages.agents.fields.prompts')"
      :sections="promptPickerSections"
      :selected="form.prompts"
      :empty-text="$t('pages.agents.fields.promptsEmpty')"
      :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
      multiple
      @update:selected="(vals) => { form.prompts = vals }"
    />

    <generic-picker-dialog
      v-model="showToolPicker"
      :title="$t('pages.agents.fields.tools')"
      :sections="toolPickerSections"
      :selected="form.tools"
      :empty-text="$t('pages.agents.fields.toolsEmpty')"
      :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
      multiple
      @update:selected="(vals) => { form.tools = vals }"
    />
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { Agent, AgentPayload } from '@/api/agents'
import { contextStrategiesApi, type ContextStrategy } from '@/api/contextStrategies'
import { promptsApi, type PromptCatalogItem } from '@/api/prompts'
import { toolsApi, type ToolDefinition } from '@/api/tools'
import AppPageHeader from '@/components/AppPageHeader.vue'
import SidebarListCard from '@/components/model-runtime/SidebarListCard.vue'
import GenericPickerDialog, {
  type GenericPickerSection,
} from '@/components/model-runtime/GenericPickerDialog.vue'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { translate } from '@/plugins/i18n'
import { useAgentsStore } from '@/stores/agents'
import { usePersonasStore } from '@/stores/personas'
import { normalizeStringList } from '@/utils/stringList'

const { t } = useI18n()
const agentsStore = useAgentsStore()
const personasStore = usePersonasStore()

const dialogVisible = ref(false)
const editingAgentUuid = ref('')
const localError = ref('')
const contextStrategies = ref<ContextStrategy[]>([])
const isLoadingContextStrategies = ref(false)
const contextStrategyType = ref('')
const promptCatalog = ref<PromptCatalogItem[]>([])
const isLoadingPrompts = ref(false)
const toolCatalog = ref<ToolDefinition[]>([])
const isLoadingTools = ref(false)
const showPromptPicker = ref(false)
const showToolPicker = ref(false)

const form = reactive({
  agentId: '',
  name: '',
  personaUuid: '',
  tags: [] as string[],
  prompts: [] as string[],
  tools: [] as string[],
  contextStrategyRef: '',
  contextStrategyParamsJson: '',
  configJson: '',
})

const {
  activeTag,
  allTags: tagOptions,
  sidebarItems: tagSidebarItems,
  filteredItems: filteredAgents,
  selectTag,
} = useTagSidebar(
  () => agentsStore.agents,
  {
    getTags: (agent) => agent.tags,
    allTitle: translate('pages.agents.tags.all'),
    allSubtitle: translate('pages.agents.tags.showAll'),
    tagSubtitle: translate('pages.agents.tags.filterByTag'),
  }
)

const personaOptions = computed(() =>
  personasStore.personas.map((persona) => ({
    title: `${persona.name} (${persona.uuid})`,
    value: persona.uuid,
  }))
)

const contextStrategyOptions = computed(() => {
  const options = [...contextStrategies.value]
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((strategy) => ({
      title: `${strategy.name} (${strategy.type})`,
      value: strategy.uuid,
      type: strategy.type,
    }))

  if (
    form.contextStrategyRef
    && !options.some((option) => option.value === form.contextStrategyRef)
  ) {
    options.push({
      title: form.contextStrategyRef,
      value: form.contextStrategyRef,
      type: contextStrategyType.value,
    })
  }

  return options
})

const promptOptions = computed(() => {
  const options = promptCatalog.value
    .map((item) => ({
      title: `${item.displayName} (${item.id})`,
      value: item.id,
    }))
    .sort((a, b) => a.title.localeCompare(b.title))

  // Include any values already in the form that aren't in catalog
  for (const id of form.prompts) {
    if (!options.some((o) => o.value === id)) {
      options.push({ title: id, value: id })
    }
  }
  return options
})

const toolOptions = computed(() => {
  const options = toolCatalog.value
    .map((item) => ({
      title: `${item.displayName || item.name} (${item.id})`,
      value: item.id,
    }))
    .sort((a, b) => a.title.localeCompare(b.title))

  for (const id of form.tools) {
    if (!options.some((o) => o.value === id)) {
      options.push({ title: id, value: id })
    }
  }
  return options
})

const promptPickerSections = computed<GenericPickerSection[]>(() => [
  {
    id: 'prompts',
    label: t('pages.agents.fields.prompts'),
    items: promptOptions.value.map((opt) => ({
      value: opt.value,
      title: opt.title,
      icon: 'mdi-text-box-outline',
      iconColor: 'primary',
    })),
  },
])

const toolPickerSections = computed<GenericPickerSection[]>(() => [
  {
    id: 'tools',
    label: t('pages.agents.fields.tools'),
    items: toolOptions.value.map((opt) => ({
      value: opt.value,
      title: opt.title,
      icon: 'mdi-tools',
      iconColor: 'secondary',
    })),
  },
])

const promptSummary = computed(() => {
  if (form.prompts.length === 0) return ''
  const first = promptOptions.value.find((o) => o.value === form.prompts[0])?.title ?? form.prompts[0]
  return form.prompts.length === 1 ? first : `${first} (+${form.prompts.length - 1})`
})

const toolSummary = computed(() => {
  if (form.tools.length === 0) return ''
  const first = toolOptions.value.find((o) => o.value === form.tools[0])?.title ?? form.tools[0]
  return form.tools.length === 1 ? first : `${first} (+${form.tools.length - 1})`
})

const syncContextStrategyType = (strategyRef: string) => {
  const selected = contextStrategyOptions.value.find((option) => option.value === strategyRef)
  if (selected) {
    contextStrategyType.value = selected.type
    return
  }
  if (!strategyRef) {
    contextStrategyType.value = ''
  }
}

const handleContextStrategyChange = (value: string | null) => {
  const strategyRef = (value ?? '').trim()
  form.contextStrategyRef = strategyRef
  syncContextStrategyType(strategyRef)
}

const fetchContextStrategies = async () => {
  isLoadingContextStrategies.value = true
  try {
    const response = await contextStrategiesApi.list()
    if (response.data.success && response.data.data) {
      contextStrategies.value = response.data.data
      syncContextStrategyType(form.contextStrategyRef.trim())
      return
    }
    throw new Error(
      response.data.error?.message || translate('pages.agents.messages.loadContextStrategiesFailed')
    )
  } catch (errorDetail: unknown) {
    localError.value = errorDetail instanceof Error
      ? errorDetail.message
      : String(errorDetail)
  } finally {
    isLoadingContextStrategies.value = false
  }
}

const fetchPromptCatalog = async () => {
  isLoadingPrompts.value = true
  try {
    const response = await promptsApi.list()
    if (response.data.success && response.data.data) {
      promptCatalog.value = response.data.data
      return
    }
  } catch {
    // Non-critical: prompts field falls back to manual entry
  } finally {
    isLoadingPrompts.value = false
  }
}

const fetchToolCatalog = async () => {
  isLoadingTools.value = true
  try {
    const response = await toolsApi.list()
    if (response.data.success && response.data.data) {
      toolCatalog.value = response.data.data
      return
    }
  } catch {
    // Non-critical: tools field falls back to manual entry
  } finally {
    isLoadingTools.value = false
  }
}

const parseJsonObject = (value: string, emptyFallback: Record<string, unknown>) => {
  const trimmed = value.trim()
  if (!trimmed) {
    return emptyFallback
  }

  try {
    const parsed = JSON.parse(trimmed)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>
    }
  } catch {
    throw new Error(translate('pages.agents.messages.invalidJson'))
  }

  throw new Error(translate('pages.agents.messages.invalidJson'))
}

const resetForm = () => {
  form.agentId = ''
  form.name = ''
  form.personaUuid = ''
  form.tags = []
  form.prompts = []
  form.tools = []
  form.contextStrategyRef = ''
  contextStrategyType.value = ''
  form.contextStrategyParamsJson = ''
  form.configJson = ''
}

const openCreateAgent = () => {
  editingAgentUuid.value = ''
  localError.value = ''
  resetForm()
  dialogVisible.value = true
}

const openEditAgent = (agent: Agent) => {
  editingAgentUuid.value = agent.uuid
  localError.value = ''
  form.agentId = agent.agentId
  form.name = agent.name
  form.personaUuid = agent.personaUuid
  form.tags = [...agent.tags]
  form.prompts = [...agent.prompts]
  form.tools = [...agent.tools]
  form.contextStrategyRef = agent.contextStrategy?.ref || ''
  contextStrategyType.value = agent.contextStrategy?.type || ''
  syncContextStrategyType(form.contextStrategyRef)
  form.contextStrategyParamsJson = agent.contextStrategy?.params
    ? JSON.stringify(agent.contextStrategy.params, null, 2)
    : ''
  form.configJson = agent.config ? JSON.stringify(agent.config, null, 2) : ''
  dialogVisible.value = true
}

const buildPayload = (): AgentPayload => {
  const agentId = form.agentId.trim()
  const name = form.name.trim()
  const personaUuid = form.personaUuid.trim()

  if (!agentId || !name || !personaUuid) {
    throw new Error(translate('pages.agents.messages.requiredFields'))
  }

  const strategyRef = form.contextStrategyRef.trim()
  const strategyType = contextStrategyType.value.trim()
  const strategyParams = parseJsonObject(form.contextStrategyParamsJson, {})

  if ((strategyRef && !strategyType) || (!strategyRef && strategyType)) {
    throw new Error(translate('pages.agents.messages.invalidContextStrategy'))
  }

  return {
    agentId,
    name,
    personaUuid,
    prompts: normalizeStringList(form.prompts),
    tools: normalizeStringList(form.tools),
    contextStrategy: {
      ref: strategyRef,
      type: strategyType,
      params: strategyParams,
    },
    config: parseJsonObject(form.configJson, {}),
    tags: normalizeStringList(form.tags),
  }
}

const saveAgent = async () => {
  localError.value = ''

  try {
    const payload = buildPayload()
    const result = editingAgentUuid.value
      ? await agentsStore.updateAgent(editingAgentUuid.value, payload)
      : await agentsStore.createAgent(payload)

    if (result) {
      dialogVisible.value = false
    }
  } catch (errorDetail: unknown) {
    localError.value = errorDetail instanceof Error
      ? errorDetail.message
      : String(errorDetail)
  }
}

const removeAgent = async (uuid: string, name: string) => {
  if (!confirm(translate('pages.agents.messages.confirmDelete', { name }))) {
    return
  }

  await agentsStore.deleteAgent(uuid)
}

const refreshAgents = async () => {
  await Promise.all([
    agentsStore.fetchAgents(),
    fetchContextStrategies(),
    fetchPromptCatalog(),
    fetchToolCatalog(),
  ])
}

onMounted(() => {
  agentsStore.fetchAgents()
  personasStore.fetchPersonas()
  fetchContextStrategies()
  fetchPromptCatalog()
  fetchToolCatalog()
})
</script>

<style scoped>
.agents-layout {
  display: flex;
  align-items: flex-start;
  gap: 16px;
}

.agents-tag-pane {
  flex: 0 0 300px;
  width: 300px;
  max-width: 300px;
}

.agents-content-pane {
  flex: 1 1 auto;
  min-width: 0;
}

.agent-card {
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  border-radius: 20px;
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.98) 0%, rgba(var(--v-theme-background), 0.98) 100%);
}

@media (max-width: 960px) {
  .agents-layout {
    flex-direction: column;
  }

  .agents-tag-pane {
    flex: 1 1 auto;
    width: 100%;
    max-width: none;
  }
}
</style>
