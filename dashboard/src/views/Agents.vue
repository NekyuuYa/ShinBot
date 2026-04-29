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
          :loading="agentsStore.isLoading || isLoadingResources"
          class="me-2"
          @click="refreshAgents"
        >
          {{ $t('pages.agents.actions.refresh') }}
        </v-btn>
        <v-btn color="primary" prepend-icon="mdi-account-plus" @click="openCreate">
          {{ $t('pages.agents.actions.addAgent') }}
        </v-btn>
      </template>
    </app-page-header>

    <dual-pane-list-view
      :items="filteredAgents"
      :loading="agentsStore.isLoading"
      :show-skeleton="agentsStore.isLoading && agentsStore.agents.length === 0"
      :empty-config="{
        icon: 'mdi-account-search-outline',
        title: $t('pages.agents.empty.title'),
        subtitle: $t('pages.agents.empty.subtitle'),
      }"
      :get-item-key="getAgentKey"
    >
      <template #sidebar>
        <sidebar-list-card
          :title="$t('pages.agents.tags.title')"
          :empty-text="$t('pages.agents.tags.empty')"
          :items="tagSidebarItems"
          :active-id="activeTag"
          :show-add-button="false"
          @select="selectTag"
        />
      </template>

      <template #card="{ item: agent }">
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
            <v-btn variant="text" prepend-icon="mdi-pencil" @click="openEdit(agent)">
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
      </template>
    </dual-pane-list-view>

    <v-alert v-if="agentsStore.error || resourceError" type="error" class="mt-4">
      {{ agentsStore.error || resourceError }}
    </v-alert>

    <v-dialog v-model="dialogVisible" max-width="860">
      <v-card>
        <v-card-title>
          {{ editingId ? $t('pages.agents.overlay.editTitle') : $t('pages.agents.overlay.createTitle') }}
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
                :loading="isLoadingResources"
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
                :loading="isLoadingResources"
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
                :items="contextStrategyOptionItems"
                item-title="title"
                item-value="value"
                variant="outlined"
                density="comfortable"
                clearable
                :loading="isLoadingResources"
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
          <v-btn color="primary" :loading="isSaving" @click="submit">
            {{ editingId ? $t('common.actions.action.save') : $t('common.actions.action.create') }}
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
import AppPageHeader from '@/components/AppPageHeader.vue'
import DualPaneListView from '@/components/DualPaneListView.vue'
import SidebarListCard from '@/components/SidebarListCard.vue'
import GenericPickerDialog, {
  type GenericPickerSection,
} from '@/components/model-runtime/GenericPickerDialog.vue'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useAgentResources } from '@/composables/useAgentResources'
import { useCrudDialog } from '@/composables/useCrudDialog'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { translate } from '@/plugins/i18n'
import { useAgentsStore } from '@/stores/agents'
import { usePersonasStore } from '@/stores/personas'
import { normalizeStringList, safeJsonParse, prettyJson } from '@/utils/format'

const { t } = useI18n()
const agentsStore = useAgentsStore()
const { confirm } = useConfirmDialog()
const personasStore = usePersonasStore()

// Resources and Logic Extraction
const {
  isLoadingResources,
  resourceError,
  fetchAllResources,
  contextStrategyOptions,
  promptOptions,
  toolOptions,
} = useAgentResources()

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

const contextStrategyType = ref('')
const showPromptPicker = ref(false)
const showToolPicker = ref(false)

const {
  visible: dialogVisible,
  editingId,
  localError,
  isSaving,
  openCreate,
  openEdit,
  submit,
} = useCrudDialog<Agent, AgentPayload>({
  resetForm: () => {
    Object.assign(form, {
      agentId: '',
      name: '',
      personaUuid: '',
      tags: [],
      prompts: [],
      tools: [],
      contextStrategyRef: '',
      contextStrategyParamsJson: '',
      configJson: '',
    })
    contextStrategyType.value = ''
  },
  populateForm: (agent) => {
    Object.assign(form, {
      agentId: agent.agentId,
      name: agent.name,
      personaUuid: agent.personaUuid,
      tags: [...agent.tags],
      prompts: [...agent.prompts],
      tools: [...agent.tools],
      contextStrategyRef: agent.contextStrategy?.ref || '',
      contextStrategyParamsJson: prettyJson(agent.contextStrategy?.params),
      configJson: prettyJson(agent.config),
    })
    contextStrategyType.value = agent.contextStrategy?.type || ''
  },
  buildPayload: () => {
    if (!form.agentId.trim() || !form.name.trim() || !form.personaUuid.trim()) {
      throw new Error(translate('pages.agents.messages.requiredFields'))
    }
    return {
      agentId: form.agentId.trim(),
      name: form.name.trim(),
      personaUuid: form.personaUuid.trim(),
      prompts: normalizeStringList(form.prompts),
      tools: normalizeStringList(form.tools),
      contextStrategy: {
        ref: form.contextStrategyRef.trim(),
        type: contextStrategyType.value.trim(),
        params: safeJsonParse(form.contextStrategyParamsJson),
      },
      config: safeJsonParse(form.configJson),
      tags: normalizeStringList(form.tags),
    }
  },
  save: async (payload, id) => {
    const res = id ? await agentsStore.updateAgent(id, payload) : await agentsStore.createAgent(payload)
    return Boolean(res)
  },
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
  personasStore.personas.map((p) => ({ title: `${p.name} (${p.uuid})`, value: p.uuid }))
)

const getAgentKey = (agent: Agent) => agent.uuid

const contextStrategyOptionItems = computed(() =>
  contextStrategyOptions(form.contextStrategyRef, contextStrategyType.value)
)

const promptOptionItems = computed(() => promptOptions(form.prompts))

const toolOptionItems = computed(() => toolOptions(form.tools))

const promptPickerSections = computed<GenericPickerSection[]>(() => [
  {
    id: 'prompts',
    label: t('pages.agents.fields.prompts'),
    items: promptOptionItems.value.map((o) => ({
      value: o.value, title: o.title, icon: 'mdi-text-box-outline', iconColor: 'primary',
    })),
  },
])

const toolPickerSections = computed<GenericPickerSection[]>(() => [
  {
    id: 'tools',
    label: t('pages.agents.fields.tools'),
    items: toolOptionItems.value.map((o) => ({
      value: o.value, title: o.title, icon: 'mdi-tools', iconColor: 'secondary',
    })),
  },
])

const promptSummary = computed(() => {
  if (form.prompts.length === 0) return ''
  const first = promptOptionItems.value.find((o) => o.value === form.prompts[0])?.title ?? form.prompts[0]
  return form.prompts.length === 1 ? first : `${first} (+${form.prompts.length - 1})`
})

const toolSummary = computed(() => {
  if (form.tools.length === 0) return ''
  const first = toolOptionItems.value.find((o) => o.value === form.tools[0])?.title ?? form.tools[0]
  return form.tools.length === 1 ? first : `${first} (+${form.tools.length - 1})`
})

const handleContextStrategyChange = (value: string | null) => {
  const refVal = (value ?? '').trim()
  form.contextStrategyRef = refVal
  const selected = contextStrategyOptionItems.value.find(o => o.value === refVal)
  contextStrategyType.value = selected?.type || ''
}

const removeAgent = async (uuid: string, name: string) => {
  if (
    await confirm({
      title: translate('common.actions.action.delete'),
      message: translate('pages.agents.messages.confirmDelete', { name }),
      confirmText: translate('common.actions.action.delete'),
      confirmColor: 'error',
      icon: 'mdi-alert-outline',
      iconColor: 'error',
    })
  ) {
    await agentsStore.deleteAgent(uuid)
  }
}

const refreshAgents = () => Promise.all([agentsStore.fetchAgents(), fetchAllResources()])

onMounted(() => {
  agentsStore.fetchAgents()
  personasStore.fetchPersonas()
  fetchAllResources()
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.agent-card {
  @include surface-card;
  @include hover-lift;
}
</style>
