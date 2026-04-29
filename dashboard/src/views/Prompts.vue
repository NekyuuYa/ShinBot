<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.prompts.title')"
      :subtitle="$t('pages.prompts.subtitle')"
      :kicker="$t('pages.prompts.kicker')"
    >
      <template #actions>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="store.isLoading"
          class="me-2"
          @click="refreshItems"
        >
          {{ $t('pages.prompts.actions.refresh') }}
        </v-btn>
        <v-btn color="primary" prepend-icon="mdi-plus" @click="openCreate">
          {{ $t('pages.prompts.actions.addPrompt') }}
        </v-btn>
      </template>
    </app-page-header>

    <dual-pane-list-view
      :items="filteredItems"
      :loading="store.isLoading"
      :show-skeleton="store.isLoading && store.items.length === 0"
      :empty-config="{
        icon: 'mdi-text-box-search-outline',
        title: $t('pages.prompts.empty.title'),
        subtitle: $t('pages.prompts.empty.subtitle'),
      }"
      :get-item-key="getPromptKey"
    >
      <template #sidebar>
        <sidebar-list-card
          :title="$t('pages.prompts.tags.title')"
          :empty-text="$t('pages.prompts.tags.empty')"
          :items="tagSidebarItems"
          :active-id="activeTag"
          :show-add-button="false"
          @select="selectTag"
        />
      </template>

      <template #card="{ item }">
        <v-card class="prompt-card h-100 d-flex flex-column" elevation="0">
          <v-card-item>
            <template #prepend>
              <v-avatar color="primary" variant="tonal" icon="mdi-text-box-outline" />
            </template>
            <v-card-title class="text-break">{{ item.name }}</v-card-title>
            <v-card-subtitle>{{ item.promptId }}</v-card-subtitle>
            <template #append>
              <v-switch
                :model-value="item.enabled"
                color="success"
                density="compact"
                hide-details
                @update:model-value="(val: boolean | null) => store.toggleEnabled(item.uuid, Boolean(val))"
              />
            </template>
          </v-card-item>

          <v-card-text class="pt-1 flex-grow-1">
            <div v-if="item.description" class="text-body-2 text-medium-emphasis mb-2">
              {{ item.description }}
            </div>
            <div class="d-flex flex-wrap ga-2 mb-2">
              <v-chip size="small" color="info" variant="tonal">
                {{ $t(`pages.prompts.stages.${item.stage}`, item.stage) }}
              </v-chip>
              <v-chip size="small" variant="tonal">
                {{ $t(`pages.prompts.kinds.${item.type}`, item.type) }}
              </v-chip>
              <v-chip size="small" variant="outlined">
                v{{ item.version }}
              </v-chip>
              <v-chip size="small" variant="outlined">
                P{{ item.priority }}
              </v-chip>
            </div>
            <div class="d-flex flex-wrap ga-2">
              <v-chip
                v-for="tag in item.tags"
                :key="`${item.uuid}-${tag}`"
                size="small"
                color="secondary"
                variant="tonal"
              >
                {{ tag }}
              </v-chip>
              <v-chip
                v-if="item.tags.length === 0"
                size="small"
                color="grey"
                variant="tonal"
              >
                {{ $t('pages.prompts.tags.untagged') }}
              </v-chip>
            </div>
          </v-card-text>

          <v-card-actions>
            <v-btn variant="text" prepend-icon="mdi-pencil" @click="openEdit(item)">
              {{ $t('common.actions.action.edit') }}
            </v-btn>
            <v-spacer />
            <v-btn
              color="error"
              variant="text"
              prepend-icon="mdi-delete-outline"
              @click="removeItem(item.uuid, item.name)"
            >
              {{ $t('common.actions.action.delete') }}
            </v-btn>
          </v-card-actions>
        </v-card>
      </template>
    </dual-pane-list-view>

    <!-- Create / Edit dialog -->
    <v-dialog v-model="dialogVisible" max-width="960">
      <v-card>
        <v-card-title>
          {{ editingId ? $t('pages.prompts.overlay.editTitle') : $t('pages.prompts.overlay.createTitle') }}
        </v-card-title>
        <v-card-text>
          <v-row>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.promptId"
                :label="$t('pages.prompts.fields.promptId')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-text-field
                v-model="form.name"
                :label="$t('pages.prompts.fields.name')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-select
                v-model="form.stage"
                :label="$t('pages.prompts.fields.stage')"
                :items="stageOptions"
                item-title="title"
                item-value="value"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-select
                v-model="form.type"
                :label="$t('pages.prompts.fields.type')"
                :items="kindOptions"
                item-title="title"
                item-value="value"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                v-model.number="form.priority"
                :label="$t('pages.prompts.fields.priority')"
                type="number"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-text-field
                v-model="form.version"
                :label="$t('pages.prompts.fields.version')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="4">
              <v-switch
                v-model="form.enabled"
                :label="$t('pages.prompts.fields.enabled')"
                color="success"
                density="comfortable"
                hide-details
              />
            </v-col>
            <v-col cols="12">
              <v-text-field
                v-model="form.description"
                :label="$t('pages.prompts.fields.description')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12">
              <v-textarea
                v-model="form.content"
                :label="$t('pages.prompts.fields.content')"
                :hint="$t('pages.prompts.hints.content')"
                persistent-hint
                rows="5"
                variant="outlined"
              />
            </v-col>
            <v-col v-if="form.type === 'template'" cols="12">
              <v-combobox
                v-model="form.templateVars"
                multiple
                chips
                closable-chips
                :label="$t('pages.prompts.fields.templateVars')"
                :hint="$t('pages.prompts.hints.templateVars')"
                persistent-hint
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col v-if="form.type === 'resolver'" cols="12" md="6">
              <v-text-field
                v-model="form.resolverRef"
                :label="$t('pages.prompts.fields.resolverRef')"
                :hint="$t('pages.prompts.hints.resolverRef')"
                persistent-hint
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col v-if="form.type === 'bundle'" cols="12">
              <v-combobox
                v-model="form.bundleRefs"
                multiple
                chips
                closable-chips
                :label="$t('pages.prompts.fields.bundleRefs')"
                :hint="$t('pages.prompts.hints.bundleRefs')"
                persistent-hint
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12">
              <v-combobox
                v-model="form.tags"
                multiple
                chips
                closable-chips
                hide-selected
                clearable
                :label="$t('pages.prompts.fields.tags')"
                :items="tagOptions"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-textarea
                v-model="form.configJson"
                :label="$t('pages.prompts.fields.config')"
                :hint="$t('pages.prompts.hints.config')"
                persistent-hint
                rows="3"
                variant="outlined"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-textarea
                v-model="form.metadataJson"
                :label="$t('pages.prompts.fields.metadata')"
                :hint="$t('pages.prompts.hints.metadata')"
                persistent-hint
                rows="3"
                variant="outlined"
              />
            </v-col>
          </v-row>

          <v-alert v-if="error" type="error" class="mt-2">
            {{ error }}
          </v-alert>
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="dialogVisible = false">
            {{ $t('common.actions.action.cancel') }}
          </v-btn>
          <v-btn color="primary" :loading="store.isSaving" @click="submit">
            {{ editingId ? $t('common.actions.action.save') : $t('common.actions.action.create') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive } from 'vue'
import type { PromptDefinition, PromptDefinitionPayload } from '@/api/promptDefinitions'
import AppPageHeader from '@/components/AppPageHeader.vue'
import DualPaneListView from '@/components/DualPaneListView.vue'
import SidebarListCard from '@/components/SidebarListCard.vue'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { useCrudDialog } from '@/composables/useCrudDialog'
import { translate } from '@/plugins/i18n'
import { usePromptDefinitionsStore } from '@/stores/promptDefinitions'
import { normalizeStringList, safeJsonParse, prettyJson } from '@/utils/format'

const store = usePromptDefinitionsStore()

const form = reactive({
  promptId: '',
  name: '',
  stage: 'system_base',
  type: 'static_text',
  priority: 100,
  version: '1.0.0',
  description: '',
  enabled: true,
  content: '',
  templateVars: [] as string[],
  resolverRef: '',
  bundleRefs: [] as string[],
  tags: [] as string[],
  configJson: '',
  metadataJson: '',
})

const {
  activeTag,
  allTags: tagOptions,
  sidebarItems: tagSidebarItems,
  filteredItems,
  selectTag,
} = useTagSidebar(
  () => store.items,
  {
    getTags: (item) => item.tags,
    allTitle: translate('pages.prompts.tags.all'),
    allSubtitle: translate('pages.prompts.tags.showAll'),
    tagSubtitle: translate('pages.prompts.tags.filterByTag'),
  }
)

const {
  visible: dialogVisible,
  editingId,
  localError: error,
  openCreate,
  openEdit,
  submit,
} = useCrudDialog<PromptDefinition, PromptDefinitionPayload>({
  resetForm: () => {
    Object.assign(form, {
      promptId: '',
      name: '',
      stage: 'system_base',
      type: 'static_text',
      priority: 100,
      version: '1.0.0',
      description: '',
      enabled: true,
      content: '',
      templateVars: [],
      resolverRef: '',
      bundleRefs: [],
      tags: [],
      configJson: '',
      metadataJson: '',
    })
  },
  populateForm: (item) => {
    Object.assign(form, {
      promptId: item.promptId,
      name: item.name,
      stage: item.stage,
      type: item.type,
      priority: item.priority,
      version: item.version,
      description: item.description,
      enabled: item.enabled,
      content: item.content,
      templateVars: [...item.templateVars],
      resolverRef: item.resolverRef,
      bundleRefs: [...item.bundleRefs],
      tags: [...item.tags],
      configJson: prettyJson(item.config),
      metadataJson: prettyJson(item.metadata),
    })
  },
  buildPayload: () => {
    const promptId = form.promptId.trim()
    const name = form.name.trim()

    if (!promptId || !name) {
      throw new Error(translate('pages.prompts.messages.requiredFields'))
    }

    return {
      promptId,
      name,
      stage: form.stage as any,
      type: form.type as any,
      priority: form.priority,
      version: form.version.trim() || '1.0.0',
      description: form.description.trim(),
      enabled: form.enabled,
      content: form.content,
      templateVars: normalizeStringList(form.templateVars),
      resolverRef: form.resolverRef.trim(),
      bundleRefs: normalizeStringList(form.bundleRefs),
      tags: normalizeStringList(form.tags),
      config: safeJsonParse(form.configJson, {}),
      metadata: safeJsonParse(form.metadataJson, {}),
    }
  },
  save: async (payload): Promise<boolean> => {
    const res = editingId.value
      ? await store.updateItem(editingId.value, payload)
      : await store.createItem(payload)
    return Boolean(res)
  },
})

const stageOptions = computed(() => [
  { title: translate('pages.prompts.stages.system_base'), value: 'system_base' },
  { title: translate('pages.prompts.stages.identity'), value: 'identity' },
  { title: translate('pages.prompts.stages.context'), value: 'context' },
  { title: translate('pages.prompts.stages.abilities'), value: 'abilities' },
  { title: translate('pages.prompts.stages.compatibility'), value: 'compatibility' },
  { title: translate('pages.prompts.stages.instructions'), value: 'instructions' },
  { title: translate('pages.prompts.stages.constraints'), value: 'constraints' },
])

const kindOptions = computed(() => [
  { title: translate('pages.prompts.kinds.static_text'), value: 'static_text' },
  { title: translate('pages.prompts.kinds.template'), value: 'template' },
  { title: translate('pages.prompts.kinds.resolver'), value: 'resolver' },
  { title: translate('pages.prompts.kinds.bundle'), value: 'bundle' },
  { title: translate('pages.prompts.kinds.external_injection'), value: 'external_injection' },
])

const getPromptKey = (item: PromptDefinition) => item.uuid

const removeItem = async (uuid: string, name: string) => {
  if (!confirm(translate('pages.prompts.messages.confirmDelete', { name }))) return
  await store.deleteItem(uuid)
}

const refreshItems = () => store.fetchItems()

onMounted(() => store.fetchItems())
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.prompt-card {
  @include surface-card;
  @include hover-lift;
}
</style>
