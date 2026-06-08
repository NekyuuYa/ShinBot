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

    <v-sheet class="prompt-filters px-4 py-3" color="surface" elevation="0">
      <v-row dense>
        <v-col cols="12" md="3">
          <v-select
            v-model="filters.layer"
            :label="$t('pages.prompts.filters.source')"
            :items="layerOptions"
            item-title="title"
            item-value="value"
            variant="outlined"
            density="compact"
            hide-details
          />
        </v-col>
        <v-col cols="12" md="3">
          <v-select
            v-model="filters.locale"
            :label="$t('pages.prompts.filters.locale')"
            :items="localeOptions"
            item-title="title"
            item-value="value"
            variant="outlined"
            density="compact"
            hide-details
          />
        </v-col>
        <v-col cols="12" md="3">
          <v-select
            v-model="filters.stage"
            :label="$t('pages.prompts.filters.stage')"
            :items="stageFilterOptions"
            item-title="title"
            item-value="value"
            variant="outlined"
            density="compact"
            hide-details
          />
        </v-col>
        <v-col cols="12" md="3">
          <v-select
            v-model="filters.tag"
            :label="$t('pages.prompts.filters.tags')"
            :items="tagFilterOptions"
            item-title="title"
            item-value="value"
            variant="outlined"
            density="compact"
            hide-details
          />
        </v-col>
      </v-row>
    </v-sheet>

    <dual-pane-list-view
      :items="filteredItems"
      :loading="store.isLoading"
      :show-skeleton="showInitialSkeleton"
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
        <prompt-card
          :item="item"
          :stage-label="$t(`pages.prompts.stages.${item.stage}`, item.stage)"
          :kind-label="$t(`pages.prompts.kinds.${item.type}`, item.type)"
          :layer-label="$t(`pages.prompts.layers.${item.layer}`, item.layer)"
          :empty-tag-label="$t('pages.prompts.tags.untagged')"
          :edit-label="$t('common.actions.action.edit')"
          :delete-label="$t('common.actions.action.delete')"
          :reset-label="$t('pages.prompts.actions.reset')"
          @edit="openEdit"
          @delete="(prompt) => removeItem(prompt.fileId, prompt.displayName)"
          @reset="(prompt) => resetItem(prompt.fileId, prompt.displayName)"
          @toggle="(fileId, enabled) => store.toggleEnabled(fileId, enabled)"
        />
      </template>
    </dual-pane-list-view>

    <v-dialog v-model="dialogVisible" max-width="960">
      <v-card>
        <v-card-title>
          {{ editingId ? $t('pages.prompts.overlay.editTitle') : $t('pages.prompts.overlay.createTitle') }}
        </v-card-title>
        <v-card-text>
          <v-row>
            <v-col cols="12" md="5">
              <v-text-field
                v-model="form.promptId"
                :label="$t('pages.prompts.fields.promptId')"
                :readonly="isRuntimeEdit"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col v-if="editingId" cols="12" md="3">
              <v-text-field
                v-model="form.locale"
                :label="$t('pages.prompts.fields.locale')"
                readonly
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" :md="editingId ? 4 : 7">
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
                class="prompt-editor"
                :label="$t('pages.prompts.fields.content')"
                :hint="$t('pages.prompts.hints.content')"
                persistent-hint
                rows="12"
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
            <v-col v-if="!isRuntimeEdit" cols="12" md="6">
              <v-textarea
                v-model="form.configJson"
                :label="$t('pages.prompts.fields.config')"
                :hint="$t('pages.prompts.hints.config')"
                persistent-hint
                rows="3"
                variant="outlined"
              />
            </v-col>
            <v-col cols="12" :md="isRuntimeEdit ? 12 : 6">
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
          <v-btn
            v-if="isRuntimeEdit && editingItem?.resettable"
            color="secondary"
            variant="tonal"
            prepend-icon="mdi-restore"
            :loading="store.isSaving"
            @click="resetEditingItem"
          >
            {{ $t('pages.prompts.actions.reset') }}
          </v-btn>
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
import { computed, onMounted, reactive, ref } from 'vue'
import type {
  CustomPromptCreatePayload,
  PromptCatalogItem,
  PromptFile,
  PromptFilePayload,
} from '@/api/prompts'
import AppPageHeader from '@/components/AppPageHeader.vue'
import DualPaneListView from '@/components/DualPaneListView.vue'
import PromptCard from '@/components/prompts/PromptCard.vue'
import SidebarListCard from '@/components/SidebarListCard.vue'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { translate } from '@/plugins/i18n'
import { usePromptFilesStore } from '@/stores/promptFiles'
import { normalizeStringList, prettyJson, safeJsonParse } from '@/utils/format'

const store = usePromptFilesStore()
const { confirm } = useConfirmDialog()

const dialogVisible = ref(false)
const editingId = ref('')
const editingItem = ref<PromptCatalogItem | null>(null)
const error = ref('')

const filters = reactive({
  layer: 'all',
  locale: 'all',
  stage: 'all',
  tag: 'all',
})

const form = reactive({
  promptId: '',
  locale: '',
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

const initialSkeletonRequested = computed(
  () => store.isLoading && store.items.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const {
  activeTag,
  allTags: tagOptions,
  sidebarItems: tagSidebarItems,
  filteredItems: tagFilteredItems,
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

const filteredItems = computed(() => {
  return tagFilteredItems.value.filter((item) => {
    if (filters.layer !== 'all' && item.layer !== filters.layer) return false
    if (filters.locale !== 'all' && item.locale !== filters.locale) return false
    if (filters.stage !== 'all' && item.stage !== filters.stage) return false
    if (filters.tag !== 'all' && !item.tags.includes(filters.tag)) return false
    return true
  })
})

const isRuntimeEdit = computed(() => editingItem.value?.layer === 'runtime')

const layerOptions = computed(() => [
  { title: translate('pages.prompts.filters.allSources'), value: 'all' },
  { title: translate('pages.prompts.layers.runtime'), value: 'runtime' },
  { title: translate('pages.prompts.layers.custom'), value: 'custom' },
])

const localeOptions = computed(() => {
  const values = Array.from(new Set(store.items.map((item) => item.locale))).sort()
  return [
    { title: translate('pages.prompts.filters.allLocales'), value: 'all' },
    ...values.map((value) => ({ title: value, value })),
  ]
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

const stageFilterOptions = computed(() => [
  { title: translate('pages.prompts.filters.allStages'), value: 'all' },
  ...stageOptions.value,
])

const tagFilterOptions = computed(() => [
  { title: translate('pages.prompts.filters.allTags'), value: 'all' },
  ...tagOptions.value.map((value) => ({ title: value, value })),
])

const kindOptions = computed(() => [
  { title: translate('pages.prompts.kinds.static_text'), value: 'static_text' },
  { title: translate('pages.prompts.kinds.template'), value: 'template' },
  { title: translate('pages.prompts.kinds.resolver'), value: 'resolver' },
  { title: translate('pages.prompts.kinds.bundle'), value: 'bundle' },
  { title: translate('pages.prompts.kinds.external_injection'), value: 'external_injection' },
])

const getPromptKey = (item: PromptCatalogItem) => item.fileId

const resetForm = () => {
  Object.assign(form, {
    promptId: '',
    locale: '',
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
}

const populateForm = (item: PromptFile) => {
  Object.assign(form, {
    promptId: item.promptId,
    locale: item.locale,
    name: item.name || item.displayName,
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
}

const openCreate = () => {
  editingId.value = ''
  editingItem.value = null
  error.value = ''
  resetForm()
  dialogVisible.value = true
}

const openEdit = async (item: PromptCatalogItem) => {
  editingId.value = item.fileId
  editingItem.value = item
  error.value = ''
  resetForm()
  const detail = await store.getItem(item.fileId)
  if (!detail) {
    editingId.value = ''
    editingItem.value = null
    return
  }
  populateForm(detail)
  dialogVisible.value = true
}

const buildPayload = (): CustomPromptCreatePayload | PromptFilePayload => {
  if (isRuntimeEdit.value) {
    return { content: form.content }
  }

  const promptId = form.promptId.trim()
  const name = form.name.trim()

  if (!promptId || !name || !form.stage || !form.type) {
    throw new Error(translate('pages.prompts.messages.requiredFields'))
  }

  return {
    promptId,
    name,
    stage: form.stage,
    type: form.type,
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
}

const submit = async () => {
  error.value = ''
  try {
    const payload = buildPayload()
    const result = editingId.value
      ? await store.updateItem(editingId.value, payload)
      : await store.createItem(payload as CustomPromptCreatePayload)
    if (result) {
      dialogVisible.value = false
    }
  } catch (err: unknown) {
    error.value = err instanceof Error ? err.message : String(err)
  }
}

const removeItem = async (fileId: string, name: string) => {
  if (
    !(await confirm({
      title: translate('common.actions.action.delete'),
      message: translate('pages.prompts.messages.confirmDelete', { name }),
      confirmText: translate('common.actions.action.delete'),
      confirmColor: 'error',
      icon: 'mdi-alert-outline',
      iconColor: 'error',
    }))
  ) return
  await store.deleteItem(fileId)
}

const resetItem = async (fileId: string, name: string) => {
  if (
    !(await confirm({
      title: translate('pages.prompts.actions.reset'),
      message: translate('pages.prompts.messages.confirmReset', { name }),
      confirmText: translate('pages.prompts.actions.reset'),
      confirmColor: 'secondary',
      icon: 'mdi-restore',
      iconColor: 'secondary',
    }))
  ) return
  await store.resetItem(fileId)
}

const resetEditingItem = async () => {
  if (!editingItem.value) return
  await resetItem(editingItem.value.fileId, editingItem.value.displayName)
  dialogVisible.value = false
}

const refreshItems = () => void store.fetchItems({ force: true })

onMounted(() => {
  void store.fetchItems()
})
</script>

<style scoped lang="scss">
.prompt-filters {
  border-bottom: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
}

.prompt-editor :deep(textarea) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  line-height: 1.55;
}
</style>
