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

    <div class="prompts-layout">
      <div class="prompts-tag-pane">
        <sidebar-list-card
          :title="$t('pages.prompts.tags.title')"
          :empty-text="$t('pages.prompts.tags.empty')"
          :items="tagSidebarItems"
          :active-id="activeTag"
          :show-add-button="false"
          @select="selectTag"
        />
      </div>

      <div class="prompts-content-pane">
        <v-row v-if="store.isLoading && store.items.length === 0" class="mx-0">
          <v-col cols="12" class="pa-0">
            <v-skeleton-loader type="card, card, card" />
          </v-col>
        </v-row>

        <v-row v-else-if="filteredItems.length === 0" justify="center" class="mx-0 py-12">
          <v-col cols="12" md="8" class="text-center pa-0">
            <v-icon size="96" color="grey-lighten-1" icon="mdi-text-box-search-outline" />
            <h3 class="text-h6 my-4">{{ $t('pages.prompts.empty.title') }}</h3>
            <p class="text-body-2 text-medium-emphasis">{{ $t('pages.prompts.empty.subtitle') }}</p>
          </v-col>
        </v-row>

        <v-row v-else class="mx-n4">
          <v-col
            v-for="item in filteredItems"
            :key="item.uuid"
            cols="12"
            sm="6"
            md="6"
            lg="4"
            class="pa-4"
          >
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
          </v-col>
        </v-row>
      </div>
    </div>

    <v-alert v-if="store.error" type="error" class="mt-4">
      {{ store.error }}
    </v-alert>

    <!-- Create / Edit dialog -->
    <v-dialog v-model="dialogVisible" max-width="960">
      <v-card>
        <v-card-title>
          {{ editingUuid ? $t('pages.prompts.overlay.editTitle') : $t('pages.prompts.overlay.createTitle') }}
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

          <v-alert v-if="localError || store.error" type="error" class="mt-2">
            {{ localError || store.error }}
          </v-alert>
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="dialogVisible = false">
            {{ $t('common.actions.action.cancel') }}
          </v-btn>
          <v-btn color="primary" :loading="store.isSaving" @click="saveItem">
            {{ editingUuid ? $t('common.actions.action.save') : $t('common.actions.action.create') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import type { PromptDefinition, PromptDefinitionPayload } from '@/api/promptDefinitions'
import AppPageHeader from '@/components/AppPageHeader.vue'
import SidebarListCard from '@/components/model-runtime/SidebarListCard.vue'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { translate } from '@/plugins/i18n'
import { usePromptDefinitionsStore } from '@/stores/promptDefinitions'
import { normalizeStringList } from '@/utils/stringList'

const store = usePromptDefinitionsStore()

const dialogVisible = ref(false)
const editingUuid = ref('')
const localError = ref('')

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

const parseJsonObject = (value: string, emptyFallback: Record<string, unknown>) => {
  const trimmed = value.trim()
  if (!trimmed) return emptyFallback
  try {
    const parsed = JSON.parse(trimmed)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return parsed
  } catch {
    throw new Error(translate('pages.prompts.messages.invalidJson'))
  }
  throw new Error(translate('pages.prompts.messages.invalidJson'))
}

const resetForm = () => {
  form.promptId = ''
  form.name = ''
  form.stage = 'system_base'
  form.type = 'static_text'
  form.priority = 100
  form.version = '1.0.0'
  form.description = ''
  form.enabled = true
  form.content = ''
  form.templateVars = []
  form.resolverRef = ''
  form.bundleRefs = []
  form.tags = []
  form.configJson = ''
  form.metadataJson = ''
}

const openCreate = () => {
  editingUuid.value = ''
  localError.value = ''
  resetForm()
  dialogVisible.value = true
}

const openEdit = (item: PromptDefinition) => {
  editingUuid.value = item.uuid
  localError.value = ''
  form.promptId = item.promptId
  form.name = item.name
  form.stage = item.stage
  form.type = item.type
  form.priority = item.priority
  form.version = item.version
  form.description = item.description
  form.enabled = item.enabled
  form.content = item.content
  form.templateVars = [...item.templateVars]
  form.resolverRef = item.resolverRef
  form.bundleRefs = [...item.bundleRefs]
  form.tags = [...item.tags]
  form.configJson = Object.keys(item.config).length ? JSON.stringify(item.config, null, 2) : ''
  form.metadataJson = Object.keys(item.metadata).length ? JSON.stringify(item.metadata, null, 2) : ''
  dialogVisible.value = true
}

const buildPayload = (): PromptDefinitionPayload => {
  const promptId = form.promptId.trim()
  const name = form.name.trim()
  const stage = form.stage.trim()
  const type = form.type.trim()

  if (!promptId || !name || !stage || !type) {
    throw new Error(translate('pages.prompts.messages.requiredFields'))
  }

  return {
    promptId,
    name,
    stage,
    type,
    priority: form.priority,
    version: form.version.trim() || '1.0.0',
    description: form.description.trim(),
    enabled: form.enabled,
    content: form.content,
    templateVars: normalizeStringList(form.templateVars),
    resolverRef: form.resolverRef.trim(),
    bundleRefs: normalizeStringList(form.bundleRefs),
    tags: normalizeStringList(form.tags),
    config: parseJsonObject(form.configJson, {}),
    metadata: parseJsonObject(form.metadataJson, {}),
  }
}

const saveItem = async () => {
  localError.value = ''
  try {
    const payload = buildPayload()
    const result = editingUuid.value
      ? await store.updateItem(editingUuid.value, payload)
      : await store.createItem(payload)
    if (result) dialogVisible.value = false
  } catch (e: unknown) {
    localError.value = e instanceof Error ? e.message : String(e)
  }
}

const removeItem = async (uuid: string, name: string) => {
  if (!confirm(translate('pages.prompts.messages.confirmDelete', { name }))) return
  await store.deleteItem(uuid)
}

const refreshItems = () => store.fetchItems()

onMounted(() => store.fetchItems())
</script>

<style scoped>
.prompts-layout {
  display: flex;
  align-items: flex-start;
  gap: 16px;
}

.prompts-tag-pane {
  flex: 0 0 300px;
  width: 300px;
  max-width: 300px;
}

.prompts-content-pane {
  flex: 1 1 auto;
  min-width: 0;
}

.prompt-card {
  border: 1px solid rgba(0, 86, 120, 0.12);
  border-radius: 20px;
  background: linear-gradient(180deg, #f6fcff 0%, #ffffff 100%);
}

@media (max-width: 960px) {
  .prompts-layout {
    flex-direction: column;
  }

  .prompts-tag-pane {
    flex: 1 1 auto;
    width: 100%;
    max-width: none;
  }
}
</style>
