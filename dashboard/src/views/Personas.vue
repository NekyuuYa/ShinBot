<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.personas.title')"
      :subtitle="$t('pages.personas.subtitle')"
      :kicker="$t('pages.personas.kicker')"
    >
      <template #actions>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="personasStore.isLoading"
          class="me-2"
          @click="refreshPersonas"
        >
          {{ $t('pages.personas.actions.refresh') }}
        </v-btn>
        <v-btn color="primary" prepend-icon="mdi-account-plus" @click="openCreatePersona">
          {{ $t('pages.personas.actions.addPersona') }}
        </v-btn>
      </template>
    </app-page-header>

    <div class="personas-layout">
      <div class="personas-tag-pane">
        <sidebar-list-card
          :title="$t('pages.personas.tags.title')"
          :empty-text="$t('pages.personas.tags.empty')"
          :items="tagSidebarItems"
          :active-id="activeTag"
          :show-add-button="false"
          @select="selectTag"
        />
      </div>

      <div class="personas-content-pane">
        <v-row v-if="personasStore.isLoading && personasStore.personas.length === 0" class="mx-0">
          <v-col cols="12" class="pa-0">
            <v-skeleton-loader type="card, card, card" />
          </v-col>
        </v-row>

        <v-row v-else-if="filteredPersonas.length === 0" justify="center" class="mx-0 py-12">
          <v-col cols="12" md="8" class="text-center pa-0">
            <v-icon size="96" color="grey-lighten-1" icon="mdi-account-search-outline" />
            <h3 class="text-h6 my-4">{{ $t('pages.personas.empty.title') }}</h3>
            <p class="text-body-2 text-medium-emphasis">{{ $t('pages.personas.empty.subtitle') }}</p>
          </v-col>
        </v-row>

        <v-row v-else class="mx-n4">
          <v-col
            v-for="persona in filteredPersonas"
            :key="persona.uuid"
            cols="12"
            sm="6"
            md="6"
            lg="4"
            class="pa-4"
          >
            <v-card class="persona-card h-100 d-flex flex-column" elevation="0">
              <v-card-item>
                <template #prepend>
                  <v-avatar color="primary" variant="tonal" icon="mdi-account-badge-outline" />
                </template>
                <v-card-title class="text-break">{{ persona.name }}</v-card-title>
                <template #append>
                  <v-chip
                    size="small"
                    :color="persona.enabled ? 'success' : 'grey'"
                    variant="tonal"
                  >
                    {{ persona.enabled ? $t('pages.personas.state.enabled') : $t('pages.personas.state.disabled') }}
                  </v-chip>
                </template>
              </v-card-item>

              <v-card-text class="pt-1 flex-grow-1">
                <div class="text-body-2 text-medium-emphasis persona-prompt-preview">
                  {{ previewPrompt(persona.promptText) }}
                </div>

                <div class="d-flex flex-wrap ga-2 mt-3">
                  <v-chip
                    v-for="tag in persona.tags"
                    :key="`${persona.uuid}-${tag}`"
                    size="small"
                    color="secondary"
                    variant="tonal"
                  >
                    {{ tag }}
                  </v-chip>
                  <v-chip
                    v-if="persona.tags.length === 0"
                    size="small"
                    color="grey"
                    variant="tonal"
                  >
                    {{ $t('pages.personas.tags.untagged') }}
                  </v-chip>
                </div>
              </v-card-text>

              <v-card-actions>
                <v-btn variant="text" prepend-icon="mdi-pencil" @click="openEditPersona(persona)">
                  {{ $t('common.actions.action.edit') }}
                </v-btn>
                <v-spacer />
                <v-btn
                  color="error"
                  variant="text"
                  prepend-icon="mdi-delete-outline"
                  @click="removePersona(persona.uuid, persona.name)"
                >
                  {{ $t('common.actions.action.delete') }}
                </v-btn>
              </v-card-actions>
            </v-card>
          </v-col>
        </v-row>
      </div>
    </div>

    <v-alert v-if="personasStore.error" type="error" class="mt-4">
      {{ personasStore.error }}
    </v-alert>

    <v-dialog v-model="dialogVisible" max-width="780">
      <v-card>
        <v-card-title>
          {{ editingPersonaUuid ? $t('pages.personas.overlay.editTitle') : $t('pages.personas.overlay.createTitle') }}
        </v-card-title>
        <v-card-text>
          <v-row>
            <v-col cols="12" md="8">
              <v-text-field
                v-model="form.name"
                :label="$t('pages.personas.fields.name')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="4" class="d-flex align-center">
              <v-switch
                v-model="form.enabled"
                inset
                color="primary"
                :label="$t('pages.personas.fields.enabled')"
                hide-details
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
                :label="$t('pages.personas.fields.tags')"
                :items="allTags"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12">
              <v-textarea
                v-model="form.promptText"
                :label="$t('pages.personas.fields.promptText')"
                :hint="$t('pages.personas.hints.promptText')"
                persistent-hint
                rows="8"
                variant="outlined"
              />
            </v-col>
          </v-row>

          <v-alert v-if="localError || personasStore.error" type="error" class="mt-2">
            {{ localError || personasStore.error }}
          </v-alert>
        </v-card-text>

        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="dialogVisible = false">
            {{ $t('common.actions.action.cancel') }}
          </v-btn>
          <v-btn color="primary" :loading="personasStore.isSaving" @click="savePersona">
            {{ editingPersonaUuid ? $t('common.actions.action.save') : $t('common.actions.action.create') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import type { Persona, PersonaPayload } from '@/api/personas'
import AppPageHeader from '@/components/AppPageHeader.vue'
import SidebarListCard from '@/components/model-runtime/SidebarListCard.vue'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { translate } from '@/plugins/i18n'
import { usePersonasStore } from '@/stores/personas'
import { normalizeStringList } from '@/utils/stringList'

const personasStore = usePersonasStore()

const dialogVisible = ref(false)
const editingPersonaUuid = ref('')
const localError = ref('')

const form = reactive({
  name: '',
  promptText: '',
  tags: [] as string[],
  enabled: true,
})

const {
  activeTag,
  allTags,
  sidebarItems: tagSidebarItems,
  filteredItems: filteredPersonas,
  selectTag,
} = useTagSidebar(
  () => personasStore.personas,
  {
    getTags: (persona) => persona.tags,
    allTitle: translate('pages.personas.tags.all'),
    allSubtitle: translate('pages.personas.tags.showAll'),
    tagSubtitle: translate('pages.personas.tags.filterByTag'),
  }
)

const resetForm = () => {
  form.name = ''
  form.promptText = ''
  form.tags = []
  form.enabled = true
}

const openCreatePersona = () => {
  editingPersonaUuid.value = ''
  localError.value = ''
  resetForm()
  dialogVisible.value = true
}

const openEditPersona = (persona: Persona) => {
  editingPersonaUuid.value = persona.uuid
  localError.value = ''
  form.name = persona.name
  form.promptText = persona.promptText
  form.tags = [...persona.tags]
  form.enabled = persona.enabled
  dialogVisible.value = true
}

const buildPayload = (): PersonaPayload => {
  const name = form.name.trim()
  const promptText = form.promptText.trim()

  if (!name || !promptText) {
    throw new Error(translate('pages.personas.messages.requiredFields'))
  }

  return {
    name,
    promptText,
    tags: normalizeStringList(form.tags),
    enabled: form.enabled,
  }
}

const savePersona = async () => {
  localError.value = ''

  try {
    const payload = buildPayload()
    const result = editingPersonaUuid.value
      ? await personasStore.updatePersona(editingPersonaUuid.value, payload)
      : await personasStore.createPersona(payload)

    if (result) {
      dialogVisible.value = false
    }
  } catch (errorDetail: unknown) {
    localError.value = errorDetail instanceof Error
      ? errorDetail.message
      : String(errorDetail)
  }
}

const removePersona = async (uuid: string, name: string) => {
  if (!confirm(translate('pages.personas.messages.confirmDelete', { name }))) {
    return
  }

  await personasStore.deletePersona(uuid)
}

const refreshPersonas = async () => {
  await personasStore.fetchPersonas()
}

const previewPrompt = (promptText: string) => {
  const text = promptText.trim()
  if (!text) {
    return translate('pages.personas.messages.emptyPrompt')
  }

  return text.length > 180 ? `${text.slice(0, 180)}...` : text
}

onMounted(() => {
  personasStore.fetchPersonas()
})
</script>

<style scoped>
.personas-layout {
  display: flex;
  align-items: flex-start;
  gap: 16px;
}

.personas-tag-pane {
  flex: 0 0 300px;
  width: 300px;
  max-width: 300px;
}

.personas-content-pane {
  flex: 1 1 auto;
  min-width: 0;
}

.persona-card {
  border: 1px solid rgba(120, 86, 0, 0.12);
  border-radius: 20px;
  background: linear-gradient(180deg, #fffef6 0%, #ffffff 100%);
}

.persona-prompt-preview {
  white-space: pre-wrap;
  line-height: 1.6;
}

@media (max-width: 960px) {
  .personas-layout {
    flex-direction: column;
  }

  .personas-tag-pane {
    flex: 1 1 auto;
    width: 100%;
    max-width: none;
  }
}
</style>
