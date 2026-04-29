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
        <v-btn color="primary" prepend-icon="mdi-account-plus" @click="openCreate">
          {{ $t('pages.personas.actions.addPersona') }}
        </v-btn>
      </template>
    </app-page-header>

    <dual-pane-list-view
      :items="filteredPersonas"
      :loading="personasStore.isLoading"
      :show-skeleton="personasStore.isLoading && personasStore.personas.length === 0"
      :empty-config="{
        icon: 'mdi-account-search-outline',
        title: $t('pages.personas.empty.title'),
        subtitle: $t('pages.personas.empty.subtitle'),
      }"
      :get-item-key="getPersonaKey"
    >
      <template #sidebar>
        <sidebar-list-card
          :title="$t('pages.personas.tags.title')"
          :empty-text="$t('pages.personas.tags.empty')"
          :items="tagSidebarItems"
          :active-id="activeTag"
          :show-add-button="false"
          @select="selectTag"
        />
      </template>

      <template #card="{ item: persona }">
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
            <v-btn variant="text" prepend-icon="mdi-pencil" @click="openEdit(persona)">
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
      </template>
    </dual-pane-list-view>

    <v-alert v-if="personasStore.error" type="error" class="mt-4">
      {{ personasStore.error }}
    </v-alert>

    <v-dialog v-model="dialogVisible" max-width="780">
      <v-card>
        <v-card-title>
          {{ editingId ? $t('pages.personas.overlay.editTitle') : $t('pages.personas.overlay.createTitle') }}
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
          <v-btn color="primary" :loading="isSaving" @click="submit">
            {{ editingId ? $t('common.actions.action.save') : $t('common.actions.action.create') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { onMounted, reactive } from 'vue'

import type { Persona, PersonaPayload } from '@/api/personas'
import AppPageHeader from '@/components/AppPageHeader.vue'
import DualPaneListView from '@/components/DualPaneListView.vue'
import SidebarListCard from '@/components/SidebarListCard.vue'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useCrudDialog } from '@/composables/useCrudDialog'
import { useTagSidebar } from '@/composables/useTagSidebar'
import { translate } from '@/plugins/i18n'
import { usePersonasStore } from '@/stores/personas'
import { normalizeStringList } from '@/utils/format'

const personasStore = usePersonasStore()
const { confirm } = useConfirmDialog()

const form = reactive({
  name: '',
  promptText: '',
  tags: [] as string[],
  enabled: true,
})

const resetForm = () => {
  form.name = ''
  form.promptText = ''
  form.tags = []
  form.enabled = true
}

const populateForm = (persona: Persona) => {
  form.name = persona.name
  form.promptText = persona.promptText
  form.tags = [...persona.tags]
  form.enabled = persona.enabled
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

const {
  visible: dialogVisible,
  editingId,
  localError,
  isSaving,
  openCreate,
  openEdit,
  submit,
} = useCrudDialog<Persona, PersonaPayload>({
  resetForm,
  populateForm,
  buildPayload,
  save: async (payload, id) => {
    const result = id
      ? await personasStore.updatePersona(id, payload)
      : await personasStore.createPersona(payload)
    return Boolean(result)
  },
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

const removePersona = async (uuid: string, name: string) => {
  if (
    !(await confirm({
      title: translate('common.actions.action.delete'),
      message: translate('pages.personas.messages.confirmDelete', { name }),
      confirmText: translate('common.actions.action.delete'),
      confirmColor: 'error',
      icon: 'mdi-alert-outline',
      iconColor: 'error',
    }))
  ) {
    return
  }

  await personasStore.deletePersona(uuid)
}

const refreshPersonas = async () => {
  await personasStore.fetchPersonas({ force: true })
}

const previewPrompt = (promptText: string) => {
  const text = promptText.trim()
  if (!text) {
    return translate('pages.personas.messages.emptyPrompt')
  }

  return text.length > 180 ? `${text.slice(0, 180)}...` : text
}

const getPersonaKey = (persona: Persona) => persona.uuid

onMounted(() => {
  void personasStore.fetchPersonas()
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.persona-card {
  @include surface-card;
  @include hover-lift;
}

.persona-prompt-preview {
  white-space: pre-wrap;
  line-height: 1.6;
}
</style>
