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
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { Instance } from '@/api/instances'
import AppPageHeader from '@/components/AppPageHeader.vue'
import InstanceCard from '@/components/InstanceCard.vue'
import InstanceFormDialog from '@/components/instances/InstanceFormDialog.vue'
import type { InstanceFormState, KeyValueEntry } from '@/components/instances/types'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import { useCrudDialog } from '@/composables/useCrudDialog'
import { useInstanceResources } from '@/composables/useInstanceResources'
import { useInstancesStore } from '@/stores/instances'

const { t } = useI18n()
const instancesStore = useInstancesStore()

const searchQuery = ref('')
const viewMode = ref<'card' | 'list'>('list')
const form = ref<InstanceFormState>(instancesStore.createFormState())
const botConfigEntries = ref<KeyValueEntry[]>([])

const {
  agents,
  promptCatalog,
  adapterOptions,
  activeAdapterSchema,
  fetchAllResources,
  getBotConfigForInstance,
} = useInstanceResources(form)

const handleRefresh = () => Promise.all([
  instancesStore.fetchInstances({ force: true }),
  fetchAllResources({ force: true }),
])

const {
  visible: dialogVisible,
  editingId,
  openCreate: openCreateBase,
  openEdit: openEditBase,
  submit,
} = useCrudDialog<Instance, InstanceFormState>({
  resetForm: () => {
    form.value = instancesStore.createFormState(adapterOptions.value[0] || 'satori')
    botConfigEntries.value = []
  },
  populateForm: (instance) => {
    const editorState = instancesStore.buildEditorState(instance, getBotConfigForInstance(instance.id))
    form.value = editorState.form
    botConfigEntries.value = editorState.botConfigEntries
  },
  buildPayload: () => form.value,
  save: async (payload, id) => {
    const saved = await instancesStore.saveInstanceForm(payload, botConfigEntries.value, id || undefined)
    if (saved) {
      await handleRefresh()
    }
    return saved
  },
})

const openCreate = () => openCreateBase()
const openEdit = (item: Instance) => openEditBase(item)

const tableHeaders = computed(() => [
  { title: t('pages.instances.table.name'), value: 'name', width: '20%' },
  { title: t('pages.instances.table.adapterType'), value: 'adapterType', width: '25%' },
  { title: t('pages.instances.table.status'), value: 'status', width: '15%' },
  { title: t('pages.instances.table.created'), value: 'createdAt', width: '20%' },
  { title: t('pages.instances.table.actions'), value: 'actions', width: '20%', sortable: false },
])

type TableRowItem = Instance | { raw: Instance }

const tableRow = (item: TableRowItem): Instance => ('raw' in item ? item.raw : item)

const instancePendingAction = (item: TableRowItem) => instancesStore.pendingActions[tableRow(item).id]

const instanceStatusColor = (item: TableRowItem) => (
  instancePendingAction(item) ? 'warning' : tableRow(item).status === 'running' ? 'success' : 'error'
)

const instanceStatusLabel = (item: TableRowItem) => (
  instancePendingAction(item)
    ? t('common.actions.status.loading')
    : tableRow(item).status === 'running'
      ? t('pages.instances.card.isRunning')
      : t('pages.instances.card.isStopped')
)

const filteredInstances = computed(() =>
  instancesStore.instances.filter((instance) =>
    instance.name.toLowerCase().includes(searchQuery.value.toLowerCase())
  )
)

const deleteInstance = (instance: Instance) => instancesStore.deleteInstance(instance.id)
const startInstance = (instance: Instance) => instancesStore.startInstance(instance.id)
const stopInstance = (instance: Instance) => instancesStore.stopInstance(instance.id)

onMounted(handleRefresh)
</script>
