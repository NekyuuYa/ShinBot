<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.commands.title')"
      :subtitle="$t('pages.commands.subtitle')"
      :kicker="$t('pages.commands.kicker')"
    >
      <template #actions>
        <v-btn color="secondary" prepend-icon="mdi-refresh" @click="handleRefresh">
          {{ $t('pages.commands.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <summary-metric-band :metrics="summaryMetrics" />

    <resource-filter-toolbar
      :search="searchQuery"
      :search-label="$t('common.actions.action.search')"
      :filters="filterConfigs"
      :layout-mode="commandsStore.layoutMode"
      :list-label="$t('pages.commands.layout.list')"
      :card-label="$t('pages.commands.layout.card')"
      @update:search="searchQuery = $event"
      @update:filter="handleFilterChange"
      @update:layout-mode="handleLayoutChange"
    />

    <resource-collection-view
      :items="filteredCommands"
      :loading="showInitialSkeleton"
      :loaded="hasLoadedCommands"
      :layout-mode="commandsStore.layoutMode"
      :empty-config="emptyConfig"
      :get-item-key="(command) => command.name"
    >
      <template #card="{ item: command }">
        <command-card
          :command="command"
          :saving="commandsStore.isSaving"
          @update:enabled="handleEnabledChange"
          @add-permission="openAddPermissionDialog"
        />
      </template>

      <template #row="{ item: command }">
        <command-list-row
          :key="command.name"
          :command="command"
          :saving="commandsStore.isSaving"
          @update:enabled="handleEnabledChange"
          @add-permission="openAddPermissionDialog"
        />
      </template>
    </resource-collection-view>

    <v-alert v-if="commandsStore.error" type="error" class="mt-4">
      {{ commandsStore.error }}
    </v-alert>

    <v-alert v-if="permissionGroupError" type="error" class="mt-4">
      {{ permissionGroupError }}
    </v-alert>

    <command-permission-dialog
      v-model="addPermissionDialog"
      v-model:target-group-id="targetPermissionGroupId"
      :command="selectedPermissionCommand"
      :group-items="permissionGroupItems"
      :saving="isSavingPermissionGroup"
      :can-submit="canAddPermissionToGroup"
      @submit="addPermissionToGroup"
    />
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import CommandCard from '@/components/commands/CommandCard.vue'
import CommandListRow from '@/components/commands/CommandListRow.vue'
import CommandPermissionDialog from '@/components/commands/CommandPermissionDialog.vue'
import ResourceCollectionView from '@/components/resources/ResourceCollectionView.vue'
import ResourceFilterToolbar, {
  type ResourceFilter,
} from '@/components/resources/ResourceFilterToolbar.vue'
import SummaryMetricBand, {
  type SummaryMetric,
} from '@/components/resources/SummaryMetricBand.vue'
import { apiClient } from '@/api/client'
import { permissionsApi, type PermissionGroup } from '@/api/permissions'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useCommandsStore, type CommandLayoutMode } from '@/stores/commands'
import { translate } from '@/plugins/i18n'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

const commandsStore = useCommandsStore()
const uiStore = useUiStore()

const searchQuery = ref('')
const statusFilter = ref('all')
const ownerFilter = ref('all')
const hasLoadedCommands = ref(false)
const permissionGroups = ref<PermissionGroup[]>([])
const addPermissionDialog = ref(false)
const selectedPermissionCommandName = ref('')
const targetPermissionGroupId = ref('')
const isSavingPermissionGroup = ref(false)
const permissionGroupError = ref('')

const initialSkeletonRequested = computed(
  () => commandsStore.isLoading && commandsStore.commands.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const summaryMetrics = computed<SummaryMetric[]>(() => [
  { key: 'total', label: translate('pages.commands.summary.total'), value: commandsStore.commands.length },
  { key: 'enabled', label: translate('pages.commands.summary.enabled'), value: commandsStore.enabledCount },
  { key: 'plugin', label: translate('pages.commands.summary.pluginOwned'), value: commandsStore.pluginOwnedCount },
])

const statusItems = computed(() => [
  { title: translate('pages.commands.filters.all'), value: 'all' },
  { title: translate('pages.commands.status.enabled'), value: 'enabled' },
  { title: translate('pages.commands.status.disabled'), value: 'disabled' },
])

const ownerItems = computed(() => [
  { title: translate('pages.commands.filters.all'), value: 'all' },
  { title: translate('pages.commands.owner.core'), value: 'core' },
  { title: translate('pages.commands.owner.plugin'), value: 'plugin' },
])

const filterConfigs = computed<ResourceFilter[]>(() => [
  {
    key: 'status',
    label: translate('pages.commands.filters.status'),
    value: statusFilter.value,
    items: statusItems.value,
  },
  {
    key: 'owner',
    label: translate('pages.commands.filters.owner'),
    value: ownerFilter.value,
    items: ownerItems.value,
  },
])

const emptyConfig = computed(() => ({
  icon: 'mdi-console-line',
  title: translate('pages.commands.empty.title'),
  subtitle: translate('pages.commands.empty.subtitle'),
}))

const filteredCommands = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()

  return commandsStore.commands.filter((command) => {
    const matchesQuery =
      !query ||
      [
        command.name,
        command.description,
        command.usage,
        command.permission,
        command.owner,
        command.pattern,
        ...command.triggers,
      ]
        .filter(Boolean)
        .some((value) => value.toLowerCase().includes(query))

    const matchesStatus =
      statusFilter.value === 'all'
      || (statusFilter.value === 'enabled' && command.enabled)
      || (statusFilter.value === 'disabled' && !command.enabled)

    const isPluginOwned = Boolean(command.owner)
    const matchesOwner =
      ownerFilter.value === 'all'
      || (ownerFilter.value === 'plugin' && isPluginOwned)
      || (ownerFilter.value === 'core' && !isPluginOwned)

    return matchesQuery && matchesStatus && matchesOwner
  })
})

const selectedPermissionCommand = computed(() =>
  commandsStore.commands.find((command) => command.name === selectedPermissionCommandName.value)
)

const permissionGroupItems = computed(() =>
  permissionGroups.value.map((group) => ({
    title: group.name || group.id,
    value: group.id,
  }))
)

const canAddPermissionToGroup = computed(() =>
  Boolean(selectedPermissionCommand.value?.permission && targetPermissionGroupId.value)
)

async function loadCommands(force = false) {
  try {
    await Promise.all([
      commandsStore.fetchCommands({ force }),
      loadPermissionGroups(),
    ])
  } finally {
    hasLoadedCommands.value = true
  }
}

async function loadPermissionGroups() {
  try {
    const groups = await apiClient.unwrap(permissionsApi.listGroups())
    permissionGroups.value = groups
  } catch (error) {
    permissionGroupError.value = getErrorMessage(error, translate('pages.commands.permission.loadGroupsFailed'))
  }
}

onMounted(() => {
  void loadCommands()
})

const handleRefresh = () => {
  void loadCommands(true)
}

const handleLayoutChange = (mode: CommandLayoutMode) => {
  if (mode) {
    commandsStore.setLayoutMode(mode)
  }
}

const handleFilterChange = (key: string, value: string) => {
  if (key === 'status') {
    statusFilter.value = value
    return
  }

  if (key === 'owner') {
    ownerFilter.value = value
  }
}

const handleEnabledChange = async (name: string, enabled: boolean) => {
  await commandsStore.updateCommandEnabled(name, enabled)
}

const openAddPermissionDialog = (commandName: string) => {
  selectedPermissionCommandName.value = commandName
  if (!targetPermissionGroupId.value && permissionGroups.value.length > 0) {
    targetPermissionGroupId.value = permissionGroups.value[0].id
  }
  addPermissionDialog.value = true
}

async function addPermissionToGroup() {
  const command = selectedPermissionCommand.value
  const group = permissionGroups.value.find((item) => item.id === targetPermissionGroupId.value)
  if (!command?.permission || !group) {
    return
  }

  isSavingPermissionGroup.value = true
  permissionGroupError.value = ''
  try {
    const permissions = Array.from(new Set([...group.permissions, command.permission])).sort()
    const updated = await apiClient.unwrap(
      permissionsApi.updateGroup(group.id, {
        name: group.name,
        description: group.description,
        protected: group.protected,
        permissions,
      })
    )
    const index = permissionGroups.value.findIndex((item) => item.id === updated.id)
    if (index === -1) {
      permissionGroups.value = [...permissionGroups.value, updated]
    } else {
      permissionGroups.value[index] = updated
    }
    addPermissionDialog.value = false
    uiStore.showSnackbar(translate('pages.commands.permission.added'), 'success')
  } catch (error) {
    permissionGroupError.value = getErrorMessage(error, translate('pages.commands.permission.addFailed'))
  } finally {
    isSavingPermissionGroup.value = false
  }
}
</script>
