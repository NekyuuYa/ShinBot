<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.permissions.title')"
      :subtitle="$t('pages.permissions.subtitle')"
      :kicker="$t('pages.permissions.kicker')"
    >
      <template #actions>
        <v-btn color="secondary" prepend-icon="mdi-refresh" :loading="isLoading" @click="refreshAll">
          {{ $t('pages.permissions.actions.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <summary-metric-band :metrics="summaryMetrics" />

    <v-row class="mx-n3">
      <v-col cols="12" lg="4" class="pa-3">
        <permission-group-list-panel
          v-model:group-search="groupSearch"
          :groups="filteredGroups"
          :selected-group-id="selectedGroupId"
          :loading="isLoading"
          :display-group-name="displayGroupName"
          :binding-count="bindingCount"
          @select-group="selectGroup"
          @create-group="startCreateGroup"
        />
      </v-col>

      <v-col cols="12" lg="8" class="pa-3">
        <permission-group-editor-panel
          v-model:create-group-id="createGroupId"
          v-model:form="groupForm"
          :title="detailTitle"
          :selected-group="selectedGroup"
          :is-creating-group="isCreatingGroup"
          :permission-suggestions="permissionSuggestions"
          :orphan-permissions="orphanPermissions"
          :saving="isSavingGroup"
          :can-save="canSaveGroup"
          @save="saveSelectedGroup"
          @delete="deleteSelectedGroup"
        />

        <v-row class="mx-n3">
          <v-col cols="12" xl="6" class="pa-3">
            <permission-bindings-panel
              v-model:scope-key="bindingScopeKey"
              v-model:group-ids="bindingGroupIds"
              :bindings="bindings"
              :scope-items="bindingScopeItems"
              :group-items="bindingGroupSelectItems"
              :saving="isSavingBinding"
              :can-save="canSaveBinding"
              :display-group-name-by-id="displayGroupNameById"
              @save="saveBinding"
              @delete="deleteBinding"
              @edit="editBinding"
            />
          </v-col>

          <v-col cols="12" xl="6" class="pa-3">
            <permission-command-panel
              v-model:selected-command-name="selectedCommandName"
              v-model:permission-draft="commandPermissionDraft"
              v-model:target-group-id="commandTargetGroupId"
              :selected-command="selectedCommand"
              :command-items="commandSelectItems"
              :group-items="groupSelectItems"
              :saving-command="isSavingCommand"
              :saving-group="isSavingGroup"
              :can-add-to-group="canAddCommandToGroup"
              @save-permission="saveCommandPermission"
              @reset-permission="resetCommandPermission"
              @add-to-group="addCommandPermissionToGroup"
            />
          </v-col>
        </v-row>
      </v-col>
    </v-row>

    <v-alert v-if="errorMessage" type="error" class="mt-4">
      {{ errorMessage }}
    </v-alert>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { commandsApi } from '@/api/commands'
import {
  permissionsApi,
  type PermissionBinding,
  type PermissionGroup,
} from '@/api/permissions'
import { apiClient } from '@/api/client'
import AppPageHeader from '@/components/AppPageHeader.vue'
import PermissionBindingsPanel from '@/components/permissions/PermissionBindingsPanel.vue'
import PermissionCommandPanel from '@/components/permissions/PermissionCommandPanel.vue'
import PermissionGroupEditorPanel from '@/components/permissions/PermissionGroupEditorPanel.vue'
import PermissionGroupListPanel from '@/components/permissions/PermissionGroupListPanel.vue'
import {
  createEmptyPermissionGroupForm,
  formFromGroup,
  groupPayloadFromForm,
  normalizeBinding,
  normalizeGroup,
  stripDenyPrefix,
  type PermissionCommand,
  type PermissionGroupForm,
} from '@/components/permissions/permissionUtils'
import SummaryMetricBand, { type SummaryMetric } from '@/components/resources/SummaryMetricBand.vue'
import { translate } from '@/plugins/i18n'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

const uiStore = useUiStore()

const groups = ref<PermissionGroup[]>([])
const bindings = ref<PermissionBinding[]>([])
const commands = ref<PermissionCommand[]>([])
const groupSearch = ref('')
const selectedGroupId = ref('')
const createGroupId = ref('')
const isCreatingGroup = ref(false)
const isLoading = ref(false)
const isSavingGroup = ref(false)
const isSavingBinding = ref(false)
const isSavingCommand = ref(false)
const errorMessage = ref('')

const bindingScopeKey = ref('')
const bindingGroupIds = ref<string[]>([])
const selectedCommandName = ref('')
const commandPermissionDraft = ref('')
const commandTargetGroupId = ref('')

const groupForm = ref<PermissionGroupForm>(createEmptyPermissionGroupForm())

const selectedGroup = computed(() => groups.value.find((group) => group.id === selectedGroupId.value))
const selectedCommand = computed(() =>
  commands.value.find((command) => command.name === selectedCommandName.value)
)

const overriddenCommandCount = computed(
  () => commands.value.filter((command) => Boolean(command.permissionOverridden)).length
)

const summaryMetrics = computed<SummaryMetric[]>(() => [
  {
    key: 'groups',
    label: translate('pages.permissions.summary.groups'),
    value: groups.value.length,
  },
  {
    key: 'bindings',
    label: translate('pages.permissions.summary.bindings'),
    value: bindings.value.length,
  },
  {
    key: 'overrides',
    label: translate('pages.permissions.summary.overrides'),
    value: overriddenCommandCount.value,
  },
])

const filteredGroups = computed(() => {
  const query = groupSearch.value.trim().toLowerCase()
  if (!query) {
    return groups.value
  }
  return groups.value.filter((group) =>
    [
      group.id,
      group.name,
      group.description,
      ...group.permissions,
    ]
      .filter(Boolean)
      .some((value) => value.toLowerCase().includes(query))
  )
})

const detailTitle = computed(() => {
  if (isCreatingGroup.value) {
    return translate('pages.permissions.groups.createTitle')
  }
  if (selectedGroup.value) {
    return displayGroupName(selectedGroup.value)
  }
  return translate('pages.permissions.groups.detailTitle')
})

const permissionSuggestions = computed(() => {
  const values = new Set<string>()
  for (const command of commands.value) {
    if (command.permission) {
      values.add(command.permission)
    }
    if (command.defaultPermission) {
      values.add(command.defaultPermission)
    }
  }
  for (const group of groups.value) {
    for (const permission of group.permissions) {
      values.add(stripDenyPrefix(permission))
    }
  }
  return Array.from(values).filter(Boolean).sort()
})

const knownPermissionSet = computed(() => new Set(permissionSuggestions.value))

const orphanPermissions = computed(() =>
  selectedGroup.value?.orphanPermissions.length
    ? selectedGroup.value.orphanPermissions
    : [...groupForm.value.permissions, ...groupForm.value.deniedPermissions]
    .map(stripDenyPrefix)
    .filter((permission) => isOrphanPermission(permission))
)

const groupSelectItems = computed(() =>
  groups.value.map((group) => ({
    title: displayGroupName(group),
    value: group.id,
  }))
)

const bindingGroupSelectItems = computed(() =>
  groupSelectItems.value.filter((group) => group.value !== 'admin')
)

const bindingScopeItems = computed(() => bindings.value.map((binding) => binding.scopeKey))

const commandSelectItems = computed(() =>
  commands.value.map((command) => ({
    title: `${command.name} · ${command.permission || translate('pages.permissions.empty.permission')}`,
    value: command.name,
  }))
)

const canSaveGroup = computed(() => {
  if (isCreatingGroup.value) {
    return Boolean(createGroupId.value.trim())
  }
  return Boolean(selectedGroup.value)
})

const canSaveBinding = computed(
  () => Boolean(bindingScopeKey.value?.trim()) && bindingGroupIds.value.length > 0
)

const canAddCommandToGroup = computed(() => {
  const permission = commandPermissionDraft.value.trim() || selectedCommand.value?.permission || ''
  return Boolean(commandTargetGroupId.value && permission)
})

watch(selectedGroup, (group) => {
  if (!group) {
    return
  }
  isCreatingGroup.value = false
  createGroupId.value = group.id
  groupForm.value = formFromGroup(group)
})

watch(selectedCommand, (command) => {
  commandPermissionDraft.value = command?.permission ?? ''
})

async function refreshAll() {
  isLoading.value = true
  errorMessage.value = ''
  try {
    const [groupData, bindingData, commandData] = await Promise.all([
      apiClient.unwrap(permissionsApi.listGroups()),
      apiClient.unwrap(permissionsApi.listBindings()),
      apiClient.unwrap(commandsApi.list()),
    ])
    groups.value = groupData.map(normalizeGroup)
    bindings.value = bindingData.map(normalizeBinding)
    commands.value = commandData as PermissionCommand[]
    ensureSelections()
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.loadFailed'))
  } finally {
    isLoading.value = false
  }
}

function ensureSelections() {
  if (!selectedGroupId.value || !groups.value.some((group) => group.id === selectedGroupId.value)) {
    selectedGroupId.value = groups.value[0]?.id ?? ''
  }
  if (!selectedCommandName.value || !commands.value.some((command) => command.name === selectedCommandName.value)) {
    selectedCommandName.value = commands.value[0]?.name ?? ''
  }
  if (!commandTargetGroupId.value || !groups.value.some((group) => group.id === commandTargetGroupId.value)) {
    commandTargetGroupId.value = selectedGroupId.value
  }
}

function selectGroup(groupId: string) {
  selectedGroupId.value = groupId
  commandTargetGroupId.value = groupId
}

function startCreateGroup() {
  isCreatingGroup.value = true
  selectedGroupId.value = ''
  createGroupId.value = ''
  groupForm.value = createEmptyPermissionGroupForm()
}

async function saveSelectedGroup() {
  const payload = groupPayloadFromForm(groupForm.value)
  isSavingGroup.value = true
  errorMessage.value = ''
  try {
    const group = isCreatingGroup.value
      ? await apiClient.unwrap(
        permissionsApi.createGroup({
          id: createGroupId.value.trim(),
          ...payload,
        })
      )
      : await apiClient.unwrap(permissionsApi.updateGroup(selectedGroupId.value, payload))

    upsertGroup(normalizeGroup(group))
    selectedGroupId.value = group.id
    isCreatingGroup.value = false
    uiStore.showSnackbar(translate('pages.permissions.messages.groupSaved'), 'success')
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.groupSaveFailed'))
  } finally {
    isSavingGroup.value = false
  }
}

async function deleteSelectedGroup() {
  if (!selectedGroup.value) {
    return
  }
  isSavingGroup.value = true
  errorMessage.value = ''
  try {
    await apiClient.unwrap(permissionsApi.deleteGroup(selectedGroup.value.id))
    groups.value = groups.value.filter((group) => group.id !== selectedGroup.value?.id)
    await refreshBindings()
    ensureSelections()
    uiStore.showSnackbar(translate('pages.permissions.messages.groupDeleted'), 'success')
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.groupDeleteFailed'))
  } finally {
    isSavingGroup.value = false
  }
}

async function refreshBindings() {
  const data = await apiClient.unwrap(permissionsApi.listBindings())
  bindings.value = data.map(normalizeBinding)
}

async function saveBinding() {
  isSavingBinding.value = true
  errorMessage.value = ''
  try {
    const binding = await apiClient.unwrap(
      permissionsApi.setBinding(String(bindingScopeKey.value ?? '').trim(), { groups: bindingGroupIds.value })
    )
    upsertBinding(normalizeBinding(binding))
    uiStore.showSnackbar(translate('pages.permissions.messages.bindingSaved'), 'success')
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.bindingSaveFailed'))
  } finally {
    isSavingBinding.value = false
  }
}

async function deleteBinding() {
  isSavingBinding.value = true
  errorMessage.value = ''
  try {
    await apiClient.unwrap(permissionsApi.deleteBinding(String(bindingScopeKey.value ?? '').trim()))
    bindings.value = bindings.value.filter((binding) => binding.scopeKey !== String(bindingScopeKey.value ?? '').trim())
    bindingScopeKey.value = ''
    bindingGroupIds.value = []
    uiStore.showSnackbar(translate('pages.permissions.messages.bindingDeleted'), 'success')
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.bindingDeleteFailed'))
  } finally {
    isSavingBinding.value = false
  }
}

function editBinding(binding: PermissionBinding) {
  bindingScopeKey.value = binding.scopeKey
  bindingGroupIds.value = [...binding.groups]
}

async function saveCommandPermission() {
  if (!selectedCommand.value) {
    return
  }
  isSavingCommand.value = true
  errorMessage.value = ''
  try {
    await apiClient.unwrap(
      permissionsApi.updateCommandPermission(selectedCommand.value.name, {
        permission: commandPermissionDraft.value.trim(),
      })
    )
    await refreshCommands()
    uiStore.showSnackbar(translate('pages.permissions.messages.commandSaved'), 'success')
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.commandSaveFailed'))
  } finally {
    isSavingCommand.value = false
  }
}

async function resetCommandPermission() {
  if (!selectedCommand.value) {
    return
  }
  isSavingCommand.value = true
  errorMessage.value = ''
  try {
    await apiClient.unwrap(permissionsApi.resetCommandPermission(selectedCommand.value.name))
    await refreshCommands()
    uiStore.showSnackbar(translate('pages.permissions.messages.commandReset'), 'success')
  } catch (error) {
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.commandResetFailed'))
  } finally {
    isSavingCommand.value = false
  }
}

async function refreshCommands() {
  const data = await apiClient.unwrap(commandsApi.list())
  commands.value = data as PermissionCommand[]
  const command = commands.value.find((item) => item.name === selectedCommandName.value)
  commandPermissionDraft.value = command?.permission ?? ''
}

async function addCommandPermissionToGroup() {
  const command = selectedCommand.value
  if (!command) {
    return
  }

  const commandName = command.name
  const originalPermission = command.permission
  const originallyOverridden = command.permissionOverridden
  const draftPermission = commandPermissionDraft.value.trim()
  const shouldUpdateCommandPermission = Boolean(draftPermission && draftPermission !== originalPermission)
  let commandPermissionUpdated = false
  let permission = draftPermission || originalPermission || ''
  const targetGroup = groups.value.find((group) => group.id === commandTargetGroupId.value)
  if (!permission || !targetGroup) {
    return
  }

  isSavingGroup.value = true
  errorMessage.value = ''
  try {
    if (shouldUpdateCommandPermission) {
      await apiClient.unwrap(
        permissionsApi.updateCommandPermission(commandName, {
          permission: draftPermission,
        })
      )
      commandPermissionUpdated = true
      await refreshCommands()
      permission = commands.value.find((item) => item.name === commandName)?.permission || draftPermission
    }

    const mergedPermissions = Array.from(new Set([...targetGroup.permissions, permission])).sort()
    const group = await apiClient.unwrap(
      permissionsApi.updateGroup(targetGroup.id, {
        name: targetGroup.name,
        description: targetGroup.description,
        protected: targetGroup.protected,
        permissions: mergedPermissions,
      })
    )
    upsertGroup(normalizeGroup(group))
    selectedGroupId.value = targetGroup.id
    uiStore.showSnackbar(translate('pages.permissions.messages.commandAdded'), 'success')
  } catch (error) {
    if (commandPermissionUpdated) {
      await restoreCommandPermission(commandName, originalPermission, originallyOverridden)
    }
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.commandAddFailed'))
  } finally {
    isSavingGroup.value = false
  }
}

async function restoreCommandPermission(commandName: string, permission: string, overridden: boolean) {
  try {
    if (overridden) {
      await apiClient.unwrap(
        permissionsApi.updateCommandPermission(commandName, {
          permission,
        })
      )
    } else {
      await apiClient.unwrap(permissionsApi.resetCommandPermission(commandName))
    }
    await refreshCommands()
  } catch {
    await refreshCommands()
  }
}

function isOrphanPermission(permission: string) {
  if (!permission || permission === '*' || permission.endsWith('.*')) {
    return false
  }
  if (knownPermissionSet.value.has(permission)) {
    return false
  }
  return permission.startsWith('cmd.')
}

function bindingCount(groupId: string) {
  return bindings.value.filter((binding) => binding.groups.includes(groupId)).length
}

function displayGroupName(group: PermissionGroup) {
  return group.name || group.id
}

function displayGroupNameById(groupId: string) {
  const group = groups.value.find((item) => item.id === groupId)
  return group ? displayGroupName(group) : groupId
}

function upsertGroup(group: PermissionGroup) {
  const index = groups.value.findIndex((item) => item.id === group.id)
  if (index === -1) {
    groups.value = [...groups.value, group].sort((left, right) => left.id.localeCompare(right.id))
    return
  }
  groups.value[index] = group
}

function upsertBinding(binding: PermissionBinding) {
  const index = bindings.value.findIndex((item) => item.scopeKey === binding.scopeKey)
  if (index === -1) {
    bindings.value = [...bindings.value, binding].sort((left, right) => left.scopeKey.localeCompare(right.scopeKey))
    return
  }
  bindings.value[index] = binding
}

onMounted(() => {
  void refreshAll()
})
</script>
