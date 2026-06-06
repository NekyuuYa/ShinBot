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

    <v-row class="mx-0 mb-6" align="stretch">
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.permissions.summary.groups') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ groups.length }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.permissions.summary.bindings') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ bindings.length }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.permissions.summary.overrides') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ overriddenCommandCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-row class="mx-n3">
      <v-col cols="12" lg="4" class="pa-3">
        <v-card rounded="xl" elevation="0" class="panel-card">
          <v-card-title class="d-flex align-center justify-space-between ga-3">
            <span>{{ $t('pages.permissions.groups.title') }}</span>
            <v-btn size="small" color="primary" prepend-icon="mdi-plus" @click="startCreateGroup">
              {{ $t('pages.permissions.actions.newGroup') }}
            </v-btn>
          </v-card-title>
          <v-card-text>
            <v-text-field
              v-model="groupSearch"
              :label="$t('common.actions.action.search')"
              prepend-inner-icon="mdi-magnify"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              class="mb-4"
            />

            <v-skeleton-loader
              v-if="isLoading && groups.length === 0"
              type="list-item-two-line, list-item-two-line, list-item-two-line"
            />

            <div v-else-if="filteredGroups.length === 0" class="empty-state py-8">
              <v-icon icon="mdi-shield-key-outline" size="64" color="grey-lighten-1" />
              <div class="text-body-2 text-medium-emphasis mt-3">
                {{ $t('pages.permissions.empty.groups') }}
              </div>
            </div>

            <v-list v-else lines="three" class="group-list pa-0">
              <v-list-item
                v-for="group in filteredGroups"
                :key="group.id"
                :active="group.id === selectedGroupId"
                rounded="lg"
                class="group-list-item mb-2"
                @click="selectGroup(group.id)"
              >
                <template #prepend>
                  <v-avatar color="primary" variant="tonal" size="36">
                    <v-icon icon="mdi-shield-account-outline" />
                  </v-avatar>
                </template>

                <v-list-item-title class="font-weight-bold text-break">
                  {{ displayGroupName(group) }}
                </v-list-item-title>
                <v-list-item-subtitle>
                  <div class="d-flex flex-wrap ga-2 mt-2">
                    <v-chip size="x-small" variant="tonal">
                      {{ $t('pages.permissions.groups.permissionCount', { count: group.permissions.length }) }}
                    </v-chip>
                    <v-chip size="x-small" variant="outlined">
                      {{ $t('pages.permissions.groups.bindingCount', { count: bindingCount(group.id) }) }}
                    </v-chip>
                    <v-chip v-if="group.builtin" size="x-small" color="info" variant="tonal">
                      {{ $t('pages.permissions.groups.builtin') }}
                    </v-chip>
                    <v-chip v-if="group.protected" size="x-small" color="warning" variant="tonal">
                      {{ $t('pages.permissions.groups.protected') }}
                    </v-chip>
                  </div>
                </v-list-item-subtitle>
              </v-list-item>
            </v-list>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" lg="8" class="pa-3">
        <v-card rounded="xl" elevation="0" class="panel-card mb-6">
          <v-card-title class="d-flex flex-column flex-md-row align-md-center justify-space-between ga-3">
            <div>
              <div class="text-h6">{{ detailTitle }}</div>
              <div v-if="selectedGroup" class="text-caption text-medium-emphasis">
                {{ selectedGroup.id }}
              </div>
            </div>
            <div class="d-flex flex-wrap ga-2">
              <v-btn
                color="primary"
                prepend-icon="mdi-content-save-outline"
                :loading="isSavingGroup"
                :disabled="!canSaveGroup"
                @click="saveSelectedGroup"
              >
                {{ $t('common.actions.action.save') }}
              </v-btn>
              <v-btn
                v-if="selectedGroup && !selectedGroup.builtin && !selectedGroup.protected"
                color="error"
                variant="tonal"
                prepend-icon="mdi-delete-outline"
                :loading="isSavingGroup"
                @click="deleteSelectedGroup"
              >
                {{ $t('common.actions.action.delete') }}
              </v-btn>
            </div>
          </v-card-title>

          <v-card-text>
            <v-row class="mx-n2">
              <v-col cols="12" md="4" class="pa-2">
                <v-text-field
                  v-model.trim="createGroupId"
                  :label="$t('pages.permissions.fields.groupId')"
                  :disabled="!isCreatingGroup"
                  variant="outlined"
                  density="comfortable"
                  rounded="lg"
                />
              </v-col>
              <v-col cols="12" md="4" class="pa-2">
                <v-text-field
                  v-model="groupForm.name"
                  :label="$t('pages.permissions.fields.name')"
                  variant="outlined"
                  density="comfortable"
                  rounded="lg"
                />
              </v-col>
              <v-col cols="12" md="4" class="pa-2">
                <v-switch
                  v-model="groupForm.protected"
                  :label="$t('pages.permissions.fields.protected')"
                  :disabled="Boolean(selectedGroup?.builtin)"
                  color="warning"
                  inset
                />
              </v-col>
              <v-col cols="12" class="pa-2">
                <v-textarea
                  v-model="groupForm.description"
                  :label="$t('pages.permissions.fields.description')"
                  rows="2"
                  auto-grow
                  variant="outlined"
                  density="comfortable"
                  rounded="lg"
                />
              </v-col>
              <v-col cols="12" md="6" class="pa-2">
                <v-combobox
                  v-model="groupForm.permissions"
                  :items="permissionSuggestions"
                  :label="$t('pages.permissions.fields.permissions')"
                  multiple
                  chips
                  closable-chips
                  clearable
                  variant="outlined"
                  density="comfortable"
                  rounded="lg"
                />
              </v-col>
              <v-col cols="12" md="6" class="pa-2">
                <v-combobox
                  v-model="groupForm.deniedPermissions"
                  :items="permissionSuggestions"
                  :label="$t('pages.permissions.fields.deniedPermissions')"
                  multiple
                  chips
                  closable-chips
                  clearable
                  variant="outlined"
                  density="comfortable"
                  rounded="lg"
                />
              </v-col>
            </v-row>

            <v-alert
              v-if="orphanPermissions.length > 0"
              type="warning"
              variant="tonal"
              class="mt-2"
              density="comfortable"
            >
              {{ $t('pages.permissions.orphans.title') }}:
              {{ orphanPermissions.join(', ') }}
            </v-alert>
          </v-card-text>
        </v-card>

        <v-row class="mx-n3">
          <v-col cols="12" xl="6" class="pa-3">
            <v-card rounded="xl" elevation="0" class="panel-card fill-height">
              <v-card-title>{{ $t('pages.permissions.bindings.title') }}</v-card-title>
              <v-card-text>
                <v-row class="mx-n2">
                  <v-col cols="12" md="7" class="pa-2">
                    <v-combobox
                      v-model="bindingScopeKey"
                      :items="bindingScopeItems"
                      :label="$t('pages.permissions.fields.scopeKey')"
                      variant="outlined"
                      density="comfortable"
                      rounded="lg"
                      clearable
                    />
                  </v-col>
                  <v-col cols="12" md="5" class="pa-2">
                    <v-select
                      v-model="bindingGroupIds"
                      :items="groupSelectItems"
                      :label="$t('pages.permissions.fields.groups')"
                      item-title="title"
                      item-value="value"
                      multiple
                      chips
                      closable-chips
                      variant="outlined"
                      density="comfortable"
                      rounded="lg"
                    />
                  </v-col>
                </v-row>
                <div class="d-flex flex-wrap ga-2 mb-4">
                  <v-btn
                    color="primary"
                    prepend-icon="mdi-link-variant"
                    :loading="isSavingBinding"
                    :disabled="!canSaveBinding"
                    @click="saveBinding"
                  >
                    {{ $t('pages.permissions.actions.saveBinding') }}
                  </v-btn>
                  <v-btn
                    color="error"
                    variant="tonal"
                    prepend-icon="mdi-link-variant-off"
                    :loading="isSavingBinding"
                    :disabled="!bindingScopeKey"
                    @click="deleteBinding"
                  >
                    {{ $t('pages.permissions.actions.deleteBinding') }}
                  </v-btn>
                </div>

                <div class="d-grid ga-3">
                  <div
                    v-for="binding in bindings"
                    :key="binding.scopeKey"
                    class="binding-row"
                    @click="editBinding(binding)"
                  >
                    <div class="text-body-2 font-weight-bold text-break">{{ binding.scopeKey }}</div>
                    <div class="d-flex flex-wrap ga-2 mt-2">
                      <v-chip
                        v-for="groupId in binding.groups"
                        :key="groupId"
                        size="small"
                        variant="tonal"
                      >
                        {{ displayGroupNameById(groupId) }}
                      </v-chip>
                    </div>
                  </div>
                  <div v-if="bindings.length === 0" class="empty-state py-8">
                    <v-icon icon="mdi-link-variant-off" size="56" color="grey-lighten-1" />
                    <div class="text-body-2 text-medium-emphasis mt-3">
                      {{ $t('pages.permissions.empty.bindings') }}
                    </div>
                  </div>
                </div>
              </v-card-text>
            </v-card>
          </v-col>

          <v-col cols="12" xl="6" class="pa-3">
            <v-card rounded="xl" elevation="0" class="panel-card fill-height">
              <v-card-title>{{ $t('pages.permissions.commands.title') }}</v-card-title>
              <v-card-text>
                <v-select
                  v-model="selectedCommandName"
                  :items="commandSelectItems"
                  :label="$t('pages.permissions.fields.command')"
                  item-title="title"
                  item-value="value"
                  variant="outlined"
                  density="comfortable"
                  rounded="lg"
                  class="mb-4"
                />

                <template v-if="selectedCommand">
                  <div class="d-grid ga-3 mb-4">
                    <div>
                      <div class="text-caption text-medium-emphasis">
                        {{ $t('pages.permissions.commands.defaultPermission') }}
                      </div>
                      <div class="text-body-2 text-break">
                        {{ selectedCommand.defaultPermission || $t('pages.permissions.empty.permission') }}
                      </div>
                    </div>
                    <div>
                      <div class="text-caption text-medium-emphasis">
                        {{ $t('pages.permissions.commands.currentPermission') }}
                      </div>
                      <div class="d-flex flex-wrap align-center ga-2">
                        <span class="text-body-2 text-break">
                          {{ selectedCommand.permission || $t('pages.permissions.empty.permission') }}
                        </span>
                        <v-chip
                          v-if="selectedCommand.permissionOverridden"
                          size="x-small"
                          color="warning"
                          variant="tonal"
                        >
                          {{ $t('pages.permissions.commands.overridden') }}
                        </v-chip>
                      </div>
                    </div>
                  </div>

                  <v-text-field
                    v-model.trim="commandPermissionDraft"
                    :label="$t('pages.permissions.fields.commandPermission')"
                    variant="outlined"
                    density="comfortable"
                    rounded="lg"
                  />

                  <div class="d-flex flex-wrap ga-2 mb-4">
                    <v-btn
                      color="primary"
                      prepend-icon="mdi-shield-edit-outline"
                      :loading="isSavingCommand"
                      @click="saveCommandPermission"
                    >
                      {{ $t('pages.permissions.actions.saveCommandPermission') }}
                    </v-btn>
                    <v-btn
                      variant="tonal"
                      prepend-icon="mdi-restore"
                      :loading="isSavingCommand"
                      @click="resetCommandPermission"
                    >
                      {{ $t('pages.permissions.actions.resetCommandPermission') }}
                    </v-btn>
                  </div>

                  <v-row class="mx-n2">
                    <v-col cols="12" md="7" class="pa-2">
                      <v-select
                        v-model="commandTargetGroupId"
                        :items="groupSelectItems"
                        :label="$t('pages.permissions.fields.targetGroup')"
                        item-title="title"
                        item-value="value"
                        variant="outlined"
                        density="comfortable"
                        rounded="lg"
                      />
                    </v-col>
                    <v-col cols="12" md="5" class="pa-2 d-flex align-start">
                      <v-btn
                        block
                        color="secondary"
                        prepend-icon="mdi-plus-box-outline"
                        :loading="isSavingGroup"
                        :disabled="!canAddCommandToGroup"
                        @click="addCommandPermissionToGroup"
                      >
                        {{ $t('pages.permissions.actions.addCommandToGroup') }}
                      </v-btn>
                    </v-col>
                  </v-row>
                </template>

                <div v-else class="empty-state py-8">
                  <v-icon icon="mdi-console-line" size="56" color="grey-lighten-1" />
                  <div class="text-body-2 text-medium-emphasis mt-3">
                    {{ $t('pages.permissions.empty.commands') }}
                  </div>
                </div>
              </v-card-text>
            </v-card>
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
import AppPageHeader from '@/components/AppPageHeader.vue'
import { commandsApi, type CommandDefinition } from '@/api/commands'
import {
  permissionsApi,
  type PermissionBinding,
  type PermissionGroup,
} from '@/api/permissions'
import { apiClient } from '@/api/client'
import { translate } from '@/plugins/i18n'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

interface PermissionCommand extends CommandDefinition {
  defaultPermission: string
  permissionOverridden: boolean
}

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

const groupForm = ref({
  name: '',
  description: '',
  permissions: [] as string[],
  deniedPermissions: [] as string[],
  protected: false,
})

const selectedGroup = computed(() => groups.value.find((group) => group.id === selectedGroupId.value))
const selectedCommand = computed(() =>
  commands.value.find((command) => command.name === selectedCommandName.value)
)

const overriddenCommandCount = computed(
  () => commands.value.filter((command) => Boolean(command.permissionOverridden)).length
)

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
  groupForm.value = {
    name: '',
    description: '',
    permissions: [],
    deniedPermissions: [],
    protected: false,
  }
}

async function saveSelectedGroup() {
  const payload = groupPayloadFromForm()
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
  const permission = commandPermissionDraft.value.trim() || selectedCommand.value?.permission || ''
  const targetGroup = groups.value.find((group) => group.id === commandTargetGroupId.value)
  if (!permission || !targetGroup) {
    return
  }

  const mergedPermissions = Array.from(new Set([...targetGroup.permissions, permission])).sort()
  isSavingGroup.value = true
  errorMessage.value = ''
  try {
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
    errorMessage.value = getErrorMessage(error, translate('pages.permissions.messages.commandAddFailed'))
  } finally {
    isSavingGroup.value = false
  }
}

function formFromGroup(group: PermissionGroup) {
  return {
    name: group.name,
    description: group.description,
    permissions: group.permissions.filter((permission) => !permission.startsWith('-')).sort(),
    deniedPermissions: group.permissions
      .filter((permission) => permission.startsWith('-'))
      .map(stripDenyPrefix)
      .sort(),
    protected: group.protected,
  }
}

function groupPayloadFromForm() {
  return {
    name: groupForm.value.name,
    description: groupForm.value.description,
    protected: groupForm.value.protected,
    permissions: [
      ...normalizePermissionList(groupForm.value.permissions),
      ...normalizePermissionList(groupForm.value.deniedPermissions).map((permission) => `-${permission}`),
    ],
  }
}

function normalizePermissionList(values: string[]) {
  return Array.from(new Set(values.map(stripDenyPrefix).map((value) => value.trim()).filter(Boolean))).sort()
}

function stripDenyPrefix(value: string) {
  return String(value).trim().replace(/^-+/, '')
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

function normalizeGroup(group: PermissionGroup): PermissionGroup {
  const builtin = Boolean(group.builtin || group.system)
  return {
    id: String(group.id),
    name: group.name || '',
    description: group.description || '',
    permissions: Array.isArray(group.permissions) ? [...group.permissions].sort() : [],
    orphanPermissions: Array.isArray(group.orphanPermissions)
      ? [...group.orphanPermissions].sort()
      : [],
    builtin,
    system: builtin,
    protected: Boolean(group.protected),
  }
}

function normalizeBinding(binding: PermissionBinding & { key?: string; groupIds?: string[] }): PermissionBinding {
  const groups = Array.isArray(binding.groups)
    ? [...binding.groups].sort()
    : Array.isArray(binding.groupIds)
      ? [...binding.groupIds].sort()
      : []
  return {
    scopeKey: String(binding.scopeKey ?? binding.key ?? ''),
    groups,
    groupIds: groups,
  }
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

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.summary-card,
.panel-card {
  @include surface-card;
  @include hover-lift;
}

.group-list {
  background: transparent;
}

.group-list-item {
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
}

.binding-row {
  cursor: pointer;
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
  border-radius: 12px;
  padding: 14px;
  transition: border-color 0.16s ease, background-color 0.16s ease;
}

.binding-row:hover {
  border-color: rgba(var(--v-theme-primary), 0.24);
  background: rgba(var(--v-theme-primary), 0.04);
}

.empty-state {
  text-align: center;
}

.d-grid {
  display: grid;
}

.min-w-0 {
  min-width: 0;
}
</style>
