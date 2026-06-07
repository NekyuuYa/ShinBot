<template>
  <v-card rounded="xl" elevation="0" class="panel-card mb-6">
    <v-card-title class="d-flex flex-column flex-md-row align-md-center justify-space-between ga-3">
      <div>
        <div class="text-h6">{{ title }}</div>
        <div v-if="selectedGroup" class="text-caption text-medium-emphasis">
          {{ selectedGroup.id }}
        </div>
      </div>
      <div class="d-flex flex-wrap ga-2">
        <v-btn
          color="primary"
          prepend-icon="mdi-content-save-outline"
          :loading="saving"
          :disabled="!canSave"
          @click="$emit('save')"
        >
          {{ $t('common.actions.action.save') }}
        </v-btn>
        <v-btn
          v-if="selectedGroup && !selectedGroup.builtin && !selectedGroup.protected"
          color="error"
          variant="tonal"
          prepend-icon="mdi-delete-outline"
          :loading="saving"
          @click="$emit('delete')"
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
            v-model="form.name"
            :label="$t('pages.permissions.fields.name')"
            variant="outlined"
            density="comfortable"
            rounded="lg"
          />
        </v-col>
        <v-col cols="12" md="4" class="pa-2">
          <v-switch
            v-model="form.protected"
            :label="$t('pages.permissions.fields.protected')"
            :disabled="Boolean(selectedGroup?.builtin)"
            color="warning"
            inset
          />
        </v-col>
        <v-col cols="12" class="pa-2">
          <v-textarea
            v-model="form.description"
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
            v-model="form.permissions"
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
            v-model="form.deniedPermissions"
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
</template>

<script setup lang="ts">
import type { PermissionGroup } from '@/api/permissions'
import type { PermissionGroupForm } from '@/components/permissions/permissionUtils'

const createGroupId = defineModel<string>('createGroupId', { required: true })
const form = defineModel<PermissionGroupForm>('form', { required: true })

defineProps<{
  title: string
  selectedGroup: PermissionGroup | undefined
  isCreatingGroup: boolean
  permissionSuggestions: string[]
  orphanPermissions: string[]
  saving: boolean
  canSave: boolean
}>()

defineEmits<{
  save: []
  delete: []
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.panel-card {
  @include surface-card;
  @include hover-lift;
}
</style>
