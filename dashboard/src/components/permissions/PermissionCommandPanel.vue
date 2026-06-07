<template>
  <v-card rounded="xl" elevation="0" class="panel-card fill-height">
    <v-card-title>{{ $t('pages.permissions.commands.title') }}</v-card-title>
    <v-card-text>
      <v-select
        v-model="selectedCommandName"
        :items="commandItems"
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
          v-model.trim="permissionDraft"
          :label="$t('pages.permissions.fields.commandPermission')"
          variant="outlined"
          density="comfortable"
          rounded="lg"
        />

        <div class="d-flex flex-wrap ga-2 mb-4">
          <v-btn
            color="primary"
            prepend-icon="mdi-shield-edit-outline"
            :loading="savingCommand"
            @click="$emit('save-permission')"
          >
            {{ $t('pages.permissions.actions.saveCommandPermission') }}
          </v-btn>
          <v-btn
            variant="tonal"
            prepend-icon="mdi-restore"
            :loading="savingCommand"
            @click="$emit('reset-permission')"
          >
            {{ $t('pages.permissions.actions.resetCommandPermission') }}
          </v-btn>
        </div>

        <v-row class="mx-n2">
          <v-col cols="12" md="7" class="pa-2">
            <v-select
              v-model="targetGroupId"
              :items="groupItems"
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
              :loading="savingGroup"
              :disabled="!canAddToGroup"
              @click="$emit('add-to-group')"
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
</template>

<script setup lang="ts">
import type {
  PermissionCommand,
  PermissionSelectItem,
} from '@/components/permissions/permissionUtils'

const selectedCommandName = defineModel<string>('selectedCommandName', { required: true })
const permissionDraft = defineModel<string>('permissionDraft', { required: true })
const targetGroupId = defineModel<string>('targetGroupId', { required: true })

defineProps<{
  selectedCommand: PermissionCommand | undefined
  commandItems: PermissionSelectItem[]
  groupItems: PermissionSelectItem[]
  savingCommand: boolean
  savingGroup: boolean
  canAddToGroup: boolean
}>()

defineEmits<{
  'save-permission': []
  'reset-permission': []
  'add-to-group': []
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.panel-card {
  @include surface-card;
  @include hover-lift;
}

.empty-state {
  text-align: center;
}

.d-grid {
  display: grid;
}
</style>
