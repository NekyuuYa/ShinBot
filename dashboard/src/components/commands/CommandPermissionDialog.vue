<template>
  <v-dialog
    :model-value="modelValue"
    max-width="520"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <v-card rounded="xl">
      <v-card-title>{{ $t('pages.commands.permission.addDialogTitle') }}</v-card-title>
      <v-card-text>
        <div v-if="command" class="mb-4">
          <div class="text-caption text-medium-emphasis">
            {{ $t('pages.commands.permission.required') }}
          </div>
          <div class="text-body-2 text-break">
            {{ command.permission || $t('pages.commands.empty.permission') }}
          </div>
        </div>

        <v-select
          :model-value="targetGroupId"
          :items="groupItems"
          :label="$t('pages.commands.permission.targetGroup')"
          item-title="title"
          item-value="value"
          variant="outlined"
          density="comfortable"
          rounded="lg"
          @update:model-value="emit('update:targetGroupId', String($event ?? ''))"
        />
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="emit('update:modelValue', false)">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn
          color="primary"
          :loading="saving"
          :disabled="!canSubmit"
          @click="emit('submit')"
        >
          {{ $t('common.actions.action.add') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import type { CommandDefinition } from '@/api/commands'

export interface CommandPermissionGroupItem {
  title: string
  value: string
}

interface Props {
  modelValue: boolean
  command?: CommandDefinition
  targetGroupId: string
  groupItems: readonly CommandPermissionGroupItem[]
  saving?: boolean
  canSubmit: boolean
}

withDefaults(defineProps<Props>(), {
  command: undefined,
  saving: false,
})

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'update:targetGroupId': [value: string]
  submit: []
}>()
</script>
