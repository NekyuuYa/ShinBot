<template>
  <v-card rounded="xl" elevation="0" class="command-row">
    <v-card-text class="d-flex flex-column flex-lg-row align-lg-center ga-4">
      <div class="flex-grow-1 min-w-0">
        <div class="d-flex flex-wrap align-center ga-2 mb-1">
          <div class="text-h6 font-weight-bold text-break">{{ command.name }}</div>
          <v-chip size="small" variant="tonal">{{ $t(`pages.commands.mode.${command.mode}`) }}</v-chip>
          <v-chip size="small" variant="outlined">{{ command.priorityLabel }}</v-chip>
        </div>
        <div class="text-body-2 text-medium-emphasis">
          {{ command.description || $t('pages.commands.empty.description') }}
        </div>
        <div class="mt-3 d-flex flex-wrap ga-4 text-body-2">
          <div class="text-break">
            <span class="text-medium-emphasis">{{ $t('pages.commands.fields.owner') }}:</span>
            {{ command.owner || $t('pages.commands.owner.core') }}
          </div>
          <div class="text-break">
            <span class="text-medium-emphasis">{{ $t('pages.commands.fields.triggers') }}:</span>
            {{ command.triggers.join(', ') }}
          </div>
          <div class="text-break">
            <span class="text-medium-emphasis">{{ $t('pages.commands.fields.permission') }}:</span>
            {{ command.permission || $t('pages.commands.empty.permission') }}
          </div>
          <div class="text-break">
            <span class="text-medium-emphasis">{{ $t('pages.commands.fields.defaultPermission') }}:</span>
            {{ command.defaultPermission || $t('pages.commands.empty.permission') }}
          </div>
          <div class="d-flex flex-wrap align-center ga-2">
            <span class="text-medium-emphasis">{{ $t('pages.commands.fields.permissionOverridden') }}:</span>
            <v-chip
              size="small"
              :color="command.permissionOverridden ? 'warning' : 'default'"
              variant="tonal"
            >
              {{
                command.permissionOverridden
                  ? $t('pages.commands.permission.overridden')
                  : $t('pages.commands.permission.inherited')
              }}
            </v-chip>
          </div>
        </div>
      </div>

      <div class="d-flex flex-wrap align-center ga-3">
        <v-btn
          size="small"
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-shield-plus-outline"
          :disabled="!command.permission"
          @click="emit('add-permission', command.name)"
        >
          {{ $t('pages.commands.actions.addToGroup') }}
        </v-btn>
        <v-chip :color="command.enabled ? 'success' : 'default'" variant="tonal">
          {{ command.enabled ? $t('pages.commands.status.enabled') : $t('pages.commands.status.disabled') }}
        </v-chip>
        <v-switch
          :model-value="command.enabled"
          color="primary"
          hide-details
          inset
          :loading="saving"
          @update:model-value="emit('update:enabled', command.name, Boolean($event))"
        />
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import type { CommandDefinition } from '@/api/commands'

interface Props {
  command: CommandDefinition
  saving?: boolean
}

withDefaults(defineProps<Props>(), {
  saving: false,
})

const emit = defineEmits<{
  'update:enabled': [name: string, enabled: boolean]
  'add-permission': [name: string]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.command-row {
  @include surface-card;
  @include hover-lift;
}
</style>
