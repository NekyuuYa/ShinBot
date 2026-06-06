<template>
  <v-card rounded="xl" elevation="0" class="command-card">
    <v-card-text>
      <div class="d-flex align-start justify-space-between ga-4">
        <div class="min-w-0">
          <div class="text-overline text-medium-emphasis">
            {{ command.owner || $t('pages.commands.owner.core') }}
          </div>
          <div class="text-h6 font-weight-bold text-break">{{ command.name }}</div>
        </div>
        <v-switch
          :model-value="command.enabled"
          color="primary"
          hide-details
          inset
          :loading="saving"
          @update:model-value="emit('update:enabled', command.name, Boolean($event))"
        />
      </div>

      <div class="mt-3 d-flex flex-wrap ga-2">
        <v-chip size="small" variant="tonal">{{ $t(`pages.commands.mode.${command.mode}`) }}</v-chip>
        <v-chip size="small" variant="outlined">{{ command.priorityLabel }}</v-chip>
        <v-chip size="small" :color="command.enabled ? 'success' : 'default'" variant="tonal">
          {{ command.enabled ? $t('pages.commands.status.enabled') : $t('pages.commands.status.disabled') }}
        </v-chip>
      </div>

      <p class="text-body-2 text-medium-emphasis mt-4 mb-0">
        {{ command.description || $t('pages.commands.empty.description') }}
      </p>

      <div class="mt-4 d-grid ga-3 text-body-2">
        <div>
          <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.triggers') }}</div>
          <div class="text-break">{{ command.triggers.join(', ') }}</div>
        </div>
        <div v-if="command.usage">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.usage') }}</div>
          <div class="text-break">{{ command.usage }}</div>
        </div>
        <div>
          <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.permission') }}</div>
          <div class="d-flex flex-wrap align-center ga-2">
            <span class="text-break">{{ command.permission || $t('pages.commands.empty.permission') }}</span>
            <v-chip
              v-if="command.permissionOverridden"
              size="x-small"
              color="warning"
              variant="tonal"
            >
              {{ $t('pages.commands.permission.overridden') }}
            </v-chip>
          </div>
          <div
            v-if="command.defaultPermission && command.defaultPermission !== command.permission"
            class="text-caption text-medium-emphasis mt-1"
          >
            {{ $t('pages.commands.permission.default') }}:
            {{ command.defaultPermission }}
          </div>
        </div>
        <div v-if="command.pattern">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.pattern') }}</div>
          <div class="text-break">{{ command.pattern }}</div>
        </div>
      </div>

      <div class="mt-4">
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

.command-card {
  @include surface-card;
  @include hover-lift;
}
</style>
