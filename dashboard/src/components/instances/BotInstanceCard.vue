<template>
  <v-card class="bot-card h-100" hover>
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="primary" variant="tonal" icon="mdi-robot-outline" />
      </template>
      <v-card-title class="text-break">
        {{ displayName }}
      </v-card-title>
      <v-card-subtitle class="text-truncate">
        {{ bot.id }}
      </v-card-subtitle>
      <template #append>
        <v-menu>
          <template #activator="{ props }">
            <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
          </template>
          <v-list>
            <v-list-item @click="emit('edit', bot)">
              <v-list-item-title>{{ editLabel }}</v-list-item-title>
            </v-list-item>
            <v-list-item @click="emit('delete', bot)">
              <v-list-item-title>{{ deleteLabel }}</v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
      </template>
    </v-card-item>

    <v-card-text class="pt-2">
      <div class="bot-card-chips">
        <v-chip :color="bot.enabled ? 'success' : 'grey'" size="small" variant="tonal">
          {{ bot.enabled ? enabledLabel : disabledLabel }}
        </v-chip>
        <v-chip color="info" size="small" variant="tonal">
          {{ agentModeLabel }}
        </v-chip>
      </div>

      <div class="bot-meta-row">
        <span>{{ bindingsLabel }}</span>
        <strong>{{ bot.bindings.length }}</strong>
      </div>
      <div class="bot-meta-row">
        <span>{{ platformsLabel }}</span>
        <strong>{{ platformSummary }}</strong>
      </div>
      <div class="bot-meta-row">
        <span>{{ platformHealthLabel }}</span>
        <strong>{{ platformHealthSummary }}</strong>
      </div>
      <div class="bot-meta-row">
        <span>{{ commandsLabel }}</span>
        <strong>{{
          bot.commands.enabled ? bot.commands.prefixes.join(' ') : disabledLabel
        }}</strong>
      </div>
    </v-card-text>

    <v-card-actions>
      <v-btn
        color="primary"
        variant="text"
        size="small"
        prepend-icon="mdi-pencil"
        @click="emit('edit', bot)"
      >
        {{ configureLabel }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import type { BotInstanceDraft } from './botTypes'

interface Props {
  bot: BotInstanceDraft
  displayName: string
  platformSummary: string
  platformHealthLabel: string
  platformHealthSummary: string
  agentModeLabel: string
  bindingsLabel: string
  platformsLabel: string
  commandsLabel: string
  configureLabel: string
  editLabel: string
  deleteLabel: string
  enabledLabel: string
  disabledLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  edit: [bot: BotInstanceDraft]
  delete: [bot: BotInstanceDraft]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.bot-card {
  @include surface-card;
  @include hover-lift;
}

.bot-card-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 14px;
}

.bot-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 0;
  border-top: 1px solid $border-color-soft;
  color: rgba(var(--v-theme-on-surface), 0.66);
  font-size: $font-size-sm;
}

.bot-meta-row strong {
  max-width: 62%;
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-weight: 700;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
