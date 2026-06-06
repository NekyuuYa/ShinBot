<template>
  <resource-entity-card-shell
    :title="displayName"
    :subtitle="bot.id"
    icon="mdi-robot-outline"
    :configure-label="configureLabel"
    :edit-label="editLabel"
    :delete-label="deleteLabel"
    @edit="emit('edit', bot)"
    @delete="emit('delete', bot)"
  >
    <template #chips>
        <v-chip :color="bot.enabled ? 'success' : 'grey'" size="small" variant="tonal">
          {{ bot.enabled ? enabledLabel : disabledLabel }}
        </v-chip>
        <v-chip color="info" size="small" variant="tonal">
          {{ agentModeLabel }}
        </v-chip>
    </template>

    <resource-meta-row :label="bindingsLabel" :value="bot.bindings.length" />
    <resource-meta-row :label="platformsLabel" :value="platformSummary" truncate-value />
    <resource-meta-row :label="platformHealthLabel" :value="platformHealthSummary" truncate-value />
    <resource-meta-row
      :label="commandsLabel"
      :value="bot.commands.enabled ? bot.commands.prefixes.join(' ') : disabledLabel"
      truncate-value
    />
  </resource-entity-card-shell>
</template>

<script setup lang="ts">
import ResourceEntityCardShell from '@/components/resources/ResourceEntityCardShell.vue'
import ResourceMetaRow from '@/components/resources/ResourceMetaRow.vue'
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
