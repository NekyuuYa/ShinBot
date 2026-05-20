<template>
  <v-data-table
    :headers="headers"
    :items="items"
    :loading="loading"
    hide-default-footer
    class="bot-table"
  >
    <template #item.display_name="{ item }">
      <div class="bot-name-cell">
        <span class="font-weight-medium">{{ displayName(row(item)) }}</span>
        <span class="text-caption text-medium-emphasis">{{ row(item).id }}</span>
      </div>
    </template>

    <template #item.enabled="{ item }">
      <v-chip :color="row(item).enabled ? 'success' : 'grey'" size="small" variant="tonal">
        {{ row(item).enabled ? enabledLabel : disabledLabel }}
      </v-chip>
    </template>

    <template #item.agent="{ item }">
      <v-chip color="info" size="small" variant="tonal">
        {{ agentModeLabel(row(item).agent.mode) }}
      </v-chip>
    </template>

    <template #item.bindings="{ item }">
      {{ row(item).bindings.length }}
    </template>

    <template #item.platforms="{ item }">
      {{ platformSummary(row(item)) }}
    </template>

    <template #item.actions="{ item }">
      <v-btn
        icon="mdi-pencil"
        size="small"
        variant="text"
        @click="emit('edit', row(item))"
      />
      <v-btn
        icon="mdi-delete"
        size="small"
        variant="text"
        color="error"
        @click="emit('delete', row(item))"
      />
    </template>
  </v-data-table>
</template>

<script setup lang="ts">
import type { BotInstanceDraft } from './botTypes'

interface TableHeader {
  title: string
  value: string
  width?: string
  sortable?: boolean
}

interface Props {
  headers: TableHeader[]
  items: BotInstanceDraft[]
  loading: boolean
  displayName: (bot: BotInstanceDraft) => string
  platformSummary: (bot: BotInstanceDraft) => string
  agentModeLabel: (mode: string) => string
  enabledLabel: string
  disabledLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  edit: [bot: BotInstanceDraft]
  delete: [bot: BotInstanceDraft]
}>()

function row(item: { raw: BotInstanceDraft } | BotInstanceDraft): BotInstanceDraft {
  return 'raw' in item ? item.raw : item
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.bot-table {
  @include surface-card;
}

.bot-name-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}
</style>
