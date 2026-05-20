<template>
  <v-data-table
    :headers="headers"
    :items="items"
    :loading="loading"
    hide-default-footer
    class="platform-table"
  >
    <template #item.name="{ item }">
      <div class="platform-name-cell">
        <span class="font-weight-medium">{{ displayName(row(item)) }}</span>
        <span class="text-caption text-medium-emphasis">{{ row(item).id }}</span>
      </div>
    </template>

    <template #item.adapter="{ item }">
      <v-chip size="small" color="info" variant="tonal">
        {{ adapterLabel(row(item).adapter) }}
      </v-chip>
    </template>

    <template #item.enabled="{ item }">
      <v-chip :color="row(item).enabled ? 'success' : 'grey'" size="small" variant="tonal">
        {{ row(item).enabled ? enabledLabel : disabledLabel }}
      </v-chip>
    </template>

    <template #item.connection="{ item }">
      <v-chip
        :color="connectionStatus(row(item)).color"
        size="small"
        variant="tonal"
        :prepend-icon="connectionStatus(row(item)).icon"
      >
        {{ connectionStatus(row(item)).label }}
      </v-chip>
    </template>

    <template #item.config="{ item }">
      {{ configFieldCount(row(item).config) }}
    </template>

    <template #item.lastModified="{ item }">
      {{ formatTimestamp(row(item).lastModified) }}
    </template>

    <template #item.actions="{ item }">
      <v-btn icon="mdi-pencil" size="small" variant="text" @click="emit('edit', row(item))" />
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
import type { ConfigRecord } from '@/api/config'
import type { MessagePlatformDraft } from './types'

interface TableHeader {
  title: string
  value: string
  width?: string
  sortable?: boolean
}

interface Props {
  headers: TableHeader[]
  items: MessagePlatformDraft[]
  loading: boolean
  displayName: (platform: MessagePlatformDraft) => string
  adapterLabel: (adapter: string) => string
  configFieldCount: (config: ConfigRecord) => string
  formatTimestamp: (value?: number) => string
  connectionStatus: (platform: MessagePlatformDraft) => {
    color: string
    icon: string
    label: string
  }
  enabledLabel: string
  disabledLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  edit: [platform: MessagePlatformDraft]
  delete: [platform: MessagePlatformDraft]
}>()

function row(item: { raw: MessagePlatformDraft } | MessagePlatformDraft): MessagePlatformDraft {
  return 'raw' in item ? item.raw : item
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.platform-table {
  @include surface-card;
}

.platform-name-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}
</style>
