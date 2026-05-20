<template>
  <v-card class="platform-card h-100" hover>
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="primary" variant="tonal" icon="mdi-message-processing-outline" />
      </template>
      <v-card-title class="text-break">
        {{ displayName }}
      </v-card-title>
      <v-card-subtitle class="text-truncate">
        {{ platform.id }}
      </v-card-subtitle>
      <template #append>
        <v-menu>
          <template #activator="{ props }">
            <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
          </template>
          <v-list>
            <v-list-item @click="emit('edit', platform)">
              <v-list-item-title>{{ editLabel }}</v-list-item-title>
            </v-list-item>
            <v-list-item @click="emit('delete', platform)">
              <v-list-item-title>{{ deleteLabel }}</v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
      </template>
    </v-card-item>

    <v-card-text class="pt-2">
      <div class="platform-card-chips">
        <v-chip :color="platform.enabled ? 'success' : 'grey'" size="small" variant="tonal">
          {{ platform.enabled ? enabledLabel : disabledLabel }}
        </v-chip>
        <v-chip
          :color="connection.color"
          size="small"
          variant="tonal"
          :prepend-icon="connection.icon"
        >
          {{ connection.label }}
        </v-chip>
        <v-chip color="info" size="small" variant="tonal">
          {{ adapterLabel }}
        </v-chip>
      </div>

      <div class="platform-meta-row">
        <span>{{ configLabel }}</span>
        <strong>{{ configFieldCount }}</strong>
      </div>
      <div class="platform-meta-row">
        <span>{{ updatedLabel }}</span>
        <strong>{{ updatedAt }}</strong>
      </div>
    </v-card-text>

    <v-card-actions>
      <v-btn
        color="primary"
        variant="text"
        size="small"
        prepend-icon="mdi-pencil"
        @click="emit('edit', platform)"
      >
        {{ configureLabel }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import type { MessagePlatformDraft } from './types'

interface Props {
  platform: MessagePlatformDraft
  displayName: string
  adapterLabel: string
  configFieldCount: string
  updatedAt: string
  enabledLabel: string
  disabledLabel: string
  configLabel: string
  updatedLabel: string
  configureLabel: string
  editLabel: string
  deleteLabel: string
  connection: {
    color: string
    icon: string
    label: string
  }
}

defineProps<Props>()

const emit = defineEmits<{
  edit: [platform: MessagePlatformDraft]
  delete: [platform: MessagePlatformDraft]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.platform-card {
  @include surface-card;
  @include hover-lift;
}

.platform-card-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 14px;
}

.platform-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 0;
  border-top: 1px solid $border-color-soft;
  color: rgba(var(--v-theme-on-surface), 0.66);
  font-size: $font-size-sm;
}

.platform-meta-row strong {
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-weight: 700;
}
</style>
