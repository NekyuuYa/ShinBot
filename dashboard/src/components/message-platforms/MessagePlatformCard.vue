<template>
  <resource-entity-card-shell
    :title="displayName"
    :subtitle="platform.id"
    icon="mdi-message-processing-outline"
    :configure-label="configureLabel"
    :edit-label="editLabel"
    :delete-label="deleteLabel"
    @edit="emit('edit', platform)"
    @delete="emit('delete', platform)"
  >
    <template #chips>
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
    </template>

    <resource-meta-row :label="configLabel" :value="configFieldCount" />
    <resource-meta-row :label="updatedLabel" :value="updatedAt" />
  </resource-entity-card-shell>
</template>

<script setup lang="ts">
import ResourceEntityCardShell from '@/components/resources/ResourceEntityCardShell.vue'
import ResourceMetaRow from '@/components/resources/ResourceMetaRow.vue'
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
