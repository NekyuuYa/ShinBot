<template>
  <v-card class="agent-card h-100 d-flex flex-column" elevation="0">
    <v-card-item>
      <template #prepend>
        <v-avatar color="primary" variant="tonal" icon="mdi-account-cog-outline" />
      </template>
      <v-card-title class="text-break">
        {{ profile.agentId || profile.fileName }}
      </v-card-title>
      <v-card-subtitle>{{ profile.path }}</v-card-subtitle>
      <template #append>
        <v-chip
          :color="profile.issues.length > 0 ? 'warning' : 'success'"
          size="small"
          variant="tonal"
        >
          {{
            profile.issues.length > 0
              ? issueCountLabel(profile.issues.length)
              : validLabel
          }}
        </v-chip>
      </template>
    </v-card-item>

    <v-card-text class="pt-1 flex-grow-1">
      <div class="agent-meta-row">
        <span>{{ modeLabel }}</span>
        <strong>{{ profile.mode || noValueLabel }}</strong>
      </div>
      <div class="agent-meta-row">
        <span>{{ personaLabel }}</span>
        <strong>{{ profile.personaId || noValueLabel }}</strong>
      </div>
      <div class="agent-meta-row">
        <span>{{ updatedLabel }}</span>
        <strong>{{ formatTimestamp(profile.lastModified) }}</strong>
      </div>
    </v-card-text>

    <v-card-actions>
      <v-btn variant="text" prepend-icon="mdi-pencil" @click="$emit('edit', profile)">
        {{ editLabel }}
      </v-btn>
      <v-spacer />
      <v-btn
        color="error"
        variant="text"
        prepend-icon="mdi-delete-outline"
        @click="$emit('remove', profile)"
      >
        {{ deleteLabel }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import type { AgentConfigProfile } from '@/api/agentConfigs'

defineProps<{
  profile: AgentConfigProfile
  modeLabel: string
  personaLabel: string
  updatedLabel: string
  noValueLabel: string
  validLabel: string
  editLabel: string
  deleteLabel: string
  issueCountLabel: (count: number) => string
  formatTimestamp: (value: number) => string
}>()

defineEmits<{
  edit: [profile: AgentConfigProfile]
  remove: [profile: AgentConfigProfile]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.agent-card {
  @include surface-card;
  @include hover-lift;
}

.agent-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 0;
  border-bottom: 1px solid $border-color-soft;

  &:last-child {
    border-bottom: 0;
  }

  span {
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
  }

  strong {
    min-width: 0;
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.88);
    font-size: $font-size-xs;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}
</style>
