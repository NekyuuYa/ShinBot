<template>
  <v-card class="prompt-card h-100 d-flex flex-column" elevation="0">
    <v-card-item>
      <template #prepend>
        <v-avatar color="primary" variant="tonal" icon="mdi-text-box-outline" />
      </template>
      <v-card-title class="text-break">{{ item.name }}</v-card-title>
      <v-card-subtitle>{{ item.promptId }}</v-card-subtitle>
      <template #append>
        <v-switch
          :model-value="item.enabled"
          color="success"
          density="compact"
          hide-details
          @update:model-value="handleToggle"
        />
      </template>
    </v-card-item>

    <v-card-text class="pt-1 flex-grow-1">
      <div v-if="item.description" class="text-body-2 text-medium-emphasis mb-2">
        {{ item.description }}
      </div>
      <div class="d-flex flex-wrap ga-2 mb-2">
        <v-chip size="small" color="info" variant="tonal">
          {{ stageLabel }}
        </v-chip>
        <v-chip size="small" variant="tonal">
          {{ kindLabel }}
        </v-chip>
        <v-chip size="small" variant="outlined">
          v{{ item.version }}
        </v-chip>
        <v-chip size="small" variant="outlined">
          P{{ item.priority }}
        </v-chip>
      </div>
      <div class="d-flex flex-wrap ga-2">
        <v-chip
          v-for="tag in item.tags"
          :key="`${item.uuid}-${tag}`"
          size="small"
          color="secondary"
          variant="tonal"
        >
          {{ tag }}
        </v-chip>
        <v-chip
          v-if="item.tags.length === 0"
          size="small"
          color="grey"
          variant="tonal"
        >
          {{ emptyTagLabel }}
        </v-chip>
      </div>
    </v-card-text>

    <v-card-actions>
      <v-btn variant="text" prepend-icon="mdi-pencil" @click="emit('edit', item)">
        {{ editLabel }}
      </v-btn>
      <v-spacer />
      <v-btn
        color="error"
        variant="text"
        prepend-icon="mdi-delete-outline"
        @click="emit('delete', item)"
      >
        {{ deleteLabel }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import type { PromptDefinition } from '@/api/promptDefinitions'

interface Props {
  item: PromptDefinition
  stageLabel: string
  kindLabel: string
  emptyTagLabel: string
  editLabel: string
  deleteLabel: string
}

const props = defineProps<Props>()

const emit = defineEmits<{
  edit: [item: PromptDefinition]
  delete: [item: PromptDefinition]
  toggle: [uuid: string, enabled: boolean]
}>()

const handleToggle = (value: boolean | null) => {
  emit('toggle', props.item.uuid, Boolean(value))
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.prompt-card {
  @include surface-card;
  @include hover-lift;
}
</style>
