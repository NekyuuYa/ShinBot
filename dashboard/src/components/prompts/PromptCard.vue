<template>
  <v-card class="prompt-card h-100 d-flex flex-column" elevation="0">
    <v-card-item>
      <template #prepend>
        <v-avatar color="primary" variant="tonal" icon="mdi-text-box-outline" />
      </template>
      <v-card-title class="text-break">{{ item.displayName }}</v-card-title>
      <v-card-subtitle>{{ item.id }}</v-card-subtitle>
      <template #append>
        <v-switch
          :model-value="item.enabled"
          color="success"
          density="compact"
          :disabled="item.layer === 'runtime'"
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
        <v-chip size="small" color="primary" variant="tonal">
          {{ layerLabel }}
        </v-chip>
        <v-chip size="small" variant="outlined">
          {{ item.locale }}
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
          :key="`${item.fileId}-${tag}`"
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
        v-if="item.resettable"
        color="secondary"
        variant="text"
        prepend-icon="mdi-restore"
        @click="emit('reset', item)"
      >
        {{ resetLabel }}
      </v-btn>
      <v-btn
        v-if="item.deletable"
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
import type { PromptCatalogItem } from '@/api/prompts'

interface Props {
  item: PromptCatalogItem
  stageLabel: string
  kindLabel: string
  layerLabel: string
  emptyTagLabel: string
  editLabel: string
  deleteLabel: string
  resetLabel: string
}

const props = defineProps<Props>()

const emit = defineEmits<{
  edit: [item: PromptCatalogItem]
  delete: [item: PromptCatalogItem]
  reset: [item: PromptCatalogItem]
  toggle: [uuid: string, enabled: boolean]
}>()

const handleToggle = (value: boolean | null) => {
  emit('toggle', props.item.fileId, Boolean(value))
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.prompt-card {
  @include surface-card;
  @include hover-lift;
}
</style>
