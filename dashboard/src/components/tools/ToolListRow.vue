<template>
  <v-card class="tool-row" rounded="xl" elevation="0">
    <v-card-text class="pa-4">
      <div class="tool-row-grid">
        <div>
          <div class="text-subtitle1 font-weight-bold">{{ tool.displayName }}</div>
          <div class="text-caption text-medium-emphasis">{{ tool.name }}</div>
        </div>
        <div class="text-body-2 text-medium-emphasis tool-row-description">
          {{ tool.description || $t('pages.tools.empty.description') }}
        </div>
        <div class="text-body-2">
          <div class="font-weight-medium">{{ $t(`pages.tools.ownerTypeOptions.${tool.ownerType}`) }}</div>
          <div class="text-caption text-medium-emphasis">{{ tool.ownerId }}</div>
        </div>
        <div class="text-body-2">
          <div class="font-weight-medium">{{ tool.permission || $t('pages.tools.empty.permission') }}</div>
          <div class="text-caption text-medium-emphasis">
            {{ $t('pages.tools.timeoutValue', { value: tool.timeoutSeconds }) }}
          </div>
        </div>
        <tool-meta-chips :tool="tool" />
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import type { ToolDefinition } from '@/api/tools'
import ToolMetaChips from './ToolMetaChips.vue'

interface Props {
  tool: ToolDefinition
}

defineProps<Props>()
</script>

<style scoped>
.tool-row {
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  background: rgba(var(--v-theme-surface), 0.95);
}

.tool-row-grid {
  display: grid;
  grid-template-columns: minmax(180px, 1.2fr) minmax(220px, 1.8fr) minmax(160px, 1fr) minmax(180px, 1.2fr) minmax(220px, 1.2fr);
  gap: 16px;
  align-items: center;
}

.tool-row-description {
  line-height: 1.6;
}

@media (max-width: 1264px) {
  .tool-row-grid {
    grid-template-columns: 1fr 1fr;
  }
}

@media (max-width: 760px) {
  .tool-row-grid {
    grid-template-columns: 1fr;
  }
}
</style>
