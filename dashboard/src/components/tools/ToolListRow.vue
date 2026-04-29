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
        <div class="tool-meta-chips">
          <v-chip size="small" :color="tool.enabled ? 'success' : 'grey'" variant="tonal">
            {{ tool.enabled ? $t('pages.tools.status.enabled') : $t('pages.tools.status.disabled') }}
          </v-chip>
          <v-chip size="small" variant="outlined">
            {{ $t(`pages.tools.visibilityOptions.${tool.visibility}`) }}
          </v-chip>
          <v-chip size="small" :color="riskColor" variant="outlined">
            {{ $t(`pages.tools.riskOptions.${tool.riskLevel}`) }}
          </v-chip>
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { ToolDefinition } from '@/api/tools'

interface Props {
  tool: ToolDefinition
}

const props = defineProps<Props>()

const riskColor = computed(() => {
  if (props.tool.riskLevel === 'high') return 'error'
  if (props.tool.riskLevel === 'medium') return 'warning'
  return 'success'
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.tool-row {
  @include surface-card-soft;
  @include hover-lift;
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

.tool-meta-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
</style>
