<template>
  <v-card class="tool-card h-100" rounded="xl" elevation="0">
    <v-card-text class="pa-5">
      <div class="d-flex justify-space-between align-start ga-4 mb-4">
        <div>
          <div class="text-overline text-medium-emphasis">{{ tool.name }}</div>
          <div class="text-h6 font-weight-bold">{{ tool.displayName }}</div>
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

      <p class="text-body-2 text-medium-emphasis mb-4 tool-description">
        {{ tool.description || $t('pages.tools.empty.description') }}
      </p>

      <div class="tool-facts">
        <div class="tool-fact">
          <span>{{ $t('pages.tools.fields.owner') }}</span>
          <strong>{{ $t(`pages.tools.ownerTypeOptions.${tool.ownerType}`) }}</strong>
        </div>
        <div class="tool-fact">
          <span>{{ $t('pages.tools.fields.permission') }}</span>
          <code>{{ tool.permission || $t('pages.tools.empty.permission') }}</code>
        </div>
        <div class="tool-fact">
          <span>{{ $t('pages.tools.fields.timeout') }}</span>
          <strong>{{ $t('pages.tools.timeoutValue', { value: tool.timeoutSeconds }) }}</strong>
        </div>
      </div>

      <div v-if="tool.tags.length" class="mt-4">
        <div class="text-caption text-medium-emphasis mb-2">{{ $t('pages.tools.fields.tags') }}</div>
        <div class="d-flex flex-wrap ga-2">
          <v-chip
            v-for="tag in tool.tags"
            :key="tag"
            size="small"
            color="secondary"
            variant="tonal"
          >
            {{ tag }}
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

.tool-card {
  @include surface-card;
  @include hover-lift;
}

.tool-description {
  min-height: 42px;
}

.tool-facts {
  display: grid;
  gap: 12px;
}

.tool-fact {
  display: grid;
  gap: 4px;
}

.tool-fact span {
  font-size: 0.78rem;
  color: rgba(var(--v-theme-on-surface), 0.56);
}

.tool-fact code {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.tool-meta-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
</style>
