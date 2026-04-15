<template>
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
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { ToolDefinition } from '@/api/tools'

interface Props {
  tool: ToolDefinition
}

const props = defineProps<Props>()

const riskColor = computed(() => {
  if (props.tool.riskLevel === 'high') {
    return 'error'
  }
  if (props.tool.riskLevel === 'medium') {
    return 'warning'
  }
  return 'success'
})
</script>

<style scoped>
.tool-meta-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
</style>
