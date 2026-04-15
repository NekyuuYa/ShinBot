<template>
  <div v-if="layoutMode === 'card'">
    <v-row class="mx-n3">
      <v-col
        v-for="tool in tools"
        :key="tool.id"
        cols="12"
        md="6"
        xl="4"
        class="pa-3"
      >
        <tool-card :tool="tool" />
      </v-col>
    </v-row>
  </div>

  <div v-else class="d-grid ga-4">
    <tool-list-row v-for="tool in tools" :key="tool.id" :tool="tool" />
  </div>
</template>

<script setup lang="ts">
import type { ToolDefinition } from '@/api/tools'
import type { ToolLayoutMode } from '@/stores/tools'
import ToolCard from './ToolCard.vue'
import ToolListRow from './ToolListRow.vue'

interface Props {
  tools: ToolDefinition[]
  layoutMode: ToolLayoutMode
}

defineProps<Props>()
</script>
