<template>
  <v-alert
    v-if="error"
    type="error"
    variant="tonal"
    density="comfortable"
    class="mb-6"
  >
    {{ error }}
  </v-alert>

  <v-alert
    v-if="issues.length > 0"
    type="warning"
    variant="tonal"
    density="comfortable"
    class="mb-6"
  >
    <div class="font-weight-medium mb-2">
      {{ title }}
    </div>
    <div
      v-for="issue in visibleIssues"
      :key="`${issue.path}:${issue.code}:${issue.message}`"
      class="text-body-2 validation-issue-line"
    >
      <span class="font-weight-medium">{{ issue.path }}</span>
      <span>{{ formatIssue(issue) }}</span>
    </div>
    <div v-if="hiddenIssueCount > 0" class="text-body-2 mt-1 text-medium-emphasis">
      {{ moreLabel(hiddenIssueCount) }}
    </div>
  </v-alert>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { ConfigValidationIssue } from '@/api/config'

interface Props {
  error: string
  issues: readonly ConfigValidationIssue[]
  title: string
  formatIssue: (issue: ConfigValidationIssue) => string
  moreLabel: (count: number) => string
  visibleCount?: number
}

const props = withDefaults(defineProps<Props>(), {
  visibleCount: 5,
})

const visibleIssues = computed(() => props.issues.slice(0, props.visibleCount))
const hiddenIssueCount = computed(() =>
  Math.max(props.issues.length - visibleIssues.value.length, 0)
)
</script>

<style scoped lang="scss">
.validation-issue-line {
  display: flex;
  gap: 8px;
  align-items: baseline;
  min-width: 0;
}
</style>
