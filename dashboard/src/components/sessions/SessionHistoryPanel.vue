<template>
  <v-card class="sessions-panel h-100" elevation="0">
    <v-card-title class="text-subtitle-1 font-weight-bold">
      {{ historyLabel }}
    </v-card-title>
    <v-card-text>
      <div class="session-history-header">
        <div class="text-body-2 text-medium-emphasis">
          {{ historyRangeLabel }}
        </div>
        <div class="session-history-controls">
          <v-select
            :model-value="historyPageSize"
            :items="historyPageSizeOptions"
            item-title="label"
            item-value="value"
            density="compact"
            variant="outlined"
            hide-details
            class="session-history-page-size"
            :label="pageSizeLabel"
            @update:model-value="(value) => $emit('update:historyPageSize', Number(value))"
          />
          <div class="session-history-nav">
            <v-btn
              icon="mdi-chevron-left"
              variant="tonal"
              size="small"
              :disabled="historyLoading || historyPage <= 1"
              :aria-label="previousPageLabel"
              @click="$emit('previous-history-page')"
            />
            <v-btn
              icon="mdi-chevron-right"
              variant="tonal"
              size="small"
              :disabled="historyLoading || !historyHasNextPage"
              :aria-label="nextPageLabel"
              @click="$emit('next-history-page')"
            />
          </div>
        </div>
      </div>
      <v-progress-linear
        v-if="historyLoading"
        indeterminate
        color="primary"
        rounded
        class="mb-4"
      />
      <div
        v-if="historyItems.length === 0"
        class="text-body-2 text-medium-emphasis py-6 text-center"
      >
        {{ emptyHistoryLabel }}
      </div>
      <div v-else class="session-message-list">
        <div
          v-for="message in historyItems"
          :key="message.id"
          class="session-message-row"
        >
          <div class="session-message-row__head">
            <strong>{{
              message.senderName || message.senderId || unknownSenderLabel
            }}</strong>
            <span>{{ formatTimestamp(message.createdAt) }}</span>
          </div>
          <div class="session-message-row__meta">
            <v-chip size="x-small" variant="tonal">{{ message.role }}</v-chip>
            <v-chip
              size="x-small"
              variant="tonal"
              :color="agentReadStateColor(message.agentReadState)"
            >
              {{ agentReadStateLabel(message.agentReadState) }}
            </v-chip>
            <v-chip
              size="x-small"
              variant="tonal"
              :color="routingColor(message.routingStatus)"
            >
              {{ routingStatusLabel(message.routingStatus) }}
            </v-chip>
            <v-chip
              v-if="message.routingSkipReason"
              size="x-small"
              variant="tonal"
              color="grey"
            >
              {{ routingSkipReasonLabel(message.routingSkipReason) }}
            </v-chip>
            <v-chip
              v-if="message.isMentioned"
              size="x-small"
              variant="tonal"
              color="warning"
            >
              {{ mentionedLabel }}
            </v-chip>
          </div>
          <div class="session-message-row__body">
            {{ message.rawText || stringifyContent(message.content) }}
          </div>
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import type { SessionMessage } from '@/api/sessions'

defineProps<{
  historyItems: SessionMessage[]
  historyLoading: boolean
  historyPage: number
  historyPageSize: number
  historyPageSizeOptions: Array<{ label: string; value: number }>
  historyHasNextPage: boolean
  historyLabel: string
  historyRangeLabel: string
  emptyHistoryLabel: string
  pageSizeLabel: string
  previousPageLabel: string
  nextPageLabel: string
  unknownSenderLabel: string
  mentionedLabel: string
  formatTimestamp: (value: number | string | null | undefined) => string
  stringifyContent: (content: unknown[]) => string
  agentReadStateLabel: (state: string) => string
  agentReadStateColor: (state: string) => string
  routingStatusLabel: (status: string) => string
  routingSkipReasonLabel: (reason: string) => string
  routingColor: (status: string) => string
}>()

defineEmits<{
  'update:historyPageSize': [value: number]
  'previous-history-page': []
  'next-history-page': []
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.sessions-panel {
  @include surface-card;
}

.session-message-list {
  display: grid;
  gap: 12px;
}

.session-history-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
}

.session-history-controls {
  display: flex;
  align-items: center;
  gap: 12px;
}

.session-history-page-size {
  width: 112px;
}

.session-history-nav {
  display: flex;
  gap: 8px;
}

.session-message-row {
  padding: 12px 14px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.7);
}

.session-message-row__head,
.session-message-row__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.session-message-row__head {
  justify-content: space-between;
  margin-bottom: 8px;
}

.session-message-row__head span {
  color: rgba(var(--v-theme-on-surface), 0.52);
  font-size: 0.8rem;
}

.session-message-row__body {
  margin-top: 8px;
  white-space: pre-wrap;
  word-break: break-word;
  color: rgba(var(--v-theme-on-surface), 0.84);
}

@media (max-width: 960px) {
  .session-history-header,
  .session-history-controls {
    flex-direction: column;
    align-items: stretch;
  }

  .session-history-page-size {
    width: 100%;
  }

  .session-history-nav {
    justify-content: flex-end;
  }
}
</style>
