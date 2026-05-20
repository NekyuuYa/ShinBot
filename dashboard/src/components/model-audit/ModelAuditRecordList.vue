<template>
  <section class="audit-record-panel">
    <div class="record-panel-head">
      <div>
        <div class="panel-kicker">{{ recordsLabel }}</div>
        <h2>{{ pageLabel }}</h2>
      </div>
      <div class="pagination-actions">
        <v-btn
          icon="mdi-chevron-left"
          variant="tonal"
          :disabled="offset === 0 || loading"
          :aria-label="previousLabel"
          @click="$emit('previous')"
        />
        <v-btn
          icon="mdi-chevron-right"
          variant="tonal"
          :disabled="!hasNextPage || loading"
          :aria-label="nextLabel"
          @click="$emit('next')"
        />
      </div>
    </div>

    <v-progress-linear
      v-if="loading"
      indeterminate
      color="primary"
      rounded
      class="mb-4"
    />

    <v-empty-state
      v-if="!loading && records.length === 0"
      icon="mdi-clipboard-search-outline"
      :title="emptyLabel"
      variant="plain"
    />

    <div v-else class="record-list">
      <article
        v-for="record in records"
        :key="record.id"
        class="record-item"
        :class="{ 'record-item--active': selectedRecordId === record.id }"
      >
        <button type="button" class="record-summary" @click="$emit('toggle', record.id)">
          <span class="record-status" :class="record.success ? 'record-status--success' : 'record-status--failed'">
            <v-icon :icon="record.success ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline'" size="18" />
            {{ record.success ? successLabel : failedLabel }}
          </span>

          <span class="record-main">
            <strong>{{ record.modelId || noneLabel }}</strong>
            <small>{{ formatDateTime(record.startedAt) }} · {{ record.providerId || noneLabel }}</small>
          </span>

          <span class="record-meta">
            <span>{{ record.caller || noneLabel }}</span>
            <span>{{ record.sessionId || record.instanceId || noneLabel }}</span>
          </span>

          <span class="record-usage">
            <strong>{{ formatDuration(record.latencyMs) }}</strong>
            <small>{{ formatCompactNumber(record.inputTokens + record.outputTokens) }} tokens</small>
          </span>

          <v-icon
            :icon="selectedRecordId === record.id ? 'mdi-chevron-up' : 'mdi-chevron-down'"
            size="20"
            class="record-chevron"
          />
        </button>

        <v-expand-transition>
          <div v-if="selectedRecordId === record.id" class="record-detail">
            <slot name="detail" :record="record" />
          </div>
        </v-expand-transition>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import type { ModelExecutionRecord } from '@/api/modelRuntime'

defineProps<{
  records: ModelExecutionRecord[]
  loading: boolean
  offset: number
  hasNextPage: boolean
  selectedRecordId: string
  pageLabel: string
  recordsLabel: string
  emptyLabel: string
  successLabel: string
  failedLabel: string
  noneLabel: string
  previousLabel: string
  nextLabel: string
  formatDateTime: (value: string | null | undefined) => string
  formatCompactNumber: (value: number) => string
  formatDuration: (value: number | null | undefined) => string
}>()

defineEmits<{
  previous: []
  next: []
  toggle: [id: string]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.audit-record-panel {
  @include surface-card;
  padding: 20px;
}

.record-panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}

.panel-kicker {
  color: rgb(var(--v-theme-primary));
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.record-panel-head h2 {
  margin: 4px 0 0;
  color: rgba(var(--v-theme-on-surface), 0.88);
  font-size: $font-size-md;
  font-weight: 800;
}

.pagination-actions {
  display: flex;
  gap: 8px;
}

.record-list {
  display: grid;
  gap: 10px;
}

.record-item {
  border: 1px solid $border-color-soft;
  border-radius: $radius-sm;
  background: rgba(var(--v-theme-surface), 0.74);
  overflow: hidden;
  transition:
    border-color $transition-fast,
    background-color $transition-fast,
    box-shadow $transition-fast;
}

.record-item:hover,
.record-item--active {
  border-color: $border-color-primary;
  background: rgba(var(--v-theme-surface), 0.9);
}

.record-item--active {
  box-shadow: inset 0 0 0 1px rgba(var(--v-theme-primary), 0.08);
}

.record-summary {
  width: 100%;
  min-height: 72px;
  display: grid;
  grid-template-columns: minmax(96px, 0.7fr) minmax(220px, 1.6fr) minmax(180px, 1.4fr) minmax(120px, 0.8fr) 32px;
  align-items: center;
  gap: 16px;
  padding: 12px 16px;
  border: 0;
  background: transparent;
  color: inherit;
  text-align: left;
  cursor: pointer;
}

.record-status,
.record-main,
.record-meta,
.record-usage {
  min-width: 0;
}

.record-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: $font-size-sm;
  font-weight: 700;
}

.record-status--success {
  color: rgb(var(--v-theme-success));
}

.record-status--failed {
  color: rgb(var(--v-theme-error));
}

.record-main,
.record-meta,
.record-usage {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.record-main strong,
.record-usage strong {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.9);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.record-main small,
.record-meta,
.record-usage small {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.58);
  font-size: $font-size-xs;
  text-overflow: ellipsis;
}

.record-meta span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.record-chevron {
  justify-self: end;
  color: rgba(var(--v-theme-on-surface), 0.54);
}

.record-detail {
  padding: 12px 18px 20px;
}

@media (max-width: 1280px) {
  .record-summary {
    grid-template-columns: minmax(92px, 0.7fr) minmax(200px, 1.6fr) minmax(140px, 1fr) 32px;
  }

  .record-usage {
    display: none;
  }
}

@media (max-width: 760px) {
  .record-panel-head {
    align-items: flex-start;
    flex-direction: column;
  }

  .record-summary {
    grid-template-columns: 1fr 28px;
    gap: 8px;
  }

  .record-status,
  .record-main,
  .record-meta {
    grid-column: 1;
  }

  .record-chevron {
    grid-column: 2;
    grid-row: 1;
  }
}
</style>
