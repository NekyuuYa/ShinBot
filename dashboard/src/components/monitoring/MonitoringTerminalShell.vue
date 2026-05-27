<template>
  <section class="terminal-shell">
    <div class="terminal-shell__topbar">
      <div class="terminal-title-group">
        <div class="terminal-title">
          <v-icon icon="mdi-console-line" size="18" class="me-2" />
          {{ terminalTitle }}
        </div>
        <div class="terminal-caption">
          {{ terminalSubtitle }}
        </div>
      </div>

      <div class="terminal-status-row">
        <span class="terminal-status-pill">
          <span class="terminal-status-pill__label">{{ statusLabel }}</span>
          <span class="terminal-status-pill__value" :class="isOnline ? 'text-success' : 'text-error'">
            {{ isOnline ? onlineLabel : offlineLabel }}
          </span>
        </span>
        <span class="terminal-status-pill">
          <span class="terminal-status-pill__label">{{ systemCpuLabel }}</span>
          <span class="terminal-status-pill__value">{{ formatPercent(status.systemCpuUsage) }}</span>
        </span>
        <span class="terminal-status-pill">
          <span class="terminal-status-pill__label">{{ systemMemoryLabel }}</span>
          <span class="terminal-status-pill__value">
            {{
              formatSystemMemoryUsage(
                status.systemMemoryUsage,
                status.systemMemoryUsedMb,
                status.systemMemoryTotalMb,
              )
            }}
          </span>
        </span>
        <span class="terminal-status-pill">
          <span class="terminal-status-pill__label">{{ processMemoryLabel }}</span>
          <span class="terminal-status-pill__value">
            {{ formatMemorySize(status.processMemoryMb) }}
          </span>
        </span>
        <span class="terminal-status-pill">
          <span class="terminal-status-pill__label">{{ logsLabel }}</span>
          <span class="terminal-status-pill__value">{{ terminalLogs.length }}</span>
        </span>
      </div>
    </div>

    <div class="terminal-toolbar">
      <v-btn-toggle
        v-model="enabledLogLevels"
        multiple
        divided
        density="comfortable"
        class="terminal-level-toggle"
      >
        <v-btn
          v-for="option in logLevelOptions"
          :key="option.value"
          :value="option.value"
          rounded="lg"
        >
          <span
            class="terminal-level-toggle__label"
            :class="`terminal-level-toggle__label--${option.value.toLowerCase()}`"
          >
            {{ option.label }}
          </span>
        </v-btn>
      </v-btn-toggle>

      <div class="terminal-toolbar__actions">
        <v-text-field
          v-model="logSearchQuery"
          :label="logSearchLabel"
          density="compact"
          variant="outlined"
          hide-details
          clearable
          prepend-inner-icon="mdi-magnify"
          class="terminal-search"
        />

        <v-chip
          size="small"
          variant="flat"
          :color="logConnected ? 'success' : 'error'"
          class="terminal-stream-chip"
        >
          {{ logConnected ? streamConnectedLabel : streamDisconnectedLabel }}
        </v-chip>

        <v-tooltip location="bottom">
          <template #activator="{ props }">
            <v-btn
              v-bind="props"
              :icon="followTail ? 'mdi-pause-circle-outline' : 'mdi-arrow-collapse-down'"
              variant="text"
              rounded="lg"
              class="terminal-tool-btn"
              @click="toggleFollowTail"
            />
          </template>
          <span>{{ followTail ? pauseFollowLabel : followLatestLabel }}</span>
        </v-tooltip>

        <v-tooltip location="bottom">
          <template #activator="{ props }">
            <v-btn
              v-bind="props"
              icon="mdi-arrow-down-circle-outline"
              variant="text"
              rounded="lg"
              class="terminal-tool-btn"
              @click="jumpToLatest"
            />
          </template>
          <span>{{ jumpLatestLabel }}</span>
        </v-tooltip>
      </div>
    </div>

    <div class="terminal-screen">
      <div ref="logScrollArea" class="terminal-scroll-area" @scroll="handleScroll">
        <div v-if="terminalLogs.length > 0" class="terminal-stream">
          <div
            v-for="item in terminalLogs"
            :key="item.id"
            class="terminal-line"
            :class="`terminal-line--${item.level.toLowerCase()}`"
          >
            <span class="terminal-line__time">[{{ formatTimestamp(item.timestamp) }}]</span>
            <span class="terminal-line__level">{{ item.level }}</span>
            <span class="terminal-line__source">
              {{ item.source ?? systemSourceLabel }}
            </span>
            <span class="terminal-line__message">
              <span v-if="item.event" class="terminal-line__event">{{ item.event }}</span>
              {{ item.message }}
            </span>
          </div>
        </div>

        <v-empty-state
          v-else
          icon="mdi-console-line"
          :title="noDataLabel"
          :text="noDataSubtitleLabel"
          variant="plain"
          class="terminal-empty-state"
        />
      </div>
    </div>

    <div class="terminal-footer">
      <div class="terminal-footer__meta">
        <span>{{ logsLabel }} {{ terminalLogs.length }}</span>
        <span>{{ followTail ? followingLabel : followPausedLabel }}</span>
      </div>
      <div class="terminal-footer__meta">
        <span>{{ systemCpuLabel }} {{ formatPercent(status.systemCpuUsage) }}</span>
        <span>{{
          formatSystemMemoryUsage(
            status.systemMemoryUsage,
            status.systemMemoryUsedMb,
            status.systemMemoryTotalMb,
          )
        }}</span>
        <span>{{ processMemoryLabel }} {{ formatMemorySize(status.processMemoryMb) }}</span>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { nextTick, ref, watch } from 'vue'

import type { LogLevel, MonitoringLogEntry, SystemStatus } from '@/stores/monitoring'

const enabledLogLevels = defineModel<LogLevel[]>('enabledLogLevels', { required: true })
const logSearchQuery = defineModel<string>('logSearchQuery', { required: true })

interface Props {
  terminalTitle: string
  terminalSubtitle: string
  statusLabel: string
  systemCpuLabel: string
  systemMemoryLabel: string
  processMemoryLabel: string
  logsLabel: string
  logSearchLabel: string
  streamConnectedLabel: string
  streamDisconnectedLabel: string
  pauseFollowLabel: string
  followLatestLabel: string
  jumpLatestLabel: string
  followingLabel: string
  followPausedLabel: string
  systemSourceLabel: string
  noDataLabel: string
  noDataSubtitleLabel: string
  onlineLabel: string
  offlineLabel: string
  isOnline: boolean
  logConnected: boolean
  status: SystemStatus
  terminalLogs: MonitoringLogEntry[]
  formatPercent: (value: number) => string
  formatMemorySize: (value: number) => string
  formatSystemMemoryUsage: (percent: number, usedMb: number, totalMb: number) => string
  formatTimestamp: (timestamp: number) => string
}

const props = defineProps<Props>()

const logScrollArea = ref<HTMLElement | null>(null)
const followTail = ref(true)

const logLevelOptions: ReadonlyArray<{ value: LogLevel; label: string }> = [
  { value: 'DEBUG', label: 'DEBUG' },
  { value: 'INFO', label: 'INFO' },
  { value: 'WARN', label: 'WARN' },
  { value: 'ERROR', label: 'ERROR' },
]

const isNearBottom = (element: HTMLElement) =>
  element.scrollHeight - element.scrollTop - element.clientHeight <= 32

const scrollToBottom = () => {
  if (!logScrollArea.value) {
    return
  }
  logScrollArea.value.scrollTop = logScrollArea.value.scrollHeight
}

const jumpToLatest = async () => {
  followTail.value = true
  await nextTick()
  scrollToBottom()
}

const toggleFollowTail = async () => {
  followTail.value = !followTail.value
  if (followTail.value) {
    await nextTick()
    scrollToBottom()
  }
}

const handleScroll = () => {
  if (!logScrollArea.value) {
    return
  }
  if (!isNearBottom(logScrollArea.value) && followTail.value) {
    followTail.value = false
  }
}

watch(
  () => props.terminalLogs,
  async () => {
    if (!followTail.value) {
      return
    }
    await nextTick()
    scrollToBottom()
  },
  { deep: true },
)

watch(
  () => props.terminalLogs.length,
  async () => {
    if (!followTail.value) {
      return
    }
    await nextTick()
    scrollToBottom()
  },
)

watch(
  () => props.logConnected,
  async (connected) => {
    if (connected) {
      await nextTick()
      scrollToBottom()
    }
  },
)
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

$term-font: 'Roboto Mono', monospace, sans-serif;

.terminal-shell {
  display: flex;
  flex-direction: column;
  min-height: 76vh;
  border: 1px solid rgba(var(--v-theme-primary), 0.18);
  border-radius: 20px;
  background: linear-gradient(
    180deg,
    rgba(var(--v-theme-surface), 0.98) 0%,
    rgba(var(--v-theme-background), 0.98) 100%
  );
  box-shadow: 0 24px 40px rgba(var(--v-theme-on-surface), 0.1);
  overflow: hidden;
}

.terminal-shell__topbar {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  padding: 18px 20px 14px;
  border-bottom: 1px solid rgba(var(--v-theme-primary), 0.12);
  background: rgba(var(--v-theme-surface), 0.96);
}

.terminal-title-group {
  min-width: 0;
}

.terminal-title {
  display: flex;
  align-items: center;
  color: rgb(var(--v-theme-on-surface));
  font-size: 0.98rem;
  font-weight: 700;
}

.terminal-caption {
  margin-top: 6px;
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-size: 0.82rem;
}

.terminal-status-row {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.terminal-status-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  border-radius: 12px;
  background: rgba(var(--v-theme-on-surface), 0.03);
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-family: $term-font;
  font-size: 0.75rem;
}

.terminal-status-pill__label {
  color: rgba(var(--v-theme-on-surface), 0.62);
  text-transform: uppercase;
}

.terminal-status-pill__value {
  color: rgb(var(--v-theme-on-surface));
  font-weight: 700;
}

.terminal-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 18px;
  border-bottom: 1px solid rgba(var(--v-theme-primary), 0.1);
  background: rgba(var(--v-theme-surface), 0.92);
}

.terminal-level-toggle {
  overflow: visible;
  padding: 2px;
}

.terminal-level-toggle :deep(.v-btn) {
  margin: 2px;
  border-radius: 12px;
  min-width: 72px;
}

.terminal-level-toggle__label {
  font-family: $term-font;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0;
}

.terminal-level-toggle__label--debug {
  color: rgba(var(--v-theme-on-surface), 0.6);
}
.terminal-level-toggle__label--info {
  color: rgb(var(--v-theme-info));
}
.terminal-level-toggle__label--warn {
  color: rgb(var(--v-theme-warning));
}
.terminal-level-toggle__label--error {
  color: rgb(var(--v-theme-error));
}

.terminal-toolbar__actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.terminal-search {
  width: min(360px, 42vw);
}

.terminal-stream-chip {
  font-family: $term-font;
}

.terminal-tool-btn {
  color: rgba(var(--v-theme-on-surface), 0.82);
}

.terminal-screen {
  flex: 1;
  min-height: 0;
  background: linear-gradient(
    180deg,
    rgba(var(--v-theme-background), 0.92) 0%,
    rgba(var(--v-theme-surface), 0.96) 100%
  );
}

.terminal-scroll-area {
  height: 100%;
  min-height: 520px;
  overflow-y: auto;
  padding: 12px 0;
  font-family: $term-font;
  scrollbar-width: thin;
  scrollbar-color: rgba(var(--v-theme-primary), 0.32) transparent;
}

.terminal-scroll-area::-webkit-scrollbar {
  width: 10px;
}

.terminal-scroll-area::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: rgba(var(--v-theme-primary), 0.28);
}

.terminal-stream {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 0 16px;
}

.terminal-line {
  display: grid;
  grid-template-columns: auto auto minmax(120px, 0.8fr) minmax(0, 1fr);
  align-items: start;
  gap: 12px;
  padding: 8px 12px;
  border: 1px solid transparent;
  border-radius: 10px;
  line-height: 1.5;
  transition:
    border-color 0.18s ease,
    background-color 0.18s ease;
}

.terminal-line:hover {
  border-color: rgba(var(--v-theme-primary), 0.12);
  background: rgba(var(--v-theme-primary), 0.035);
}

.terminal-line--debug {
  color: rgba(var(--v-theme-on-surface), 0.7);
}

.terminal-line--info {
  color: rgb(var(--v-theme-info));
}

.terminal-line--warn {
  color: rgb(var(--v-theme-warning));
}

.terminal-line--error {
  color: rgb(var(--v-theme-error));
}

.terminal-line__time,
.terminal-line__level,
.terminal-line__source {
  font-size: 0.8rem;
  white-space: nowrap;
}

.terminal-line__time {
  color: rgba(var(--v-theme-on-surface), 0.46);
}

.terminal-line__level {
  font-weight: 700;
}

.terminal-line__source {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.56);
  text-overflow: ellipsis;
}

.terminal-line__message {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.9);
  text-overflow: ellipsis;
  white-space: pre-wrap;
  word-break: break-word;
}

.terminal-line__event {
  display: inline-block;
  margin-right: 8px;
  color: rgba(var(--v-theme-primary), 0.88);
  font-weight: 700;
}

.terminal-empty-state {
  min-height: 100%;
}

.terminal-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 12px 18px 16px;
  border-top: 1px solid rgba(var(--v-theme-primary), 0.1);
  background: rgba(var(--v-theme-surface), 0.96);
}

.terminal-footer__meta {
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-family: $term-font;
  font-size: 0.76rem;
}

@media (max-width: 960px) {
  .terminal-shell__topbar,
  .terminal-toolbar,
  .terminal-footer {
    align-items: flex-start;
    flex-direction: column;
  }

  .terminal-status-row,
  .terminal-toolbar__actions,
  .terminal-footer__meta {
    justify-content: flex-start;
  }

  .terminal-search {
    width: min(100%, 420px);
  }
}
</style>
