<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.monitoring.title')"
      :subtitle="$t('pages.monitoring.subtitle')"
      :kicker="$t('pages.monitoring.kicker')"
    >
      <template #actions>
        <v-tooltip location="bottom">
          <template #activator="{ props }">
            <v-btn
              v-bind="props"
              icon="mdi-broom-variant"
              variant="tonal"
              rounded="lg"
              @click="clearLogs"
            />
          </template>
          <span>{{ $t('common.actions.action.reset') }}</span>
        </v-tooltip>

        <v-tooltip location="bottom">
          <template #activator="{ props }">
            <v-btn
              v-bind="props"
              icon="mdi-lan-connect"
              color="primary"
              variant="tonal"
              rounded="lg"
              @click="connectLogs"
            />
          </template>
          <span>{{ $t('pages.monitoring.connectLogs') }}</span>
        </v-tooltip>
      </template>
    </app-page-header>

    <section class="terminal-shell">
      <div class="terminal-shell__topbar">
        <div class="terminal-title-group">
          <div class="terminal-title">
            <v-icon icon="mdi-console-line" size="18" class="me-2" />
            {{ $t('pages.monitoring.terminalTitle') }}
          </div>
          <div class="terminal-caption">
            {{ $t('pages.monitoring.terminalSubtitle') }}
          </div>
        </div>

        <div class="terminal-status-row">
          <span class="terminal-status-pill">
            <span class="terminal-status-pill__label">{{ $t('pages.monitoring.status') }}</span>
            <span class="terminal-status-pill__value" :class="isOnline ? 'text-success' : 'text-error'">
              {{ isOnline ? $t('common.actions.status.online') : $t('common.actions.status.offline') }}
            </span>
          </span>
          <span class="terminal-status-pill">
            <span class="terminal-status-pill__label">{{ $t('pages.monitoring.cpu') }}</span>
            <span class="terminal-status-pill__value">{{ formatPercent(status.cpuUsage) }}</span>
          </span>
          <span class="terminal-status-pill">
            <span class="terminal-status-pill__label">{{ $t('pages.monitoring.memory') }}</span>
            <span class="terminal-status-pill__value">
              {{ $t('pages.monitoring.memoryUsageMb', { value: formatMemory(status.memoryUsage) }) }}
            </span>
          </span>
          <span class="terminal-status-pill">
            <span class="terminal-status-pill__label">{{ $t('pages.monitoring.logs') }}</span>
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
          <v-btn v-for="option in logLevelOptions" :key="option.value" :value="option.value" rounded="lg">
            <span class="terminal-level-toggle__label" :class="`terminal-level-toggle__label--${option.value.toLowerCase()}`">
              {{ option.label }}
            </span>
          </v-btn>
        </v-btn-toggle>

        <div class="terminal-toolbar__actions">
          <v-chip
            size="small"
            variant="flat"
            :color="logConnected ? 'success' : 'error'"
            class="terminal-stream-chip"
          >
            {{ logConnected ? $t('pages.monitoring.streamConnected') : $t('pages.monitoring.streamDisconnected') }}
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
            <span>
              {{ followTail ? $t('pages.monitoring.pauseFollow') : $t('pages.monitoring.followLatest') }}
            </span>
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
            <span>{{ $t('pages.monitoring.jumpLatest') }}</span>
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
              <span class="terminal-line__source">{{ item.source ?? $t('pages.monitoring.systemSource') }}</span>
              <span class="terminal-line__message">{{ item.message }}</span>
            </div>
          </div>

          <v-empty-state
            v-else
            icon="mdi-console-line"
            :title="$t('pages.monitoring.noData')"
            :text="$t('pages.monitoring.noDataSubtitle')"
            variant="plain"
            class="terminal-empty-state"
          />
        </div>
      </div>

      <div class="terminal-footer">
        <div class="terminal-footer__meta">
          <span>{{ $t('pages.monitoring.logs') }} {{ terminalLogs.length }}</span>
          <span>{{ followTail ? $t('pages.monitoring.following') : $t('pages.monitoring.followPaused') }}</span>
        </div>
        <div class="terminal-footer__meta">
          <span>{{ $t('pages.monitoring.cpu') }} {{ formatPercent(status.cpuUsage) }}</span>
          <span>{{ $t('pages.monitoring.memoryUsageMb', { value: formatMemory(status.memoryUsage) }) }}</span>
        </div>
      </div>
    </section>
  </v-container>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import AppPageHeader from '@/components/AppPageHeader.vue'
import { useMonitoringStore } from '@/stores/monitoring'
import type { LogLevel } from '@/stores/monitoring'

const monitoringStore = useMonitoringStore()
const { filteredLogs, enabledLogLevels, isOnline, logConnected, status } = storeToRefs(monitoringStore)

const logScrollArea = ref<HTMLElement | null>(null)
const followTail = ref(true)

const logLevelOptions: ReadonlyArray<{ value: LogLevel; label: string }> = [
  { value: 'DEBUG', label: 'DEBUG' },
  { value: 'INFO', label: 'INFO' },
  { value: 'WARN', label: 'WARN' },
  { value: 'ERROR', label: 'ERROR' },
]

const terminalLogs = computed(() => filteredLogs.value.slice().reverse())

const connectLogs = () => monitoringStore.connectLogs()
const clearLogs = () => monitoringStore.clearLogs()

const formatPercent = (value: number) => `${Number(value || 0).toFixed(1)}%`
const formatMemory = (value: number) => Number(value || 0).toFixed(1)

const formatTimestamp = (timestamp: number) => {
  const date = new Date(timestamp)
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  const seconds = String(date.getSeconds()).padStart(2, '0')
  const milliseconds = String(date.getMilliseconds()).padStart(3, '0')
  return `${hours}:${minutes}:${seconds}.${milliseconds}`
}

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
  terminalLogs,
  async () => {
    if (!followTail.value) {
      return
    }
    await nextTick()
    scrollToBottom()
  },
  { deep: true }
)

onMounted(async () => {
  await nextTick()
  scrollToBottom()
})
</script>

<style scoped>
.terminal-shell {
  display: flex;
  flex-direction: column;
  min-height: 76vh;
  border: 1px solid rgba(120, 146, 187, 0.2);
  border-radius: 20px;
  background:
    linear-gradient(180deg, rgba(15, 18, 24, 0.98) 0%, rgba(8, 11, 16, 0.98) 100%);
  box-shadow: 0 24px 40px rgba(8, 11, 16, 0.22);
  overflow: hidden;
}

.terminal-shell__topbar {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
  padding: 18px 20px 14px;
  border-bottom: 1px solid rgba(120, 146, 187, 0.12);
  background: rgba(18, 22, 30, 0.96);
}

.terminal-title-group {
  min-width: 0;
}

.terminal-title {
  display: flex;
  align-items: center;
  color: #ecf2ff;
  font-size: 0.98rem;
  font-weight: 700;
}

.terminal-caption {
  margin-top: 6px;
  color: rgba(201, 213, 236, 0.68);
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
  border: 1px solid rgba(120, 146, 187, 0.12);
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.03);
  color: #d3def3;
  font-family: 'Roboto Mono', monospace, sans-serif;
  font-size: 0.75rem;
}

.terminal-status-pill__label {
  color: rgba(179, 194, 219, 0.7);
  text-transform: uppercase;
}

.terminal-status-pill__value {
  color: #f4f7ff;
  font-weight: 700;
}

.terminal-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 14px 18px;
  border-bottom: 1px solid rgba(120, 146, 187, 0.1);
  background: rgba(10, 13, 18, 0.92);
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
  font-family: 'Roboto Mono', monospace, sans-serif;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0;
}

.terminal-level-toggle__label--debug {
  color: #8ba3c7;
}

.terminal-level-toggle__label--info {
  color: #7de2ff;
}

.terminal-level-toggle__label--warn {
  color: #ffca6b;
}

.terminal-level-toggle__label--error {
  color: #ff8f90;
}

.terminal-toolbar__actions {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.terminal-stream-chip {
  font-family: 'Roboto Mono', monospace, sans-serif;
}

.terminal-tool-btn {
  color: rgba(222, 231, 247, 0.82);
}

.terminal-screen {
  flex: 1;
  min-height: 0;
  background:
    linear-gradient(180deg, rgba(7, 9, 14, 0.96) 0%, rgba(9, 11, 16, 0.96) 100%);
}

.terminal-scroll-area {
  height: 100%;
  min-height: 520px;
  overflow-y: auto;
  padding: 12px 0;
  font-family: 'Roboto Mono', monospace, sans-serif;
  scrollbar-width: thin;
  scrollbar-color: rgba(120, 146, 187, 0.32) transparent;
}

.terminal-scroll-area::-webkit-scrollbar {
  width: 10px;
}

.terminal-scroll-area::-webkit-scrollbar-thumb {
  border-radius: 999px;
  background: rgba(120, 146, 187, 0.28);
}

.terminal-stream {
  display: flex;
  flex-direction: column;
}

.terminal-line {
  display: grid;
  grid-template-columns: 132px 56px 160px minmax(0, 1fr);
  gap: 12px;
  align-items: start;
  padding: 4px 18px;
  color: #d8e3f7;
  font-size: 0.78rem;
  line-height: 1.45;
}

.terminal-line:hover {
  background: rgba(121, 150, 196, 0.07);
}

.terminal-line--debug {
  color: #94a8c8;
}

.terminal-line--info {
  color: #d8e3f7;
}

.terminal-line--warn {
  color: #ffd28b;
}

.terminal-line--error {
  color: #ff9f9f;
  background: rgba(255, 128, 128, 0.05);
}

.terminal-line__time,
.terminal-line__level,
.terminal-line__source {
  white-space: nowrap;
}

.terminal-line__time {
  color: rgba(164, 184, 214, 0.8);
}

.terminal-line__level {
  font-weight: 700;
}

.terminal-line__source {
  overflow: hidden;
  color: rgba(154, 174, 205, 0.9);
  text-overflow: ellipsis;
}

.terminal-line__message {
  min-width: 0;
  white-space: pre-wrap;
  word-break: break-word;
}

.terminal-empty-state {
  color: rgba(220, 229, 244, 0.74);
}

.terminal-empty-state :deep(.v-empty-state__title) {
  color: #e2ebfb;
}

.terminal-empty-state :deep(.v-empty-state__text) {
  color: rgba(195, 209, 233, 0.7);
}

.terminal-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 10px 18px 12px;
  border-top: 1px solid rgba(120, 146, 187, 0.1);
  background: rgba(10, 13, 18, 0.94);
  color: rgba(177, 193, 220, 0.82);
  font-family: 'Roboto Mono', monospace, sans-serif;
  font-size: 0.74rem;
}

.terminal-footer__meta {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

@media (max-width: 960px) {
  .terminal-shell__topbar,
  .terminal-toolbar,
  .terminal-footer {
    flex-direction: column;
    align-items: flex-start;
  }

  .terminal-status-row,
  .terminal-toolbar__actions,
  .terminal-footer__meta {
    width: 100%;
    justify-content: flex-start;
  }

  .terminal-line {
    grid-template-columns: 116px 52px 120px minmax(0, 1fr);
    gap: 10px;
    padding-inline: 14px;
  }
}

@media (max-width: 640px) {
  .terminal-line {
    grid-template-columns: 1fr;
    gap: 2px;
  }

  .terminal-line__time,
  .terminal-line__level,
  .terminal-line__source {
    white-space: normal;
  }
}
</style>
