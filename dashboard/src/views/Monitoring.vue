<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.monitoring.title')"
      :subtitle="$t('pages.monitoring.subtitle')"
      :kicker="$t('pages.monitoring.kicker')"
    >
      <template #actions>
        <v-btn variant="tonal" class="me-2" @click="clearLogs">
          {{ $t('common.actions.action.reset') }}
        </v-btn>
        <v-btn color="primary" @click="connectLogs">
          {{ $t('pages.monitoring.connectLogs') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-row class="mb-4">
      <v-col cols="12" md="3">
        <v-card class="pa-4" elevation="5">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.monitoring.status') }}</div>
          <div class="text-h6">{{ isOnline ? $t('common.actions.status.online') : $t('common.actions.status.offline') }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" md="3">
        <v-card class="pa-4" elevation="5">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.monitoring.cpu') }}</div>
          <div class="text-h6">{{ status.cpuUsage }}%</div>
        </v-card>
      </v-col>
      <v-col cols="12" md="3">
        <v-card class="pa-4" elevation="5">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.monitoring.memory') }}</div>
          <div class="text-h6">{{ $t('pages.monitoring.memoryUsageMb', { value: status.memoryUsage }) }}</div>
        </v-card>
      </v-col>
      <v-col cols="12" md="3">
        <v-card class="pa-4" elevation="5">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.monitoring.logs') }}</div>
          <div class="text-h6">{{ filteredLogs.length }}</div>
        </v-card>
      </v-col>
    </v-row>

    <v-card class="pa-4">
      <v-row class="mb-4" align="center">
        <v-col cols="12" md="8">
          <v-btn-toggle
            v-model="enabledLogLevels"
            multiple
            divided
            density="comfortable"
            class="monitor-filter-toggle"
          >
            <v-btn v-for="option in logLevelOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </v-btn>
          </v-btn-toggle>
        </v-col>
        <v-col cols="12" md="4" class="text-end">
          <v-chip :color="logConnected ? 'success' : 'error'" variant="tonal">
            {{ logConnected ? $t('common.actions.status.online') : $t('common.actions.status.offline') }}
          </v-chip>
        </v-col>
      </v-row>

      <div class="monitor-log-container">
        <div v-if="filteredLogs.length > 0" class="log-scroll-area">
          <div
            v-for="item in filteredLogs"
            :key="item.id"
            class="log-row"
            :class="`log-row--${item.level.toLowerCase()}`"
          >
            <span class="log-col-level">
              <v-chip size="x-small" variant="flat" :color="logColor(item.level)" class="font-weight-bold">{{ displayLogLevel(item.level) }}</v-chip>
            </span>
            <span class="log-col-time text-caption text-medium-emphasis">{{ formatTime(item.timestamp) }}</span>
            <span class="log-col-source text-caption text-medium-emphasis">{{ item.source ?? '-' }}</span>
            <span class="log-col-message">{{ item.message }}</span>
          </div>
        </div>
        <v-empty-state
          v-else
          icon="mdi-text-search"
          :title="$t('pages.monitoring.noData')"
          variant="plain"
        />
      </div>
    </v-card>
  </v-container>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia'
import AppPageHeader from '@/components/AppPageHeader.vue'
import { useMonitoringStore } from '@/stores/monitoring'
import type { LogLevel } from '@/stores/monitoring'

const monitoringStore = useMonitoringStore()
const { filteredLogs, enabledLogLevels, isOnline, logConnected, status } = storeToRefs(monitoringStore)

const logLevelOptions: ReadonlyArray<{ value: LogLevel; label: string }> = [
  { value: 'DEBUG', label: 'DEBUG' },
  { value: 'INFO', label: 'INFO' },
  { value: 'WARN', label: 'WARNING' },
  { value: 'ERROR', label: 'ERROR' },
]

const connectLogs = () => monitoringStore.connectLogs()
const clearLogs = () => monitoringStore.clearLogs()

const displayLogLevel = (level: LogLevel) => (level === 'WARN' ? 'WARNING' : level)

const logColor = (level: string) => {
  if (level === 'ERROR') return 'error'
  if (level === 'WARN') return 'warning'
  if (level === 'INFO') return 'info'
  return 'grey'
}

const formatTime = (timestamp: number) => new Date(timestamp).toLocaleTimeString()
</script>

<style scoped>
.monitor-filter-toggle {
  overflow: visible;
  padding: 2px;
}

.monitor-filter-toggle :deep(.v-btn) {
  border-radius: 12px;
  margin: 2px;
}

.log-scroll-area {
  max-height: 600px;
  overflow-y: auto;
  border: 1px solid rgba(var(--v-theme-on-surface), 0.1);
  border-radius: 8px;
  font-family: 'Roboto Mono', monospace, sans-serif;
  font-size: 0.78rem;
}

.log-row {
  display: flex;
  align-items: baseline;
  gap: 12px;
  padding: 5px 12px;
  border-bottom: 1px solid rgba(var(--v-theme-on-surface), 0.06);
  line-height: 1.5;
}

.log-row:last-child {
  border-bottom: none;
}

.log-row--error {
  background: rgba(var(--v-theme-error), 0.06);
}

.log-row--warn {
  background: rgba(var(--v-theme-warning), 0.06);
}

.log-col-level {
  flex: 0 0 72px;
}

.log-col-time {
  flex: 0 0 90px;
  white-space: nowrap;
}

.log-col-source {
  flex: 0 0 140px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.log-col-message {
  flex: 1;
  word-break: break-word;
  color: rgba(var(--v-theme-on-surface), 0.82);
}
</style>
