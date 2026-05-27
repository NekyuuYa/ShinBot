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

    <monitoring-terminal-shell
      v-model:enabled-log-levels="enabledLogLevels"
      v-model:log-search-query="logSearchQuery"
      :terminal-title="$t('pages.monitoring.terminalTitle')"
      :terminal-subtitle="$t('pages.monitoring.terminalSubtitle')"
      :status-label="$t('pages.monitoring.status')"
      :system-cpu-label="$t('pages.monitoring.systemCpu')"
      :system-memory-label="$t('pages.monitoring.systemMemory')"
      :process-memory-label="$t('pages.monitoring.processMemory')"
      :logs-label="$t('pages.monitoring.logs')"
      :log-search-label="$t('pages.monitoring.logSearch')"
      :stream-connected-label="$t('pages.monitoring.streamConnected')"
      :stream-disconnected-label="$t('pages.monitoring.streamDisconnected')"
      :pause-follow-label="$t('pages.monitoring.pauseFollow')"
      :follow-latest-label="$t('pages.monitoring.followLatest')"
      :jump-latest-label="$t('pages.monitoring.jumpLatest')"
      :following-label="$t('pages.monitoring.following')"
      :follow-paused-label="$t('pages.monitoring.followPaused')"
      :system-source-label="$t('pages.monitoring.systemSource')"
      :no-data-label="$t('pages.monitoring.noData')"
      :no-data-subtitle-label="$t('pages.monitoring.noDataSubtitle')"
      :online-label="$t('common.actions.status.online')"
      :offline-label="$t('common.actions.status.offline')"
      :is-online="isOnline"
      :log-connected="logConnected"
      :status="status"
      :terminal-logs="terminalLogs"
      :format-percent="formatPercent"
      :format-memory-size="formatMemorySize"
      :format-system-memory-usage="formatSystemMemoryUsage"
      :format-timestamp="formatTimestamp"
    />
  </v-container>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { storeToRefs } from 'pinia'

import AppPageHeader from '@/components/AppPageHeader.vue'
import MonitoringTerminalShell from '@/components/monitoring/MonitoringTerminalShell.vue'
import { useMonitoringStore } from '@/stores/monitoring'

const monitoringStore = useMonitoringStore()
const { filteredLogs, enabledLogLevels, logSearchQuery, isOnline, logConnected, status } =
  storeToRefs(monitoringStore)

const terminalLogs = computed(() => filteredLogs.value.slice().reverse())

const connectLogs = () => monitoringStore.connectLogs()
const clearLogs = () => monitoringStore.clearLogs()

const formatPercent = (value: number) => `${Number(value || 0).toFixed(1)}%`
const formatMemorySize = (value: number) => {
  const mb = Number(value || 0)
  if (mb >= 1024) {
    return `${(mb / 1024).toFixed(mb >= 10240 ? 0 : 1)} GB`
  }
  return `${mb.toFixed(1)} MB`
}
const formatSystemMemoryUsage = (
  percent: number,
  usedMb: number,
  totalMb: number,
) => {
  if (!totalMb) {
    return formatPercent(percent)
  }
  return `${formatPercent(percent)} · ${formatMemorySize(usedMb)}/${formatMemorySize(totalMb)}`
}

const formatTimestamp = (timestamp: number) => {
  const date = new Date(timestamp)
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  const seconds = String(date.getSeconds()).padStart(2, '0')
  const milliseconds = String(date.getMilliseconds()).padStart(3, '0')
  return `${hours}:${minutes}:${seconds}.${milliseconds}`
}
</script>
