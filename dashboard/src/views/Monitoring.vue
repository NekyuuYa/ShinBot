<template>
  <v-container fluid class="pa-0">
    <v-row class="mb-8" align="center">
      <v-col cols="12" md="6">
        <h1 class="text-h4 font-weight-bold">{{ $t('pages.monitoring.title') }}</h1>
      </v-col>
      <v-col cols="12" md="6" class="text-end">
        <v-btn variant="tonal" class="me-2" @click="clearLogs">
          {{ $t('common.actions.action.reset') }}
        </v-btn>
        <v-btn color="primary" @click="connectLogs">
          {{ $t('pages.monitoring.connectLogs') }}
        </v-btn>
      </v-col>
    </v-row>

    <v-row class="mb-4">
      <v-col cols="12" md="3">
        <v-card class="pa-4" elevation="5">
          <div class="text-caption text-medium-emphasis">{{ $t('pages.monitoring.status') }}</div>
          <div class="text-h6">{{ status.online ? $t('common.actions.status.online') : $t('common.actions.status.offline') }}</div>
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
          <v-btn-toggle v-model="logLevelFilter" mandatory divided density="comfortable" class="monitor-filter-toggle">
            <v-btn value="ALL">ALL</v-btn>
            <v-btn value="INFO">INFO</v-btn>
            <v-btn value="WARN">WARN</v-btn>
            <v-btn value="ERROR">ERROR</v-btn>
          </v-btn-toggle>
        </v-col>
        <v-col cols="12" md="4" class="text-end">
          <v-chip :color="logConnected ? 'success' : 'error'" variant="tonal">
            {{ logConnected ? $t('common.actions.status.online') : $t('common.actions.status.offline') }}
          </v-chip>
        </v-col>
      </v-row>

      <div class="monitor-log-container">
        <template v-for="item in filteredLogs" :key="item.id">
          <v-card class="mb-2 log-row" variant="tonal" :color="logCardColor(item.level)">
            <v-card-text class="py-3 px-4">
              <v-row align="start" no-gutters>
                <v-col cols="12" md="2" class="d-flex align-center">
                  <v-chip size="small" variant="flat" :color="logColor(item.level)" class="font-weight-bold">{{ item.level }}</v-chip>
                </v-col>
                <v-col cols="12" md="2" class="text-caption text-medium-emphasis d-flex align-center">
                  {{ formatTime(item.timestamp) }}
                </v-col>
                <v-col cols="12" md="2" class="text-caption text-medium-emphasis d-flex align-center">
                  {{ item.source ?? '-' }}
                </v-col>
                <v-col cols="12" md="6" class="log-message">
                  {{ item.message }}
                </v-col>
              </v-row>
            </v-card-text>
          </v-card>
        </template>
        <v-empty-state
          v-if="filteredLogs.length === 0"
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
import { useMonitoringStore } from '@/stores/monitoring'

const monitoringStore = useMonitoringStore()
const { filteredLogs, logLevelFilter, logConnected, status } = storeToRefs(monitoringStore)

const connectLogs = () => monitoringStore.connectLogs()
const clearLogs = () => monitoringStore.clearLogs()

const logColor = (level: string) => {
  if (level === 'ERROR') return 'error'
  if (level === 'WARN') return 'warning'
  if (level === 'INFO') return 'info'
  return 'grey'
}

const logCardColor = (level: string) => {
  if (level === 'ERROR') return 'error'
  if (level === 'WARN') return 'warning'
  // Keep INFO/DEBUG rows neutral to avoid heavy blue tint and improve readability.
  return 'surface'
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

.monitor-log-scroll {
  padding: 12px 14px;
  background: rgba(199, 144, 0, 0.07);
  border-radius: 16px;
  border: 1px solid rgba(199, 144, 0, 0.1);
  box-shadow: inset 0 -1px 0 rgba(199, 144, 0, 0.08);
}

.monitor-log-scroll :deep(.v-virtual-scroll__container) {
  padding: 0;
}

.log-row {
  border: 1px solid rgba(199, 144, 0, 0.14);
  box-shadow: none;
  margin-inline: 2px;
}

.log-message {
  word-break: break-word;
  line-height: 1.4;
  color: rgba(0, 0, 0, 0.82);
}
</style>
