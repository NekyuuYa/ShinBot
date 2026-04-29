import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { useWebSocket } from '@/composables/useWebSocket'
import { useInstancesStore } from '@/stores/instances'
import { useAuthStore } from '@/stores/auth'

export type LogLevel = 'DEBUG' | 'INFO' | 'WARN' | 'ERROR'

export interface MonitoringLogEntry {
  id: string
  level: LogLevel
  message: string
  timestamp: number
  source?: string
}

export interface SystemStatus {
  totalInstances: number
  runningInstances: number
  stoppedInstances: number
  totalPlugins: number
  enabledPlugins: number
  cpuUsage: number
  memoryUsage: number
  online: boolean
}

interface WsEnvelope<T> {
  success?: boolean
  data?: T
  type?: string
  timestamp?: number
}

interface LogWsPayload {
  level?: string
  message?: string
  timestamp?: number
  source?: string
}

interface StatusWsPayload {
  totalInstances?: number
  runningInstances?: number
  stoppedInstances?: number
  totalPlugins?: number
  enabledPlugins?: number
  cpuUsage?: number
  memoryUsage?: number
  online?: boolean
  instances?: Array<{ id?: string; running?: boolean }>
}

const LOG_LEVEL_ORDER: readonly LogLevel[] = ['DEBUG', 'INFO', 'WARN', 'ERROR']

function normalizeLogLevel(level: string | undefined): LogLevel {
  const upper = (level ?? 'INFO').toUpperCase()
  if (upper === 'WARNING') {
    return 'WARN'
  }
  if (upper === 'CRITICAL' || upper === 'FATAL') {
    return 'ERROR'
  }
  if (LOG_LEVEL_ORDER.includes(upper as LogLevel)) {
    return upper as LogLevel
  }
  return 'INFO'
}

function makeLogId(entry: Pick<MonitoringLogEntry, 'timestamp' | 'level' | 'message'>): string {
  return `${entry.timestamp}-${entry.level}-${entry.message.slice(0, 24)}`
}

function parseEnvelope<T>(payload: string): WsEnvelope<T> | T | null {
  try {
    return JSON.parse(payload) as WsEnvelope<T> | T
  } catch {
    return null
  }
}

function extractLogEntries(payload: unknown): MonitoringLogEntry[] {
  if (Array.isArray(payload)) {
    return payload.flatMap((item) => extractLogEntries(item))
  }

  if (!payload || typeof payload !== 'object') {
    return []
  }

  const record = payload as Record<string, unknown>
  const level = normalizeLogLevel(typeof record.level === 'string' ? record.level : undefined)
  const message = typeof record.message === 'string' ? record.message : ''
  const timestamp =
    typeof record.timestamp === 'number'
      ? record.timestamp
      : typeof record.ts === 'number'
        ? record.ts * 1000
        : Date.now()
  const source =
    typeof record.source === 'string'
      ? record.source
      : typeof record.logger === 'string'
        ? record.logger
        : undefined

  if (!message) {
    return []
  }

  return [
    {
      id: makeLogId({ timestamp, level, message }),
      level,
      message,
      timestamp,
      source,
    },
  ]
}

function extractStatus(payload: unknown): SystemStatus | null {
  if (!payload || typeof payload !== 'object') {
    return null
  }

  const record = payload as Record<string, unknown>

  const totalInstances = typeof record.totalInstances === 'number' ? record.totalInstances : 0
  const runningInstances = typeof record.runningInstances === 'number' ? record.runningInstances : 0
  const stoppedInstances = typeof record.stoppedInstances === 'number' ? record.stoppedInstances : 0
  const totalPlugins = typeof record.totalPlugins === 'number' ? record.totalPlugins : 0
  const enabledPlugins = typeof record.enabledPlugins === 'number' ? record.enabledPlugins : 0
  const cpuUsage = typeof record.cpuUsage === 'number' ? record.cpuUsage : 0
  const memoryUsage = typeof record.memoryUsage === 'number' ? record.memoryUsage : 0
  const online = typeof record.online === 'boolean' ? record.online : true

  return {
    totalInstances,
    runningInstances,
    stoppedInstances,
    totalPlugins,
    enabledPlugins,
    cpuUsage,
    memoryUsage,
    online,
  }
}

function extractInstanceStatuses(
  payload: unknown
): Array<{ id: string; status: 'running' | 'stopped' }> {
  if (!payload || typeof payload !== 'object') {
    return []
  }

  const record = payload as Record<string, unknown>
  if (!Array.isArray(record.instances)) {
    return []
  }

  return record.instances
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null
      }

      const instance = item as Record<string, unknown>
      const id = typeof instance.id === 'string' ? instance.id : ''
      const running = typeof instance.running === 'boolean' ? instance.running : false
      if (!id) return null
      return { id, status: (running ? 'running' : 'stopped') as 'running' | 'stopped' }
    })
    .filter((i): i is { id: string; status: 'running' | 'stopped' } => i !== null)
}

export const useMonitoringStore = defineStore('monitoring', () => {
  const instancesStore = useInstancesStore()
  const authStore = useAuthStore()
  const logs = ref<MonitoringLogEntry[]>([])
  const enabledLogLevels = ref<LogLevel[]>([...LOG_LEVEL_ORDER])
  const status = ref<SystemStatus>({
    totalInstances: 0,
    runningInstances: 0,
    stoppedInstances: 0,
    totalPlugins: 0,
    enabledPlugins: 0,
    cpuUsage: 0,
    memoryUsage: 0,
    online: false,
  })

  const reconnectDelayMs = 5000
  const heartbeatIntervalMs = 30000

  const filteredLogs = computed(() => {
    const activeLevels = new Set(enabledLogLevels.value)
    if (activeLevels.size === 0) {
      return []
    }

    return logs.value.filter((entry) => activeLevels.has(entry.level))
  })

  const MAX_LOG_ENTRIES = 100

  const pushLogs = (entries: MonitoringLogEntry[]) => {
    logs.value = [...entries, ...logs.value].slice(0, MAX_LOG_ENTRIES)
  }

  const logConnection = useWebSocket({
    defaultPath: '/ws/logs',
    configuredUrl: () => import.meta.env.VITE_WS_LOGS_URL,
    token: () => authStore.token,
    reconnectDelayMs,
    heartbeatIntervalMs,
    onMessage: (event) => {
      const parsed = parseEnvelope<LogWsPayload | LogWsPayload[]>(event.data)
      const payload = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed
      const entries = extractLogEntries(payload)
      if (entries.length > 0) {
        pushLogs(entries)
      }
    },
  })

  const connectLogs = (endpoint?: string) => {
    logConnection.connect(endpoint)
  }

  const disconnectLogs = () => {
    logConnection.disconnect()
  }

  const statusConnection = useWebSocket({
    defaultPath: '/ws/status',
    configuredUrl: () => import.meta.env.VITE_WS_STATUS_URL,
    token: () => authStore.token,
    reconnectDelayMs,
    onAuthMissing: () => {
      status.value.online = false
    },
    onOpen: () => {
      status.value.online = true
    },
    onClose: () => {
      status.value.online = false
    },
    onError: () => {
      status.value.online = false
    },
    onMessage: (event) => {
      const parsed = parseEnvelope<StatusWsPayload>(event.data)
      const payload = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed
      const nextStatus = extractStatus(payload)
      if (nextStatus) {
        status.value = {
          ...nextStatus,
          online: statusConnected.value && nextStatus.online,
        }
      }
      const instanceStatuses = extractInstanceStatuses(payload)
      if (instanceStatuses.length > 0) {
        instancesStore.syncInstanceStatuses(instanceStatuses)
      }
    },
  })

  const connectStatus = (endpoint?: string) => {
    statusConnection.connect(endpoint)
  }

  const disconnectStatus = () => {
    status.value.online = false
    statusConnection.disconnect()
  }

  const logConnected = logConnection.connected
  const statusConnected = statusConnection.connected
  const isOnline = computed(() => statusConnected.value && status.value.online)

  const clearLogs = () => {
    logs.value = []
  }

  return {
    logs,
    filteredLogs,
    enabledLogLevels,
    status,
    isOnline,
    logConnected,
    statusConnected,
    connectLogs,
    disconnectLogs,
    connectStatus,
    disconnectStatus,
    clearLogs,
  }
})
