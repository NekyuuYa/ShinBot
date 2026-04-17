import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
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
const LOCALHOST_HOSTS = new Set(['localhost', '127.0.0.1', '::1'])
const AUTH_FAILURE_CLOSE_CODE = 1008

function normalizeToken(rawToken: string): string {
  const trimmed = (rawToken ?? '').trim()
  if (!trimmed) {
    return ''
  }

  if (trimmed.toLowerCase().startsWith('bearer ')) {
    return trimmed.slice(7).trim()
  }

  return trimmed
}

function resolveWsEndpoint(configured: string | undefined, fallback: string): string {
  const raw = (configured ?? '').trim()
  if (!raw) {
    return fallback
  }

  if (raw.startsWith('/')) {
    return `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}${raw}`
  }

  try {
    const parsed = new URL(raw, window.location.origin)
    if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
      parsed.protocol = parsed.protocol === 'https:' ? 'wss:' : 'ws:'
    }

    const currentHost = window.location.hostname.toLowerCase()
    const isConfiguredLocalhost = LOCALHOST_HOSTS.has(parsed.hostname.toLowerCase())
    const isCurrentLocalhost = LOCALHOST_HOSTS.has(currentHost)
    if (isConfiguredLocalhost && !isCurrentLocalhost) {
      return fallback
    }

    return parsed.toString()
  } catch {
    return fallback
  }
}

/**
 * Append auth token to URL query parameters.
 */
function appendAuthToken(url: string, token: string): string {
  const normalizedToken = normalizeToken(token)
  if (!normalizedToken) return url
  try {
    const parsed = new URL(url)
    parsed.searchParams.set('token', normalizedToken)
    return parsed.toString()
  } catch {
    const separator = url.includes('?') ? '&' : '?'
    return `${url}${separator}token=${encodeURIComponent(normalizedToken)}`
  }
}

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
  const logConnected = ref(false)
  const statusConnected = ref(false)

  let logSocket: WebSocket | null = null
  let statusSocket: WebSocket | null = null
  let logReconnectTimer: ReturnType<typeof setTimeout> | null = null
  let statusReconnectTimer: ReturnType<typeof setTimeout> | null = null
  let logHeartbeatTimer: ReturnType<typeof setInterval> | null = null

  const reconnectDelayMs = 5000
  const heartbeatIntervalMs = 30000

  const filteredLogs = computed(() => {
    const activeLevels = new Set(enabledLogLevels.value)
    if (activeLevels.size === 0) {
      return []
    }

    return logs.value.filter((entry) => activeLevels.has(entry.level))
  })

  const isOnline = computed(() => statusConnected.value && status.value.online)

  const pushLogs = (entries: MonitoringLogEntry[]) => {
    logs.value = [...entries, ...logs.value].slice(0, 1000)
  }

  const clearLogTimers = () => {
    if (logReconnectTimer) clearTimeout(logReconnectTimer)
    if (logHeartbeatTimer) clearInterval(logHeartbeatTimer)
    logReconnectTimer = null
    logHeartbeatTimer = null
  }

  const scheduleLogReconnect = (endpoint?: string) => {
    if (typeof window === 'undefined') {
      return
    }
    if (logReconnectTimer) {
      clearTimeout(logReconnectTimer)
    }
    logReconnectTimer = setTimeout(() => {
      if (logSocket && logSocket.readyState !== WebSocket.CLOSED) {
        return
      }
      logSocket = null
      connectLogs(endpoint)
    }, reconnectDelayMs)
  }

  const armLogHeartbeat = () => {
    if (logHeartbeatTimer) clearInterval(logHeartbeatTimer)
    logHeartbeatTimer = setInterval(() => {
      if (logSocket?.readyState === WebSocket.OPEN) {
        logSocket.send('ping')
      }
    }, heartbeatIntervalMs)
  }

  const connectLogs = (endpoint?: string) => {
    if (typeof window === 'undefined' || logSocket) {
      return
    }

    if (!normalizeToken(authStore.token)) {
      logConnected.value = false
      return
    }

    const defaultUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/logs`
    let finalEndpoint = resolveWsEndpoint(endpoint ?? import.meta.env.VITE_WS_LOGS_URL, defaultUrl)
    finalEndpoint = appendAuthToken(finalEndpoint, authStore.token)

    clearLogTimers()
    logSocket = new WebSocket(finalEndpoint)
    logSocket.onopen = () => {
      logConnected.value = true
      armLogHeartbeat()
    }
    logSocket.onclose = (event) => {
      logConnected.value = false
      logSocket = null
      if (event.code === AUTH_FAILURE_CLOSE_CODE) {
        return
      }
      scheduleLogReconnect(endpoint)
    }
    logSocket.onerror = () => {
      logConnected.value = false
      const socket = logSocket
      if (!socket) {
        scheduleLogReconnect(endpoint)
        return
      }

      if (socket.readyState === WebSocket.CLOSED) {
        logSocket = null
        scheduleLogReconnect(endpoint)
        return
      }

      // Some browsers may emit onerror before onclose. Close proactively and
      // arm a reconnect fallback so the stream can recover deterministically.
      socket.close()
      scheduleLogReconnect(endpoint)
    }
    logSocket.onmessage = (event: MessageEvent<string>) => {
      const parsed = parseEnvelope<LogWsPayload | LogWsPayload[]>(event.data)
      const payload = parsed && typeof parsed === 'object' && 'data' in parsed ? parsed.data : parsed
      const entries = extractLogEntries(payload)
      if (entries.length > 0) {
        pushLogs(entries)
      }
    }
  }

  const disconnectLogs = () => {
    clearLogTimers()
    logSocket?.close()
    logSocket = null
    logConnected.value = false
  }

  const clearStatusTimers = () => {
    if (statusReconnectTimer) clearTimeout(statusReconnectTimer)
    statusReconnectTimer = null
  }

  const scheduleStatusReconnect = (endpoint?: string) => {
    if (typeof window === 'undefined') {
      return
    }
    if (statusReconnectTimer) {
      clearTimeout(statusReconnectTimer)
    }
    statusReconnectTimer = setTimeout(() => {
      if (statusSocket && statusSocket.readyState !== WebSocket.CLOSED) {
        return
      }
      statusSocket = null
      connectStatus(endpoint)
    }, reconnectDelayMs)
  }

  const connectStatus = (endpoint?: string) => {
    if (typeof window === 'undefined' || statusSocket) {
      return
    }

    if (!normalizeToken(authStore.token)) {
      statusConnected.value = false
      status.value.online = false
      return
    }

    const defaultUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/status`
    let finalEndpoint = resolveWsEndpoint(
      endpoint ?? import.meta.env.VITE_WS_STATUS_URL,
      defaultUrl
    )
    finalEndpoint = appendAuthToken(finalEndpoint, authStore.token)

    clearStatusTimers()
    statusSocket = new WebSocket(finalEndpoint)
    statusSocket.onopen = () => {
      statusConnected.value = true
      status.value.online = true
    }
    statusSocket.onclose = (event) => {
      statusConnected.value = false
      statusSocket = null
      status.value.online = false
      if (event.code === AUTH_FAILURE_CLOSE_CODE) {
        return
      }
      scheduleStatusReconnect(endpoint)
    }
    statusSocket.onerror = () => {
      statusConnected.value = false
      status.value.online = false
      const socket = statusSocket
      if (!socket) {
        scheduleStatusReconnect(endpoint)
        return
      }

      if (socket.readyState === WebSocket.CLOSED) {
        statusSocket = null
        scheduleStatusReconnect(endpoint)
        return
      }

      socket.close()
      scheduleStatusReconnect(endpoint)
    }
    statusSocket.onmessage = (event: MessageEvent<string>) => {
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
    }
  }

  const disconnectStatus = () => {
    clearStatusTimers()
    statusSocket?.close()
    statusSocket = null
    statusConnected.value = false
  }

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
