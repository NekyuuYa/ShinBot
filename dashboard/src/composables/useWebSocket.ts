import { ref, toValue, type MaybeRefOrGetter } from 'vue'

const LOCALHOST_HOSTS = new Set(['localhost', '127.0.0.1', '::1'])
const DEFAULT_AUTH_FAILURE_CLOSE_CODE = 1008
const DEFAULT_RECONNECT_DELAY_MS = 5000

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

function appendAuthToken(url: string, token: string): string {
  const normalizedToken = normalizeToken(token)
  if (!normalizedToken) {
    return url
  }

  try {
    const parsed = new URL(url)
    parsed.searchParams.set('token', normalizedToken)
    return parsed.toString()
  } catch {
    const separator = url.includes('?') ? '&' : '?'
    return `${url}${separator}token=${encodeURIComponent(normalizedToken)}`
  }
}

export interface UseWebSocketOptions {
  defaultPath: string
  configuredUrl?: MaybeRefOrGetter<string | undefined>
  token: MaybeRefOrGetter<string>
  reconnectDelayMs?: number
  heartbeatIntervalMs?: number
  authFailureCloseCode?: number
  onAuthMissing?: () => void
  onOpen?: (socket: WebSocket) => void
  onClose?: (event: CloseEvent) => void
  onError?: (event: Event) => void
  onMessage?: (event: MessageEvent<string>) => void
}

export function useWebSocket(options: UseWebSocketOptions) {
  const connected = ref(false)

  let socket: WebSocket | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null
  let manualClose = false
  let lastEndpointOverride: string | undefined

  const reconnectDelayMs = options.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS
  const authFailureCloseCode =
    options.authFailureCloseCode ?? DEFAULT_AUTH_FAILURE_CLOSE_CODE

  const clearReconnectTimer = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  const clearHeartbeatTimer = () => {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer)
      heartbeatTimer = null
    }
  }

  const clearTimers = () => {
    clearReconnectTimer()
    clearHeartbeatTimer()
  }

  const scheduleReconnect = () => {
    if (typeof window === 'undefined' || manualClose) {
      return
    }

    clearReconnectTimer()
    reconnectTimer = setTimeout(() => {
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        return
      }

      socket = null
      connect(lastEndpointOverride)
    }, reconnectDelayMs)
  }

  const armHeartbeat = () => {
    clearHeartbeatTimer()

    if (!options.heartbeatIntervalMs) {
      return
    }

    heartbeatTimer = setInterval(() => {
      if (socket?.readyState === WebSocket.OPEN) {
        socket.send('ping')
      }
    }, options.heartbeatIntervalMs)
  }

  const connect = (endpoint?: string) => {
    if (typeof window === 'undefined' || socket) {
      return
    }

    manualClose = false
    lastEndpointOverride = endpoint

    const token = normalizeToken(toValue(options.token) ?? '')
    if (!token) {
      connected.value = false
      clearTimers()
      options.onAuthMissing?.()
      return
    }

    const defaultUrl = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}${options.defaultPath}`
    const finalEndpoint = appendAuthToken(
      resolveWsEndpoint(endpoint ?? toValue(options.configuredUrl), defaultUrl),
      token
    )

    clearTimers()
    socket = new WebSocket(finalEndpoint)
    socket.onopen = () => {
      connected.value = true
      armHeartbeat()
      if (socket) {
        options.onOpen?.(socket)
      }
    }
    socket.onclose = (event) => {
      connected.value = false
      socket = null
      clearHeartbeatTimer()
      options.onClose?.(event)
      if (manualClose || event.code === authFailureCloseCode) {
        return
      }
      scheduleReconnect()
    }
    socket.onerror = (event) => {
      connected.value = false
      clearHeartbeatTimer()
      options.onError?.(event)

      const activeSocket = socket
      if (!activeSocket) {
        scheduleReconnect()
        return
      }

      if (activeSocket.readyState === WebSocket.CLOSED) {
        socket = null
        scheduleReconnect()
        return
      }

      activeSocket.close()
      scheduleReconnect()
    }
    socket.onmessage = (event: MessageEvent<string>) => {
      options.onMessage?.(event)
    }
  }

  const disconnect = () => {
    manualClose = true
    clearTimers()

    const activeSocket = socket
    socket = null
    connected.value = false
    activeSocket?.close()
  }

  return {
    connected,
    connect,
    disconnect,
  }
}