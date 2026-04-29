import axios, {
  type AxiosResponse,
  type AxiosInstance,
  type AxiosRequestConfig,
  type InternalAxiosRequestConfig,
} from 'axios'
import type { Router } from 'vue-router'
import { currentLocale, translate } from '@/plugins/i18n'

export interface ApiError {
  code: string
  message: string
}

export interface ApiResponse<T = unknown> {
  success: boolean
  data?: T
  error?: ApiError
  timestamp: number
}

type ApiErrorNotifier = (message: string) => void
type UnauthorizedHandler = () => void
type RequestTracker = {
  start: () => void
  stop: () => void
}
const SESSION_AUTH_ERROR_CODES = new Set([
  'AUTH_TOKEN_MISSING',
  'AUTH_TOKEN_INVALID',
  'AUTH_TOKEN_EXPIRED',
])

export type ApiRequestConfig = AxiosRequestConfig & {
  suppressErrorNotify?: boolean
}

class ApiClient {
  private instance: AxiosInstance
  private router: Router | null = null
  private errorNotifier: ApiErrorNotifier | null = null
  private unauthorizedHandler: UnauthorizedHandler | null = null
  private requestTracker: RequestTracker | null = null

  constructor(baseURL: string = import.meta.env.VITE_API_BASE_URL || '/api/v1') {
    this.instance = axios.create({
      baseURL,
      timeout: 10000,
      withCredentials: true,
      headers: {
        'Content-Type': 'application/json',
      },
    })

    this.instance.interceptors.request.use(
      (config: InternalAxiosRequestConfig) => {
        this.requestTracker?.start()
        config.headers['Accept-Language'] = currentLocale()
        return config
      },
      (error) => {
        this.requestTracker?.stop()
        return Promise.reject(error)
      }
    )

    // 响应拦截器：统一处理错误
    this.instance.interceptors.response.use(
      (response) => {
        this.requestTracker?.stop()
        return response
      },
      (error) => {
        this.requestTracker?.stop()
        
        // 处理取消请求的情况
        if (axios.isCancel(error)) {
          return Promise.reject(error)
        }

        const responseData = error.response?.data as {
          error?: { code?: string; message?: string }
          message?: string
        } | undefined
        const responseErrorCode = responseData?.error?.code ?? ''
        const isSessionAuthError =
          error.response?.status === 401
          && SESSION_AUTH_ERROR_CODES.has(responseErrorCode)

        if (isSessionAuthError) {
          this.unauthorizedHandler?.()
          if (this.router && this.router.currentRoute.value.path !== '/login') {
            void this.router.push('/login')
          }
        }

        const fallbackMessage =
          typeof error.message === 'string' && error.message
            ? error.message
            : translate('common.actions.message.requestFailed')
        const responseMessage = isSessionAuthError
          ? translate('pages.auth.sessionExpired')
          : responseData?.error?.message
            ?? responseData?.message
            ?? fallbackMessage

        const requestConfig = error.config as ApiRequestConfig | undefined
        if (!requestConfig?.suppressErrorNotify) {
          this.errorNotifier?.(responseMessage)
        }
        return Promise.reject(error)
      }
    )
  }

  setRouter(router: Router) {
    this.router = router
  }

  setErrorNotifier(notifier: ApiErrorNotifier) {
    this.errorNotifier = notifier
  }

  setUnauthorizedHandler(handler: UnauthorizedHandler) {
    this.unauthorizedHandler = handler
  }

  setRequestTracker(tracker: RequestTracker) {
    this.requestTracker = tracker
  }

  get<T = unknown>(url: string, config?: ApiRequestConfig) {
    return this.instance.get<ApiResponse<T>>(url, config)
  }

  post<T = unknown>(url: string, data?: unknown, config?: ApiRequestConfig) {
    return this.instance.post<ApiResponse<T>>(url, data, config)
  }

  patch<T = unknown>(url: string, data?: unknown, config?: ApiRequestConfig) {
    return this.instance.patch<ApiResponse<T>>(url, data, config)
  }

  delete<T = unknown>(url: string, config?: ApiRequestConfig) {
    return this.instance.delete<ApiResponse<T>>(url, config)
  }

  async unwrap<T>(request: Promise<AxiosResponse<ApiResponse<T>>>): Promise<T> {
    const response = await request
    if (response.data.success && response.data.data !== undefined) {
      return response.data.data
    }

    throw new Error(response.data.error?.message || translate('common.actions.message.operationFailed'))
  }
}

export const apiClient = new ApiClient()
