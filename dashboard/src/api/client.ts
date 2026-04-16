import axios, {
  type AxiosInstance,
  type AxiosRequestConfig,
  type InternalAxiosRequestConfig,
} from 'axios'
import type { Router } from 'vue-router'
import { translate } from '@/plugins/i18n'

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
type RequestTracker = {
  start: () => void
  stop: () => void
}
type ApiRequestConfig = AxiosRequestConfig & {
  suppressErrorNotify?: boolean
}

class ApiClient {
  private instance: AxiosInstance
  private router: Router | null = null
  private errorNotifier: ApiErrorNotifier | null = null
  private requestTracker: RequestTracker | null = null

  constructor(baseURL: string = import.meta.env.VITE_API_BASE_URL || '/api/v1') {
    this.instance = axios.create({
      baseURL,
      timeout: 10000,
      headers: {
        'Content-Type': 'application/json',
      },
    })

    // 请求拦截器：添加 JWT token
    this.instance.interceptors.request.use(
      (config: InternalAxiosRequestConfig) => {
        this.requestTracker?.start()
        const token = localStorage.getItem('auth_token')
        if (token) {
          config.headers.Authorization = `Bearer ${token}`
        }
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

        if (error.response?.status === 401) {
          // Token 已失效，清除 Token 并跳转登录
          localStorage.removeItem('auth_token')
          localStorage.removeItem('auth_username')
          localStorage.removeItem('auth_must_change_credentials')
          if (this.router) {
            this.router.push('/login')
          }
        }

        const responseMessage =
          (error.response?.data as { error?: { message?: string }; message?: string } | undefined)
            ?.error?.message ??
          (error.response?.data as { message?: string } | undefined)?.message ??
          error.message ??
          translate('common.actions.message.requestFailed')

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
}

export const apiClient = new ApiClient()
