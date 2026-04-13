import type { AxiosError } from 'axios'

type ErrorPayload = {
  error?: {
    code?: string
    message?: string
  }
  message?: string
}

export function getErrorMessage(error: unknown, fallback = 'Unexpected error'): string {
  if (typeof error === 'string') {
    return error
  }

  if (isAxiosError(error)) {
    const data = error.response?.data as ErrorPayload | undefined
    return data?.error?.message ?? data?.message ?? error.message ?? fallback
  }

  if (error instanceof Error) {
    return error.message
  }

  return fallback
}

export function isAxiosError(error: unknown): error is AxiosError<ErrorPayload> {
  return error !== null && typeof error === 'object' && 'isAxiosError' in error
}
