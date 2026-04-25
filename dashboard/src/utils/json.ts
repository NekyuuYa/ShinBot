import { translate } from '@/plugins/i18n'

/**
 * Safely parse a JSON string into an object.
 * Throws a localized error message if parsing fails.
 */
export function safeJsonParse<T = Record<string, unknown>>(
  value: string,
  emptyFallback: T = {} as T,
  errorMessage: string = translate('pages.agents.messages.invalidJson')
): T {
  const trimmed = value.trim()
  if (!trimmed) {
    return emptyFallback
  }

  try {
    const parsed = JSON.parse(trimmed)
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as T
    }
  } catch (err) {
    throw new Error(errorMessage)
  }

  throw new Error(errorMessage)
}

/**
 * Format an object into a pretty JSON string.
 */
export function prettyJson(value: any): string {
  if (!value || (typeof value === 'object' && Object.keys(value).length === 0)) {
    return ''
  }
  return JSON.stringify(value, null, 2)
}
