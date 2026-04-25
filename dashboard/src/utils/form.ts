import { translate } from '@/plugins/i18n'

/**
 * Format a number (or null/undefined) to a string for form input.
 */
export function formatOptionalNumber(value: number | null | undefined): string {
  return value === null || value === undefined ? '' : String(value)
}

/**
 * Normalize a string value: trim and return null if empty.
 */
export function normalizeNullableString(value: string): string | null {
  const normalized = value.trim()
  return normalized || null
}

/**
 * Parse a string into an integer. Throws localized error if invalid.
 */
export function parseOptionalInteger(value: string, labelKey: string): number | null {
  const normalized = value.trim()
  if (!normalized) return null
  const parsed = Number.parseInt(normalized, 10)
  if (!Number.isFinite(parsed)) {
    throw new Error(translate('pages.instances.form.invalidNumericValue', { field: translate(labelKey) }))
  }
  return parsed
}

/**
 * Parse a string into a float. Throws localized error if invalid.
 */
export function parseOptionalFloat(value: string, labelKey: string): number | null {
  const normalized = value.trim()
  if (!normalized) return null
  const parsed = Number.parseFloat(normalized)
  if (!Number.isFinite(parsed)) {
    throw new Error(translate('pages.instances.form.invalidNumericValue', { field: translate(labelKey) }))
  }
  return parsed
}

/**
 * Convert an object to KeyValueEntry array.
 */
export function objectToEntries(value: Record<string, unknown>): Array<{ key: string; value: string }> {
  return Object.entries(value).map(([key, entryValue]) => ({
    key,
    value: typeof entryValue === 'string' ? entryValue : JSON.stringify(entryValue),
  }))
}

/**
 * Convert KeyValueEntry array back to object, attempting to parse JSON values.
 */
export function entriesToObject(rows: Array<{ key: string; value: string }>): Record<string, unknown> {
  const output: Record<string, unknown> = {}
  for (const row of rows) {
    const key = row.key.trim()
    if (!key) continue
    const rawValue = row.value.trim()
    if (!rawValue) {
      output[key] = ''
      continue
    }
    try {
      output[key] = JSON.parse(rawValue)
    } catch {
      output[key] = rawValue
    }
  }
  return output
}
