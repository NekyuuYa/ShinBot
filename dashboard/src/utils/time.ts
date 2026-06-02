export function normalizeTimestampMs(value: number | null | undefined): number | null {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return null
  }

  const absValue = Math.abs(value)
  if (absValue >= 1_000_000_000_000) {
    return value
  }
  if (absValue >= 1_000_000_000) {
    return value * 1000
  }
  return value
}
