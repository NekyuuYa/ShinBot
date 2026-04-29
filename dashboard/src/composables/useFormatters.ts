import type { Ref } from 'vue'

type DateLike = string | number | Date

const EMPTY_VALUE = '—'

const toDate = (value: DateLike) => (value instanceof Date ? value : new Date(value))

export function useFormatters(
  locale: Readonly<Ref<string>>,
  currency: Readonly<Ref<string>>
) {
  const formatNumber = (value: number) =>
    new Intl.NumberFormat(locale.value, { maximumFractionDigits: 0 }).format(value)

  const formatCompactNumber = (value: number) =>
    new Intl.NumberFormat(locale.value, {
      maximumFractionDigits: value >= 1000 ? 1 : 0,
      notation: value >= 1000 ? 'compact' : 'standard',
    }).format(value)

  const formatCurrency = (value: number) =>
    new Intl.NumberFormat(locale.value, {
      style: 'currency',
      currency: currency.value || 'CNY',
      minimumFractionDigits: value >= 100 ? 0 : 2,
      maximumFractionDigits: value >= 100 ? 0 : 2,
    }).format(value)

  const formatPercent = (value: number) =>
    new Intl.NumberFormat(locale.value, {
      style: 'percent',
      minimumFractionDigits: 0,
      maximumFractionDigits: 1,
    }).format(value)

  const formatDateValue = (
    value: DateLike | null | undefined,
    options: Intl.DateTimeFormatOptions
  ) => {
    if (!value) {
      return EMPTY_VALUE
    }

    return new Intl.DateTimeFormat(locale.value, options).format(toDate(value))
  }

  const formatDate = (value: string | null) =>
    formatDateValue(value, { month: 'short', day: 'numeric' })

  const formatDateRangeStart = (value: string | null) =>
    formatDateValue(value, { year: 'numeric', month: 'short', day: 'numeric' })

  const formatDateTime = (value: string | null) =>
    formatDateValue(value, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })

  const formatHour = (value: DateLike | null | undefined) =>
    formatDateValue(value, { hour: '2-digit' })

  const formatShortDate = (value: DateLike | null | undefined) =>
    formatDateValue(value, { month: 'short', day: 'numeric' })

  const formatDuration = (value: number | null) => {
    if (value === null || Number.isNaN(value)) {
      return EMPTY_VALUE
    }

    return value >= 1000 ? `${(value / 1000).toFixed(2)}s` : `${Math.round(value)}ms`
  }

  return {
    formatNumber,
    formatCompactNumber,
    formatCurrency,
    formatPercent,
    formatDate,
    formatDateRangeStart,
    formatDateTime,
    formatHour,
    formatShortDate,
    formatDuration,
  }
}