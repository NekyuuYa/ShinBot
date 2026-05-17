import type { ConfigRecord, ConfigValue } from '@/api/config'
import { getConfigPathValue } from './paths'

type CompareOperator = '==' | '!=' | '>=' | '<=' | '>' | '<'

const COMPARISON_PATTERN = /^([A-Za-z0-9_.-]+)\s*(==|!=|>=|<=|>|<)\s*(.+)$/

function parseLiteral(value: string): ConfigValue | undefined {
  const normalized = value.trim()
  const quoted = normalized.match(/^(['"])(.*)\1$/)
  if (quoted) {
    return quoted[2]
  }
  if (normalized === 'true') {
    return true
  }
  if (normalized === 'false') {
    return false
  }
  if (normalized === 'null') {
    return null
  }
  if (/^-?\d+(\.\d+)?$/.test(normalized)) {
    return Number(normalized)
  }
  return normalized
}

function scalarComparable(value: ConfigValue | undefined): string | number | boolean | null | undefined {
  if (
    value === undefined
    || value === null
    || typeof value === 'string'
    || typeof value === 'number'
    || typeof value === 'boolean'
  ) {
    return value
  }
  return JSON.stringify(value)
}

function compareValues(
  actual: ConfigValue | undefined,
  operator: CompareOperator,
  expected: ConfigValue | undefined
): boolean {
  const left = scalarComparable(actual)
  const right = scalarComparable(expected)

  if (operator === '==') {
    return left === right
  }
  if (operator === '!=') {
    return left !== right
  }
  if (typeof left !== 'number' || typeof right !== 'number') {
    return true
  }
  if (operator === '>=') {
    return left >= right
  }
  if (operator === '<=') {
    return left <= right
  }
  if (operator === '>') {
    return left > right
  }
  return left < right
}

function evaluateComparison(expression: string, values: ConfigRecord): boolean {
  const match = expression.trim().match(COMPARISON_PATTERN)
  if (!match) {
    return true
  }

  const [, path, operator, rawExpected] = match
  const actual = getConfigPathValue(values, path)
  return compareValues(actual, operator as CompareOperator, parseLiteral(rawExpected))
}

export function isVisibleWhenSatisfied(
  expression: string | undefined,
  values: ConfigRecord
): boolean {
  const normalized = expression?.trim()
  if (!normalized) {
    return true
  }

  return normalized
    .split(/\s+\|\|\s+/)
    .some((orTerm) =>
      orTerm
        .split(/\s+&&\s+/)
        .every((andTerm) => evaluateComparison(andTerm, values))
    )
}
