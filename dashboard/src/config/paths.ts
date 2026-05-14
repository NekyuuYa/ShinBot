import type { ConfigDocument, ConfigRecord, ConfigValue } from '@/api/config'

export type ConfigPathSegment = string | number

type ConfigContainer = ConfigRecord | ConfigValue[]

const isRecord = (value: unknown): value is ConfigRecord =>
  Boolean(value) && typeof value === 'object' && !Array.isArray(value)

const cloneContainer = (value: unknown, nextSegment?: ConfigPathSegment): ConfigContainer => {
  if (Array.isArray(value)) {
    return [...value]
  }
  if (isRecord(value)) {
    return { ...value }
  }
  return typeof nextSegment === 'number' ? [] : {}
}

export function parseConfigPath(path: string): ConfigPathSegment[] {
  const result: ConfigPathSegment[] = []
  let token = ''

  for (let index = 0; index < path.length; index += 1) {
    const char = path[index]

    if (char === '.') {
      if (token) {
        result.push(token)
        token = ''
      }
      continue
    }

    if (char === '[') {
      if (token) {
        result.push(token)
        token = ''
      }

      const closeIndex = path.indexOf(']', index + 1)
      if (closeIndex === -1) {
        token += char
        continue
      }

      const rawSegment = path.slice(index + 1, closeIndex).trim()
      if (rawSegment) {
        result.push(/^\d+$/.test(rawSegment) ? Number(rawSegment) : rawSegment)
      }
      index = closeIndex
      continue
    }

    token += char
  }

  if (token) {
    result.push(token)
  }
  return result
}

export function joinConfigPath(...parts: Array<string | undefined | null>): string {
  return parts
    .map((part) => part?.trim() ?? '')
    .filter(Boolean)
    .join('.')
}

export function getConfigPathValue(
  source: ConfigDocument | ConfigRecord | ConfigValue[] | null | undefined,
  path: string
): ConfigValue | undefined {
  const segments = parseConfigPath(path)
  if (segments.length === 0) {
    return source as ConfigValue | undefined
  }

  let current: unknown = source
  for (const segment of segments) {
    if (typeof segment === 'number') {
      if (!Array.isArray(current) || segment >= current.length) {
        return undefined
      }
      current = current[segment]
      continue
    }

    if (!isRecord(current) || !(segment in current)) {
      return undefined
    }
    current = current[segment]
  }

  return current as ConfigValue | undefined
}

export function hasConfigPathValue(
  source: ConfigDocument | ConfigRecord | ConfigValue[] | null | undefined,
  path: string
): boolean {
  const segments = parseConfigPath(path)
  if (segments.length === 0) {
    return source !== undefined
  }

  let current: unknown = source
  for (const segment of segments) {
    if (typeof segment === 'number') {
      if (!Array.isArray(current) || segment >= current.length) {
        return false
      }
      current = current[segment]
      continue
    }

    if (!isRecord(current) || !Object.prototype.hasOwnProperty.call(current, segment)) {
      return false
    }
    current = current[segment]
  }

  return true
}

export function setConfigPathValue<T extends ConfigDocument | ConfigRecord>(
  source: T,
  path: string,
  value: ConfigValue
): T {
  const segments = parseConfigPath(path)
  if (segments.length === 0) {
    return value as T
  }

  const root = cloneContainer(source)
  let current: ConfigContainer = root

  segments.forEach((segment, index) => {
    const isLast = index === segments.length - 1
    const nextSegment = segments[index + 1]

    if (isLast) {
      if (Array.isArray(current) && typeof segment === 'number') {
        current[segment] = value
        return
      }
      if (!Array.isArray(current) && typeof segment === 'string') {
        current[segment] = value
      }
      return
    }

    if (Array.isArray(current) && typeof segment === 'number') {
      const nextValue = cloneContainer(current[segment], nextSegment)
      current[segment] = nextValue
      current = nextValue
      return
    }

    if (!Array.isArray(current) && typeof segment === 'string') {
      const nextValue = cloneContainer(current[segment], nextSegment)
      current[segment] = nextValue
      current = nextValue
    }
  })

  return root as T
}

export function deleteConfigPathValue<T extends ConfigDocument | ConfigRecord>(
  source: T,
  path: string
): T {
  const segments = parseConfigPath(path)
  if (segments.length === 0) {
    return {} as T
  }

  const root = cloneContainer(source)
  let current: ConfigContainer = root

  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index]
    const nextSegment = segments[index + 1]

    if (Array.isArray(current) && typeof segment === 'number') {
      const nextValue = cloneContainer(current[segment], nextSegment)
      current[segment] = nextValue
      current = nextValue
      continue
    }

    if (!Array.isArray(current) && typeof segment === 'string') {
      const nextValue = cloneContainer(current[segment], nextSegment)
      current[segment] = nextValue
      current = nextValue
    }
  }

  const lastSegment = segments[segments.length - 1]
  if (Array.isArray(current) && typeof lastSegment === 'number') {
    current.splice(lastSegment, 1)
  } else if (!Array.isArray(current) && typeof lastSegment === 'string') {
    delete current[lastSegment]
  }

  return root as T
}
