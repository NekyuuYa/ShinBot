interface JwtPayload {
  exp?: number
}

const decodeBase64Url = (value: string): string => {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/')
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=')
  const binary = atob(padded)
  const bytes = new Uint8Array(binary.length)

  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index)
  }

  return new TextDecoder().decode(bytes)
}

const parseJwtPayload = (token: string): JwtPayload | null => {
  const [, payload] = token.split('.')
  if (!payload) {
    return null
  }

  try {
    return JSON.parse(decodeBase64Url(payload)) as JwtPayload
  } catch {
    return null
  }
}

export const getJwtExpiry = (token: string): number | null => {
  if (!token) {
    return null
  }

  const payload = parseJwtPayload(token)
  return typeof payload?.exp === 'number' ? payload.exp : null
}

export const hasValidJwtToken = (token: string): boolean => {
  const expiry = getJwtExpiry(token)
  if (expiry === null) {
    return false
  }

  return expiry * 1000 > Date.now()
}
