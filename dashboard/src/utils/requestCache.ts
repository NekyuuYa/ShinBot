export interface CachedRequestOptions {
  force?: boolean
}

export const createCachedRequest = <T>(
  fetcher: () => Promise<T>,
  staleTimeMs: number
) => {
  let inflightRequest: Promise<T> | null = null
  let cachedValue: T | undefined
  let hasCachedValue = false
  let fetchedAt = 0

  return async (options: CachedRequestOptions = {}): Promise<T> => {
    if (inflightRequest) {
      return inflightRequest
    }

    const isCacheFresh =
      !options.force
      && hasCachedValue
      && Date.now() - fetchedAt < staleTimeMs

    if (isCacheFresh) {
      return cachedValue as T
    }

    inflightRequest = fetcher()
      .then((value) => {
        cachedValue = value
        hasCachedValue = true
        fetchedAt = Date.now()
        return value
      })
      .finally(() => {
        inflightRequest = null
      })

    return inflightRequest
  }
}
