export const normalizeStringList = (items: string[]) => {
  const seen = new Set<string>()
  const list: string[] = []

  for (const item of items) {
    const value = item.trim()
    if (!value || seen.has(value)) {
      continue
    }
    seen.add(value)
    list.push(value)
  }

  return list
}
