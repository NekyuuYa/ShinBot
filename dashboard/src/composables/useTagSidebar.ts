import { computed, ref, watch } from 'vue'

export interface TagSidebarItem {
  id: string
  title: string
  subtitle?: string
  icon: string
  badge?: string | number
  badgeColor?: string
}

interface UseTagSidebarOptions<T> {
  getTags: (item: T) => string[]
  allTitle: string
  allSubtitle: string
  tagSubtitle: string
  allIcon?: string
  tagIcon?: string
  allBadgeColor?: string
  tagBadgeColor?: string
}

const normalizeTags = (tags: string[]) => {
  const values: string[] = []
  const seen = new Set<string>()

  for (const tag of tags) {
    const value = tag.trim()
    if (!value || seen.has(value)) {
      continue
    }
    seen.add(value)
    values.push(value)
  }

  return values
}

export const useTagSidebar = <T>(
  listGetter: () => T[],
  options: UseTagSidebarOptions<T>
) => {
  const activeTag = ref('all')

  const allTags = computed(() => {
    const tagSet = new Set<string>()

    for (const item of listGetter()) {
      for (const tag of normalizeTags(options.getTags(item))) {
        tagSet.add(tag)
      }
    }

    return Array.from(tagSet).sort((left, right) => left.localeCompare(right))
  })

  const filteredItems = computed(() => {
    const list = listGetter()
    if (activeTag.value === 'all') {
      return list
    }

    return list.filter((item) => normalizeTags(options.getTags(item)).includes(activeTag.value))
  })

  const sidebarItems = computed<TagSidebarItem[]>(() => {
    const list = listGetter()

    const allItem: TagSidebarItem = {
      id: 'all',
      title: options.allTitle,
      subtitle: options.allSubtitle,
      icon: options.allIcon ?? 'mdi-view-grid-outline',
      badge: list.length,
      badgeColor: options.allBadgeColor ?? 'primary',
    }

    const tagItems = allTags.value.map((tag) => ({
      id: tag,
      title: tag,
      subtitle: options.tagSubtitle,
      icon: options.tagIcon ?? 'mdi-tag-outline',
      badge: list.filter((item) => normalizeTags(options.getTags(item)).includes(tag)).length,
      badgeColor: options.tagBadgeColor ?? 'secondary',
    }))

    return [allItem, ...tagItems]
  })

  watch(allTags, (tags) => {
    if (activeTag.value !== 'all' && !tags.includes(activeTag.value)) {
      activeTag.value = 'all'
    }
  })

  const selectTag = (tag: string) => {
    activeTag.value = tag
  }

  return {
    activeTag,
    allTags,
    sidebarItems,
    filteredItems,
    selectTag,
  }
}
