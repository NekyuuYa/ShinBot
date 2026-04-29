import { defineStore } from 'pinia'
import { computed } from 'vue'

import {
  promptDefinitionsApi,
  type PromptDefinition,
  type PromptDefinitionPayload,
} from '@/api/promptDefinitions'
import { createCrudStore } from './crud'

export const usePromptDefinitionsStore = defineStore('promptDefinitions', () => {
  const crud = createCrudStore<
    PromptDefinition,
    PromptDefinitionPayload,
    Partial<PromptDefinitionPayload>,
    string
  >({
    api: promptDefinitionsApi,
    i18nKey: 'pages.prompts.messages',
    idOf: (item) => item.uuid,
  })
  const items = crud.items

  const allTags = computed(() => {
    const tagSet = new Set<string>()
    for (const item of items.value) {
      for (const tag of item.tags) {
        const trimmed = tag.trim()
        if (trimmed) tagSet.add(trimmed)
      }
    }
    return Array.from(tagSet).sort((a, b) => a.localeCompare(b))
  })

  const toggleEnabled = async (uuid: string, enabled: boolean) => {
    return crud.updateItem(uuid, { enabled })
  }

  return {
    items,
    allTags,
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    fetchItems: crud.fetchItems,
    createItem: crud.createItem,
    updateItem: crud.updateItem,
    deleteItem: crud.deleteItem,
    toggleEnabled,
  }
})
