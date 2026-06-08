import { defineStore } from 'pinia'
import { computed } from 'vue'

import {
  promptsApi,
  type CustomPromptCreatePayload,
  type PromptCatalogItem,
  type PromptFile,
  type PromptFilePayload,
} from '@/api/prompts'
import { createCrudStore } from './crud'

export const usePromptFilesStore = defineStore('promptFiles', () => {
  const crud = createCrudStore<
    PromptCatalogItem,
    CustomPromptCreatePayload,
    PromptFilePayload,
    string
  >({
    api: promptsApi,
    i18nKey: 'pages.prompts.messages',
    idOf: (item) => item.fileId,
    listStaleTimeMs: 30_000,
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

  const getItem = async (fileId: string): Promise<PromptFile | null> => {
    const result = await crud.runRequest(() => promptsApi.get(fileId), {
      mode: 'saving',
      errorKey: 'pages.prompts.messages.loadFailed',
    })
    return result.ok ? (result.data ?? null) : null
  }

  const createItem = async (payload: CustomPromptCreatePayload) => {
    const result = await crud.runRequest(() => promptsApi.create(payload), {
      mode: 'saving',
      errorKey: 'pages.prompts.messages.createFailed',
      successKey: 'pages.prompts.messages.created',
      onSuccess: (data) => {
        if (data) crud.appendItem(data)
      },
    })
    return result.ok ? (result.data ?? null) : null
  }

  const updateItem = async (fileId: string, payload: PromptFilePayload) => {
    const result = await crud.runRequest(() => promptsApi.update(fileId, payload), {
      mode: 'saving',
      errorKey: 'pages.prompts.messages.updateFailed',
      successKey: 'pages.prompts.messages.updated',
      onSuccess: (data) => {
        if (data) crud.replaceItem(data)
      },
    })
    return result.ok ? (result.data ?? null) : null
  }

  const deleteItem = async (fileId: string) => {
    return crud.deleteItem(fileId)
  }

  const resetItem = async (fileId: string) => {
    const result = await crud.runRequest(() => promptsApi.reset(fileId), {
      mode: 'saving',
      errorKey: 'pages.prompts.messages.resetFailed',
      successKey: 'pages.prompts.messages.reset',
      expectData: false,
    })
    if (result.ok) {
      await crud.fetchItems({ force: true })
    }
    return result.ok
  }

  const toggleEnabled = async (fileId: string, enabled: boolean) => {
    return updateItem(fileId, { enabled })
  }

  return {
    items,
    allTags,
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    fetchItems: crud.fetchItems,
    getItem,
    createItem,
    updateItem,
    deleteItem,
    resetItem,
    toggleEnabled,
  }
})
