import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import {
  promptDefinitionsApi,
  type PromptDefinition,
  type PromptDefinitionPayload,
} from '@/api/promptDefinitions'
import { translate } from '@/plugins/i18n'
import { getErrorMessage } from '@/utils/error'
import { useUiStore } from './ui'

export const usePromptDefinitionsStore = defineStore('promptDefinitions', () => {
  const items = ref<PromptDefinition[]>([])
  const isLoading = ref(false)
  const isSaving = ref(false)
  const error = ref('')

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

  const fetchItems = async () => {
    isLoading.value = true
    error.value = ''
    try {
      const response = await promptDefinitionsApi.list()
      if (response.data.success && response.data.data) {
        items.value = response.data.data
      } else {
        error.value =
          response.data.error?.message || translate('pages.prompts.messages.loadFailed')
      }
    } catch (e: unknown) {
      error.value = getErrorMessage(e, translate('common.actions.message.networkError'))
    } finally {
      isLoading.value = false
    }
  }

  const createItem = async (payload: PromptDefinitionPayload) => {
    isSaving.value = true
    error.value = ''
    try {
      const response = await promptDefinitionsApi.create(payload)
      if (response.data.success && response.data.data) {
        items.value = [...items.value, response.data.data]
        useUiStore().showSnackbar(translate('pages.prompts.messages.created'), 'success')
        return response.data.data
      }
      error.value =
        response.data.error?.message || translate('pages.prompts.messages.createFailed')
      return null
    } catch (e: unknown) {
      error.value = getErrorMessage(e, translate('common.actions.message.networkError'))
      return null
    } finally {
      isSaving.value = false
    }
  }

  const updateItem = async (uuid: string, payload: Partial<PromptDefinitionPayload>) => {
    isSaving.value = true
    error.value = ''
    try {
      const response = await promptDefinitionsApi.update(uuid, payload)
      if (response.data.success && response.data.data) {
        const idx = items.value.findIndex((i) => i.uuid === uuid)
        if (idx !== -1) items.value[idx] = response.data.data
        useUiStore().showSnackbar(translate('pages.prompts.messages.updated'), 'success')
        return response.data.data
      }
      error.value =
        response.data.error?.message || translate('pages.prompts.messages.updateFailed')
      return null
    } catch (e: unknown) {
      error.value = getErrorMessage(e, translate('common.actions.message.networkError'))
      return null
    } finally {
      isSaving.value = false
    }
  }

  const deleteItem = async (uuid: string) => {
    error.value = ''
    try {
      const response = await promptDefinitionsApi.delete(uuid)
      if (response.data.success) {
        items.value = items.value.filter((i) => i.uuid !== uuid)
        useUiStore().showSnackbar(translate('pages.prompts.messages.deleted'), 'info')
        return true
      }
      error.value =
        response.data.error?.message || translate('pages.prompts.messages.deleteFailed')
      return false
    } catch (e: unknown) {
      error.value = getErrorMessage(e, translate('common.actions.message.networkError'))
      return false
    }
  }

  const toggleEnabled = async (uuid: string, enabled: boolean) => {
    return updateItem(uuid, { enabled })
  }

  return {
    items,
    allTags,
    isLoading,
    isSaving,
    error,
    fetchItems,
    createItem,
    updateItem,
    deleteItem,
    toggleEnabled,
  }
})
