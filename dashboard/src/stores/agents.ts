import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { agentsApi, type Agent, type AgentPayload } from '@/api/agents'
import { translate } from '@/plugins/i18n'
import { getErrorMessage } from '@/utils/error'
import { useUiStore } from './ui'

export const useAgentsStore = defineStore('agents', () => {
  const agents = ref<Agent[]>([])
  const isLoading = ref(false)
  const isSaving = ref(false)
  const error = ref('')

  const allTags = computed(() => {
    const tagSet = new Set<string>()
    for (const agent of agents.value) {
      for (const tag of agent.tags) {
        const trimmed = tag.trim()
        if (!trimmed) {
          continue
        }
        tagSet.add(trimmed)
      }
    }
    return Array.from(tagSet).sort((a, b) => a.localeCompare(b))
  })

  const fetchAgents = async () => {
    isLoading.value = true
    error.value = ''

    try {
      const response = await agentsApi.list()
      if (response.data.success && response.data.data) {
        agents.value = response.data.data
      } else {
        error.value = response.data.error?.message || translate('pages.agents.messages.loadFailed')
      }
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
    } finally {
      isLoading.value = false
    }
  }

  const createAgent = async (payload: AgentPayload) => {
    isSaving.value = true
    error.value = ''

    try {
      const response = await agentsApi.create(payload)
      if (response.data.success && response.data.data) {
        agents.value = [...agents.value, response.data.data]
        useUiStore().showSnackbar(translate('pages.agents.messages.created'), 'success')
        return response.data.data
      }

      error.value = response.data.error?.message || translate('pages.agents.messages.createFailed')
      return null
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return null
    } finally {
      isSaving.value = false
    }
  }

  const updateAgent = async (uuid: string, payload: Partial<AgentPayload>) => {
    isSaving.value = true
    error.value = ''

    try {
      const response = await agentsApi.update(uuid, payload)
      if (response.data.success && response.data.data) {
        const index = agents.value.findIndex((item) => item.uuid === uuid)
        if (index !== -1) {
          agents.value[index] = response.data.data
        }
        useUiStore().showSnackbar(translate('pages.agents.messages.updated'), 'success')
        return response.data.data
      }

      error.value = response.data.error?.message || translate('pages.agents.messages.updateFailed')
      return null
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return null
    } finally {
      isSaving.value = false
    }
  }

  const deleteAgent = async (uuid: string) => {
    error.value = ''

    try {
      const response = await agentsApi.delete(uuid)
      if (response.data.success) {
        agents.value = agents.value.filter((item) => item.uuid !== uuid)
        useUiStore().showSnackbar(translate('pages.agents.messages.deleted'), 'info')
        return true
      }

      error.value = response.data.error?.message || translate('pages.agents.messages.deleteFailed')
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  return {
    agents,
    allTags,
    isLoading,
    isSaving,
    error,
    fetchAgents,
    createAgent,
    updateAgent,
    deleteAgent,
  }
})
