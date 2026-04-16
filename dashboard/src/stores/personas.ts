import { defineStore } from 'pinia'
import { ref } from 'vue'

import { personasApi, type Persona, type PersonaPayload } from '@/api/personas'
import { translate } from '@/plugins/i18n'
import { getErrorMessage } from '@/utils/error'
import { useUiStore } from './ui'

export const usePersonasStore = defineStore('personas', () => {
  const personas = ref<Persona[]>([])
  const isLoading = ref(false)
  const isSaving = ref(false)
  const error = ref('')

  const fetchPersonas = async () => {
    isLoading.value = true
    error.value = ''

    try {
      const response = await personasApi.list()
      if (response.data.success && response.data.data) {
        personas.value = response.data.data
      } else {
        error.value = response.data.error?.message || translate('pages.personas.messages.loadFailed')
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

  const createPersona = async (payload: PersonaPayload) => {
    isSaving.value = true
    error.value = ''

    try {
      const response = await personasApi.create(payload)
      if (response.data.success && response.data.data) {
        personas.value = [...personas.value, response.data.data]
        useUiStore().showSnackbar(translate('pages.personas.messages.created'), 'success')
        return response.data.data
      }

      error.value = response.data.error?.message || translate('pages.personas.messages.createFailed')
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

  const updatePersona = async (uuid: string, payload: Partial<PersonaPayload>) => {
    isSaving.value = true
    error.value = ''

    try {
      const response = await personasApi.update(uuid, payload)
      if (response.data.success && response.data.data) {
        const index = personas.value.findIndex((item) => item.uuid === uuid)
        if (index !== -1) {
          personas.value[index] = response.data.data
        }
        useUiStore().showSnackbar(translate('pages.personas.messages.updated'), 'success')
        return response.data.data
      }

      error.value = response.data.error?.message || translate('pages.personas.messages.updateFailed')
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

  const deletePersona = async (uuid: string) => {
    error.value = ''

    try {
      const response = await personasApi.delete(uuid)
      if (response.data.success) {
        personas.value = personas.value.filter((item) => item.uuid !== uuid)
        useUiStore().showSnackbar(translate('pages.personas.messages.deleted'), 'info')
        return true
      }

      error.value = response.data.error?.message || translate('pages.personas.messages.deleteFailed')
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
    personas,
    isLoading,
    isSaving,
    error,
    fetchPersonas,
    createPersona,
    updatePersona,
    deletePersona,
  }
})
