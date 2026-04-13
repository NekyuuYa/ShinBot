import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  instancesApi,
  type Instance,
  type CreateInstanceRequest,
  type UpdateInstanceRequest,
} from '@/api/instances'
import { useUiStore } from './ui'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export const useInstancesStore = defineStore('instances', () => {
  const instances = ref<Instance[]>([])
  const isLoading = ref(false)
  const error = ref<string>('')
  const pendingActions = ref<Record<string, 'start' | 'stop' | null>>({})

  const fetchInstances = async () => {
    isLoading.value = true
    error.value = ''

    try {
      const response = await instancesApi.list()
      if (response.data.success && response.data.data) {
        instances.value = response.data.data
      } else {
        error.value = response.data.error?.message || translate('pages.instances.loadFailed')
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

  const createInstance = async (data: CreateInstanceRequest) => {
    try {
      const response = await instancesApi.create(data)
      if (response.data.success && response.data.data) {
        instances.value = [...instances.value, response.data.data]
        useUiStore().showSnackbar(translate('pages.instances.created'), 'success')
        return true
      } else {
        error.value = response.data.error?.message || translate('pages.instances.createFailed')
        return false
      }
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const updateInstance = async (id: string, data: UpdateInstanceRequest) => {
    try {
      const response = await instancesApi.update(id, data)
      if (response.data.success && response.data.data) {
        const index = instances.value.findIndex((i) => i.id === id)
        if (index !== -1) {
          instances.value[index] = response.data.data
        }
        useUiStore().showSnackbar(translate('pages.instances.updated'), 'success')
        return true
      } else {
        error.value = response.data.error?.message || translate('pages.instances.updateFailed')
        return false
      }
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const deleteInstance = async (id: string) => {
    try {
      const response = await instancesApi.delete(id)
      if (response.data.success) {
        instances.value = instances.value.filter((i) => i.id !== id)
        useUiStore().showSnackbar(translate('pages.instances.deleted'), 'info')
        return true
      } else {
        error.value = response.data.error?.message || translate('pages.instances.deleteFailed')
        return false
      }
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      return false
    }
  }

  const startInstance = async (id: string) => {
    try {
      pendingActions.value[id] = 'start'
      const response = await instancesApi.start(id)
      if (response.data.success) {
        useUiStore().showSnackbar(translate('pages.instances.started'), 'success')
        return true
      }
      useUiStore().showSnackbar(translate('pages.instances.startFailed'), 'error')
      pendingActions.value[id] = null
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      pendingActions.value[id] = null
      return false
    }
  }

  const stopInstance = async (id: string) => {
    try {
      pendingActions.value[id] = 'stop'
      const response = await instancesApi.stop(id)
      if (response.data.success) {
        useUiStore().showSnackbar(translate('pages.instances.stopRequested'), 'info')
        return true
      }
      useUiStore().showSnackbar(translate('pages.instances.stopFailed'), 'error')
      pendingActions.value[id] = null
      return false
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.networkError')
      )
      pendingActions.value[id] = null
      return false
    }
  }

  const clearPendingAction = (id: string) => {
    pendingActions.value[id] = null
  }

  const isInstancePending = (id: string) => pendingActions.value[id] !== null

  const syncInstanceStatuses = (snapshot: Array<{ id: string; status: Instance['status'] }>) => {
    const byId = new Map(snapshot.map((item) => [item.id, item.status]))
    instances.value = instances.value.map((instance) => {
      const status = byId.get(instance.id)
      if (!status) {
        return instance
      }

      if (pendingActions.value[instance.id] !== null) {
        pendingActions.value[instance.id] = null
      }

      if (instance.status === status) {
        return instance
      }

      return {
        ...instance,
        status,
      }
    })
  }

  return {
    instances,
    isLoading,
    error,
    pendingActions,
    isInstancePending,
    clearPendingAction,
    syncInstanceStatuses,
    fetchInstances,
    createInstance,
    updateInstance,
    deleteInstance,
    startInstance,
    stopInstance,
  }
})
