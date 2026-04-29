import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  instancesApi,
  type Instance,
  type CreateInstanceRequest,
  type UpdateInstanceRequest,
} from '@/api/instances'
import { createCrudStore } from './crud'

export const useInstancesStore = defineStore('instances', () => {
  const crud = createCrudStore<Instance, CreateInstanceRequest, UpdateInstanceRequest, string>({
    api: instancesApi,
    i18nKey: {
      loadFailed: 'pages.instances.loadFailed',
      createFailed: 'pages.instances.createFailed',
      updateFailed: 'pages.instances.updateFailed',
      deleteFailed: 'pages.instances.deleteFailed',
      created: 'pages.instances.created',
      updated: 'pages.instances.updated',
      deleted: 'pages.instances.deleted',
    },
    idOf: (instance) => instance.id,
  })
  const instances = crud.items
  const pendingActions = ref<Record<string, 'start' | 'stop' | null>>({})

  const startInstance = async (id: string) => {
    pendingActions.value[id] = 'start'
    const result = await crud.runRequest(() => instancesApi.start(id), {
      errorKey: 'pages.instances.startFailed',
      failureNotifyKey: 'pages.instances.startFailed',
      successKey: 'pages.instances.started',
      successColor: 'success',
      expectData: false,
    })

    if (!result.ok) {
      pendingActions.value[id] = null
    }

    return result.ok
  }

  const stopInstance = async (id: string) => {
    pendingActions.value[id] = 'stop'
    const result = await crud.runRequest(() => instancesApi.stop(id), {
      errorKey: 'pages.instances.stopFailed',
      failureNotifyKey: 'pages.instances.stopFailed',
      successKey: 'pages.instances.stopRequested',
      successColor: 'info',
      expectData: false,
    })

    if (!result.ok) {
      pendingActions.value[id] = null
    }

    return result.ok
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
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    pendingActions,
    isInstancePending,
    clearPendingAction,
    syncInstanceStatuses,
    fetchInstances: crud.fetchItems,
    createInstance: crud.createItem,
    updateInstance: crud.updateItem,
    deleteInstance: crud.deleteItem,
    startInstance,
    stopInstance,
  }
})
