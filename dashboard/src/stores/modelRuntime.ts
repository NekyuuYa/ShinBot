import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import {
  modelRuntimeApi,
  type ModelExecutionRecord,
  type ModelRuntimeModel,
  type ModelRuntimeProvider,
  type ModelRuntimeRoute,
  type ProviderCatalogItem,
  type ProviderPayload,
  type RoutePayload,
  type ModelPayload,
} from '@/api/modelRuntime'
import { useUiStore } from './ui'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'
import type { ModelRuntimeTab } from '@/utils/modelRuntimeSources'

export const useModelRuntimeStore = defineStore(
  'modelRuntime',
  () => {
    const providers = ref<ModelRuntimeProvider[]>([])
    const models = ref<ModelRuntimeModel[]>([])
    const routes = ref<ModelRuntimeRoute[]>([])
    const executions = ref<ModelExecutionRecord[]>([])
    const catalogItems = ref<Record<string, ProviderCatalogItem[]>>({})
    const selectedTab = ref<ModelRuntimeTab>('routes')
    const selectedKind = ref<'provider' | 'route'>('provider')
    const selectedId = ref('')
    const isLoading = ref(false)
    const isSaving = ref(false)
    const error = ref('')

    const modelsByProvider = computed<Record<string, ModelRuntimeModel[]>>(() => {
      const grouped: Record<string, ModelRuntimeModel[]> = {}
      for (const model of models.value) {
        grouped[model.providerId] = grouped[model.providerId] || []
        grouped[model.providerId].push(model)
      }
      return grouped
    })

    const fetchAll = async () => {
      isLoading.value = true
      error.value = ''
      try {
        const [providersResp, modelsResp, routesResp, executionsResp] = await Promise.all([
          modelRuntimeApi.listProviders(),
          modelRuntimeApi.listModels(),
          modelRuntimeApi.listRoutes(),
          modelRuntimeApi.listExecutions(30),
        ])
        providers.value = providersResp.data.data || []
        models.value = modelsResp.data.data || []
        routes.value = routesResp.data.data || []
        executions.value = executionsResp.data.data || []
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.loadFailed')
        )
      } finally {
        isLoading.value = false
      }
    }

    const createProvider = async (payload: ProviderPayload) => {
      isSaving.value = true
      try {
        const response = await modelRuntimeApi.createProvider(payload)
        if (response.data.success && response.data.data) {
          providers.value.push(response.data.data)
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.providerCreated'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.providerSaveFailed')
        )
      } finally {
        isSaving.value = false
      }
      return null
    }

    const updateProvider = async (id: string, payload: Partial<ProviderPayload>) => {
      isSaving.value = true
      try {
        const response = await modelRuntimeApi.updateProvider(id, payload)
        if (response.data.success && response.data.data) {
          const index = providers.value.findIndex((item) => item.id === id)
          if (index !== -1) {
            providers.value[index] = response.data.data
          }
          if (response.data.data.id !== id) {
            models.value = models.value.map((item) =>
              item.providerId === id ? { ...item, providerId: response.data.data!.id } : item
            )
            if (catalogItems.value[id]) {
              catalogItems.value[response.data.data.id] = catalogItems.value[id]
              delete catalogItems.value[id]
            }
          }
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.providerUpdated'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.providerSaveFailed')
        )
      } finally {
        isSaving.value = false
      }
      return null
    }

    const deleteProvider = async (id: string) => {
      try {
        const response = await modelRuntimeApi.deleteProvider(id)
        if (response.data.success) {
          providers.value = providers.value.filter((item) => item.id !== id)
          models.value = models.value.filter((item) => item.providerId !== id)
          delete catalogItems.value[id]
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.providerDeleted'),
            'info'
          )
          return true
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.providerDeleteFailed')
        )
      }
      return false
    }

    const fetchProviderCatalog = async (id: string) => {
      try {
        const response = await modelRuntimeApi.fetchProviderCatalog(id)
        if (response.data.success && response.data.data) {
          catalogItems.value[id] = response.data.data
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.catalogFetchFailed')
        )
      }
      return []
    }

    const probeProvider = async (id: string, modelId?: string) => {
      try {
        const response = await modelRuntimeApi.probeProvider(id, modelId)
        if (response.data.success && response.data.data) {
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.providerProbeSuccess'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.providerProbeFailed')
        )
      }
      return null
    }

    const createModel = async (payload: ModelPayload) => {
      try {
        const response = await modelRuntimeApi.createModel(payload)
        if (response.data.success && response.data.data) {
          models.value.push(response.data.data)
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.modelCreated'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.modelSaveFailed')
        )
      }
      return null
    }

    const updateModel = async (id: string, payload: Partial<ModelPayload>) => {
      try {
        const response = await modelRuntimeApi.updateModel(id, payload)
        if (response.data.success && response.data.data) {
          const index = models.value.findIndex((item) => item.id === id)
          if (index !== -1) {
            models.value[index] = response.data.data
          }
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.modelUpdated'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.modelSaveFailed')
        )
      }
      return null
    }

    const deleteModel = async (id: string) => {
      try {
        const response = await modelRuntimeApi.deleteModel(id)
        if (response.data.success) {
          models.value = models.value.filter((item) => item.id !== id)
          routes.value = routes.value.map((route) => ({
            ...route,
            members: route.members.filter((member) => member.modelId !== id),
          }))
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.modelDeleted'),
            'info'
          )
          return true
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.modelDeleteFailed')
        )
      }
      return false
    }

    const createRoute = async (payload: RoutePayload) => {
      try {
        const response = await modelRuntimeApi.createRoute(payload)
        if (response.data.success && response.data.data) {
          routes.value.push(response.data.data)
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.routeCreated'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.routeSaveFailed')
        )
      }
      return null
    }

    const updateRoute = async (id: string, payload: Partial<RoutePayload>) => {
      try {
        const response = await modelRuntimeApi.updateRoute(id, payload)
        if (response.data.success && response.data.data) {
          const index = routes.value.findIndex((item) => item.id === id)
          if (index !== -1) {
            routes.value[index] = response.data.data
          }
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.routeUpdated'),
            'success'
          )
          return response.data.data
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.routeSaveFailed')
        )
      }
      return null
    }

    const deleteRoute = async (id: string) => {
      try {
        const response = await modelRuntimeApi.deleteRoute(id)
        if (response.data.success) {
          routes.value = routes.value.filter((item) => item.id !== id)
          useUiStore().showSnackbar(
            translate('pages.modelRuntime.messages.routeDeleted'),
            'info'
          )
          return true
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.routeDeleteFailed')
        )
      }
      return false
    }

    const updateSelected = (kind: 'provider' | 'route', id: string) => {
      selectedKind.value = kind
      selectedId.value = id
    }

    const updateSelectedTab = (tab: ModelRuntimeTab) => {
      selectedTab.value = tab
    }

    return {
      providers,
      models,
      routes,
      executions,
      catalogItems,
      selectedTab,
      selectedKind,
      selectedId,
      isLoading,
      isSaving,
      error,
      modelsByProvider,
      fetchAll,
      createProvider,
      updateProvider,
      deleteProvider,
      fetchProviderCatalog,
      probeProvider,
      createModel,
      updateModel,
      deleteModel,
      createRoute,
      updateRoute,
      deleteRoute,
      updateSelected,
      updateSelectedTab,
    }
  },
  {
    persist: {
      paths: ['selectedTab', 'selectedKind', 'selectedId'],
    },
  }
)
