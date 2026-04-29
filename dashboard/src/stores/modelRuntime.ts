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
import { apiClient } from '@/api/client'
import { createCrudStore } from './crud'
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

    const providerCrud = createCrudStore<
      ModelRuntimeProvider,
      ProviderPayload,
      Partial<ProviderPayload>,
      string
    >({
      api: {
        list: modelRuntimeApi.listProviders,
        create: modelRuntimeApi.createProvider,
        update: modelRuntimeApi.updateProvider,
        delete: modelRuntimeApi.deleteProvider,
      },
      i18nKey: {
        createFailed: 'pages.modelRuntime.messages.providerSaveFailed',
        updateFailed: 'pages.modelRuntime.messages.providerSaveFailed',
        deleteFailed: 'pages.modelRuntime.messages.providerDeleteFailed',
        created: 'pages.modelRuntime.messages.providerCreated',
        updated: 'pages.modelRuntime.messages.providerUpdated',
        deleted: 'pages.modelRuntime.messages.providerDeleted',
      },
      idOf: (provider) => provider.id,
      items: providers,
      state: {
        isSaving,
        error,
      },
      hooks: {
        onUpdateSuccess: (provider, context) => {
          if (provider.id === context.id) {
            return
          }

          models.value = models.value.map((item) =>
            item.providerId === context.id ? { ...item, providerId: provider.id } : item
          )

          if (catalogItems.value[context.id]) {
            catalogItems.value[provider.id] = catalogItems.value[context.id]
            delete catalogItems.value[context.id]
          }
        },
        onDeleteSuccess: ({ id }) => {
          models.value = models.value.filter((item) => item.providerId !== id)
          delete catalogItems.value[id]
        },
      },
    })

    const modelCrud = createCrudStore<ModelRuntimeModel, ModelPayload, Partial<ModelPayload>, string>({
      api: {
        list: modelRuntimeApi.listModels,
        create: modelRuntimeApi.createModel,
        update: modelRuntimeApi.updateModel,
        delete: modelRuntimeApi.deleteModel,
      },
      i18nKey: {
        createFailed: 'pages.modelRuntime.messages.modelSaveFailed',
        updateFailed: 'pages.modelRuntime.messages.modelSaveFailed',
        deleteFailed: 'pages.modelRuntime.messages.modelDeleteFailed',
        created: 'pages.modelRuntime.messages.modelCreated',
        updated: 'pages.modelRuntime.messages.modelUpdated',
        deleted: 'pages.modelRuntime.messages.modelDeleted',
      },
      idOf: (model) => model.id,
      items: models,
      state: {
        isSaving,
        error,
      },
      hooks: {
        onDeleteSuccess: ({ id }) => {
          routes.value = routes.value.map((route) => ({
            ...route,
            members: route.members.filter((member) => member.modelId !== id),
          }))
        },
      },
    })

    const routeCrud = createCrudStore<ModelRuntimeRoute, RoutePayload, Partial<RoutePayload>, string>({
      api: {
        list: modelRuntimeApi.listRoutes,
        create: modelRuntimeApi.createRoute,
        update: modelRuntimeApi.updateRoute,
        delete: modelRuntimeApi.deleteRoute,
      },
      i18nKey: {
        createFailed: 'pages.modelRuntime.messages.routeSaveFailed',
        updateFailed: 'pages.modelRuntime.messages.routeSaveFailed',
        deleteFailed: 'pages.modelRuntime.messages.routeDeleteFailed',
        created: 'pages.modelRuntime.messages.routeCreated',
        updated: 'pages.modelRuntime.messages.routeUpdated',
        deleted: 'pages.modelRuntime.messages.routeDeleted',
      },
      idOf: (route) => route.id,
      items: routes,
      state: {
        isSaving,
        error,
      },
    })

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

    const createProvider = providerCrud.createItem

    const updateProvider = providerCrud.updateItem

    const deleteProvider = providerCrud.deleteItem

    const fetchProviderCatalog = async (id: string) => {
      try {
        const data = await apiClient.unwrap(modelRuntimeApi.fetchProviderCatalog(id))
        catalogItems.value[id] = data
        return data
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
        const data = await apiClient.unwrap(modelRuntimeApi.probeProvider(id, modelId))
        useUiStore().showSnackbar(
          translate('pages.modelRuntime.messages.providerProbeSuccess'),
          'success'
        )
        return data
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.modelRuntime.messages.providerProbeFailed')
        )
      }
      return null
    }

    const createModel = modelCrud.createItem

    const updateModel = modelCrud.updateItem

    const deleteModel = modelCrud.deleteItem

    const createRoute = routeCrud.createItem

    const updateRoute = routeCrud.updateItem

    const deleteRoute = routeCrud.deleteItem

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
