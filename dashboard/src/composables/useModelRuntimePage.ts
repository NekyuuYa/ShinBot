import { inject, onMounted, ref, watch, type InjectionKey } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { useModelRuntimeStore } from '@/stores/modelRuntime'
import { useSystemSettingsStore } from '@/stores/systemSettings'
import type { ModelRuntimeTab } from '@/utils/modelRuntimeSources'
import type { RuntimeSelectionKind } from './modelRuntime/types'
import { useModelForm } from './modelRuntime/useModelForm'
import { useProviderForm } from './modelRuntime/useProviderForm'
import { useRouteForm } from './modelRuntime/useRouteForm'
import { isRuntimeTab, useRuntimeTabs } from './modelRuntime/useRuntimeTabs'
import { useRuntimeSelection } from './modelRuntime/useRuntimeSelection'

export function useModelRuntimePage() {
  const router = useRouter()
  const route = useRoute()
  const store = useModelRuntimeStore()
  const systemSettingsStore = useSystemSettingsStore()

  const activeTab = ref<ModelRuntimeTab>('routes')
  const selectedKind = ref<RuntimeSelectionKind>('provider')
  const selectedId = ref('')
  const isCreatingProvider = ref(false)
  const isCreatingRoute = ref(false)

  const tabs = useRuntimeTabs(activeTab)

  const selection = useRuntimeSelection({
    store,
    activeTab,
    selectedKind,
    selectedId,
    isCreatingProvider,
    isCreatingRoute,
    isRouteMode: tabs.isRouteMode,
  })

  const modelForm = useModelForm({
    store,
    systemSettingsStore,
    activeTab,
    selectedProvider: selection.selectedProvider,
    routeDomainLabels: tabs.routeDomainLabels,
  })

  const selectProvider = (id: string) => {
    isCreatingProvider.value = false
    modelForm.showModelIdPicker.value = false
    modelForm.catalogSearch.value = ''
    selection.selectProvider(id)
  }

  const selectRoute = (id: string) => {
    isCreatingRoute.value = false
    modelForm.showModelIdPicker.value = false
    selection.selectRoute(id)
  }

  const providerForm = useProviderForm({
    store,
    activeTab,
    selectedProvider: selection.selectedProvider,
    isCreatingProvider,
    selectProvider,
    ensureSelection: selection.ensureSelection,
  })

  const routeForm = useRouteForm({
    store,
    activeTab,
    selectedRoute: selection.selectedRoute,
    isCreatingRoute,
    routeDomainOptions: tabs.routeDomainOptions,
    selectRoute,
    ensureSelection: selection.ensureSelection,
  })

  const syncQuery = () => {
    router.replace({
      query: {
        ...route.query,
        tab: activeTab.value,
        kind: selectedKind.value,
        id: selectedId.value || undefined,
      },
    })
  }

  const handleSidebarSelect = (id: string) => {
    if (tabs.isRouteMode.value) {
      selectRoute(id)
      return
    }
    selectProvider(id)
  }

  const startCreateProvider = () => {
    isCreatingProvider.value = true
    selectedKind.value = 'provider'
    selectedId.value = ''
    providerForm.resetProviderForm()
    modelForm.showModelIdPicker.value = false
    modelForm.showInlineModelEditor.value = false
  }

  const startCreateRoute = () => {
    isCreatingRoute.value = true
    selectedKind.value = 'route'
    selectedId.value = ''
    modelForm.showModelIdPicker.value = false
    routeForm.resetRouteForm()
  }

  const startCreateCurrent = () => {
    if (tabs.isRouteMode.value) {
      startCreateRoute()
      return
    }
    startCreateProvider()
  }

  const refreshPage = async () => {
    await store.fetchAll()
    selection.ensureSelection()
  }

  watch(activeTab, (nextTab, previousTab) => {
    modelForm.showInlineModelEditor.value = false
    modelForm.showModelIdPicker.value = false
    modelForm.editingModelId.value = ''

    if (nextTab !== previousTab) {
      isCreatingProvider.value = false
      isCreatingRoute.value = false
    }

    store.updateSelectedTab(nextTab)
    selection.ensureSelection()
    syncQuery()
  })

  watch([selectedKind, selectedId], () => {
    store.updateSelected(selectedKind.value, selectedId.value)
    syncQuery()
  })

  watch(
    () => route.query,
    (query) => {
      const tab = query.tab
      const kind = query.kind
      const id = query.id

      activeTab.value = isRuntimeTab(tab) ? tab : store.selectedTab
      selectedKind.value =
        kind === 'provider' || kind === 'route' ? kind : store.selectedKind
      selectedId.value = typeof id === 'string' ? id : store.selectedId
    },
    { immediate: true }
  )

  onMounted(async () => {
    await store.fetchAll()
    if (activeTab.value === 'routes') {
      routeForm.resetRouteForm()
    } else {
      providerForm.resetProviderForm()
    }
    selection.ensureSelection()
    syncQuery()
  })

  return {
    store,
    activeTab,
    runtimeTabs: tabs.runtimeTabs,
    isRouteMode: tabs.isRouteMode,
    sidebarTitle: selection.sidebarTitle,
    sidebarEmptyText: selection.sidebarEmptyText,
    sidebarItems: selection.sidebarItems,
    sidebarActiveId: selection.sidebarActiveId,
    sidebarAddLabel: selection.sidebarAddLabel,
    startCreateCurrent,
    handleSidebarSelect,
    isCreatingRoute,
    selectedRoute: selection.selectedRoute,
    routeSaveLabel: routeForm.routeSaveLabel,
    routeForm: routeForm.routeForm,
    routeDomainOptions: tabs.routeDomainOptions,
    routeStrategies: tabs.routeStrategies,
    saveRoute: routeForm.saveRoute,
    deleteCurrentRoute: routeForm.deleteCurrentRoute,
    routeMembersEditor: routeForm.routeMembersEditor,
    activeRouteDomainLabel: routeForm.activeRouteDomainLabel,
    availableRouteModels: routeForm.availableRouteModels,
    availableRouteModelsGrouped: routeForm.availableRouteModelsGrouped,
    isRouteMemberEnabled: routeForm.isRouteMemberEnabled,
    toggleRouteMember: routeForm.toggleRouteMember,
    routeMemberByModel: routeForm.routeMemberByModel,
    updateRouteMemberField: routeForm.updateRouteMemberField,
    updateRouteTimeout: routeForm.updateRouteTimeout,
    isCreatingProvider,
    selectedProvider: selection.selectedProvider,
    providerSaveLabel: providerForm.providerSaveLabel,
    providerForm: providerForm.providerForm,
    providerSourceOptions: providerForm.providerSourceOptions,
    onProviderSourceChange: providerForm.onProviderSourceChange,
    showProviderTokenField: providerForm.showProviderTokenField,
    selectedProviderSource: providerForm.selectedProviderSource,
    sourceSupportsThinking: providerForm.sourceSupportsThinking,
    sourceSupportsFilters: providerForm.sourceSupportsFilters,
    showApiVersionField: providerForm.showApiVersionField,
    probingProviderId: providerForm.probingProviderId,
    probeSelectedProvider: providerForm.probeSelectedProvider,
    providerHeaderRows: providerForm.providerHeaderRows,
    fetchCatalogInline: modelForm.fetchCatalogInline,
    catalogLoading: modelForm.catalogLoading,
    catalogSearch: modelForm.catalogSearch,
    pricingCurrency: modelForm.pricingCurrency,
    pricingTokenUnit: modelForm.pricingTokenUnit,
    providerCanManageModels: providerForm.providerCanManageModels,
    openInlineModelEditor: modelForm.openInlineModelEditor,
    showInlineModelEditor: modelForm.showInlineModelEditor,
    showModelIdPicker: modelForm.showModelIdPicker,
    cancelInlineModelEditor: modelForm.cancelInlineModelEditor,
    saveModel: modelForm.saveModel,
    inlineModelSaveLabel: modelForm.inlineModelSaveLabel,
    editingModelId: modelForm.editingModelId,
    modelForm: modelForm.modelForm,
    modelIdPickerRouteOptions: modelForm.modelIdPickerRouteOptions,
    modelIdPickerProviderGroups: modelForm.modelIdPickerProviderGroups,
    openModelIdPicker: modelForm.openModelIdPicker,
    closeModelIdPicker: modelForm.closeModelIdPicker,
    applyPickedModelId: modelForm.applyPickedModelId,
    selectedProviderModels: modelForm.selectedProviderModels,
    providerModelMeta: modelForm.providerModelMeta,
    removeModel: modelForm.removeModel,
    toggleModel: modelForm.toggleModel,
    availableCatalogItems: modelForm.availableCatalogItems,
    filteredCatalogItems: modelForm.filteredCatalogItems,
    importCatalogItem: modelForm.importCatalogItem,
    deleteCurrentProvider: providerForm.deleteCurrentProvider,
    saveProvider: providerForm.saveProvider,
    refreshPage,
  }
}

export type ModelRuntimePageContext = ReturnType<typeof useModelRuntimePage>

export const modelRuntimePageKey: InjectionKey<ModelRuntimePageContext> =
  Symbol('model-runtime-page')

export function useModelRuntimeContext() {
  const context = inject(modelRuntimePageKey)
  if (!context) {
    throw new Error('ModelRuntime page context is not available')
  }
  return context
}
