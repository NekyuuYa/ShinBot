import { computed, type ComputedRef, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { ModelRuntimeProvider, ModelRuntimeRoute } from '@/api/modelRuntime'
import type { useModelRuntimeStore } from '@/stores/modelRuntime'
import {
  resolveProviderSource,
  tabToCapabilityType,
  type ModelRuntimeTab,
} from '@/utils/modelRuntimeSources'
import type { RuntimeSelectionKind, RuntimeSidebarItem } from './types'

interface RuntimeSelectionOptions {
  store: ReturnType<typeof useModelRuntimeStore>
  activeTab: Ref<ModelRuntimeTab>
  selectedKind: Ref<RuntimeSelectionKind>
  selectedId: Ref<string>
  isCreatingProvider: Ref<boolean>
  isCreatingRoute: Ref<boolean>
  isRouteMode: ComputedRef<boolean>
}

export function useRuntimeSelection({
  store,
  activeTab,
  selectedKind,
  selectedId,
  isCreatingProvider,
  isCreatingRoute,
  isRouteMode,
}: RuntimeSelectionOptions) {
  const { t } = useI18n()

  const filteredProviders = computed(() => {
    const capabilityType = tabToCapabilityType(activeTab.value)
    return store.providers
      .filter((provider) => provider.capabilityType === capabilityType)
      .map((provider) => ({
        provider,
        matchedModelCount: store.modelsByProvider[provider.id]?.length || 0,
      }))
  })

  const routeSidebarItems = computed<RuntimeSidebarItem[]>(() =>
    store.routes.map((item) => ({
      id: item.id,
      title: item.id,
      subtitle: item.purpose || String(item.metadata.domain || ''),
      icon: 'mdi-router-network',
      badge: item.members.length,
      badgeColor: item.enabled ? 'success' : 'grey',
    }))
  )

  const providerSidebarItems = computed<RuntimeSidebarItem[]>(() =>
    filteredProviders.value.map(({ provider, matchedModelCount }) => ({
      id: provider.id,
      title: provider.displayName || provider.id,
      subtitle: resolveProviderSource(provider.type)?.label || provider.type,
      icon: 'mdi-cloud-outline',
      badge: matchedModelCount,
      badgeColor: provider.enabled ? 'success' : 'grey',
    }))
  )

  const sidebarItems = computed(() =>
    isRouteMode.value ? routeSidebarItems.value : providerSidebarItems.value
  )

  const sidebarActiveId = computed(() =>
    selectedKind.value === (isRouteMode.value ? 'route' : 'provider') ? selectedId.value : ''
  )

  const sidebarTitle = computed(() =>
    isRouteMode.value
      ? t('pages.modelRuntime.sidebar.routes')
      : t('pages.modelRuntime.sidebar.providers')
  )

  const sidebarEmptyText = computed(() =>
    isRouteMode.value
      ? t('pages.modelRuntime.sidebar.noRoutes')
      : t('pages.modelRuntime.sidebar.noProviders')
  )

  const sidebarAddLabel = computed(() =>
    isRouteMode.value
      ? t('pages.modelRuntime.actions.addRoute')
      : t('pages.modelRuntime.actions.addProvider')
  )

  const selectedProvider = computed<ModelRuntimeProvider | null>(() => {
    if (selectedKind.value !== 'provider') {
      return null
    }
    const provider = store.providers.find((item) => item.id === selectedId.value) || null
    if (!provider) {
      return null
    }
    const capabilityType = tabToCapabilityType(activeTab.value)
    return provider.capabilityType === capabilityType ? provider : null
  })

  const selectedRoute = computed<ModelRuntimeRoute | null>(() =>
    selectedKind.value === 'route'
      ? store.routes.find((item) => item.id === selectedId.value) || null
      : null
  )

  const selectProvider = (id: string) => {
    selectedKind.value = 'provider'
    selectedId.value = id
  }

  const selectRoute = (id: string) => {
    selectedKind.value = 'route'
    selectedId.value = id
  }

  const ensureSelection = () => {
    if (isRouteMode.value) {
      selectedKind.value = 'route'
      if (isCreatingRoute.value || selectedRoute.value) {
        return
      }
      selectedId.value = store.routes[0]?.id || ''
      return
    }

    selectedKind.value = 'provider'
    if (isCreatingProvider.value) {
      return
    }
    if (selectedProvider.value) {
      const isValid = filteredProviders.value.some(
        (item) => item.provider.id === selectedProvider.value!.id
      )
      if (isValid) {
        return
      }
    }
    selectedId.value = filteredProviders.value[0]?.provider.id || ''
  }

  return {
    filteredProviders,
    selectedProvider,
    selectedRoute,
    sidebarTitle,
    sidebarEmptyText,
    sidebarItems,
    sidebarActiveId,
    sidebarAddLabel,
    selectProvider,
    selectRoute,
    ensureSelection,
  }
}
