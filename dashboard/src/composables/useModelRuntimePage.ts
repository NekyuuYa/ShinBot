import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { useModelRuntimeStore } from '@/stores/modelRuntime'
import type { ModelRuntimeModel, ModelRuntimeProvider } from '@/api/modelRuntime'
import {
  DEFAULT_CAPABILITIES_FOR_TYPE,
  makeModelId,
  providerSourceTemplates,
  resolveProviderSource,
  tabToCapabilityType,
  type ModelRuntimeTab,
  type ProviderCapabilityType,
} from '@/utils/modelRuntimeSources'

export function useModelRuntimePage() {
  const router = useRouter()
  const route = useRoute()
  const { t } = useI18n()
  const store = useModelRuntimeStore()

  const activeTab = ref<ModelRuntimeTab>('routes')
  const selectedKind = ref<'provider' | 'route'>('provider')
  const selectedId = ref('')
  const probingProviderId = ref('')
  const catalogLoading = ref(false)
  const isCreatingProvider = ref(false)
  const isCreatingRoute = ref(false)
  const showInlineModelEditor = ref(false)
  const editingModelId = ref('')

  const runtimeTabs = computed(() => [
    {
      value: 'routes' as const,
      label: t('pages.modelRuntime.tabs.routes'),
      icon: 'mdi-transit-connection-variant',
    },
    { value: 'chat' as const, label: t('pages.modelRuntime.tabs.chat'), icon: 'mdi-message-text-outline' },
    { value: 'embedding' as const, label: t('pages.modelRuntime.tabs.embedding'), icon: 'mdi-vector-line' },
    { value: 'rerank' as const, label: t('pages.modelRuntime.tabs.rerank'), icon: 'mdi-sort-descending' },
    { value: 'tts' as const, label: t('pages.modelRuntime.tabs.tts'), icon: 'mdi-text-to-speech' },
    { value: 'stt' as const, label: t('pages.modelRuntime.tabs.stt'), icon: 'mdi-microphone-outline' },
    { value: 'image' as const, label: t('pages.modelRuntime.tabs.image'), icon: 'mdi-image-outline' },
    { value: 'video' as const, label: t('pages.modelRuntime.tabs.video'), icon: 'mdi-video-outline' },
  ])

  const routeStrategies = ['priority', 'weighted']
  const routeDomainOptions = computed(() => [
    { label: t('pages.modelRuntime.tabs.chat'), value: 'chat' },
    { label: t('pages.modelRuntime.tabs.embedding'), value: 'embedding' },
    { label: t('pages.modelRuntime.tabs.rerank'), value: 'rerank' },
    { label: t('pages.modelRuntime.tabs.tts'), value: 'tts' },
    { label: t('pages.modelRuntime.tabs.stt'), value: 'stt' },
    { label: t('pages.modelRuntime.tabs.image'), value: 'image' },
    { label: t('pages.modelRuntime.tabs.video'), value: 'video' },
  ])

  const providerForm = ref({
    id: '',
    displayName: '',
    sourceType: 'openai',
    baseUrl: '',
    token: '',
    enabled: true,
    proxyAddress: '',
    thinkingJson: '',
    filtersJson: '',
    apiVersion: '',
  })

  const providerHeaderRows = ref<Array<{ key: string; value: string }>>([])

  const routeForm = ref({
    id: '',
    purpose: '',
    strategy: 'priority',
    enabled: true,
    stickySessions: false,
    domain: 'chat',
  })

  const routeMembersEditor = ref<
    Array<{
      modelId: string
      priority: number
      weight: number
      timeoutOverride: number | null
      conditions: Record<string, unknown>
      enabled: boolean
    }>
  >([])

  const modelForm = ref({
    id: '',
    displayName: '',
    litellmModel: '',
    capabilities: [] as string[],
    contextWindow: null as number | null,
    enabled: true,
  })

  const isRouteMode = computed(() => activeTab.value === 'routes')
  const providerSourceOptions = providerSourceTemplates

  const filteredProviders = computed(() => {
    const capabilityType = tabToCapabilityType(activeTab.value)
    return store.providers
      .filter((provider) => provider.capabilityType === capabilityType)
      .map((provider) => ({
        provider,
        matchedModelCount: store.modelsByProvider[provider.id]?.length || 0,
      }))
  })

  const routeSidebarItems = computed(() =>
    store.routes.map((item) => ({
      id: item.id,
      title: item.id,
      subtitle: item.purpose || String(item.metadata.domain || ''),
      icon: 'mdi-router-network',
      badge: item.members.length,
      badgeColor: item.enabled ? 'success' : 'grey',
    }))
  )

  const providerSidebarItems = computed(() =>
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
    isRouteMode.value ? t('pages.modelRuntime.sidebar.routes') : t('pages.modelRuntime.sidebar.providers')
  )
  const sidebarEmptyText = computed(() =>
    isRouteMode.value ? t('pages.modelRuntime.sidebar.noRoutes') : t('pages.modelRuntime.sidebar.noProviders')
  )
  const sidebarAddLabel = computed(() =>
    isRouteMode.value ? t('pages.modelRuntime.actions.addRoute') : t('pages.modelRuntime.actions.addProvider')
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

  const selectedRoute = computed(() =>
    selectedKind.value === 'route' ? store.routes.find((item) => item.id === selectedId.value) || null : null
  )

  const selectedProviderModels = computed(() =>
    selectedProvider.value ? store.modelsByProvider[selectedProvider.value.id] || [] : []
  )

  const selectedProviderSource = computed(() => resolveProviderSource(providerForm.value.sourceType))
  const sourceSupportsThinking = computed(() => selectedProviderSource.value?.supportsThinking ?? false)
  const sourceSupportsFilters = computed(() => selectedProviderSource.value?.supportsFilters ?? false)
  const showProviderTokenField = computed(() => selectedProviderSource.value?.supportsToken ?? true)
  const showApiVersionField = computed(() => selectedProviderSource.value?.showApiVersion ?? false)
  const providerCanManageModels = computed(() => !!selectedProvider.value && !isCreatingProvider.value)

  const activeRouteDomain = computed(() => routeForm.value.domain || 'chat')
  const activeRouteDomainLabel = computed(
    () =>
      routeDomainOptions.value.find((item) => item.value === activeRouteDomain.value)?.label ||
      activeRouteDomain.value
  )

  const availableRouteModels = computed(() => {
    const capabilityType = tabToCapabilityType(activeRouteDomain.value as ModelRuntimeTab)
    const providerIds = new Set(
      store.providers
        .filter((p) => p.capabilityType === capabilityType)
        .map((p) => p.id)
    )
    return store.models.filter((item) => providerIds.has(item.providerId))
  })

  const availableRouteModelsGrouped = computed(() => {
    const groups: { providerId: string; providerName: string; models: ModelRuntimeModel[] }[] = []

    for (const model of availableRouteModels.value) {
      let group = groups.find((g) => g.providerId === model.providerId)
      if (!group) {
        const provider = store.providers.find((p) => p.id === model.providerId)
        group = {
          providerId: model.providerId,
          providerName: provider?.displayName || provider?.id || model.providerId,
          models: [],
        }
        groups.push(group)
      }
      group.models.push(model)
    }

    return groups
  })

  const availableCatalogItems = computed(() => {
    if (!selectedProvider.value) {
      return []
    }
    const items = store.catalogItems[selectedProvider.value.id] || []
    return items.filter((item) => {
      const generatedId = makeModelId(selectedProvider.value!.id, item.id)
      return !store.models.some(
        (model) =>
          model.providerId === selectedProvider.value!.id &&
          (model.id === generatedId || model.litellmModel === item.litellmModel)
      )
    })
  })

  const providerSaveLabel = computed(() =>
    isCreatingProvider.value ? t('common.actions.action.create') : t('pages.modelRuntime.actions.saveProvider')
  )
  const routeSaveLabel = computed(() =>
    isCreatingRoute.value ? t('common.actions.action.create') : t('pages.modelRuntime.actions.saveRoute')
  )
  const inlineModelSaveLabel = computed(() =>
    editingModelId.value ? t('common.actions.action.save') : t('common.actions.action.create')
  )

  const rowsToObject = (rows: Array<{ key: string; value: string }>) =>
    rows.reduce<Record<string, string>>((acc, row) => {
      const key = row.key.trim()
      if (!key) {
        return acc
      }
      acc[key] = row.value
      return acc
    }, {})

  const objectToRows = (value: unknown) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return []
    }
    return Object.entries(value as Record<string, unknown>).map(([key, item]) => ({
      key,
      value: String(item ?? ''),
    }))
  }

  const parseJsonField = (value: string, fallback: Record<string, unknown>) => {
    const trimmed = value.trim()
    if (!trimmed) {
      return fallback
    }
    try {
      const parsed = JSON.parse(trimmed)
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>
      }
    } catch {
      throw new Error(t('pages.modelRuntime.messages.invalidJson'))
    }
    throw new Error(t('pages.modelRuntime.messages.invalidJson'))
  }

  const cloneRouteMembers = (members: Array<Record<string, unknown>>) =>
    members.map((member) => ({
      modelId: String(member.modelId),
      priority: Number(member.priority || 0),
      weight: Number(member.weight || 1),
      timeoutOverride:
        member.timeoutOverride === null || member.timeoutOverride === undefined
          ? null
          : Number(member.timeoutOverride),
      conditions:
        member.conditions && typeof member.conditions === 'object'
          ? (member.conditions as Record<string, unknown>)
          : {},
      enabled: Boolean(member.enabled),
    }))

  const defaultCapabilitiesForTab = () => {
    const type = (selectedProvider.value?.capabilityType || tabToCapabilityType(activeTab.value)) as ProviderCapabilityType
    return DEFAULT_CAPABILITIES_FOR_TYPE[type] ?? DEFAULT_CAPABILITIES_FOR_TYPE.completion
  }

  const resetProviderForm = (type = '') => {
    const source = type ? resolveProviderSource(type) || providerSourceTemplates[0] : null
    providerForm.value = {
      id: '',
      displayName: '',
      sourceType: source?.type || '',
      baseUrl: source?.defaultBaseUrl || '',
      token: '',
      enabled: true,
      proxyAddress: '',
      thinkingJson: '',
      filtersJson: '',
      apiVersion: '',
    }
    providerHeaderRows.value = []
  }

  const resetRouteForm = () => {
    routeForm.value = {
      id: '',
      purpose: '',
      strategy: 'priority',
      enabled: true,
      stickySessions: false,
      domain: activeTab.value === 'routes' ? 'chat' : activeTab.value,
    }
    routeMembersEditor.value = []
  }

  const resetModelForm = () => {
    const providerId = selectedProvider.value?.id || providerForm.value.id
    modelForm.value = {
      id: providerId ? `${providerId}/` : '',
      displayName: '',
      litellmModel: '',
      capabilities: defaultCapabilitiesForTab(),
      contextWindow: null,
      enabled: true,
    }
  }

  const providerModelMeta = (model: ModelRuntimeModel) => {
    const lines = [`${t('pages.modelRuntime.fields.contextWindow')}: ${model.contextWindow || '—'}`]
    if (model.id !== model.litellmModel) {
      lines.push(`${t('pages.modelRuntime.fields.id')}: ${model.id}`)
    }
    return lines
  }

  const applyProviderSource = (type: string, previousType?: string) => {
    const previousSource = resolveProviderSource(previousType ?? providerForm.value.sourceType)
    const source = resolveProviderSource(type)
    if (!source) {
      return
    }

    const shouldUseDefaultBaseUrl =
      !providerForm.value.baseUrl || providerForm.value.baseUrl === previousSource?.defaultBaseUrl

    providerForm.value.sourceType = source.type
    if (shouldUseDefaultBaseUrl) {
      providerForm.value.baseUrl = source.defaultBaseUrl
    }

    if (!source.supportsToken) {
      providerForm.value.token = ''
    }

    if (!source.showApiVersion) {
      providerForm.value.apiVersion = ''
    }
    if (!source.supportsThinking) {
      providerForm.value.thinkingJson = ''
    }
    if (!source.supportsFilters) {
      providerForm.value.filtersJson = ''
    }
  }

  const onProviderSourceChange = (value: string | null) => {
    if (!value) {
      return
    }
    const previousType = providerForm.value.sourceType
    applyProviderSource(value, previousType)
  }

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

  const selectProvider = (id: string) => {
    isCreatingProvider.value = false
    selectedKind.value = 'provider'
    selectedId.value = id
  }

  const selectRoute = (id: string) => {
    isCreatingRoute.value = false
    selectedKind.value = 'route'
    selectedId.value = id
  }

  const handleSidebarSelect = (id: string) => {
    if (isRouteMode.value) {
      selectRoute(id)
      return
    }
    selectProvider(id)
  }

  const startCreateProvider = () => {
    isCreatingProvider.value = true
    selectedKind.value = 'provider'
    selectedId.value = ''
    resetProviderForm()
    showInlineModelEditor.value = false
  }

  const startCreateRoute = () => {
    isCreatingRoute.value = true
    selectedKind.value = 'route'
    selectedId.value = ''
    resetRouteForm()
  }

  const startCreateCurrent = () => {
    if (isRouteMode.value) {
      startCreateRoute()
      return
    }
    startCreateProvider()
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
      const isValid = filteredProviders.value.some((p) => p.provider.id === selectedProvider.value!.id)
      if (isValid) {
        return
      }
    }
    selectedId.value = filteredProviders.value[0]?.provider.id || ''
  }

  const saveProvider = async () => {
    try {
      const existingDefaults = selectedProvider.value?.defaultParams || {}
      const nextDefaults: Record<string, unknown> = {
        ...existingDefaults,
        requestHeaders: rowsToObject(providerHeaderRows.value),
        proxy: providerForm.value.proxyAddress || undefined,
      }

      if (showApiVersionField.value) {
        nextDefaults.apiVersion = providerForm.value.apiVersion || undefined
      } else {
        delete nextDefaults.apiVersion
      }

      if (sourceSupportsThinking.value) {
        nextDefaults.thinking = parseJsonField(providerForm.value.thinkingJson, {})
      } else {
        delete nextDefaults.thinking
      }

      if (sourceSupportsFilters.value) {
        nextDefaults.filters = parseJsonField(providerForm.value.filtersJson, {})
      } else {
        delete nextDefaults.filters
      }

      const payload: Record<string, unknown> = {
        id: providerForm.value.id.trim(),
        displayName: providerForm.value.displayName.trim() || providerForm.value.id.trim(),
        type: providerForm.value.sourceType,
        capabilityType: tabToCapabilityType(activeTab.value),
        baseUrl: providerForm.value.baseUrl.trim(),
        enabled: providerForm.value.enabled,
        defaultParams: nextDefaults,
      }

      if (providerForm.value.token.trim()) {
        payload.auth = { api_key: providerForm.value.token.trim() }
      }

      if (isCreatingProvider.value) {
        const created = await store.createProvider(payload as never)
        if (created) {
          isCreatingProvider.value = false
          selectProvider(created.id)
        }
      } else if (selectedProvider.value) {
        const updated = await store.updateProvider(selectedProvider.value.id, payload)
        if (updated) {
          selectProvider(updated.id)
        }
      }
      providerForm.value.token = ''
    } catch (errorDetail: unknown) {
      store.error = String((errorDetail as Error).message || errorDetail)
    }
  }

  const deleteCurrentProvider = async () => {
    if (!selectedProvider.value) {
      return
    }
    if (!confirm(t('pages.modelRuntime.messages.confirmDeleteProvider', { id: selectedProvider.value.id }))) {
      return
    }
    const deleted = await store.deleteProvider(selectedProvider.value.id)
    if (deleted) {
      selectedId.value = ''
      ensureSelection()
    }
  }

  const saveRoute = async () => {
    const payload = {
      id: routeForm.value.id.trim(),
      purpose: routeForm.value.purpose.trim(),
      strategy: routeForm.value.strategy,
      enabled: routeForm.value.enabled,
      stickySessions: routeForm.value.stickySessions,
      metadata: { domain: routeForm.value.domain },
      members: cloneRouteMembers(routeMembersEditor.value as Array<Record<string, unknown>>),
    }

    if (isCreatingRoute.value) {
      const created = await store.createRoute(payload)
      if (created) {
        isCreatingRoute.value = false
        selectRoute(created.id)
      }
      return
    }

    if (!selectedRoute.value) {
      return
    }

    const updated = await store.updateRoute(selectedRoute.value.id, payload)
    if (updated) {
      selectRoute(updated.id)
    }
  }

  const deleteCurrentRoute = async () => {
    if (!selectedRoute.value) {
      return
    }
    if (!confirm(t('pages.modelRuntime.messages.confirmDeleteRoute', { id: selectedRoute.value.id }))) {
      return
    }
    const deleted = await store.deleteRoute(selectedRoute.value.id)
    if (deleted) {
      selectedId.value = ''
      ensureSelection()
    }
  }

  const routeMemberByModel = (modelId: string) =>
    routeMembersEditor.value.find((member) => member.modelId === modelId) || null

  const isRouteMemberEnabled = (modelId: string) => !!routeMemberByModel(modelId)

  const toggleRouteMember = (modelId: string, enabled: boolean) => {
    const index = routeMembersEditor.value.findIndex((member) => member.modelId === modelId)
    if (!enabled) {
      if (index !== -1) {
        routeMembersEditor.value.splice(index, 1)
      }
      return
    }
    if (index === -1) {
      routeMembersEditor.value.push({
        modelId,
        priority: routeMembersEditor.value.length * 10,
        weight: 1,
        timeoutOverride: null,
        conditions: {},
        enabled: true,
      })
    }
  }

  const updateRouteMemberField = (modelId: string, field: 'priority' | 'weight', value: number) => {
    const member = routeMemberByModel(modelId)
    if (!member) {
      return
    }
    member[field] = value
  }

  const updateRouteTimeout = (modelId: string, value: unknown) => {
    const member = routeMemberByModel(modelId)
    if (!member) {
      return
    }
    const stringValue = String(value ?? '').trim()
    member.timeoutOverride = stringValue ? Number(stringValue) : null
  }

  const openInlineModelEditor = (modelId = '') => {
    showInlineModelEditor.value = true
    editingModelId.value = modelId
    if (!modelId) {
      resetModelForm()
      return
    }
    const model = store.models.find((item) => item.id === modelId)
    if (!model) {
      resetModelForm()
      return
    }
    modelForm.value = {
      id: model.id,
      displayName: model.displayName,
      litellmModel: model.litellmModel,
      capabilities: [...model.capabilities],
      contextWindow: model.contextWindow,
      enabled: model.enabled,
    }
  }

  const cancelInlineModelEditor = () => {
    showInlineModelEditor.value = false
    editingModelId.value = ''
    resetModelForm()
  }

  const saveModel = async () => {
    if (!selectedProvider.value) {
      return
    }

    let saved = null
    if (editingModelId.value) {
      saved = await store.updateModel(editingModelId.value, {
        displayName: modelForm.value.displayName,
        litellmModel: modelForm.value.litellmModel,
        capabilities: modelForm.value.capabilities,
        enabled: modelForm.value.enabled,
        defaultParams: {},
        costMetadata: {},
      })
    } else {
      saved = await store.createModel({
        id: modelForm.value.id.trim(),
        providerId: selectedProvider.value.id,
        displayName: modelForm.value.displayName.trim() || modelForm.value.id.trim(),
        litellmModel: modelForm.value.litellmModel.trim(),
        capabilities: modelForm.value.capabilities,
        contextWindow: null,
        enabled: modelForm.value.enabled,
        defaultParams: {},
        costMetadata: {},
      })
    }

    if (saved) {
      cancelInlineModelEditor()
    }
  }

  const removeModel = async (modelId: string) => {
    const deleted = await store.deleteModel(modelId)
    if (deleted && editingModelId.value === modelId) {
      cancelInlineModelEditor()
    }
  }

  const toggleModel = async (modelId: string, enabled: boolean) => {
    await store.updateModel(modelId, { enabled })
  }

  const probeSelectedProvider = async (modelId?: string) => {
    if (!selectedProvider.value) {
      return
    }
    probingProviderId.value = selectedProvider.value.id
    await store.probeProvider(selectedProvider.value.id, modelId)
    probingProviderId.value = ''
  }

  const fetchCatalogInline = async () => {
    if (!selectedProvider.value) {
      return
    }
    catalogLoading.value = true
    await store.fetchProviderCatalog(selectedProvider.value.id)
    catalogLoading.value = false
  }

  const importCatalogItem = async (catalogId: string) => {
    if (!selectedProvider.value) {
      return
    }
    const item = availableCatalogItems.value.find((entry) => entry.id === catalogId)
    if (!item) {
      return
    }
    await store.createModel({
      id: makeModelId(selectedProvider.value.id, item.id),
      providerId: selectedProvider.value.id,
      displayName: item.displayName,
      litellmModel: item.litellmModel,
      capabilities: defaultCapabilitiesForTab(),
      contextWindow: null,
      enabled: true,
      defaultParams: {},
      costMetadata: {},
    })
  }

  const refreshPage = async () => {
    await store.fetchAll()
    ensureSelection()
  }

  watch(
    () => selectedProvider.value?.id,
    () => {
      if (isCreatingProvider.value || !selectedProvider.value) {
        return
      }
      const source = resolveProviderSource(selectedProvider.value.type)
      providerForm.value = {
        id: selectedProvider.value.id,
        displayName: selectedProvider.value.displayName,
        sourceType: selectedProvider.value.type,
        baseUrl: selectedProvider.value.baseUrl,
        token: '',
        enabled: selectedProvider.value.enabled,
        proxyAddress: String(selectedProvider.value.defaultParams.proxy || ''),
        thinkingJson: selectedProvider.value.defaultParams.thinking
          ? JSON.stringify(selectedProvider.value.defaultParams.thinking, null, 2)
          : '',
        filtersJson: selectedProvider.value.defaultParams.filters
          ? JSON.stringify(selectedProvider.value.defaultParams.filters, null, 2)
          : '',
        apiVersion: String(selectedProvider.value.defaultParams.apiVersion || ''),
      }
      providerHeaderRows.value = objectToRows(selectedProvider.value.defaultParams.requestHeaders)
      if (source && !providerForm.value.baseUrl) {
        providerForm.value.baseUrl = source.defaultBaseUrl
      }
    },
    { immediate: true }
  )

  watch(
    () => selectedRoute.value?.id,
    () => {
      if (isCreatingRoute.value || !selectedRoute.value) {
        return
      }
      routeForm.value = {
        id: selectedRoute.value.id,
        purpose: selectedRoute.value.purpose,
        strategy: selectedRoute.value.strategy,
        enabled: selectedRoute.value.enabled,
        stickySessions: selectedRoute.value.stickySessions,
        domain: String(selectedRoute.value.metadata.domain || 'chat'),
      }
      routeMembersEditor.value = cloneRouteMembers(selectedRoute.value.members as Array<Record<string, unknown>>)
    },
    { immediate: true }
  )

  watch(activeTab, (nextTab, previousTab) => {
    showInlineModelEditor.value = false
    editingModelId.value = ''

    if (nextTab !== previousTab) {
      isCreatingProvider.value = false
      isCreatingRoute.value = false
    }

    store.updateSelectedTab(nextTab)
    ensureSelection()
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
      if (typeof tab === 'string' && ['routes', 'chat', 'embedding', 'rerank', 'tts', 'stt', 'image', 'video'].includes(tab)) {
        activeTab.value = tab as ModelRuntimeTab
      } else {
        activeTab.value = store.selectedTab
      }
      if (kind === 'provider' || kind === 'route') {
        selectedKind.value = kind
      } else {
        selectedKind.value = store.selectedKind
      }
      selectedId.value = typeof id === 'string' ? id : store.selectedId
    },
    { immediate: true }
  )

  onMounted(async () => {
    await store.fetchAll()
    if (activeTab.value === 'routes') {
      resetRouteForm()
    } else {
      resetProviderForm()
    }
    ensureSelection()
    syncQuery()
  })

  return {
    store,
    activeTab,
    runtimeTabs,
    isRouteMode,
    sidebarTitle,
    sidebarEmptyText,
    sidebarItems,
    sidebarActiveId,
    sidebarAddLabel,
    startCreateCurrent,
    handleSidebarSelect,
    isCreatingRoute,
    selectedRoute,
    routeSaveLabel,
    routeForm,
    routeDomainOptions,
    routeStrategies,
    saveRoute,
    deleteCurrentRoute,
    routeMembersEditor,
    activeRouteDomainLabel,
    availableRouteModels,
    availableRouteModelsGrouped,
    isRouteMemberEnabled,
    toggleRouteMember,
    routeMemberByModel,
    updateRouteMemberField,
    updateRouteTimeout,
    isCreatingProvider,
    selectedProvider,
    providerSaveLabel,
    providerForm,
    providerSourceOptions,
    onProviderSourceChange,
    showProviderTokenField,
    selectedProviderSource,
    sourceSupportsThinking,
    sourceSupportsFilters,
    showApiVersionField,
    probingProviderId,
    probeSelectedProvider,
    providerHeaderRows,
    fetchCatalogInline,
    catalogLoading,
    providerCanManageModels,
    openInlineModelEditor,
    showInlineModelEditor,
    cancelInlineModelEditor,
    saveModel,
    inlineModelSaveLabel,
    editingModelId,
    modelForm,
    selectedProviderModels,
    providerModelMeta,
    removeModel,
    toggleModel,
    availableCatalogItems,
    importCatalogItem,
    deleteCurrentProvider,
    saveProvider,
    refreshPage,
  }
}
