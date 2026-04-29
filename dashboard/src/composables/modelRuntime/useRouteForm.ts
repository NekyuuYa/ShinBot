import { computed, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { ModelRuntimeRoute } from '@/api/modelRuntime'
import type { useModelRuntimeStore } from '@/stores/modelRuntime'
import { tabToCapabilityType, type ModelRuntimeTab } from '@/utils/modelRuntimeSources'
import type { RouteFormState, RouteMemberDraft, RuntimeDomainOption } from './types'

interface RouteFormOptions {
  store: ReturnType<typeof useModelRuntimeStore>
  activeTab: Ref<ModelRuntimeTab>
  selectedRoute: ComputedRef<ModelRuntimeRoute | null>
  isCreatingRoute: Ref<boolean>
  routeDomainOptions: ComputedRef<RuntimeDomainOption[]>
  selectRoute: (id: string) => void
  ensureSelection: () => void
}

export function useRouteForm({
  store,
  activeTab,
  selectedRoute,
  isCreatingRoute,
  routeDomainOptions,
  selectRoute,
  ensureSelection,
}: RouteFormOptions) {
  const { t } = useI18n()

  const routeForm = ref<RouteFormState>({
    id: '',
    purpose: '',
    strategy: 'priority',
    enabled: true,
    stickySessions: false,
    domain: 'chat',
  })

  const routeMembersEditor = ref<RouteMemberDraft[]>([])

  const routeSaveLabel = computed(() =>
    isCreatingRoute.value
      ? t('common.actions.action.create')
      : t('pages.modelRuntime.actions.saveRoute')
  )

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
        .filter((provider) => provider.capabilityType === capabilityType)
        .map((provider) => provider.id)
    )
    return store.models.filter((item) => providerIds.has(item.providerId))
  })

  const availableRouteModelsGrouped = computed(() => {
    const groups: { providerId: string; providerName: string; models: typeof store.models }[] = []

    for (const model of availableRouteModels.value) {
      let group = groups.find((item) => item.providerId === model.providerId)
      if (!group) {
        const provider = store.providers.find((item) => item.id === model.providerId)
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

  const cloneRouteMembers = (members: Array<Partial<RouteMemberDraft>>) =>
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

  const resetRouteForm = () => {
    Object.assign(routeForm.value, {
      id: '',
      purpose: '',
      strategy: 'priority',
      enabled: true,
      stickySessions: false,
      domain: activeTab.value === 'routes' ? 'chat' : activeTab.value,
    })
    routeMembersEditor.value = []
  }

  const saveRoute = async () => {
    const payload = {
      id: routeForm.value.id.trim(),
      purpose: routeForm.value.purpose.trim(),
      strategy: routeForm.value.strategy,
      enabled: routeForm.value.enabled,
      stickySessions: routeForm.value.stickySessions,
      metadata: { domain: routeForm.value.domain },
      members: cloneRouteMembers(routeMembersEditor.value),
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

  watch(
    () => selectedRoute.value?.id,
    () => {
      if (isCreatingRoute.value || !selectedRoute.value) {
        return
      }
      Object.assign(routeForm.value, {
        id: selectedRoute.value.id,
        purpose: selectedRoute.value.purpose,
        strategy: selectedRoute.value.strategy,
        enabled: selectedRoute.value.enabled,
        stickySessions: selectedRoute.value.stickySessions,
        domain: String(selectedRoute.value.metadata.domain || 'chat'),
      })
      routeMembersEditor.value = cloneRouteMembers(selectedRoute.value.members)
    },
    { immediate: true }
  )

  return {
    routeSaveLabel,
    routeForm,
    routeMembersEditor,
    activeRouteDomainLabel,
    availableRouteModels,
    availableRouteModelsGrouped,
    resetRouteForm,
    saveRoute,
    deleteCurrentRoute,
    routeMemberByModel,
    isRouteMemberEnabled,
    toggleRouteMember,
    updateRouteMemberField,
    updateRouteTimeout,
  }
}
