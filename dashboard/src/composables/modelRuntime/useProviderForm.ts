import { computed, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { ModelRuntimeProvider, ProviderPayload } from '@/api/modelRuntime'
import type { useModelRuntimeStore } from '@/stores/modelRuntime'
import { entriesToObject, objectToEntries, prettyJson, safeJsonParse } from '@/utils/format'
import {
  providerSourceTemplates,
  resolveProviderSource,
  resolveProviderSourceKey,
  tabToCapabilityType,
  type ModelRuntimeTab,
} from '@/utils/modelRuntimeSources'
import type { KeyValueEntry, ProviderFormState } from './types'

interface ProviderFormOptions {
  store: ReturnType<typeof useModelRuntimeStore>
  activeTab: Ref<ModelRuntimeTab>
  selectedProvider: ComputedRef<ModelRuntimeProvider | null>
  isCreatingProvider: Ref<boolean>
  selectProvider: (id: string) => void
  ensureSelection: () => void
}

export function useProviderForm({
  store,
  activeTab,
  selectedProvider,
  isCreatingProvider,
  selectProvider,
  ensureSelection,
}: ProviderFormOptions) {
  const { t } = useI18n()

  const providerForm = ref<ProviderFormState>({
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

  const providerHeaderRows = ref<KeyValueEntry[]>([])
  const probingProviderId = ref('')
  const providerSourceOptions = providerSourceTemplates

  const selectedProviderSource = computed(() => resolveProviderSource(providerForm.value.sourceType))
  const sourceSupportsThinking = computed(() => selectedProviderSource.value?.supportsThinking ?? false)
  const sourceSupportsFilters = computed(() => selectedProviderSource.value?.supportsFilters ?? false)
  const showProviderTokenField = computed(() => selectedProviderSource.value?.supportsToken ?? true)
  const showApiVersionField = computed(() => selectedProviderSource.value?.showApiVersion ?? false)
  const providerCanManageModels = computed(() => !!selectedProvider.value && !isCreatingProvider.value)

  const providerSaveLabel = computed(() =>
    isCreatingProvider.value
      ? t('common.actions.action.create')
      : t('pages.modelRuntime.actions.saveProvider')
  )

  const resetProviderForm = (type = '') => {
    const source = type ? resolveProviderSource(type) || providerSourceTemplates[0] : null
    Object.assign(providerForm.value, {
      id: '',
      displayName: '',
      sourceType: source?.key || '',
      baseUrl: source?.defaultBaseUrl || '',
      token: '',
      enabled: true,
      proxyAddress: '',
      thinkingJson: '',
      filtersJson: '',
      apiVersion: '',
    })
    providerHeaderRows.value = []
  }

  const applyProviderSource = (type: string, previousType?: string) => {
    const previousSource = resolveProviderSource(previousType ?? providerForm.value.sourceType)
    const source = resolveProviderSource(type)
    if (!source) {
      return
    }

    const shouldUseDefaultBaseUrl =
      !providerForm.value.baseUrl || providerForm.value.baseUrl === previousSource?.defaultBaseUrl

    providerForm.value.sourceType = source.key
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

  const saveProvider = async () => {
    try {
      const existingDefaults = selectedProvider.value?.defaultParams || {}
      const nextDefaults: Record<string, unknown> = {
        ...existingDefaults,
        requestHeaders: entriesToObject(providerHeaderRows.value),
        proxy: providerForm.value.proxyAddress || undefined,
      }

      if (showApiVersionField.value) {
        nextDefaults.apiVersion = providerForm.value.apiVersion || undefined
      } else {
        delete nextDefaults.apiVersion
      }

      nextDefaults.thinking = sourceSupportsThinking.value
        ? safeJsonParse(providerForm.value.thinkingJson, null)
        : undefined

      nextDefaults.filters = sourceSupportsFilters.value
        ? safeJsonParse(providerForm.value.filtersJson, null)
        : undefined

      const payload: ProviderPayload = {
        id: providerForm.value.id.trim(),
        displayName: providerForm.value.displayName.trim() || providerForm.value.id.trim(),
        type: resolveProviderSource(providerForm.value.sourceType)?.type ?? providerForm.value.sourceType,
        capabilityType: tabToCapabilityType(activeTab.value),
        baseUrl: providerForm.value.baseUrl.trim(),
        enabled: providerForm.value.enabled,
        defaultParams: nextDefaults,
      }

      if (providerForm.value.token.trim()) {
        payload.auth = { api_key: providerForm.value.token.trim() }
      }

      if (isCreatingProvider.value) {
        const created = await store.createProvider(payload)
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
      ensureSelection()
    }
  }

  const probeSelectedProvider = async (modelId?: string) => {
    if (!selectedProvider.value) {
      return
    }
    probingProviderId.value = selectedProvider.value.id
    await store.probeProvider(selectedProvider.value.id, modelId)
    probingProviderId.value = ''
  }

  watch(
    () => selectedProvider.value?.id,
    () => {
      if (isCreatingProvider.value || !selectedProvider.value) {
        return
      }
      const source = resolveProviderSource(selectedProvider.value.type)
      Object.assign(providerForm.value, {
        id: selectedProvider.value.id,
        displayName: selectedProvider.value.displayName,
        sourceType: resolveProviderSourceKey(
          selectedProvider.value.type,
          selectedProvider.value.baseUrl
        ),
        baseUrl: selectedProvider.value.baseUrl,
        token: '',
        enabled: selectedProvider.value.enabled,
        proxyAddress: String(selectedProvider.value.defaultParams.proxy || ''),
        thinkingJson: prettyJson(selectedProvider.value.defaultParams.thinking),
        filtersJson: prettyJson(selectedProvider.value.defaultParams.filters),
        apiVersion: String(selectedProvider.value.defaultParams.apiVersion || ''),
      })
      providerHeaderRows.value = objectToEntries(
        selectedProvider.value.defaultParams.requestHeaders as Record<string, unknown>
      )
      if (source && !providerForm.value.baseUrl) {
        providerForm.value.baseUrl = source.defaultBaseUrl
      }
    },
    { immediate: true }
  )

  return {
    providerForm,
    providerHeaderRows,
    providerSourceOptions,
    providerSaveLabel,
    selectedProviderSource,
    sourceSupportsThinking,
    sourceSupportsFilters,
    showProviderTokenField,
    showApiVersionField,
    providerCanManageModels,
    probingProviderId,
    resetProviderForm,
    applyProviderSource,
    onProviderSourceChange,
    saveProvider,
    deleteCurrentProvider,
    probeSelectedProvider,
  }
}
