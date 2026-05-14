import { computed, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type {
  ModelRuntimeProvider,
  ProviderPayload,
  ProviderProbeResult,
} from '@/api/modelRuntime'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
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
  const { confirm } = useConfirmDialog()

  const providerForm = ref<ProviderFormState>({
    id: '',
    displayName: '',
    sourceType: 'openai',
    baseUrl: '',
    token: '',
    clearAuthOnSave: false,
    enabled: true,
    proxyAddress: '',
    thinkingJson: '',
    filtersJson: '',
    apiVersion: '',
  })

  const providerHeaderRows = ref<KeyValueEntry[]>([])
  const probingProviderId = ref('')
  const lastProviderProbeResult = ref<ProviderProbeResult | null>(null)
  const providerSourceOptions = providerSourceTemplates

  const providerCapabilityType = computed(() =>
    selectedProvider.value?.capabilityType || tabToCapabilityType(activeTab.value)
  )
  const selectedProviderSource = computed(() => resolveProviderSource(providerForm.value.sourceType))
  const sourceSupportsThinking = computed(() => selectedProviderSource.value?.supportsThinking ?? false)
  const sourceSupportsFilters = computed(() => selectedProviderSource.value?.supportsFilters ?? false)
  const showProviderTokenField = computed(() => selectedProviderSource.value?.supportsToken ?? true)
  const showApiVersionField = computed(() => selectedProviderSource.value?.showApiVersion ?? false)
  const providerCanManageModels = computed(() => !!selectedProvider.value && !isCreatingProvider.value)
  const hasStoredCredential = computed(() =>
    Boolean(selectedProvider.value?.hasAuth) && !isCreatingProvider.value
  )
  const credentialWillBeCleared = computed(() =>
    hasStoredCredential.value
    && (!showProviderTokenField.value || providerForm.value.clearAuthOnSave)
  )

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
      clearAuthOnSave: false,
      enabled: true,
      proxyAddress: '',
      thinkingJson: '',
      filtersJson: '',
      apiVersion: '',
    })
    providerHeaderRows.value = []
    lastProviderProbeResult.value = null
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
      providerForm.value.clearAuthOnSave = hasStoredCredential.value
    } else if (previousSource?.supportsToken === false) {
      providerForm.value.clearAuthOnSave = false
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

  const onProviderSourceChange = async (value: string | null) => {
    if (!value || value === providerForm.value.sourceType) {
      return
    }
    const source = resolveProviderSource(value)
    if (!source) {
      return
    }

    if (!isCreatingProvider.value && selectedProvider.value) {
      const confirmed = await confirm({
        title: t('pages.modelRuntime.dialogs.confirmProviderSourceChange'),
        message: t('pages.modelRuntime.messages.confirmProviderSourceChange', {
          source: source.label,
        }),
        confirmText: t('common.actions.action.confirm'),
        confirmColor: 'primary',
        icon: 'mdi-alert-outline',
        iconColor: 'warning',
      })
      if (!confirmed) {
        return
      }
    }

    const previousType = providerForm.value.sourceType
    applyProviderSource(value, previousType)
    lastProviderProbeResult.value = null
  }

  const toggleStoredCredentialClear = () => {
    if (!hasStoredCredential.value || !showProviderTokenField.value) {
      return
    }
    providerForm.value.clearAuthOnSave = !providerForm.value.clearAuthOnSave
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
        capabilityType: providerCapabilityType.value,
        baseUrl: providerForm.value.baseUrl.trim(),
        enabled: providerForm.value.enabled,
        defaultParams: nextDefaults,
      }

      if (!showProviderTokenField.value || providerForm.value.clearAuthOnSave) {
        payload.auth = {}
      } else if (providerForm.value.token.trim()) {
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
      providerForm.value.clearAuthOnSave = false
    } catch (errorDetail: unknown) {
      store.error = String((errorDetail as Error).message || errorDetail)
    }
  }

  const deleteCurrentProvider = async () => {
    if (!selectedProvider.value) {
      return
    }
    if (
      !(await confirm({
        title: t('common.actions.action.delete'),
        message: t('pages.modelRuntime.messages.confirmDeleteProvider', { id: selectedProvider.value.id }),
        confirmText: t('common.actions.action.delete'),
        confirmColor: 'error',
        icon: 'mdi-alert-outline',
        iconColor: 'error',
      }))
    ) {
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
    const result = await store.probeProvider(selectedProvider.value.id, modelId)
    if (result) {
      lastProviderProbeResult.value = result
    }
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
        clearAuthOnSave: false,
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
      lastProviderProbeResult.value = null
    },
    { immediate: true }
  )

  return {
    providerForm,
    providerHeaderRows,
    providerSourceOptions,
    providerCapabilityType,
    providerSaveLabel,
    selectedProviderSource,
    sourceSupportsThinking,
    sourceSupportsFilters,
    showProviderTokenField,
    showApiVersionField,
    providerCanManageModels,
    hasStoredCredential,
    credentialWillBeCleared,
    probingProviderId,
    lastProviderProbeResult,
    resetProviderForm,
    applyProviderSource,
    onProviderSourceChange,
    toggleStoredCredentialClear,
    saveProvider,
    deleteCurrentProvider,
    probeSelectedProvider,
  }
}
