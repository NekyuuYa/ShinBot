import { computed, ref, watch, type ComputedRef, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type {
  ModelRuntimeProvider,
  ProviderTypeMetadata,
  ProviderPayload,
  ProviderProbeResult,
} from '@/api/modelRuntime'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import type { useModelRuntimeStore } from '@/stores/modelRuntime'
import { entriesToObject, objectToEntries, prettyJson, safeJsonParse } from '@/utils/format'
import {
  buildProviderSourceCatalog,
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

const AUTH_FIELD_DEFAULT_KEY = 'api_key'

const MANAGED_DEFAULT_PARAM_KEYS = [
  'apiVersion',
  'filters',
  'proxy',
  'requestHeaders',
  'thinking',
]

const stripManagedDefaultParams = (value: Record<string, unknown>) => {
  const result = { ...value }
  for (const key of MANAGED_DEFAULT_PARAM_KEYS) {
    delete result[key]
  }
  return result
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
    defaultParamsJson: '',
  })

  const providerHeaderRows = ref<KeyValueEntry[]>([])
  const probingProviderId = ref('')
  const lastProviderProbeResult = ref<ProviderProbeResult | null>(null)
  const providerTypeMetadataByType = computed<Record<string, ProviderTypeMetadata>>(() =>
    Object.fromEntries(store.providerTypes.map((item) => [item.type, item])),
  )
  const providerSourceOptions = computed(() => {
    const dynamic = buildProviderSourceCatalog(store.providerTypes)
    return dynamic.length > 0 ? dynamic : providerSourceTemplates
  })

  const providerCapabilityType = computed(() =>
    selectedProvider.value?.capabilityType || tabToCapabilityType(activeTab.value)
  )
  const selectedProviderSource = computed(() =>
    resolveProviderSource(providerForm.value.sourceType, store.providerTypes),
  )
  const selectedProviderTypeMetadata = computed(() => {
    const explicitType = selectedProvider.value?.type
    if (explicitType) {
      return providerTypeMetadataByType.value[explicitType] ?? null
    }
    const sourceType = selectedProviderSource.value?.type
    if (!sourceType) {
      return null
    }
    return providerTypeMetadataByType.value[sourceType] ?? null
  })
  const configFields = computed(() => selectedProviderTypeMetadata.value?.configFields ?? [])
  const hasConfigField = (location: 'auth' | 'default_params', key: string) =>
    computed(() =>
      configFields.value.some((field) => field.location === location && field.key === key),
    )
  const sourceSupportsThinking = computed(
    () =>
      hasConfigField('default_params', 'thinking').value
      || selectedProviderSource.value?.supportsThinking
      || false,
  )
  const sourceSupportsFilters = computed(
    () =>
      hasConfigField('default_params', 'filters').value
      || selectedProviderSource.value?.supportsFilters
      || false,
  )
  const showProviderTokenField = computed(
    () =>
      configFields.value.some((field) => field.location === 'auth' && field.secret)
      || selectedProviderSource.value?.supportsToken
      || false,
  )
  const showApiVersionField = computed(
    () =>
      hasConfigField('default_params', 'apiVersion').value
      || selectedProviderSource.value?.showApiVersion
      || false,
  )
  const providerCanManageModels = computed(() => !!selectedProvider.value && !isCreatingProvider.value)
  const hasStoredCredential = computed(() =>
    Boolean(selectedProvider.value?.hasAuth) && !isCreatingProvider.value
  )
  const credentialWillBeCleared = computed(() =>
    hasStoredCredential.value
    && (!showProviderTokenField.value || providerForm.value.clearAuthOnSave)
  )

  const parseAdditionalDefaultParams = () =>
    stripManagedDefaultParams(
      safeJsonParse<Record<string, unknown>>(
        providerForm.value.defaultParamsJson,
        {},
        t('pages.modelRuntime.messages.invalidDefaultParamsJson'),
      ),
    )

  const setOptionalDefaultParam = (
    target: Record<string, unknown>,
    key: string,
    value: unknown,
  ) => {
    if (
      value === undefined ||
      value === null ||
      value === '' ||
      (typeof value === 'object' &&
        !Array.isArray(value) &&
        Object.keys(value).length === 0)
    ) {
      delete target[key]
      return
    }
    target[key] = value
  }

  const buildProviderDefaultParams = () => {
    const nextDefaults = parseAdditionalDefaultParams()
    const requestHeaders = entriesToObject(providerHeaderRows.value)

    setOptionalDefaultParam(nextDefaults, 'requestHeaders', requestHeaders)
    setOptionalDefaultParam(
      nextDefaults,
      'proxy',
      providerForm.value.proxyAddress.trim(),
    )

    if (showApiVersionField.value) {
      setOptionalDefaultParam(
        nextDefaults,
        'apiVersion',
        providerForm.value.apiVersion.trim(),
      )
    } else {
      delete nextDefaults.apiVersion
    }

    if (sourceSupportsThinking.value) {
      setOptionalDefaultParam(
        nextDefaults,
        'thinking',
        safeJsonParse(providerForm.value.thinkingJson, null),
      )
    } else {
      delete nextDefaults.thinking
    }

    if (sourceSupportsFilters.value) {
      setOptionalDefaultParam(
        nextDefaults,
        'filters',
        safeJsonParse(providerForm.value.filtersJson, null),
      )
    } else {
      delete nextDefaults.filters
    }

    return nextDefaults
  }

  const defaultParamsJsonError = computed(() => {
    try {
      parseAdditionalDefaultParams()
      return ''
    } catch (errorDetail: unknown) {
      return String((errorDetail as Error).message || errorDetail)
    }
  })
  const defaultParamsPreviewJson = computed(() => {
    try {
      return prettyJson(buildProviderDefaultParams()) || '{}'
    } catch {
      return ''
    }
  })

  const providerSaveLabel = computed(() =>
    isCreatingProvider.value
      ? t('common.actions.action.create')
      : t('pages.modelRuntime.actions.saveProvider')
  )

  const resetProviderForm = (type = '') => {
    const source = type
      ? resolveProviderSource(type, store.providerTypes) || providerSourceOptions.value[0] || null
      : null
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
      defaultParamsJson: '',
    })
    providerHeaderRows.value = []
    lastProviderProbeResult.value = null
  }

  const applyProviderSource = (type: string, previousType?: string) => {
    const previousSource = resolveProviderSource(
      previousType ?? providerForm.value.sourceType,
      store.providerTypes,
    )
    const source = resolveProviderSource(type, store.providerTypes)
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
    const source = resolveProviderSource(value, store.providerTypes)
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
      const nextDefaults = buildProviderDefaultParams()

      const payload: ProviderPayload = {
        id: providerForm.value.id.trim(),
        displayName: providerForm.value.displayName.trim() || providerForm.value.id.trim(),
        type:
          resolveProviderSource(providerForm.value.sourceType, store.providerTypes)?.type
          ?? providerForm.value.sourceType,
        capabilityType: providerCapabilityType.value,
        baseUrl: providerForm.value.baseUrl.trim(),
        enabled: providerForm.value.enabled,
        defaultParams: nextDefaults,
      }

      const authParamKey = selectedProviderTypeMetadata.value?.authParamKey || AUTH_FIELD_DEFAULT_KEY

      if (!showProviderTokenField.value || providerForm.value.clearAuthOnSave) {
        payload.auth = {}
      } else if (providerForm.value.token.trim()) {
        payload.auth = { [authParamKey]: providerForm.value.token.trim() }
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

  const syncProviderFormFromSelection = () => {
    if (isCreatingProvider.value || !selectedProvider.value) {
      return
    }
    const source = resolveProviderSource(selectedProvider.value.type, store.providerTypes)
    Object.assign(providerForm.value, {
      id: selectedProvider.value.id,
      displayName: selectedProvider.value.displayName,
      sourceType: resolveProviderSourceKey(
        selectedProvider.value.type,
        selectedProvider.value.baseUrl,
        store.providerTypes,
      ),
      baseUrl: selectedProvider.value.baseUrl,
      token: '',
      clearAuthOnSave: false,
      enabled: selectedProvider.value.enabled,
      proxyAddress: String(selectedProvider.value.defaultParams.proxy || ''),
      thinkingJson: prettyJson(selectedProvider.value.defaultParams.thinking),
      filtersJson: prettyJson(selectedProvider.value.defaultParams.filters),
      apiVersion: String(selectedProvider.value.defaultParams.apiVersion || ''),
      defaultParamsJson: prettyJson(
        stripManagedDefaultParams(selectedProvider.value.defaultParams),
      ),
    })
    providerHeaderRows.value = objectToEntries(
      selectedProvider.value.defaultParams.requestHeaders as Record<string, unknown>
    )
    if (source && !providerForm.value.baseUrl) {
      providerForm.value.baseUrl = source.defaultBaseUrl
    }
    lastProviderProbeResult.value = null
  }

  watch(() => selectedProvider.value, syncProviderFormFromSelection, { immediate: true })

  return {
    providerForm,
    providerHeaderRows,
    providerSourceOptions,
    providerCapabilityType,
    providerSaveLabel,
    selectedProviderSource,
    selectedProviderTypeMetadata,
    sourceSupportsThinking,
    sourceSupportsFilters,
    showProviderTokenField,
    showApiVersionField,
    providerCanManageModels,
    hasStoredCredential,
    credentialWillBeCleared,
    defaultParamsJsonError,
    defaultParamsPreviewJson,
    probingProviderId,
    lastProviderProbeResult,
    resetProviderForm,
    syncProviderFormFromSelection,
    applyProviderSource,
    onProviderSourceChange,
    toggleStoredCredentialClear,
    saveProvider,
    deleteCurrentProvider,
    probeSelectedProvider,
  }
}
