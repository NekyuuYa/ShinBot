import { computed, ref, type ComputedRef, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { ModelRuntimeModel, ModelRuntimeProvider } from '@/api/modelRuntime'
import type { useModelRuntimeStore } from '@/stores/modelRuntime'
import type { useSystemSettingsStore } from '@/stores/systemSettings'
import {
  DEFAULT_CAPABILITIES_FOR_TYPE,
  makeModelId,
  resolveProviderSource,
  routeMatchesTab,
  tabToCapabilityType,
  type ModelRuntimeTab,
  type ProviderCapabilityType,
} from '@/utils/modelRuntimeSources'
import type { ModelFormState } from './types'

interface ModelFormOptions {
  store: ReturnType<typeof useModelRuntimeStore>
  systemSettingsStore: ReturnType<typeof useSystemSettingsStore>
  activeTab: Ref<ModelRuntimeTab>
  selectedProvider: ComputedRef<ModelRuntimeProvider | null>
  routeDomainLabels: ComputedRef<Record<string, string>>
}

export function useModelForm({
  store,
  systemSettingsStore,
  activeTab,
  selectedProvider,
  routeDomainLabels,
}: ModelFormOptions) {
  const { t } = useI18n()

  const catalogLoading = ref(false)
  const catalogSearch = ref('')
  const showInlineModelEditor = ref(false)
  const showModelIdPicker = ref(false)
  const editingModelId = ref('')

  const modelForm = ref<ModelFormState>({
    id: '',
    displayName: '',
    litellmModel: '',
    capabilities: [],
    contextWindow: null,
    inputPrice: '',
    outputPrice: '',
    cacheWritePrice: '',
    cacheReadPrice: '',
    enabled: true,
  })

  const selectedProviderModels = computed(() =>
    selectedProvider.value ? store.modelsByProvider[selectedProvider.value.id] || [] : []
  )

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

  const filteredCatalogItems = computed(() => {
    const keyword = catalogSearch.value.trim().toLowerCase()
    if (!keyword) {
      return availableCatalogItems.value
    }
    return availableCatalogItems.value.filter((item) =>
      `${item.displayName} ${item.id} ${item.litellmModel}`.toLowerCase().includes(keyword)
    )
  })

  const modelIdPickerRouteOptions = computed(() =>
    store.routes
      .filter((item) => routeMatchesTab(item.metadata, activeTab.value))
      .map((item) => {
        const domain =
          typeof item.metadata.domain === 'string' && item.metadata.domain
            ? item.metadata.domain
            : activeTab.value
        const domainLabel = routeDomainLabels.value[domain] || domain
        return {
          id: item.id,
          title: item.id,
          subtitle: item.purpose ? `${item.purpose} - ${domainLabel}` : domainLabel,
          enabled: item.enabled,
        }
      })
      .sort((left, right) => {
        if (left.enabled !== right.enabled) {
          return left.enabled ? -1 : 1
        }
        return left.title.localeCompare(right.title)
      })
  )

  const modelIdPickerProviderGroups = computed(() => {
    const capabilityType = tabToCapabilityType(activeTab.value)
    return store.providers
      .filter((provider) => provider.capabilityType === capabilityType)
      .map((provider) => {
        const items = new Map<
          string,
          {
            value: string
            title: string
            subtitle: string
            kind: 'catalog' | 'configured'
          }
        >()

        for (const item of store.catalogItems[provider.id] || []) {
          const value = item.litellmModel.trim()
          if (!value) {
            continue
          }
          items.set(value, {
            value,
            title: item.displayName || item.id || value,
            subtitle: item.id && item.id !== value ? `${item.id} - ${value}` : value,
            kind: 'catalog',
          })
        }

        for (const model of store.modelsByProvider[provider.id] || []) {
          const value = model.litellmModel.trim()
          if (!value || items.has(value)) {
            continue
          }
          items.set(value, {
            value,
            title: model.displayName || model.id,
            subtitle: model.id !== value ? `${model.id} - ${value}` : value,
            kind: 'configured',
          })
        }

        return {
          providerId: provider.id,
          providerName: provider.displayName || provider.id,
          providerType: resolveProviderSource(provider.type)?.label || provider.type,
          items: [...items.values()].sort((left, right) => left.title.localeCompare(right.title)),
        }
      })
      .filter((group) => group.items.length > 0)
      .sort((left, right) => {
        if (left.providerId === selectedProvider.value?.id) {
          return -1
        }
        if (right.providerId === selectedProvider.value?.id) {
          return 1
        }
        return left.providerName.localeCompare(right.providerName)
      })
  })

  const inlineModelSaveLabel = computed(() =>
    editingModelId.value
      ? t('common.actions.action.save')
      : t('common.actions.action.create')
  )

  const defaultCapabilitiesForTab = () => {
    const type = (selectedProvider.value?.capabilityType ||
      tabToCapabilityType(activeTab.value)) as ProviderCapabilityType
    return DEFAULT_CAPABILITIES_FOR_TYPE[type] ?? DEFAULT_CAPABILITIES_FOR_TYPE.completion
  }

  const resetModelForm = () => {
    const providerId = selectedProvider.value?.id || ''
    Object.assign(modelForm.value, {
      id: providerId ? `${providerId}/` : '',
      displayName: '',
      litellmModel: '',
      capabilities: [...defaultCapabilitiesForTab()],
      contextWindow: null,
      inputPrice: '',
      outputPrice: '',
      cacheWritePrice: '',
      cacheReadPrice: '',
      enabled: true,
    })
  }

  const firstNumericValue = (value: unknown, keys: string[]) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return null
    }
    const map = value as Record<string, unknown>
    for (const key of keys) {
      const candidate = map[key]
      if (candidate === null || candidate === undefined || candidate === '') {
        continue
      }
      const parsed = Number(candidate)
      if (Number.isFinite(parsed)) {
        return parsed
      }
    }
    return null
  }

  const extractPerMillionPrice = (
    costMetadata: Record<string, unknown>,
    keyGroups: string[][]
  ) => {
    const maps = [costMetadata]
    const nestedKeys = ['pricing', 'prices', 'costs']
    for (const nestedKey of nestedKeys) {
      const nested = costMetadata[nestedKey]
      if (nested && typeof nested === 'object' && !Array.isArray(nested)) {
        maps.push(nested as Record<string, unknown>)
      }
    }

    for (const map of maps) {
      for (const group of keyGroups) {
        const value = firstNumericValue(map, group)
        if (value === null) {
          continue
        }
        if (group.some((key) => key.includes('PerToken') || key.includes('_per_token'))) {
          return value * 1_000_000
        }
        if (group.some((key) => key.includes('Per1k') || key.includes('_per_1k_'))) {
          return value * 1_000
        }
        return value
      }
    }
    return null
  }

  const formatPriceInput = (value: number | null) =>
    value === null ? '' : String(systemSettingsStore.convertStoredPriceToDisplay(value))

  const formatPriceDisplay = (value: number | null) => {
    if (value === null) {
      return ''
    }
    const displayValue = systemSettingsStore.convertStoredPriceToDisplay(value)
    if (displayValue === null) {
      return ''
    }
    return new Intl.NumberFormat('zh-CN', {
      style: 'currency',
      currency: systemSettingsStore.pricingCurrency,
      minimumFractionDigits: displayValue >= 100 ? 0 : 2,
      maximumFractionDigits: displayValue >= 100 ? 0 : 4,
    }).format(displayValue)
  }

  const parsePriceInput = (value: string) => {
    const trimmed = value.trim()
    if (!trimmed) {
      return null
    }
    const parsed = Number(trimmed)
    if (!Number.isFinite(parsed) || parsed < 0) {
      throw new Error(t('pages.modelRuntime.messages.invalidPrice'))
    }
    return systemSettingsStore.convertDisplayPriceToStored(parsed)
  }

  const providerModelMeta = (model: ModelRuntimeModel) => {
    const lines = [`${t('pages.modelRuntime.fields.contextWindow')}: ${model.contextWindow || '-'}`]
    if (model.id !== model.litellmModel) {
      lines.push(`${t('pages.modelRuntime.fields.id')}: ${model.id}`)
    }
    const inputPrice = extractPerMillionPrice(model.costMetadata, [
      ['inputPerMillionTokens', 'promptPerMillionTokens', 'input_per_million_tokens', 'prompt_per_million_tokens'],
      ['inputPer1kTokens', 'promptPer1kTokens', 'input_per_1k_tokens', 'prompt_per_1k_tokens'],
      ['inputPerToken', 'promptPerToken', 'input_per_token', 'prompt_per_token'],
    ])
    const outputPrice = extractPerMillionPrice(model.costMetadata, [
      ['outputPerMillionTokens', 'completionPerMillionTokens', 'output_per_million_tokens', 'completion_per_million_tokens'],
      ['outputPer1kTokens', 'completionPer1kTokens', 'output_per_1k_tokens', 'completion_per_1k_tokens'],
      ['outputPerToken', 'completionPerToken', 'output_per_token', 'completion_per_token'],
    ])
    if (inputPrice !== null || outputPrice !== null) {
      lines.push(
        `${t('pages.modelRuntime.fields.pricingSummary')}: ${[
          inputPrice !== null
            ? `${t('pages.modelRuntime.fields.inputPriceShort')} ${formatPriceDisplay(inputPrice)}/${t(`pages.settings.pricing.units.${systemSettingsStore.pricingTokenUnit}`)}`
            : '',
          outputPrice !== null
            ? `${t('pages.modelRuntime.fields.outputPriceShort')} ${formatPriceDisplay(outputPrice)}/${t(`pages.settings.pricing.units.${systemSettingsStore.pricingTokenUnit}`)}`
            : '',
        ]
          .filter(Boolean)
          .join(' - ')}`
      )
    }
    return lines
  }

  const openInlineModelEditor = (modelId = '') => {
    showInlineModelEditor.value = true
    editingModelId.value = modelId
    showModelIdPicker.value = false
    if (!modelId) {
      resetModelForm()
      return
    }
    const model = store.models.find((item) => item.id === modelId)
    if (!model) {
      resetModelForm()
      return
    }
    Object.assign(modelForm.value, {
      id: model.id,
      displayName: model.displayName,
      litellmModel: model.litellmModel,
      capabilities: [...model.capabilities],
      contextWindow: model.contextWindow,
      inputPrice: formatPriceInput(
        extractPerMillionPrice(model.costMetadata, [
          ['inputPerMillionTokens', 'promptPerMillionTokens', 'input_per_million_tokens', 'prompt_per_million_tokens'],
          ['inputPer1kTokens', 'promptPer1kTokens', 'input_per_1k_tokens', 'prompt_per_1k_tokens'],
          ['inputPerToken', 'promptPerToken', 'input_per_token', 'prompt_per_token'],
        ])
      ),
      outputPrice: formatPriceInput(
        extractPerMillionPrice(model.costMetadata, [
          ['outputPerMillionTokens', 'completionPerMillionTokens', 'output_per_million_tokens', 'completion_per_million_tokens'],
          ['outputPer1kTokens', 'completionPer1kTokens', 'output_per_1k_tokens', 'completion_per_1k_tokens'],
          ['outputPerToken', 'completionPerToken', 'output_per_token', 'completion_per_token'],
        ])
      ),
      cacheWritePrice: formatPriceInput(
        extractPerMillionPrice(model.costMetadata, [
          ['cacheWritePerMillionTokens', 'cache_write_per_million_tokens'],
          ['cacheWritePer1kTokens', 'cache_write_per_1k_tokens'],
          ['cacheWritePerToken', 'cache_write_per_token'],
        ])
      ),
      cacheReadPrice: formatPriceInput(
        extractPerMillionPrice(model.costMetadata, [
          ['cacheReadPerMillionTokens', 'cache_read_per_million_tokens'],
          ['cacheReadPer1kTokens', 'cache_read_per_1k_tokens'],
          ['cacheReadPerToken', 'cache_read_per_token'],
        ])
      ),
      enabled: model.enabled,
    })
  }

  const cancelInlineModelEditor = () => {
    showInlineModelEditor.value = false
    showModelIdPicker.value = false
    editingModelId.value = ''
    resetModelForm()
  }

  const openModelIdPicker = () => {
    showModelIdPicker.value = true
  }

  const closeModelIdPicker = () => {
    showModelIdPicker.value = false
  }

  const applyPickedModelId = (value: string) => {
    modelForm.value.litellmModel = value
    showModelIdPicker.value = false
  }

  const saveModel = async () => {
    if (!selectedProvider.value) {
      return
    }

    const existingCostMetadata = editingModelId.value
      ? store.models.find((item) => item.id === editingModelId.value)?.costMetadata || {}
      : {}
    const costMetadata = {
      ...existingCostMetadata,
      inputPerMillionTokens: parsePriceInput(modelForm.value.inputPrice),
      outputPerMillionTokens: parsePriceInput(modelForm.value.outputPrice),
      cacheWritePerMillionTokens: parsePriceInput(modelForm.value.cacheWritePrice),
      cacheReadPerMillionTokens: parsePriceInput(modelForm.value.cacheReadPrice),
    }

    let saved = null
    if (editingModelId.value) {
      saved = await store.updateModel(editingModelId.value, {
        displayName: modelForm.value.displayName,
        litellmModel: modelForm.value.litellmModel,
        capabilities: modelForm.value.capabilities,
        enabled: modelForm.value.enabled,
        defaultParams: {},
        costMetadata,
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
        costMetadata,
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
      capabilities: [...defaultCapabilitiesForTab()],
      contextWindow: null,
      enabled: true,
      defaultParams: {},
      costMetadata: {},
    })
  }

  return {
    catalogLoading,
    catalogSearch,
    showInlineModelEditor,
    showModelIdPicker,
    editingModelId,
    modelForm,
    selectedProviderModels,
    availableCatalogItems,
    filteredCatalogItems,
    modelIdPickerRouteOptions,
    modelIdPickerProviderGroups,
    inlineModelSaveLabel,
    pricingCurrency: systemSettingsStore.pricingCurrency,
    pricingTokenUnit: systemSettingsStore.pricingTokenUnit,
    resetModelForm,
    providerModelMeta,
    openInlineModelEditor,
    cancelInlineModelEditor,
    openModelIdPicker,
    closeModelIdPicker,
    applyPickedModelId,
    saveModel,
    removeModel,
    toggleModel,
    fetchCatalogInline,
    importCatalogItem,
  }
}
