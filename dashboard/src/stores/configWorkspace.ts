import { defineStore } from 'pinia'
import { computed, ref, shallowRef } from 'vue'

import { apiClient } from '@/api/client'
import { setConfigPathValue } from '@/config/paths'
import {
  configApi,
  extractConfigValidationIssues,
  type ConfigDocument,
  type ConfigValidationIssue,
  type ConfigValidationResult,
  type ConfigValue,
  type ConfigWorkspace,
  type ConfigWorkspaceProvider,
  type SaveConfigResult,
} from '@/api/config'
import { translate } from '@/plugins/i18n'
import { getErrorMessage } from '@/utils/error'
import { useUiStore } from './ui'

interface LoadConfigWorkspaceOptions {
  preserveDraft?: boolean
}

interface SaveDraftOptions {
  validateBeforeSave?: boolean
}

const emptyNormalizedConfig = (): ConfigValidationResult['normalized'] => ({
  adapterInstances: [],
  bots: [],
})

const cloneConfig = <T extends ConfigValue | ConfigDocument>(value: T): T =>
  JSON.parse(JSON.stringify(value)) as T

const serializeConfig = (value: ConfigDocument) => JSON.stringify(value)

const providerMap = (providers: ConfigWorkspaceProvider[]) =>
  providers.reduce<Record<string, ConfigWorkspaceProvider>>((result, provider) => {
    result[provider.id] = provider
    return result
  }, {})

export const useConfigWorkspaceStore = defineStore('configWorkspace', () => {
  const uiStore = useUiStore()
  const workspace = shallowRef<ConfigWorkspace | null>(null)
  const draft = shallowRef<ConfigDocument>({})
  const validation = shallowRef<ConfigValidationResult | null>(null)
  const isLoading = ref(false)
  const isSaving = ref(false)
  const isValidating = ref(false)
  const error = ref('')
  const lastLoadedAt = ref(0)
  const lastSavedAt = ref(0)
  const baseConfigSnapshot = ref(serializeConfig({}))

  const hasWorkspace = computed(() => workspace.value !== null)
  const isDirty = computed(() => serializeConfig(draft.value) !== baseConfigSnapshot.value)
  const validationIssues = computed(() => validation.value?.issues ?? [])
  const isValid = computed(() => validation.value?.valid ?? false)
  const requiresRestartAfterSave = computed(
    () => workspace.value?.runtime.requiresRestartAfterSave ?? true
  )
  const adapterProvidersById = computed(() =>
    providerMap(workspace.value?.providers.adapters ?? [])
  )
  const pluginProvidersById = computed(() =>
    providerMap(workspace.value?.providers.plugins ?? [])
  )
  const agentProvidersById = computed(() =>
    providerMap(workspace.value?.providers.agents ?? [])
  )
  const issuesByPath = computed(() => {
    const result: Record<string, ConfigValidationIssue[]> = {}
    for (const issue of validationIssues.value) {
      result[issue.path] = result[issue.path] || []
      result[issue.path].push(issue)
    }
    return result
  })

  const applyWorkspace = (
    nextWorkspace: ConfigWorkspace,
    options: LoadConfigWorkspaceOptions = {}
  ) => {
    workspace.value = nextWorkspace
    validation.value = nextWorkspace.validation
    baseConfigSnapshot.value = serializeConfig(nextWorkspace.config)

    if (!options.preserveDraft) {
      draft.value = cloneConfig(nextWorkspace.config)
    }
  }

  const loadWorkspace = async (options: LoadConfigWorkspaceOptions = {}) => {
    isLoading.value = true
    error.value = ''

    try {
      const data = await apiClient.unwrap(configApi.getWorkspace())
      applyWorkspace(data, options)
      lastLoadedAt.value = Date.now()
      return data
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.requestFailed')
      )
      return null
    } finally {
      isLoading.value = false
    }
  }

  const resetDraft = () => {
    if (!workspace.value) {
      draft.value = {}
      validation.value = null
      baseConfigSnapshot.value = serializeConfig({})
      return
    }

    draft.value = cloneConfig(workspace.value.config)
    validation.value = workspace.value.validation
    baseConfigSnapshot.value = serializeConfig(workspace.value.config)
    error.value = ''
  }

  const setDraftConfig = (config: ConfigDocument) => {
    draft.value = cloneConfig(config)
  }

  const updateDraftConfig = (mutator: (config: ConfigDocument) => void) => {
    const nextConfig = cloneConfig(draft.value)
    mutator(nextConfig)
    draft.value = nextConfig
  }

  const setDraftSection = (section: string, value: ConfigValue) => {
    updateDraftConfig((config) => {
      config[section] = cloneConfig(value)
    })
  }

  const setDraftPath = (path: string, value: ConfigValue) => {
    draft.value = setConfigPathValue(cloneConfig(draft.value), path, cloneConfig(value))
  }

  const validateDraft = async () => {
    isValidating.value = true
    error.value = ''

    try {
      const result = await apiClient.unwrap(
        configApi.validate(
          { config: cloneConfig(draft.value) },
          { suppressErrorNotify: true }
        )
      )
      validation.value = result
      return result
    } catch (errorDetail: unknown) {
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.operationFailed')
      )
      return null
    } finally {
      isValidating.value = false
    }
  }

  const applyValidationIssues = (issues: ConfigValidationIssue[]) => {
    validation.value = {
      valid: issues.length === 0,
      issues,
      normalized: validation.value?.normalized ?? emptyNormalizedConfig(),
    }
  }

  const saveDraft = async (options: SaveDraftOptions = {}): Promise<SaveConfigResult | null> => {
    isSaving.value = true
    error.value = ''

    try {
      const result = await apiClient.unwrap(
        configApi.save({
          config: cloneConfig(draft.value),
          validateBeforeSave: options.validateBeforeSave ?? true,
        })
      )
      applyWorkspace(result.workspace)
      validation.value = result.validation
      lastSavedAt.value = Date.now()
      uiStore.showSnackbar(translate('common.actions.message.operationSuccess'), 'success')
      return result
    } catch (errorDetail: unknown) {
      const issues = extractConfigValidationIssues(errorDetail)
      if (issues.length > 0) {
        applyValidationIssues(issues)
      }
      error.value = getErrorMessage(
        errorDetail,
        translate('common.actions.message.operationFailed')
      )
      return null
    } finally {
      isSaving.value = false
    }
  }

  return {
    workspace,
    draft,
    validation,
    isLoading,
    isSaving,
    isValidating,
    error,
    lastLoadedAt,
    lastSavedAt,
    hasWorkspace,
    isDirty,
    isValid,
    validationIssues,
    issuesByPath,
    requiresRestartAfterSave,
    adapterProvidersById,
    pluginProvidersById,
    agentProvidersById,
    applyWorkspace,
    loadWorkspace,
    resetDraft,
    setDraftConfig,
    updateDraftConfig,
    setDraftSection,
    setDraftPath,
    validateDraft,
    saveDraft,
  }
})
