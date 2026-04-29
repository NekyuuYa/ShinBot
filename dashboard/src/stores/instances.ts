import { defineStore } from 'pinia'
import { ref } from 'vue'
import type { BotConfig, CreateBotConfigRequest } from '@/api/botConfigs'
import { botConfigsApi } from '@/api/botConfigs'
import {
  instancesApi,
  type Instance,
  type CreateInstanceRequest,
  type UpdateInstanceRequest,
} from '@/api/instances'
import type { InstanceFormState, KeyValueEntry } from '@/components/instances/types'
import { translate } from '@/plugins/i18n'
import { getErrorMessage } from '@/utils/error'
import {
  entriesToObject,
  formatOptionalNumber,
  normalizeNullableString,
  objectToEntries,
  parseOptionalFloat,
  parseOptionalInteger,
} from '@/utils/format'
import { createCrudStore } from './crud'
import { useUiStore } from './ui'

export const useInstancesStore = defineStore('instances', () => {
  const uiStore = useUiStore()
  const crud = createCrudStore<Instance, CreateInstanceRequest, UpdateInstanceRequest, string>({
    api: instancesApi,
    i18nKey: {
      loadFailed: 'pages.instances.loadFailed',
      createFailed: 'pages.instances.createFailed',
      updateFailed: 'pages.instances.updateFailed',
      deleteFailed: 'pages.instances.deleteFailed',
      created: 'pages.instances.created',
      updated: 'pages.instances.updated',
      deleted: 'pages.instances.deleted',
    },
    idOf: (instance) => instance.id,
  })
  const instances = crud.items
  const pendingActions = ref<Record<string, 'start' | 'stop' | null>>({})

  const createEmptyBotConfig = (): InstanceFormState['botConfig'] => ({
    uuid: '',
    defaultAgentUuid: '',
    mainLlm: '',
    explicitPromptCacheEnabled: false,
    mediaInspectionLlm: '',
    mediaInspectionPrompt: '',
    stickerSummaryLlm: '',
    stickerSummaryPrompt: '',
    contextCompressionLlm: '',
    maxContextTokens: '',
    contextEvictRatio: '',
    contextCompressionMaxChars: '',
    tags: [],
  })

  const createFormState = (adapterType = ''): InstanceFormState => ({
    name: '',
    adapterType,
    config: {},
    botConfig: createEmptyBotConfig(),
  })

  const buildEditorState = (instance: Instance, botConfig?: BotConfig | null) => ({
    form: {
      name: instance.name,
      adapterType: instance.adapterType,
      config: { ...instance.config },
      botConfig: {
        uuid: botConfig?.uuid ?? '',
        defaultAgentUuid: botConfig?.defaultAgentUuid ?? instance.botConfig?.defaultAgentUuid ?? '',
        mainLlm: botConfig?.mainLlm ?? instance.botConfig?.mainLlm ?? '',
        explicitPromptCacheEnabled: botConfig?.explicitPromptCacheEnabled ?? instance.botConfig?.explicitPromptCacheEnabled ?? false,
        mediaInspectionLlm: botConfig?.mediaInspectionLlm ?? instance.botConfig?.mediaInspectionLlm ?? '',
        mediaInspectionPrompt: botConfig?.mediaInspectionPrompt ?? instance.botConfig?.mediaInspectionPrompt ?? '',
        stickerSummaryLlm: botConfig?.stickerSummaryLlm ?? instance.botConfig?.stickerSummaryLlm ?? '',
        stickerSummaryPrompt: botConfig?.stickerSummaryPrompt ?? instance.botConfig?.stickerSummaryPrompt ?? '',
        contextCompressionLlm: botConfig?.contextCompressionLlm ?? instance.botConfig?.contextCompressionLlm ?? '',
        maxContextTokens: formatOptionalNumber(botConfig?.maxContextTokens ?? instance.botConfig?.maxContextTokens),
        contextEvictRatio: formatOptionalNumber(botConfig?.contextEvictRatio ?? instance.botConfig?.contextEvictRatio),
        contextCompressionMaxChars: formatOptionalNumber(botConfig?.contextCompressionMaxChars ?? instance.botConfig?.contextCompressionMaxChars),
        tags: [...(botConfig?.tags ?? instance.botConfig?.tags ?? [])],
      },
    },
    botConfigEntries: objectToEntries(botConfig?.config ?? {}),
  })

  const buildBotConfigPayload = (
    instanceId: string,
    botConfig: InstanceFormState['botConfig'],
    botConfigEntries: KeyValueEntry[]
  ): CreateBotConfigRequest => ({
    instanceId,
    defaultAgentUuid: botConfig.defaultAgentUuid,
    mainLlm: botConfig.mainLlm.trim(),
    explicitPromptCacheEnabled: botConfig.explicitPromptCacheEnabled,
    mediaInspectionLlm: normalizeNullableString(botConfig.mediaInspectionLlm),
    mediaInspectionPrompt: normalizeNullableString(botConfig.mediaInspectionPrompt),
    stickerSummaryLlm: normalizeNullableString(botConfig.stickerSummaryLlm),
    stickerSummaryPrompt: normalizeNullableString(botConfig.stickerSummaryPrompt),
    contextCompressionLlm: normalizeNullableString(botConfig.contextCompressionLlm),
    maxContextTokens: parseOptionalInteger(botConfig.maxContextTokens, 'pages.instances.form.maxContextTokens'),
    contextEvictRatio: parseOptionalFloat(botConfig.contextEvictRatio, 'pages.instances.form.contextEvictRatio'),
    contextCompressionMaxChars: parseOptionalInteger(botConfig.contextCompressionMaxChars, 'pages.instances.form.contextCompressionMaxChars'),
    config: entriesToObject(botConfigEntries),
    tags: botConfig.tags.map((tag) => tag.trim()).filter(Boolean),
  })

  const hasMeaningfulBotConfig = (payload: CreateBotConfigRequest) => (
    Boolean(payload.defaultAgentUuid)
    || Boolean(payload.mainLlm)
    || Boolean(payload.explicitPromptCacheEnabled)
    || Boolean(payload.mediaInspectionLlm)
    || Boolean(payload.mediaInspectionPrompt)
    || Boolean(payload.stickerSummaryLlm)
    || Boolean(payload.stickerSummaryPrompt)
    || Boolean(payload.contextCompressionLlm)
    || payload.maxContextTokens !== null
    || payload.contextEvictRatio !== null
    || payload.contextCompressionMaxChars !== null
    || (payload.tags?.length ?? 0) > 0
    || Object.keys(payload.config ?? {}).length > 0
  )

  const saveBotConfig = async (
    instanceId: string,
    botConfig: InstanceFormState['botConfig'],
    botConfigEntries: KeyValueEntry[]
  ) => {
    let payload: CreateBotConfigRequest

    try {
      payload = buildBotConfigPayload(instanceId, botConfig, botConfigEntries)
    } catch (errorDetail: unknown) {
      const message = getErrorMessage(errorDetail, translate('pages.instances.botConfigSaveFailed'))
      crud.error.value = message
      uiStore.showSnackbar(message, 'error')
      return false
    }

    if (!hasMeaningfulBotConfig(payload) && !botConfig.uuid) {
      return true
    }

    const result = botConfig.uuid
      ? await crud.runRequest(() => botConfigsApi.update(botConfig.uuid, payload), {
          mode: 'saving',
          errorKey: 'pages.instances.botConfigSaveFailed',
        })
      : await crud.runRequest(() => botConfigsApi.create(payload), {
          mode: 'saving',
          errorKey: 'pages.instances.botConfigSaveFailed',
        })

    if (!result.ok) {
      uiStore.showSnackbar(crud.error.value || translate('pages.instances.botConfigSaveFailed'), 'error')
    }

    return result.ok
  }

  const saveBaseInstance = async (formState: InstanceFormState, editingId?: string) => {
    const result = editingId
      ? await crud.runRequest(
          () => instancesApi.update(editingId, { name: formState.name, config: formState.config }),
          {
            mode: 'saving',
            errorKey: 'pages.instances.updateFailed',
            onSuccess: (data) => {
              if (data) {
                crud.replaceItem(data)
              }
            },
          }
        )
      : await crud.runRequest(
          () => instancesApi.create({ name: formState.name, adapterType: formState.adapterType, config: formState.config }),
          {
            mode: 'saving',
            errorKey: 'pages.instances.createFailed',
            onSuccess: (data) => {
              if (data) {
                crud.appendItem(data)
              }
            },
          }
        )

    return result.ok ? (result.data ?? null) : null
  }

  const saveInstanceForm = async (
    formState: InstanceFormState,
    botConfigEntries: KeyValueEntry[],
    editingId?: string
  ) => {
    const instance = await saveBaseInstance(formState, editingId)
    if (!instance) {
      return false
    }

    const botConfigSaved = await saveBotConfig(instance.id, formState.botConfig, botConfigEntries)
    if (!botConfigSaved) {
      return false
    }

    uiStore.showSnackbar(translate('pages.instances.saved'), 'success')
    return true
  }

  const startInstance = async (id: string) => {
    pendingActions.value[id] = 'start'
    const result = await crud.runRequest(() => instancesApi.start(id), {
      errorKey: 'pages.instances.startFailed',
      failureNotifyKey: 'pages.instances.startFailed',
      successKey: 'pages.instances.started',
      successColor: 'success',
      expectData: false,
    })

    if (!result.ok) {
      pendingActions.value[id] = null
    }

    return result.ok
  }

  const stopInstance = async (id: string) => {
    pendingActions.value[id] = 'stop'
    const result = await crud.runRequest(() => instancesApi.stop(id), {
      errorKey: 'pages.instances.stopFailed',
      failureNotifyKey: 'pages.instances.stopFailed',
      successKey: 'pages.instances.stopRequested',
      successColor: 'info',
      expectData: false,
    })

    if (!result.ok) {
      pendingActions.value[id] = null
    }

    return result.ok
  }

  const clearPendingAction = (id: string) => {
    pendingActions.value[id] = null
  }

  const isInstancePending = (id: string) => pendingActions.value[id] !== null

  const syncInstanceStatuses = (snapshot: Array<{ id: string; status: Instance['status'] }>) => {
    const byId = new Map(snapshot.map((item) => [item.id, item.status]))
    instances.value = instances.value.map((instance) => {
      const status = byId.get(instance.id)
      if (!status) {
        return instance
      }

      if (pendingActions.value[instance.id] !== null) {
        pendingActions.value[instance.id] = null
      }

      if (instance.status === status) {
        return instance
      }

      return {
        ...instance,
        status,
      }
    })
  }

  return {
    instances,
    isLoading: crud.isLoading,
    isSaving: crud.isSaving,
    error: crud.error,
    pendingActions,
    createFormState,
    buildEditorState,
    isInstancePending,
    clearPendingAction,
    syncInstanceStatuses,
    fetchInstances: crud.fetchItems,
    createInstance: crud.createItem,
    updateInstance: crud.updateItem,
    saveBotConfig,
    saveInstanceForm,
    deleteInstance: crud.deleteItem,
    startInstance,
    stopInstance,
  }
})
