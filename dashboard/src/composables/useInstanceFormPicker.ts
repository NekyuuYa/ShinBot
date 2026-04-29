import { computed, ref, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { ModelRuntimeRoute } from '@/api/modelRuntime'
import type { PromptCatalogItem } from '@/api/prompts'
import type { BotConfigTargetField, BotConfigTargetKey, InstanceFormState, TargetSummary } from '@/components/instances/types'
import type { GenericPickerSection } from '@/components/model-runtime/GenericPickerDialog.vue'
import { useModelRuntimeStore } from '@/stores/modelRuntime'
import { resolveProviderSource } from '@/utils/modelRuntimeSources'

const BOT_CONFIG_TARGET_FIELDS: BotConfigTargetField[] = [
  {
    key: 'mainLlm',
    labelKey: 'pages.instances.form.mainLlm',
    pickerType: 'model',
  },
  {
    key: 'mediaInspectionLlm',
    labelKey: 'pages.instances.form.mediaInspectionLlm',
    pickerType: 'model',
  },
  {
    key: 'mediaInspectionPrompt',
    labelKey: 'pages.instances.form.mediaInspectionPrompt',
    pickerType: 'prompt',
  },
  {
    key: 'stickerSummaryLlm',
    labelKey: 'pages.instances.form.stickerSummaryLlm',
    pickerType: 'model',
  },
  {
    key: 'stickerSummaryPrompt',
    labelKey: 'pages.instances.form.stickerSummaryPrompt',
    pickerType: 'prompt',
  },
  {
    key: 'contextCompressionLlm',
    labelKey: 'pages.instances.form.contextCompressionLlm',
    pickerType: 'model',
  },
]

const routeTitle = (route: ModelRuntimeRoute) => {
  const metadata = route.metadata || {}
  for (const key of ['displayName', 'name', 'title']) {
    const value = metadata[key]
    if (typeof value === 'string' && value.trim()) {
      return value.trim()
    }
  }
  return route.purpose || route.id
}

export function useInstanceFormPicker(
  form: Ref<InstanceFormState>,
  promptCatalog: Readonly<Ref<PromptCatalogItem[]>>
) {
  const { t } = useI18n()
  const modelRuntimeStore = useModelRuntimeStore()
  const activePickerKey = ref<BotConfigTargetKey | null>(null)

  const currentPickerField = computed(
    () => BOT_CONFIG_TARGET_FIELDS.find((field) => field.key === activePickerKey.value) ?? null
  )

  const activePickerVisible = computed({
    get: () => activePickerKey.value !== null,
    set: (isVisible: boolean) => {
      if (!isVisible) {
        activePickerKey.value = null
      }
    },
  })

  const mainLlmPickerSections = computed<GenericPickerSection[]>(() => {
    const result: GenericPickerSection[] = []

    const routes = modelRuntimeStore.routes
    if (routes.length > 0) {
      result.push({
        id: 'routes',
        label: t('pages.modelRuntime.labels.routeTargets'),
        items: [...routes]
          .sort((a, b) => {
            if (a.enabled !== b.enabled) {
              return a.enabled ? -1 : 1
            }
            return a.id.localeCompare(b.id)
          })
          .map((route) => ({
            value: route.id,
            title: routeTitle(route),
            subtitle: route.purpose ? `${route.id} · ${route.strategy}` : route.strategy,
            icon: 'mdi-transit-connection-variant',
            iconColor: route.enabled ? 'primary' : 'surface-variant',
            tag: route.enabled
              ? t('pages.modelRuntime.labels.enabled')
              : t('pages.modelRuntime.labels.disabled'),
            tagColor: route.enabled ? 'primary' : 'default',
          })),
      })
    }

    const providerGroups = modelRuntimeStore.providers
      .map((provider) => {
        const items = (modelRuntimeStore.modelsByProvider[provider.id] || [])
          .filter((model) => model.id.trim())
          .map((model) => ({
            value: model.id,
            title: model.displayName || model.id,
            subtitle:
              model.litellmModel && model.litellmModel !== model.id
                ? `${model.id} · ${model.litellmModel}`
                : model.id,
            enabled: model.enabled,
          }))

        return {
          id: provider.id,
          title: provider.displayName || provider.id,
          subtitle: resolveProviderSource(provider.type)?.label || provider.type,
          items: items
            .sort((a, b) => a.title.localeCompare(b.title))
            .map((item) => ({
              value: item.value,
              title: item.title,
              subtitle: item.subtitle,
              icon: 'mdi-cube-outline',
              iconColor: item.enabled ? 'secondary' : 'surface-variant',
              tag: item.enabled
                ? t('pages.modelRuntime.labels.configured')
                : t('pages.modelRuntime.labels.disabled'),
              tagColor: item.enabled ? 'primary' : 'default',
            })),
        }
      })
      .filter((group) => group.items.length > 0)
      .sort((a, b) => a.title.localeCompare(b.title))

    if (providerGroups.length > 0) {
      result.push({
        id: 'providers',
        label: t('pages.modelRuntime.sidebar.providers'),
        groups: providerGroups,
      })
    }

    return result
  })

  const summaryPromptPickerSections = computed<GenericPickerSection[]>(() => {
    const eligible = [...promptCatalog.value]
      .filter(
        (item) =>
          item.enabled &&
          (item.stage === 'system_base' || item.stage === 'identity' || item.type === 'bundle')
      )
      .sort((a, b) => a.displayName.localeCompare(b.displayName))

    const builtinItems = eligible
      .filter((item) => item.sourceType === 'builtin_system')
      .map((item) => ({
        value: item.id,
        title: item.displayName || item.id,
        subtitle: item.description || item.id,
        icon: 'mdi-shield-star-outline',
        iconColor: 'primary',
        tag: t('pages.instances.form.promptBuiltinTag'),
        tagColor: 'primary',
      }))

    const customItems = eligible
      .filter((item) => item.sourceType !== 'builtin_system')
      .map((item) => ({
        value: item.id,
        title: item.displayName || item.id,
        subtitle: item.description || item.id,
        icon: 'mdi-text-box-outline',
        iconColor: 'secondary',
        tag: item.stage,
        tagColor: 'info',
      }))

    const sections: GenericPickerSection[] = []
    if (builtinItems.length > 0) {
      sections.push({
        id: 'builtin-prompts',
        label: t('pages.instances.form.builtinSummaryPrompts'),
        items: builtinItems,
      })
    }
    if (customItems.length > 0) {
      sections.push({
        id: 'custom-prompts',
        label: t('pages.instances.form.customSummaryPrompts'),
        items: customItems,
      })
    }
    return sections
  })

  const modelTargetSummary = (value: string): TargetSummary => {
    const target = value.trim()
    if (!target) {
      return {
        title: t('pages.instances.form.notSelected'),
        subtitle: t('pages.instances.form.chooseModelTarget'),
        icon: 'mdi-database-search-outline',
        color: 'surface-variant',
      }
    }

    const route = modelRuntimeStore.routes.find((item) => item.id === target)
    if (route) {
      return {
        title: routeTitle(route),
        subtitle: route.purpose ? route.id : route.strategy,
        icon: 'mdi-transit-connection-variant',
        color: route.enabled ? 'primary' : 'surface-variant',
      }
    }

    const model = modelRuntimeStore.models.find((item) => item.id === target)
    if (model) {
      return {
        title: model.displayName || model.id,
        subtitle:
          model.litellmModel && model.litellmModel !== model.id ? model.litellmModel : model.id,
        icon: 'mdi-cube-outline',
        color: model.enabled ? 'secondary' : 'surface-variant',
      }
    }

    return {
      title: target,
      subtitle: t('pages.instances.form.missingSelection'),
      icon: 'mdi-alert-circle-outline',
      color: 'warning',
    }
  }

  const promptTargetSummary = (value: string): TargetSummary => {
    const target = value.trim()
    if (!target) {
      return {
        title: t('pages.instances.form.notSelected'),
        subtitle: t('pages.instances.form.choosePromptTarget'),
        icon: 'mdi-text-box-search-outline',
        color: 'surface-variant',
      }
    }

    const prompt = promptCatalog.value.find((item) => item.id === target)
    if (prompt) {
      return {
        title: prompt.displayName || prompt.id,
        subtitle: prompt.description || prompt.id,
        icon:
          prompt.sourceType === 'builtin_system'
            ? 'mdi-shield-star-outline'
            : 'mdi-text-box-outline',
        color: prompt.enabled ? 'primary' : 'surface-variant',
      }
    }

    return {
      title: target,
      subtitle: t('pages.instances.form.missingSelection'),
      icon: 'mdi-alert-circle-outline',
      color: 'warning',
    }
  }

  const getBotConfigTarget = (key: BotConfigTargetKey) => form.value.botConfig[key]

  const setBotConfigTarget = (key: BotConfigTargetKey, value: string) => {
    form.value.botConfig[key] = value
  }

  const selectedTarget = (key: BotConfigTargetKey) => {
    const value = getBotConfigTarget(key)
    return value ? [value] : []
  }

  const targetSummary = (key: BotConfigTargetKey) => {
    const value = getBotConfigTarget(key)
    const field = BOT_CONFIG_TARGET_FIELDS.find((item) => item.key === key)
    if (!field) {
      return modelTargetSummary(value)
    }
    return field.pickerType === 'prompt' ? promptTargetSummary(value) : modelTargetSummary(value)
  }

  const openPicker = (key: BotConfigTargetKey) => {
    activePickerKey.value = key
  }

  const updatePickerSelection = (values: string[]) => {
    if (!activePickerKey.value) {
      return
    }
    setBotConfigTarget(activePickerKey.value, values[0] ?? '')
  }

  const ensurePickerResources = async () => {
    if (modelRuntimeStore.routes.length === 0) {
      await modelRuntimeStore.fetchAll()
    }
  }

  return {
    botConfigTargetFields: BOT_CONFIG_TARGET_FIELDS,
    currentPickerField,
    activePickerVisible,
    mainLlmPickerSections,
    summaryPromptPickerSections,
    getBotConfigTarget,
    setBotConfigTarget,
    selectedTarget,
    targetSummary,
    openPicker,
    updatePickerSelection,
    ensurePickerResources,
  }
}