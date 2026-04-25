<template>
  <v-dialog v-model="visible" max-width="720">
    <v-card>
      <v-card-title>{{ t(titleKey) }}</v-card-title>
      <v-card-text>
        <v-row>
          <v-col cols="12" md="6">
            <v-text-field v-model="form.name" :label="$t('pages.instances.form.name')" />
          </v-col>
          <v-col cols="12" md="6">
            <v-select
              v-model="form.adapterType"
              :label="$t('pages.instances.form.adapterType')"
              :items="adapterOptions"
            />
          </v-col>
        </v-row>

        <schema-form
          v-if="activeAdapterSchema"
          v-model="form.config"
          :schema="activeAdapterSchema"
          :mode="String(form.config.mode ?? '')"
        />

        <v-alert v-else type="warning" variant="tonal" class="mt-3">
          {{ $t('pages.instances.form.noSchema') }}
        </v-alert>

        <v-divider class="my-5" />

        <div class="text-subtitle1 font-weight-medium mb-3">
          {{ $t('pages.instances.form.botConfigTitle') }}
        </div>
        <v-row>
          <v-col cols="12" md="6">
            <v-select
              v-model="form.botConfig.defaultAgentUuid"
              :label="$t('pages.instances.form.defaultAgent')"
              :items="agentOptions"
              item-title="title"
              item-value="value"
              clearable
            />
          </v-col>
          <v-col v-for="field in botConfigTargetFields" :key="field.key" cols="12" md="6">
            <button type="button" class="config-selector-tile" @click="openPicker(field.key)">
              <span class="selector-label">{{ $t(field.labelKey) }}</span>
              <span class="selector-body">
                <v-avatar size="34" :color="targetSummary(field.key).color" variant="tonal">
                  <v-icon :icon="targetSummary(field.key).icon" size="18" />
                </v-avatar>
                <span class="selector-copy">
                  <span class="selector-title">{{ targetSummary(field.key).title }}</span>
                  <span class="selector-subtitle">{{ targetSummary(field.key).subtitle }}</span>
                </span>
                <span
                  v-if="getBotConfigTarget(field.key)"
                  class="selector-clear"
                  @click.stop="setBotConfigTarget(field.key, '')"
                >
                  <v-icon icon="mdi-close" size="18" />
                </span>
                <v-icon v-else icon="mdi-chevron-right" size="20" class="selector-chevron" />
              </span>
            </button>
          </v-col>
          <v-col cols="12" md="6">
            <v-switch
              v-model="form.botConfig.explicitPromptCacheEnabled"
              :label="$t('pages.instances.form.explicitPromptCacheEnabled')"
              :hint="$t('pages.instances.form.explicitPromptCacheEnabledHint')"
              color="primary"
              persistent-hint
              inset
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field
              v-model="form.botConfig.maxContextTokens"
              :label="$t('pages.instances.form.maxContextTokens')"
              placeholder="32000"
              type="number"
              min="1"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field
              v-model="form.botConfig.contextEvictRatio"
              :label="$t('pages.instances.form.contextEvictRatio')"
              placeholder="0.6"
              type="number"
              min="0"
              max="1"
              step="0.05"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-text-field
              v-model="form.botConfig.contextCompressionMaxChars"
              :label="$t('pages.instances.form.contextCompressionMaxChars')"
              placeholder="240"
              type="number"
              min="1"
            />
          </v-col>
          <v-col cols="12">
            <v-combobox
              v-model="form.botConfig.tags"
              :label="$t('pages.instances.form.botTags')"
              multiple
              chips
              closable-chips
              clearable
            />
          </v-col>
          <v-col cols="12">
            <div class="text-body-2 text-medium-emphasis mb-2">
              {{ $t('pages.instances.form.botConfigFields') }}
            </div>
            <key-value-editor v-model="botConfigEntries" />
          </v-col>
        </v-row>
      </v-card-text>
      <v-card-actions>
        <v-spacer />
        <v-btn variant="text" @click="emit('close')">{{
          $t('common.actions.action.cancel')
        }}</v-btn>
        <v-btn color="primary" @click="emit('save')">{{ $t('common.actions.action.save') }}</v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>

  <generic-picker-dialog
    v-if="currentPickerField"
    v-model="activePickerVisible"
    :title="$t(currentPickerField.labelKey)"
    :sections="
      currentPickerField.pickerType === 'model'
        ? mainLlmPickerSections
        : summaryPromptPickerSections
    "
    :selected="selectedTarget(currentPickerField.key)"
    :close-on-select="false"
    :empty-text="
      currentPickerField.pickerType === 'model'
        ? $t('pages.modelRuntime.hints.modelIdPickerEmpty')
        : $t('pages.instances.form.summaryPromptPickerEmpty')
    "
    :no-results-text="
      currentPickerField.pickerType === 'model'
        ? $t('pages.modelRuntime.hints.modelIdPickerNoMatches')
        : $t('pages.instances.form.summaryPromptPickerNoMatches')
    "
    @update:selected="updatePickerSelection"
  />
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import type { AgentSummary } from '@/api/agents'
import type { PromptCatalogItem } from '@/api/prompts'
import type { PluginConfigSchema } from '@/api/plugins'
import { useModelRuntimeStore } from '@/stores/modelRuntime'
import SchemaForm from '@/components/SchemaForm.vue'
import KeyValueEditor from '@/components/model-runtime/KeyValueEditor.vue'
import GenericPickerDialog, {
  type GenericPickerSection,
} from '@/components/model-runtime/GenericPickerDialog.vue'
import { resolveProviderSource } from '@/utils/modelRuntimeSources'
import type {
  BotConfigTargetField,
  BotConfigTargetKey,
  InstanceFormState,
  KeyValueEntry,
  TargetSummary,
} from './types'

interface Props {
  titleKey: string
  adapterOptions: string[]
  activeAdapterSchema: PluginConfigSchema | null
  agents: AgentSummary[]
  promptCatalog: PromptCatalogItem[]
}

const props = defineProps<Props>()
const emit = defineEmits<{
  close: []
  save: []
}>()

const visible = defineModel<boolean>('visible', { required: true })
const form = defineModel<InstanceFormState>('form', { required: true })
const botConfigEntries = defineModel<KeyValueEntry[]>('botConfigEntries', {
  required: true,
})

const { t } = useI18n()
const modelRuntimeStore = useModelRuntimeStore()
const activePickerKey = ref<BotConfigTargetKey | null>(null)

const botConfigTargetFields: BotConfigTargetField[] = [
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

const currentPickerField = computed(
  () => botConfigTargetFields.find((field) => field.key === activePickerKey.value) ?? null,
)

const activePickerVisible = computed({
  get: () => activePickerKey.value !== null,
  set: (isVisible: boolean) => {
    if (!isVisible) {
      activePickerKey.value = null
    }
  },
})

const agentOptions = computed(() => [
  { title: t('pages.instances.form.noDefaultAgent'), value: '' },
  ...props.agents.map((agent) => ({
    title: `${agent.name} (${agent.agentId})`,
    value: agent.uuid,
  })),
])

const routeTitle = (route: { id: string; purpose: string; metadata?: Record<string, unknown> }) => {
  const metadata = route.metadata || {}
  for (const key of ['displayName', 'name', 'title']) {
    const value = metadata[key]
    if (typeof value === 'string' && value.trim()) {
      return value.trim()
    }
  }
  return route.purpose || route.id
}

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
  const eligible = [...props.promptCatalog]
    .filter(
      (item) =>
        item.enabled &&
        (item.stage === 'system_base' || item.stage === 'identity' || item.type === 'bundle'),
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

  const prompt = props.promptCatalog.find((item) => item.id === target)
  if (prompt) {
    return {
      title: prompt.displayName || prompt.id,
      subtitle: prompt.description || prompt.id,
      icon:
        prompt.sourceType === 'builtin_system' ? 'mdi-shield-star-outline' : 'mdi-text-box-outline',
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
  const field = botConfigTargetFields.find((item) => item.key === key)
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

onMounted(() => {
  if (modelRuntimeStore.routes.length === 0) {
    void modelRuntimeStore.fetchAll()
  }
})
</script>

<style scoped>
.config-selector-tile {
  width: 100%;
  min-height: 86px;
  padding: 10px 14px 12px;
  border: 1px solid rgba(var(--v-theme-outline), 0.32);
  border-radius: 8px;
  background: rgb(var(--v-theme-surface));
  color: rgb(var(--v-theme-on-surface));
  text-align: left;
  transition:
    border-color 0.16s ease,
    background-color 0.16s ease;
}

.config-selector-tile:hover {
  border-color: rgba(var(--v-theme-primary), 0.65);
  background: rgba(var(--v-theme-primary), 0.04);
}

.config-selector-tile:focus-visible {
  outline: 2px solid rgba(var(--v-theme-primary), 0.7);
  outline-offset: 2px;
}

.selector-label {
  display: block;
  margin-bottom: 8px;
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-size: 0.76rem;
  line-height: 1.1;
}

.selector-body {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
}

.selector-copy {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 2px;
}

.selector-title {
  overflow: hidden;
  font-size: 0.96rem;
  font-weight: 600;
  line-height: 1.25;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.selector-subtitle {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.58);
  font-size: 0.78rem;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.selector-chevron {
  color: rgba(var(--v-theme-on-surface), 0.48);
}

.selector-clear {
  display: inline-flex;
  width: 28px;
  height: 28px;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  color: rgba(var(--v-theme-on-surface), 0.58);
}

.selector-clear:hover {
  background: rgba(var(--v-theme-on-surface), 0.08);
  color: rgb(var(--v-theme-on-surface));
}
</style>
