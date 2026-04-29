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
import { computed, onMounted, toRef } from 'vue'
import { useI18n } from 'vue-i18n'

import type { AgentSummary } from '@/api/agents'
import type { PromptCatalogItem } from '@/api/prompts'
import type { PluginConfigSchema } from '@/api/plugins'
import SchemaForm from '@/components/SchemaForm.vue'
import KeyValueEditor from '@/components/model-runtime/KeyValueEditor.vue'
import GenericPickerDialog from '@/components/model-runtime/GenericPickerDialog.vue'
import { useInstanceFormPicker } from '@/composables/useInstanceFormPicker'
import type { InstanceFormState, KeyValueEntry } from './types'

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
const {
  botConfigTargetFields,
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
} = useInstanceFormPicker(form, toRef(props, 'promptCatalog'))

const agentOptions = computed(() => [
  { title: t('pages.instances.form.noDefaultAgent'), value: '' },
  ...props.agents.map((agent) => ({
    title: `${agent.name} (${agent.agentId})`,
    value: agent.uuid,
  })),
])

onMounted(() => {
  void ensurePickerResources()
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
