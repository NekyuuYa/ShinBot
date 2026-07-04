<template>
  <v-dialog v-model="visible" max-width="980">
    <v-card class="bot-dialog-card">
      <v-card-title class="px-6 pt-6">
        {{ title }}
      </v-card-title>

      <v-card-text class="px-6">
        <v-alert
          v-if="errorText"
          type="warning"
          variant="tonal"
          density="comfortable"
          class="mb-5"
        >
          {{ errorText }}
        </v-alert>

        <div class="dialog-section-heading">
          <v-icon icon="mdi-robot-outline" size="18" />
          <span>{{ $t('pages.instances.sections.identity') }}</span>
        </div>
        <v-row>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="form.display_name"
              :label="$t('pages.instances.form.displayName')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="form.id"
              :label="$t('pages.instances.form.id')"
              :disabled="editing"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12">
            <v-switch
              v-model="form.enabled"
              :label="$t('pages.instances.form.enabled')"
              color="primary"
              inset
              density="comfortable"
            />
          </v-col>
        </v-row>

        <v-divider class="my-5" />

        <div class="dialog-section-heading">
          <v-icon icon="mdi-console-line" size="18" />
          <span>{{ $t('pages.instances.sections.commands') }}</span>
        </div>
        <v-row>
          <v-col cols="12" md="4">
            <v-switch
              v-model="form.commands.enabled"
              :label="$t('pages.instances.form.commandsEnabled')"
              color="primary"
              inset
              density="comfortable"
            />
          </v-col>
          <v-col cols="12" md="8">
            <v-combobox
              v-model="form.commands.prefixes"
              :label="$t('pages.instances.form.commandPrefixes')"
              density="comfortable"
              variant="outlined"
              multiple
              chips
              closable-chips
              clearable
            />
          </v-col>
        </v-row>

        <v-divider class="my-5" />

        <div class="dialog-section-heading">
          <v-icon icon="mdi-puzzle-outline" size="18" />
          <span>{{ $t('pages.instances.sections.plugins') }}</span>
        </div>
        <v-row>
          <v-col cols="12" md="4">
            <v-switch
              v-model="form.plugins.enabled"
              :label="$t('pages.instances.form.pluginsEnabled')"
              color="primary"
              inset
              density="comfortable"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-combobox
              v-model="form.plugins.enabled_plugins"
              :items="pluginOptions"
              :label="$t('pages.instances.form.enabledPlugins')"
              item-title="title"
              item-value="value"
              density="comfortable"
              variant="outlined"
              multiple
              chips
              closable-chips
              clearable
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-combobox
              v-model="form.plugins.disabled_plugins"
              :items="pluginOptions"
              :label="$t('pages.instances.form.disabledPlugins')"
              item-title="title"
              item-value="value"
              density="comfortable"
              variant="outlined"
              multiple
              chips
              closable-chips
              clearable
            />
          </v-col>
        </v-row>

        <v-divider class="my-5" />

        <div class="dialog-section-heading">
          <v-icon icon="mdi-account-badge-outline" size="18" />
          <span>{{ $t('pages.instances.sections.agent') }}</span>
        </div>
        <v-row>
          <v-col cols="12" md="4">
            <v-select
              v-model="form.agent.mode"
              :items="agentModeOptions"
              :label="$t('pages.instances.form.agentMode')"
              item-title="title"
              item-value="value"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="8">
            <v-select
              v-model="form.agent.config"
              :items="agentConfigOptions"
              :label="$t('pages.instances.form.agentConfig')"
              :hint="$t('pages.instances.form.agentConfigHint')"
              item-title="title"
              item-value="value"
              density="comfortable"
              variant="outlined"
              persistent-hint
              clearable
            >
              <template v-if="agentConfigOptions.length === 0" #no-data>
                <v-list-item>
                  <v-list-item-title>{{ $t('pages.instances.empty.noAgentConfigs') }}</v-list-item-title>
                </v-list-item>
              </template>
            </v-select>
          </v-col>
        </v-row>

        <v-divider class="my-5" />

        <div class="dialog-section-heading with-action">
          <span class="heading-copy">
            <v-icon icon="mdi-routes" size="18" />
            <span>{{ $t('pages.instances.sections.bindings') }}</span>
          </span>
          <v-btn
            size="small"
            variant="tonal"
            color="primary"
            prepend-icon="mdi-plus"
            @click="addBinding"
          >
            {{ $t('pages.instances.actions.addBinding') }}
          </v-btn>
        </div>

        <v-alert
          v-if="adapterOptions.length === 0"
          type="info"
          variant="tonal"
          density="comfortable"
          class="mb-4"
        >
          {{ $t('pages.instances.empty.noMessagePlatforms') }}
        </v-alert>

        <div v-if="form.bindings.length === 0" class="empty-bindings">
          {{ $t('pages.instances.empty.noBindings') }}
        </div>
        <div v-else class="binding-list">
          <div
            v-for="(binding, index) in form.bindings"
            :key="binding.id || index"
            class="binding-row"
          >
            <div class="binding-row-header">
              <v-chip size="small" color="info" variant="tonal">
                {{ $t('pages.instances.bindingTitle', { index: index + 1 }) }}
              </v-chip>
              <v-btn
                icon="mdi-delete"
                size="small"
                variant="text"
                color="error"
                @click="removeBinding(index)"
              />
            </div>
            <v-row>
              <v-col cols="12" md="4">
                <v-text-field
                  v-model="binding.id"
                  :label="$t('pages.instances.form.bindingId')"
                  density="comfortable"
                  variant="outlined"
                />
              </v-col>
              <v-col cols="12" md="4">
                <v-select
                  v-model="binding.adapter_instance_id"
                  :items="adapterOptions"
                  :label="$t('pages.instances.form.adapterInstance')"
                  item-title="title"
                  item-value="value"
                  density="comfortable"
                  variant="outlined"
                />
              </v-col>
              <v-col cols="12" md="2">
                <v-text-field
                  v-model.number="binding.priority"
                  :label="$t('pages.instances.form.priority')"
                  type="number"
                  density="comfortable"
                  variant="outlined"
                />
              </v-col>
              <v-col cols="12" md="2">
                <v-switch
                  v-model="binding.enabled"
                  :label="$t('pages.instances.form.bindingEnabled')"
                  color="primary"
                  inset
                  density="comfortable"
                />
              </v-col>
              <v-col cols="12">
                <v-combobox
                  v-model="binding.session_patterns"
                  :items="sessionPatternSuggestions"
                  :label="$t('pages.instances.form.sessionPatterns')"
                  :hint="$t('pages.instances.form.sessionPatternsHint')"
                  density="comfortable"
                  variant="outlined"
                  multiple
                  chips
                  closable-chips
                  clearable
                  persistent-hint
                />
              </v-col>
            </v-row>
          </div>
        </div>
      </v-card-text>

      <v-card-actions class="px-6 pb-6">
        <v-spacer />
        <v-btn variant="text" @click="emit('close')">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn color="primary" :loading="saving" :disabled="saving" @click="emit('save')">
          {{ $t('common.actions.action.save') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import type { NormalizedBotBindingConfig } from '@/api/config'
import type { BotInstanceFormState, SelectOption } from './botTypes'

interface Props {
  title: string
  adapterOptions: SelectOption[]
  pluginOptions: SelectOption[]
  agentModeOptions: SelectOption[]
  agentConfigOptions: SelectOption[]
  editing?: boolean
  saving?: boolean
  errorText?: string
}

const props = withDefaults(defineProps<Props>(), {
  editing: false,
  saving: false,
  errorText: '',
  agentConfigOptions: () => [],
})

const emit = defineEmits<{
  close: []
  save: []
}>()

const visible = defineModel<boolean>('visible', { required: true })
const form = defineModel<BotInstanceFormState>('form', { required: true })

const sessionPatternSuggestions = [
  'group:*',
  'private:*',
  'group:10001',
  'private:user_id',
]

function makeBindingId() {
  const base = form.value.id.trim() || 'bot'
  const used = new Set(form.value.bindings.map((binding) => binding.id))
  let candidate = `${base}-binding`
  let counter = 2

  while (used.has(candidate)) {
    candidate = `${base}-binding-${counter}`
    counter += 1
  }
  return candidate
}

function addBinding() {
  const binding: NormalizedBotBindingConfig = {
    id: makeBindingId(),
    adapter_instance_id: props.adapterOptions[0]?.value ?? '',
    session_patterns: ['group:*'],
    enabled: true,
    priority: 0,
  }
  form.value = {
    ...form.value,
    bindings: [...form.value.bindings, binding],
  }
}

function removeBinding(index: number) {
  form.value = {
    ...form.value,
    bindings: form.value.bindings.filter((_, currentIndex) => currentIndex !== index),
  }
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.bot-dialog-card {
  border-radius: $radius-sm;
}

.dialog-section-heading {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 16px;
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-size: $font-size-sm;
  font-weight: 700;
}

.dialog-section-heading.with-action {
  justify-content: space-between;
}

.heading-copy {
  display: inline-flex;
  align-items: center;
  gap: 8px;
}

.empty-bindings {
  padding: 16px;
  border: 1px dashed $border-color-base;
  border-radius: $radius-xs;
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: $font-size-sm;
  text-align: center;
}

.binding-list {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.binding-row {
  padding: 14px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-xs;
  background: rgba(var(--v-theme-on-surface), 0.018);
}

.binding-row-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}
</style>
