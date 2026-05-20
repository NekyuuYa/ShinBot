<template>
  <v-dialog v-model="visible" max-width="1100" scrollable>
    <v-card class="agent-dialog">
      <v-card-title class="agent-dialog__title">
        {{
          editingFileName
            ? $t('pages.agents.overlay.editTitle')
            : $t('pages.agents.overlay.createTitle')
        }}
      </v-card-title>

      <v-card-text class="agent-dialog__body">
        <div class="agent-dialog__file-row">
          <v-text-field
            v-model="form.fileName"
            :label="$t('pages.agents.fields.fileName')"
            :hint="$t('pages.agents.hints.fileName')"
            :disabled="Boolean(editingFileName)"
            persistent-hint
            variant="outlined"
            density="comfortable"
          />
        </div>

        <v-tabs
          v-model="activeAgentTab"
          class="agent-tabs"
          density="comfortable"
          height="56"
          show-arrows
        >
          <v-tab v-for="tab in agentTabs" :key="tab.value" :value="tab.value">
            <v-icon :icon="tab.icon" size="18" class="me-2" />
            <span>{{ tab.label }}</span>
          </v-tab>
        </v-tabs>

        <v-window v-model="activeAgentTab" class="agent-tab-window">
          <v-window-item value="identity">
            <provider-schema-form
              v-model="form.config"
              :provider="agentProvider"
              :issues="profileIssues"
              :field-prefixes="['agent.id', 'agent.mode', 'agent.persona_id']"
              :model-ref-route-options="modelRefRouteOptions"
              :model-ref-provider-groups="modelRefProviderGroups"
              :advanced-label="$t('pages.agents.labels.advancedFields')"
              :empty-text="$t('pages.agents.empty.noFields')"
              :json-error-text="$t('pages.agents.messages.invalidJson')"
            />
          </v-window-item>

          <v-window-item value="defaults">
            <provider-schema-form
              v-model="form.config"
              :provider="agentProvider"
              :issues="profileIssues"
              :field-prefixes="['agent.prompt_files', 'agent.defaults']"
              :model-ref-route-options="modelRefRouteOptions"
              :model-ref-provider-groups="modelRefProviderGroups"
              :advanced-label="$t('pages.agents.labels.advancedFields')"
              :empty-text="$t('pages.agents.empty.noFields')"
              :json-error-text="$t('pages.agents.messages.invalidJson')"
            />
          </v-window-item>

          <v-window-item value="review">
            <provider-schema-form
              v-model="form.config"
              :provider="agentProvider"
              :issues="profileIssues"
              :field-prefixes="['agent.review']"
              :model-ref-route-options="modelRefRouteOptions"
              :model-ref-provider-groups="modelRefProviderGroups"
              :advanced-label="$t('pages.agents.labels.advancedFields')"
              :empty-text="$t('pages.agents.empty.noFields')"
              :json-error-text="$t('pages.agents.messages.invalidJson')"
            />
          </v-window-item>

          <v-window-item value="active">
            <provider-schema-form
              v-model="form.config"
              :provider="agentProvider"
              :issues="profileIssues"
              :field-prefixes="['agent.active_chat']"
              :model-ref-route-options="modelRefRouteOptions"
              :model-ref-provider-groups="modelRefProviderGroups"
              :advanced-label="$t('pages.agents.labels.advancedFields')"
              :empty-text="$t('pages.agents.empty.noFields')"
              :json-error-text="$t('pages.agents.messages.invalidJson')"
            />
          </v-window-item>

          <v-window-item value="advanced">
            <provider-schema-form
              v-model="form.config"
              :provider="agentProvider"
              :issues="profileIssues"
              :field-prefixes="['agent.context', 'agent.summaries', 'agent.media']"
              :model-ref-route-options="modelRefRouteOptions"
              :model-ref-provider-groups="modelRefProviderGroups"
              :advanced-label="$t('pages.agents.labels.advancedFields')"
              :empty-text="$t('pages.agents.empty.noFields')"
              :json-error-text="$t('pages.agents.messages.invalidJson')"
            />
          </v-window-item>
        </v-window>

        <v-alert
          v-if="profileIssues.length > 0"
          type="warning"
          variant="tonal"
          class="mx-6 mb-4"
        >
          <div class="font-weight-medium mb-2">
            {{ $t('pages.agents.validation.title') }}
          </div>
          <div
            v-for="issue in profileIssues.slice(0, 6)"
            :key="`${issue.path}:${issue.code}:${issue.message}`"
            class="text-body-2 validation-issue-line"
          >
            <span class="font-weight-medium">{{ issue.path || '-' }}</span>
            <span>{{ issue.message }}</span>
          </div>
        </v-alert>

        <v-alert v-if="dialogError" type="error" variant="tonal" class="mx-6 mb-6">
          {{ dialogError }}
        </v-alert>
      </v-card-text>

      <v-card-actions class="agent-dialog__actions">
        <v-spacer />
        <v-btn variant="text" @click="visible = false">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn color="primary" :loading="isSaving" @click="$emit('save')">
          {{
            editingFileName
              ? $t('common.actions.action.save')
              : $t('common.actions.action.create')
          }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'

import type { AgentConfigProfile } from '@/api/agentConfigs'
import type { ConfigRecord, ConfigWorkspaceProvider } from '@/api/config'
import ProviderSchemaForm from '@/components/config/ProviderSchemaForm.vue'

type AgentFormTab = 'identity' | 'defaults' | 'review' | 'active' | 'advanced'

const visible = defineModel<boolean>('visible', { required: true })
const activeAgentTab = defineModel<AgentFormTab>('activeAgentTab', { required: true })
const form = defineModel<{ fileName: string; config: ConfigRecord }>('form', {
  required: true,
})

defineProps<{
  editingFileName: string
  agentProvider: ConfigWorkspaceProvider | null
  profileIssues: AgentConfigProfile['issues']
  dialogError: string
  isSaving: boolean
  modelRefRouteOptions: Array<{ id: string; title: string; subtitle: string; enabled: boolean }>
  modelRefProviderGroups: Array<{
    providerId: string
    providerName: string
    providerType: string
    items: Array<{ value: string; title: string; subtitle: string; kind: 'configured' | 'catalog' }>
  }>
}>()

defineEmits<{
  save: []
}>()

const { t } = useI18n()

const agentTabs = computed(() => [
  {
    value: 'identity' as const,
    label: t('pages.agents.tabs.identity'),
    icon: 'mdi-card-account-details-outline',
  },
  {
    value: 'defaults' as const,
    label: t('pages.agents.tabs.defaults'),
    icon: 'mdi-tune-variant',
  },
  {
    value: 'review' as const,
    label: t('pages.agents.tabs.review'),
    icon: 'mdi-message-processing-outline',
  },
  {
    value: 'active' as const,
    label: t('pages.agents.tabs.active'),
    icon: 'mdi-chat-processing-outline',
  },
  {
    value: 'advanced' as const,
    label: t('pages.agents.tabs.advanced'),
    icon: 'mdi-code-json',
  },
])
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.agent-dialog {
  overflow: hidden;
  border-radius: $radius-base;
}

.agent-dialog__title {
  padding: 20px 24px 16px;
  border-bottom: 1px solid $border-color-soft;
  color: rgba(var(--v-theme-on-surface), 0.94);
  font-size: $font-size-lg;
  font-weight: 800;
}

.agent-dialog__body {
  padding: 0;
}

.agent-dialog__file-row {
  padding: 20px 24px 8px;
}

.agent-tabs {
  min-height: 64px;
  padding: 10px 18px 0;
  border-bottom: 1px solid $border-color-soft;
  background: rgba(var(--v-theme-on-surface), 0.018);
  overflow: visible;
}

.agent-tabs :deep(.v-slide-group__container),
.agent-tabs :deep(.v-slide-group__content) {
  min-height: 56px;
}

.agent-tabs :deep(.v-slide-group__content) {
  align-items: flex-end;
  gap: 8px;
}

.agent-tabs :deep(.v-tab) {
  position: relative;
  min-width: 0;
  min-height: 48px;
  padding-inline: 18px;
  border: 1px solid transparent;
  border-bottom: 0;
  border-radius: 14px 14px 0 0 !important;
  background: rgba(var(--v-theme-on-surface), 0.025);
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-weight: 700;
  text-transform: none;
  transition:
    background-color $transition-fast,
    border-color $transition-fast,
    color $transition-fast,
    box-shadow $transition-fast;
}

.agent-tabs :deep(.v-tab:hover) {
  background: rgba(var(--v-theme-primary), 0.08);
  color: rgba(var(--v-theme-on-surface), 0.88);
}

.agent-tabs :deep(.v-tab.v-tab--selected) {
  z-index: 2;
  border-color: $border-color-soft;
  background: rgb(var(--v-theme-surface));
  color: rgb(var(--v-theme-primary));
  box-shadow: 0 -6px 16px rgba(var(--v-theme-on-surface), 0.05);
}

.agent-tabs :deep(.v-tab.v-tab--selected::after) {
  position: absolute;
  right: 0;
  bottom: -1px;
  left: 0;
  height: 1px;
  background: rgb(var(--v-theme-surface));
  content: '';
}

.agent-tabs :deep(.v-btn__overlay),
.agent-tabs :deep(.v-btn__underlay) {
  border-radius: inherit;
}

.agent-tabs :deep(.v-tab__slider) {
  display: none;
}

.agent-tab-window {
  min-height: 430px;
  padding-top: 10px;
  background: rgb(var(--v-theme-surface));
}

.agent-tab-window :deep(.provider-schema-form) {
  padding: 24px;
}

.agent-tab-window :deep(.v-field) {
  border-radius: $radius-base;
}

.agent-dialog__actions {
  padding: 14px 24px 20px;
  border-top: 1px solid $border-color-soft;
}

.validation-issue-line {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

@include respond-to('mobile') {
  .agent-dialog__title {
    padding: 18px 18px 14px;
  }

  .agent-dialog__file-row,
  .agent-tab-window :deep(.provider-schema-form) {
    padding: 18px;
  }

  .agent-dialog__actions {
    padding: 12px 18px 18px;
  }
}
</style>
