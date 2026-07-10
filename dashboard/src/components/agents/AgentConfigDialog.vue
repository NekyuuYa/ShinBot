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
          <v-window-item
            v-for="tab in agentTabs"
            :key="tab.value"
            :value="tab.value"
          >
            <div class="agent-config-sections">
              <section
                v-for="section in agentSections[tab.value]"
                :key="section.value"
                class="agent-config-section"
              >
                <div class="agent-config-section__heading">
                  <v-icon :icon="section.icon" size="18" />
                  <span>{{ section.label }}</span>
                </div>

                <provider-schema-form
                  v-model="form.config"
                  :provider="agentProvider"
                  :issues="profileIssues"
                  :field-prefixes="section.fieldPrefixes"
                  :model-ref-route-options="modelRefRouteOptions"
                  :model-ref-provider-groups="modelRefProviderGroups"
                  :persona-ref-options="personaRefOptions ?? []"
                  :advanced-label="$t('pages.agents.labels.advancedFields')"
                  :empty-text="$t('pages.agents.empty.noFields')"
                  :json-error-text="$t('pages.agents.messages.invalidJson')"
                />
              </section>
            </div>
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

interface AgentFormSection {
  value: string
  label: string
  icon: string
  fieldPrefixes: string[]
}

const visible = defineModel<boolean>('visible', { required: true })
const activeAgentTab = defineModel<AgentFormTab>('activeAgentTab', { required: true })
let form = defineModel<{ fileName: string; config: ConfigRecord }>('form', {
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
  personaRefOptions?: Array<{ title: string; value: string }>
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

const agentSections = computed<Record<AgentFormTab, AgentFormSection[]>>(() => ({
  identity: [
    {
      value: 'profile',
      label: t('pages.agents.sections.profile'),
      icon: 'mdi-card-account-details-outline',
      fieldPrefixes: ['agent.id', 'agent.mode', 'agent.persona_id'],
    },
  ],
  defaults: [
    {
      value: 'prompt-files',
      label: t('pages.agents.sections.promptFiles'),
      icon: 'mdi-file-document-multiple-outline',
      fieldPrefixes: ['agent.prompt_files'],
    },
    {
      value: 'model-defaults',
      label: t('pages.agents.sections.modelDefaults'),
      icon: 'mdi-robot-outline',
      fieldPrefixes: [
        'agent.defaults.llm',
        'agent.defaults.caller',
        'agent.defaults.profile_id',
        'agent.defaults.max_model_retries',
        'agent.defaults.retry_backoff_seconds',
        'agent.defaults.params',
      ],
    },
    {
      value: 'message-format',
      label: t('pages.agents.sections.messageFormat'),
      icon: 'mdi-message-text-outline',
      fieldPrefixes: ['agent.defaults.message_format'],
    },
  ],
  review: [
    {
      value: 'review-schedule',
      label: t('pages.agents.sections.reviewSchedule'),
      icon: 'mdi-clock-outline',
      fieldPrefixes: [
        'agent.review.enabled',
        'agent.review.default_review_after_seconds',
        'agent.review.default_review_reason',
        'agent.review.review_due_tick_interval_seconds',
        'agent.review.mention_wake_count',
        'agent.review.mention_wake_window_seconds',
        'agent.review.nearby_candidate_merge_gap',
      ],
    },
    {
      value: 'review-context',
      label: t('pages.agents.sections.reviewContext'),
      icon: 'mdi-format-list-numbered',
      fieldPrefixes: [
        'agent.review.scan_batch_size',
        'agent.review.reply_context_before_messages',
        'agent.review.reply_context_after_messages',
        'agent.review.tail_history_before_seconds',
        'agent.review.tail_history_limit',
        'agent.review.active_chat_summary_max_age_seconds',
      ],
    },
    {
      value: 'review-overflow',
      label: t('pages.agents.sections.reviewOverflow'),
      icon: 'mdi-inbox-arrow-down-outline',
      fieldPrefixes: [
        'agent.review.overflow_threshold_messages',
        'agent.review.overflow_compression_batch_size',
        'agent.review.block_digest_concurrency',
        'agent.review.reply_commit_timeout_seconds',
        'agent.review.bootstrap_timeout_seconds',
        'agent.review.block_digest_retry_on_429',
      ],
    },
    {
      value: 'review-scan-stage',
      label: t('pages.agents.sections.reviewScanStage'),
      icon: 'mdi-radar',
      fieldPrefixes: ['agent.review.scan'],
    },
    {
      value: 'review-reply-stage',
      label: t('pages.agents.sections.reviewReplyStage'),
      icon: 'mdi-reply-outline',
      fieldPrefixes: ['agent.review.reply_decision'],
    },
    {
      value: 'review-digest-stage',
      label: t('pages.agents.sections.reviewDigestStage'),
      icon: 'mdi-text-box-search-outline',
      fieldPrefixes: [
        'agent.review.block_digest',
        'agent.review.overflow_compression',
      ],
    },
    {
      value: 'review-active-stage',
      label: t('pages.agents.sections.reviewActiveStage'),
      icon: 'mdi-chat-plus-outline',
      fieldPrefixes: ['agent.review.active_chat_bootstrap'],
    },
    {
      value: 'review-idle-planning',
      label: t('pages.agents.sections.reviewIdlePlanning'),
      icon: 'mdi-calendar-clock',
      fieldPrefixes: [
        'agent.review.idle_review_planning',
        'agent.review.idle_review_planning_min_after_seconds',
        'agent.review.idle_review_planning_max_after_seconds',
      ],
    },
  ],
  active: [
    {
      value: 'active-lifecycle',
      label: t('pages.agents.sections.activeLifecycle'),
      icon: 'mdi-progress-clock',
      fieldPrefixes: [
        'agent.active_chat.enabled',
        'agent.active_chat.initial_interest',
        'agent.active_chat.half_life_seconds',
        'agent.active_chat.tick_interval_seconds',
        'agent.active_chat.idle_interest_threshold',
        'agent.active_chat.max_interest',
        'agent.active_chat.post_round_attention_multiplier',
        'agent.active_chat.conversation_message_limit',
      ],
    },
    {
      value: 'active-interest-inputs',
      label: t('pages.agents.sections.activeInterestInputs'),
      icon: 'mdi-message-arrow-right-outline',
      fieldPrefixes: [
        'agent.active_chat.interest_delta.normal_message',
        'agent.active_chat.interest_delta.mention_self',
        'agent.active_chat.interest_delta.reply_to_self',
        'agent.active_chat.interest_delta.poke',
        'agent.active_chat.interest_delta.mention_other',
      ],
    },
    {
      value: 'active-interest-outcomes',
      label: t('pages.agents.sections.activeInterestOutcomes'),
      icon: 'mdi-chat-check-outline',
      fieldPrefixes: [
        'agent.active_chat.interest_delta.send_reply',
        'agent.active_chat.interest_delta.send_reply_low',
        'agent.active_chat.interest_delta.no_reply',
        'agent.active_chat.interest_delta.no_reply_strong',
        'agent.active_chat.interest_delta.send_poke',
        'agent.active_chat.interest_delta.request_think_mode',
        'agent.active_chat.interest_delta.retry_failed',
        'agent.active_chat.interest_delta.exit_active',
      ],
    },
    {
      value: 'active-attention-inputs',
      label: t('pages.agents.sections.activeAttentionInputs'),
      icon: 'mdi-eye-outline',
      fieldPrefixes: [
        'agent.active_chat.attention.base_contribution',
        'agent.active_chat.attention.mention_self_contribution',
        'agent.active_chat.attention.mention_other_contribution',
        'agent.active_chat.attention.reply_to_self_contribution',
        'agent.active_chat.attention.poke_self_contribution',
        'agent.active_chat.attention.poke_other_contribution',
        'agent.active_chat.attention.bot_self_contribution',
      ],
    },
    {
      value: 'active-attention-thresholds',
      label: t('pages.agents.sections.activeAttentionThresholds'),
      icon: 'mdi-speedometer',
      fieldPrefixes: [
        'agent.active_chat.attention.contribution_decay_k',
        'agent.active_chat.attention.threshold',
        'agent.active_chat.attention.reference_interest',
        'agent.active_chat.attention.threshold_min',
        'agent.active_chat.attention.threshold_max',
        'agent.active_chat.attention.semantic_wait_ms',
        'agent.active_chat.attention.post_round_accumulated_multiplier',
      ],
    },
    {
      value: 'active-fast-mode',
      label: t('pages.agents.sections.activeFastMode'),
      icon: 'mdi-run-fast',
      fieldPrefixes: ['agent.active_chat.fast_mode'],
    },
  ],
  advanced: [
    {
      value: 'context',
      label: t('pages.agents.sections.context'),
      icon: 'mdi-text-box-outline',
      fieldPrefixes: ['agent.context'],
    },
    {
      value: 'summaries',
      label: t('pages.agents.sections.summaries'),
      icon: 'mdi-file-document-edit-outline',
      fieldPrefixes: ['agent.summaries'],
    },
    {
      value: 'media',
      label: t('pages.agents.sections.media'),
      icon: 'mdi-image-outline',
      fieldPrefixes: ['agent.media'],
    },
  ],
}))
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
  background: rgb(var(--v-theme-surface));
}

.agent-config-sections {
  display: flex;
  flex-direction: column;
  gap: 0;
  padding: 18px 24px 24px;
}

.agent-config-section {
  padding: 18px 0 22px;
  border-bottom: 1px solid $border-color-soft;
}

.agent-config-section:last-child {
  border-bottom: 0;
}

.agent-config-section__heading {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 14px;
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-size: $font-size-sm;
  font-weight: 800;
}

.agent-config-section__heading .v-icon {
  color: rgb(var(--v-theme-primary));
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

  .agent-dialog__file-row {
    padding: 18px;
  }

  .agent-config-sections {
    padding: 10px 18px 18px;
  }

  .agent-config-section {
    padding: 16px 0 20px;
  }

  .agent-dialog__actions {
    padding: 12px 18px 18px;
  }
}
</style>
