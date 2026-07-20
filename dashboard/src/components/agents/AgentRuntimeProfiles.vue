<template>
  <v-row v-if="profiles.length > 0" class="mb-6">
    <v-col v-for="profile in profiles" :key="profile.botId" cols="12" lg="6">
      <v-card class="runtime-card h-100" elevation="0">
        <v-card-item>
          <template #prepend>
            <v-avatar color="info" variant="tonal" icon="mdi-robot-outline" />
          </template>
          <v-card-title class="text-break">
            {{ profile.botName || profile.botId }}
          </v-card-title>
          <v-card-subtitle class="text-break">
            {{ profile.botId }} · {{ profile.agentMode }}
          </v-card-subtitle>
        </v-card-item>
        <v-card-text class="pt-1">
          <div class="runtime-meta-row">
            <span>{{ bindingsLabel }}</span>
            <strong>{{ profile.bindings.length }}</strong>
          </div>
          <div
            v-for="binding in profile.bindings"
            :key="`${profile.botId}:${binding.adapterInstanceId}`"
            class="runtime-meta-row"
          >
            <span>{{ binding.adapterInstanceId }}</span>
            <v-chip
              size="x-small"
              variant="tonal"
              :color="platformStatus(binding.platformState).color"
            >
              <v-icon :icon="platformStatus(binding.platformState).icon" start />
              {{ platformStatus(binding.platformState).label }}
            </v-chip>
          </div>
          <div class="runtime-meta-row">
            <span>{{ sessionsLabel }}</span>
            <strong>{{ profile.sessions.length }}</strong>
          </div>
          <v-expansion-panels class="mt-3" variant="accordion">
            <v-expansion-panel v-for="session in profile.sessions" :key="session.sessionId">
              <v-expansion-panel-title>
                <div class="d-flex w-100 align-center justify-space-between gap-3">
                  <span class="text-truncate">{{ session.sessionId }}</span>
                  <v-chip size="x-small" variant="tonal" color="primary">
                    {{ session.state }}
                  </v-chip>
                </div>
              </v-expansion-panel-title>
              <v-expansion-panel-text>
                <div class="runtime-meta-row">
                  <span>{{ platformLabel }}</span>
                  <strong>{{ session.adapterInstanceId || noValueLabel }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ platformStatusLabel }}</span>
                  <v-chip
                    size="x-small"
                    variant="tonal"
                    :color="platformStatus(session.platformState).color"
                  >
                    <v-icon :icon="platformStatus(session.platformState).icon" start />
                    {{ platformStatus(session.platformState).label }}
                  </v-chip>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ reviewLabel }}</span>
                  <strong>{{
                    session.reviewPlan?.nextReviewAt
                      ? formatTimestamp(session.reviewPlan.nextReviewAt)
                      : noValueLabel
                  }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ reviewIntervalLabel }}</span>
                  <strong>{{ formatReviewInterval(session.reviewPlan?.nextReviewAt) }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ unreadLabel }}</span>
                  <strong>{{ session.unreadCount }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ activeChatLabel }}</span>
                  <strong>{{
                    session.activeChatState
                      ? `${session.activeChatState.interestValue.toFixed(1)} / ${session.activeChatState.tickCount}`
                      : noValueLabel
                  }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ lastReviewLabel }}</span>
                  <strong>{{
                    session.latestReviewSummary
                      ? formatTimestamp(session.latestReviewSummary.createdAt)
                      : session.latestReviewRun
                        ? formatTimestamp(session.latestReviewRun.startedAt)
                        : noValueLabel
                  }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ reviewNoteLabel }}</span>
                  <strong>{{
                    session.latestReviewSummary?.summary ||
                    session.latestReviewRun?.responseSummary ||
                    noValueLabel
                  }}</strong>
                </div>
                <div class="runtime-meta-row">
                  <span>{{ lastAuditLabel }}</span>
                  <strong>{{
                    session.latestAudit ? session.latestAudit.timestamp : noValueLabel
                  }}</strong>
                </div>
                <section
                  v-if="session.idleReviewPlanningDecisions.length > 0"
                  class="idle-review-planning"
                >
                  <div class="runtime-meta-row">
                    <span>{{ idleReviewPlanningLabel }}</span>
                    <strong>{{ session.idleReviewPlanningDecisions.length }}</strong>
                  </div>
                  <article
                    v-for="decision in session.idleReviewPlanningDecisions"
                    :key="decision.signalId"
                    class="planning-decision"
                  >
                    <div class="planning-decision-header">
                      <span class="text-truncate">{{ decision.trigger }}</span>
                      <v-chip
                        size="x-small"
                        variant="tonal"
                        :color="planningOutcomeColor(decision)"
                      >
                        {{ planningOutcome(decision) }}
                      </v-chip>
                    </div>
                    <div class="planning-decision-row">
                      <span>{{ modelResultLabel }}</span>
                      <strong>{{ planningModelDetail(decision) }}</strong>
                    </div>
                    <div class="planning-decision-row">
                      <span>{{ applicationLabel }}</span>
                      <strong>{{ planningApplicationDetail(decision) }}</strong>
                    </div>
                    <div class="planning-decision-row">
                      <span>{{ nextReviewPlanLabel }}</span>
                      <strong>{{ formatPlanningTimestamp(plannedReviewAt(decision)) }}</strong>
                    </div>
                    <div class="planning-decision-row">
                      <span>{{ modelExecutionLabel }}</span>
                      <strong>{{ decision.modelResult?.modelExecutionId || noValueLabel }}</strong>
                    </div>
                  </article>
                </section>
                <div class="session-actions mt-3">
                  <v-btn
                    v-if="session.state === 'idle'"
                    size="small"
                    variant="tonal"
                    color="primary"
                    prepend-icon="mdi-refresh"
                    @click="$emit('triggerReview', profile.profileId, session.sessionId)"
                  >
                    {{ triggerReviewLabel }}
                  </v-btn>
                  <v-btn
                    v-if="session.state !== 'idle'"
                    size="small"
                    variant="tonal"
                    color="warning"
                    prepend-icon="mdi-stop-circle-outline"
                    @click="$emit('forceIdle', session.sessionId)"
                  >
                    {{ forceIdleLabel }}
                  </v-btn>
                </div>
              </v-expansion-panel-text>
            </v-expansion-panel>
          </v-expansion-panels>
        </v-card-text>
      </v-card>
    </v-col>
  </v-row>
</template>

<script setup lang="ts">
import { useI18n } from 'vue-i18n'

import type {
  AgentRuntimeIdleReviewPlanningDecision,
  AgentRuntimePlatformState,
  AgentRuntimeProfile,
} from '@/api/agents'

const { t } = useI18n()

const props = defineProps<{
  profiles: AgentRuntimeProfile[]
  bindingsLabel: string
  sessionsLabel: string
  platformLabel: string
  platformStatusLabel: string
  reviewLabel: string
  reviewIntervalLabel: string
  unreadLabel: string
  activeChatLabel: string
  lastReviewLabel: string
  reviewNoteLabel: string
  lastAuditLabel: string
  idleReviewPlanningLabel: string
  modelResultLabel: string
  applicationLabel: string
  nextReviewPlanLabel: string
  modelExecutionLabel: string
  noValueLabel: string
  triggerReviewLabel: string
  forceIdleLabel: string
  formatTimestamp: (value: number) => string
  formatReviewInterval: (value: number | null | undefined) => string
}>()

defineEmits<{
  triggerReview: [profileId: string, sessionId: string]
  forceIdle: [sessionId: string]
}>()

function platformStatus(platformState: AgentRuntimePlatformState): {
  color: string
  icon: string
  label: string
} {
  if (platformState.connected) {
    return {
      color: 'success',
      icon: 'mdi-lan-connect',
      label: t('pages.sessions.connection.connected'),
    }
  }
  if (platformState.available) {
    return {
      color: 'info',
      icon: 'mdi-lan-pending',
      label: t('pages.sessions.connection.gracePeriod'),
    }
  }
  if (platformState.running) {
    return {
      color: 'warning',
      icon: 'mdi-lan-disconnect',
      label: t('pages.sessions.connection.disconnected'),
    }
  }
  return {
    color: 'grey',
    icon: 'mdi-stop-circle-outline',
    label: t('pages.sessions.connection.stopped'),
  }
}

function planningOutcome(decision: AgentRuntimeIdleReviewPlanningDecision): string {
  return decision.application?.outcome || decision.modelResult?.outcome || t('pages.agents.labels.pending')
}

function planningOutcomeColor(decision: AgentRuntimeIdleReviewPlanningDecision): string {
  const outcome = planningOutcome(decision)
  if (outcome === 'discarded') {
    return 'warning'
  }
  if (outcome.includes('fallback')) {
    return 'secondary'
  }
  if (outcome.includes('failed')) {
    return 'error'
  }
  if (outcome === 'applied_model_plan') {
    return 'success'
  }
  return 'info'
}

function planningModelDetail(decision: AgentRuntimeIdleReviewPlanningDecision): string {
  const result = decision.modelResult
  if (!result) {
    return t('pages.agents.labels.noValue')
  }
  return [result.outcome, result.reason, result.failureCode].filter(Boolean).join(' · ')
    || t('pages.agents.labels.noValue')
}

function planningApplicationDetail(decision: AgentRuntimeIdleReviewPlanningDecision): string {
  const application = decision.application
  if (!application) {
    return t('pages.agents.labels.pending')
  }
  return [
    application.outcome,
    application.decisionSkippedReason || application.reason,
  ].filter(Boolean).join(' · ') || t('pages.agents.labels.noValue')
}

function plannedReviewAt(decision: AgentRuntimeIdleReviewPlanningDecision): number | null {
  return decision.application?.appliedNextReviewAt
    ?? decision.application?.modelPlanNextReviewAt
    ?? decision.modelResult?.proposedNextReviewAt
    ?? null
}

function formatPlanningTimestamp(value: number | null): string {
  return value ? props.formatTimestamp(value) : t('pages.agents.labels.noValue')
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.runtime-card {
  @include surface-card;
}

.runtime-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 6px 0;

  span {
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
  }

  strong {
    min-width: 0;
    overflow: hidden;
    color: rgba(var(--v-theme-on-surface), 0.88);
    font-size: $font-size-xs;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

.session-actions {
  display: flex;
  gap: 8px;
  padding-top: 8px;
  border-top: 1px solid rgba(var(--v-border-color), 0.12);
}

.idle-review-planning {
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px solid rgba(var(--v-border-color), 0.12);
}

.planning-decision {
  margin-top: 8px;
  padding: 8px;
  border: 1px solid rgba(var(--v-border-color), 0.14);
  border-radius: 6px;
}

.planning-decision-header,
.planning-decision-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.planning-decision-header {
  align-items: center;
  color: rgba(var(--v-theme-on-surface), 0.88);
  font-size: $font-size-xs;
  font-weight: 600;
}

.planning-decision-row {
  padding-top: 6px;

  span {
    flex: 0 0 auto;
    color: rgba(var(--v-theme-on-surface), 0.58);
    font-size: $font-size-xs;
  }

  strong {
    min-width: 0;
    color: rgba(var(--v-theme-on-surface), 0.88);
    font-size: $font-size-xs;
    overflow-wrap: anywhere;
    text-align: right;
  }
}
</style>
