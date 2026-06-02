<template>
  <template v-if="session">
    <v-card class="sessions-panel mb-6" elevation="0">
      <v-card-item>
        <template #prepend>
          <v-avatar color="primary" variant="tonal" icon="mdi-forum-outline" />
        </template>
        <v-card-title class="text-break">
          {{ session.session.displayName || session.session.id }}
        </v-card-title>
        <v-card-subtitle class="text-break">
          {{ session.session.id }} · {{ session.session.platform || session.session.sessionType }}
        </v-card-subtitle>
        <template #append>
          <div class="session-title-chips">
            <v-chip
              size="small"
              variant="tonal"
              :color="platformStatus(session.platformState).color"
            >
              <v-icon :icon="platformStatus(session.platformState).icon" start />
              {{ platformStatus(session.platformState).label }}
            </v-chip>
            <v-chip size="small" variant="tonal" color="info">
              {{ session.agent?.state || noneLabel }}
            </v-chip>
          </div>
        </template>
      </v-card-item>

      <v-card-text class="pt-1">
        <v-row>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ instanceIdLabel }}</span>
              <strong>{{ session.session.instanceId }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ channelIdLabel }}</span>
              <strong>{{ session.session.channelId || noneLabel }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ lastActiveLabel }}</span>
              <strong>{{ formatTimestamp(session.session.lastActive) }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ platformStatusLabel }}</span>
              <strong>{{ platformStatus(session.platformState).label }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ permissionGroupLabel }}</span>
              <strong>{{ session.session.permissionGroup }}</strong>
            </div>
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-row class="mx-0 mb-6">
      <v-col cols="12" md="3" class="pa-2">
        <v-card class="session-stat-card" elevation="0">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ messagesLabel }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ session.messageCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="3" class="pa-2">
        <v-card class="session-stat-card" elevation="0">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ auditsLabel }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ session.auditCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="3" class="pa-2">
        <v-card class="session-stat-card" elevation="0">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ unreadLabel }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ session.agent?.unreadCount ?? 0 }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="3" class="pa-2">
        <v-card class="session-stat-card" elevation="0">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ priorityLabel }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ session.agent?.highPriorityCount ?? 0 }}</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-card class="sessions-panel mb-6" elevation="0">
      <v-card-title class="text-subtitle-1 font-weight-bold">
        {{ configLabel }}
      </v-card-title>
      <v-card-text>
        <v-row>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ prefixesLabel }}</span>
              <strong>{{ (session.config?.prefixes || []).join(' ') || noneLabel }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ llmEnabledLabel }}</span>
              <strong>{{ boolLabel(session.config?.llmEnabled) }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ mutedLabel }}</span>
              <strong>{{ boolLabel(session.config?.isMuted) }}</strong>
            </div>
          </v-col>
          <v-col cols="12" md="3">
            <div class="session-meta-row">
              <span>{{ auditEnabledLabel }}</span>
              <strong>{{ boolLabel(session.config?.auditEnabled) }}</strong>
            </div>
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-row class="mx-0">
      <v-col cols="12" lg="7" class="pa-2">
        <v-card class="sessions-panel h-100" elevation="0">
          <v-card-title class="text-subtitle-1 font-weight-bold">
            {{ historyLabel }}
          </v-card-title>
          <v-card-text>
            <div
              v-if="session.history.length === 0"
              class="text-body-2 text-medium-emphasis py-6 text-center"
            >
              {{ emptyHistoryLabel }}
            </div>
            <div v-else class="session-message-list">
              <div
                v-for="message in session.history"
                :key="message.id"
                class="session-message-row"
              >
                <div class="session-message-row__head">
                  <strong>{{
                    message.senderName || message.senderId || unknownSenderLabel
                  }}</strong>
                  <span>{{ formatTimestamp(message.createdAt) }}</span>
                </div>
                <div class="session-message-row__meta">
                  <v-chip size="x-small" variant="tonal">{{ message.role }}</v-chip>
                  <v-chip
                    size="x-small"
                    variant="tonal"
                    :color="routingColor(message.routingStatus)"
                  >
                    {{ message.routingStatus }}
                  </v-chip>
                  <v-chip
                    v-if="message.isMentioned"
                    size="x-small"
                    variant="tonal"
                    color="warning"
                  >
                    {{ mentionedLabel }}
                  </v-chip>
                </div>
                <div class="session-message-row__body">
                  {{ message.rawText || stringifyContent(message.content) }}
                </div>
              </div>
            </div>
          </v-card-text>
        </v-card>
      </v-col>

      <v-col cols="12" lg="5" class="pa-2">
        <v-card class="sessions-panel mb-4" elevation="0">
          <v-card-title class="text-subtitle-1 font-weight-bold">
            {{ agentLabel }}
          </v-card-title>
          <v-card-text class="pt-1">
            <div class="session-meta-row">
              <span>{{ reviewAtLabel }}</span>
              <strong>{{
                session.agent?.reviewPlan?.nextReviewAt
                  ? formatTimestamp(session.agent.reviewPlan.nextReviewAt)
                  : noneLabel
              }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ reviewIntervalLabel }}</span>
              <strong>{{ formatReviewInterval(session.agent?.reviewPlan?.nextReviewAt) }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ reviewReasonLabel }}</span>
              <strong>{{ session.agent?.reviewPlan?.reason || noneLabel }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ activeChatLabel }}</span>
              <strong>{{
                session.agent?.activeChatState
                  ? `${session.agent.activeChatState.interestValue.toFixed(1)} / ${session.agent.activeChatState.tickCount}`
                  : noneLabel
              }}</strong>
            </div>
          </v-card-text>
        </v-card>

        <v-card class="sessions-panel mb-4" elevation="0">
          <v-card-title class="text-subtitle-1 font-weight-bold">
            {{ reviewLabel }}
          </v-card-title>
          <v-card-text class="pt-1">
            <div class="session-meta-row">
              <span>{{ latestReviewLabel }}</span>
              <strong>{{ formatSummary(session.latestReviewSummary) }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ activeChatSummaryLabel }}</span>
              <strong>{{ formatSummary(session.latestActiveChatSummary) }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ overflowSummaryLabel }}</span>
              <strong>{{ formatSummary(session.latestOverflowSummary) }}</strong>
            </div>
          </v-card-text>
        </v-card>

        <v-card class="sessions-panel" elevation="0">
          <v-card-title class="text-subtitle-1 font-weight-bold">
            {{ workflowLabel }}
          </v-card-title>
          <v-card-text class="pt-1">
            <div class="session-meta-row">
              <span>{{ workflowRunLabel }}</span>
              <strong>{{ session.latestWorkflowRun?.id || noneLabel }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ workflowProfileLabel }}</span>
              <strong>{{ session.latestWorkflowRun?.responseProfile || noneLabel }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ workflowResultLabel }}</span>
              <strong>{{
                session.latestWorkflowRun?.responseSummary ||
                session.latestWorkflowRun?.finishReason ||
                noneLabel
              }}</strong>
            </div>
            <div class="session-meta-row">
              <span>{{ lastAuditLabel }}</span>
              <strong>{{
                session.latestAudit ? formatDateTime(session.latestAudit.timestamp) : noneLabel
              }}</strong>
            </div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </template>

  <v-card v-else class="sessions-panel pa-8" elevation="0">
    <div class="text-body-2 text-medium-emphasis text-center">
      {{ emptyLabel }}
    </div>
  </v-card>
</template>

<script setup lang="ts">
import { useI18n } from 'vue-i18n'
import type {
  SessionOverviewItem,
  SessionPlatformState,
  SessionSummary,
} from '@/api/sessions'

const { t } = useI18n()

defineProps<{
  session: SessionOverviewItem | null
  emptyLabel: string
  formatTimestamp: (value: number | string | null | undefined) => string
  formatDateTime: (value: string | null | undefined) => string
  formatReviewInterval: (value: number | null | undefined) => string
  boolLabel: (value: boolean | null | undefined) => string
  formatSummary: (summary: SessionSummary | null) => string
  routingColor: (status: string) => string
  stringifyContent: (content: unknown[]) => string
}>()

const noneLabel = t('pages.sessions.labels.none')
const unknownSenderLabel = t('pages.sessions.labels.unknownSender')
const mentionedLabel = t('pages.sessions.labels.mentioned')

const instanceIdLabel = t('pages.sessions.fields.instanceId')
const channelIdLabel = t('pages.sessions.fields.channelId')
const lastActiveLabel = t('pages.sessions.fields.lastActive')
const platformStatusLabel = t('pages.sessions.fields.platformStatus')
const permissionGroupLabel = t('pages.sessions.fields.permissionGroup')
const messagesLabel = t('pages.sessions.stats.messages')
const auditsLabel = t('pages.sessions.stats.audits')
const unreadLabel = t('pages.sessions.stats.unread')
const priorityLabel = t('pages.sessions.stats.highPriority')
const configLabel = t('pages.sessions.sections.config')
const prefixesLabel = t('pages.sessions.fields.prefixes')
const llmEnabledLabel = t('pages.sessions.fields.llmEnabled')
const mutedLabel = t('pages.sessions.fields.muted')
const auditEnabledLabel = t('pages.sessions.fields.auditEnabled')
const historyLabel = t('pages.sessions.sections.history')
const emptyHistoryLabel = t('pages.sessions.empty.history')
const agentLabel = t('pages.sessions.sections.agent')
const reviewAtLabel = t('pages.sessions.fields.reviewAt')
const reviewIntervalLabel = t('pages.sessions.fields.reviewInterval')
const reviewReasonLabel = t('pages.sessions.fields.reviewReason')
const activeChatLabel = t('pages.sessions.fields.activeChat')
const reviewLabel = t('pages.sessions.sections.review')
const latestReviewLabel = t('pages.sessions.fields.latestReview')
const activeChatSummaryLabel = t('pages.sessions.fields.activeChatSummary')
const overflowSummaryLabel = t('pages.sessions.fields.overflowSummary')
const workflowLabel = t('pages.sessions.sections.workflow')
const workflowRunLabel = t('pages.sessions.fields.workflowRun')
const workflowProfileLabel = t('pages.sessions.fields.workflowProfile')
const workflowResultLabel = t('pages.sessions.fields.workflowResult')
const lastAuditLabel = t('pages.sessions.fields.lastAudit')

function platformStatus(platformState: SessionPlatformState): {
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
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.session-title-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  justify-content: flex-end;
}

.sessions-panel,
.session-stat-card {
  @include surface-card;
}

.session-meta-row {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 0;
}

.session-meta-row span {
  color: rgba(var(--v-theme-on-surface), 0.6);
  font-size: 0.78rem;
}

.session-message-list {
  display: grid;
  gap: 12px;
}

.session-message-row {
  padding: 12px 14px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.7);
}

.session-message-row__head,
.session-message-row__meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.session-message-row__head {
  justify-content: space-between;
  margin-bottom: 8px;
}

.session-message-row__head span {
  color: rgba(var(--v-theme-on-surface), 0.52);
  font-size: 0.8rem;
}

.session-message-row__body {
  margin-top: 8px;
  white-space: pre-wrap;
  word-break: break-word;
  color: rgba(var(--v-theme-on-surface), 0.84);
}

@media (max-width: 960px) {
  .session-meta-row span {
    font-size: 0.75rem;
  }
}
</style>
