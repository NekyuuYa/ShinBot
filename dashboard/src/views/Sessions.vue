<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.sessions.title')"
      :subtitle="$t('pages.sessions.subtitle')"
      :kicker="$t('pages.sessions.kicker')"
    >
      <template #actions>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="loading"
          rounded="lg"
          @click="refresh"
        >
          {{ $t('pages.sessions.actions.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert
      v-if="error"
      type="error"
      variant="tonal"
      density="comfortable"
      class="mb-6"
    >
      {{ error }}
    </v-alert>

    <dual-pane-list-view
      :items="filteredSessions"
      :loading="loading"
      :show-skeleton="showInitialSkeleton"
      sidebar-width="340px"
      content-class="sessions-content"
      :empty-config="{
        icon: 'mdi-message-text-outline',
        title: $t('pages.sessions.empty.title'),
        subtitle: $t('pages.sessions.empty.subtitle'),
      }"
      :get-item-key="(item) => item.session.id"
    >
      <template #sidebar>
        <sidebar-list-card
          :title="$t('pages.sessions.sidebar.title')"
          :empty-text="$t('pages.sessions.sidebar.empty')"
          :items="sidebarItems"
          :active-id="selectedSessionId"
          :show-add-button="false"
          @select="selectedSessionId = $event"
        />
      </template>

      <template #content>
        <template v-if="selectedSession">
          <v-card class="sessions-panel mb-6" elevation="0">
            <v-card-item>
              <template #prepend>
                <v-avatar color="primary" variant="tonal" icon="mdi-forum-outline" />
              </template>
              <v-card-title class="text-break">{{ selectedSession.session.displayName || selectedSession.session.id }}</v-card-title>
              <v-card-subtitle class="text-break">
                {{ selectedSession.session.id }} · {{ selectedSession.session.platform || selectedSession.session.sessionType }}
              </v-card-subtitle>
              <template #append>
                <v-chip size="small" variant="tonal" color="info">
                  {{ selectedSession.agent?.state || $t('pages.sessions.labels.noState') }}
                </v-chip>
              </template>
            </v-card-item>

            <v-card-text class="pt-1">
              <v-row>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.instanceId') }}</span>
                    <strong>{{ selectedSession.session.instanceId }}</strong>
                  </div>
                </v-col>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.channelId') }}</span>
                    <strong>{{ selectedSession.session.channelId || $t('pages.sessions.labels.none') }}</strong>
                  </div>
                </v-col>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.lastActive') }}</span>
                    <strong>{{ formatTimestamp(selectedSession.session.lastActive) }}</strong>
                  </div>
                </v-col>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.permissionGroup') }}</span>
                    <strong>{{ selectedSession.session.permissionGroup }}</strong>
                  </div>
                </v-col>
              </v-row>
            </v-card-text>
          </v-card>

          <v-row class="mx-0 mb-6">
            <v-col cols="12" md="3" class="pa-2">
              <v-card class="session-stat-card" elevation="0">
                <v-card-text>
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.sessions.stats.messages') }}</div>
                  <div class="text-h4 font-weight-black mt-2">{{ selectedSession.messageCount }}</div>
                </v-card-text>
              </v-card>
            </v-col>
            <v-col cols="12" md="3" class="pa-2">
              <v-card class="session-stat-card" elevation="0">
                <v-card-text>
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.sessions.stats.audits') }}</div>
                  <div class="text-h4 font-weight-black mt-2">{{ selectedSession.auditCount }}</div>
                </v-card-text>
              </v-card>
            </v-col>
            <v-col cols="12" md="3" class="pa-2">
              <v-card class="session-stat-card" elevation="0">
                <v-card-text>
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.sessions.stats.unread') }}</div>
                  <div class="text-h4 font-weight-black mt-2">{{ selectedSession.agent?.unreadCount ?? 0 }}</div>
                </v-card-text>
              </v-card>
            </v-col>
            <v-col cols="12" md="3" class="pa-2">
              <v-card class="session-stat-card" elevation="0">
                <v-card-text>
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.sessions.stats.highPriority') }}</div>
                  <div class="text-h4 font-weight-black mt-2">{{ selectedSession.agent?.highPriorityCount ?? 0 }}</div>
                </v-card-text>
              </v-card>
            </v-col>
          </v-row>

          <v-card class="sessions-panel mb-6" elevation="0">
            <v-card-title class="text-subtitle-1 font-weight-bold">
              {{ $t('pages.sessions.sections.config') }}
            </v-card-title>
            <v-card-text>
              <v-row>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.prefixes') }}</span>
                    <strong>{{ (selectedSession.config?.prefixes || []).join(' ') || $t('pages.sessions.labels.none') }}</strong>
                  </div>
                </v-col>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.llmEnabled') }}</span>
                    <strong>{{ boolLabel(selectedSession.config?.llmEnabled) }}</strong>
                  </div>
                </v-col>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.muted') }}</span>
                    <strong>{{ boolLabel(selectedSession.config?.isMuted) }}</strong>
                  </div>
                </v-col>
                <v-col cols="12" md="3">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.auditEnabled') }}</span>
                    <strong>{{ boolLabel(selectedSession.config?.auditEnabled) }}</strong>
                  </div>
                </v-col>
              </v-row>
            </v-card-text>
          </v-card>

          <v-row class="mx-0">
            <v-col cols="12" lg="7" class="pa-2">
              <v-card class="sessions-panel h-100" elevation="0">
                <v-card-title class="text-subtitle-1 font-weight-bold">
                  {{ $t('pages.sessions.sections.history') }}
                </v-card-title>
                <v-card-text>
                  <div v-if="selectedSession.history.length === 0" class="text-body-2 text-medium-emphasis py-6 text-center">
                    {{ $t('pages.sessions.empty.history') }}
                  </div>
                  <div v-else class="session-message-list">
                    <div v-for="message in selectedSession.history" :key="message.id" class="session-message-row">
                      <div class="session-message-row__head">
                        <strong>{{ message.senderName || message.senderId || $t('pages.sessions.labels.unknownSender') }}</strong>
                        <span>{{ formatTimestamp(message.createdAt) }}</span>
                      </div>
                      <div class="session-message-row__meta">
                        <v-chip size="x-small" variant="tonal">{{ message.role }}</v-chip>
                        <v-chip size="x-small" variant="tonal" :color="routingColor(message.routingStatus)">{{ message.routingStatus }}</v-chip>
                        <v-chip v-if="message.isMentioned" size="x-small" variant="tonal" color="warning">{{ $t('pages.sessions.labels.mentioned') }}</v-chip>
                      </div>
                      <div class="session-message-row__body">{{ message.rawText || stringifyContent(message.content) }}</div>
                    </div>
                  </div>
                </v-card-text>
              </v-card>
            </v-col>

            <v-col cols="12" lg="5" class="pa-2">
              <v-card class="sessions-panel mb-4" elevation="0">
                <v-card-title class="text-subtitle-1 font-weight-bold">
                  {{ $t('pages.sessions.sections.agent') }}
                </v-card-title>
                <v-card-text class="pt-1">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.reviewAt') }}</span>
                    <strong>{{ selectedSession.agent?.reviewPlan?.nextReviewAt ? formatTimestamp(selectedSession.agent.reviewPlan.nextReviewAt) : t('pages.sessions.labels.none') }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.reviewInterval') }}</span>
                    <strong>{{ formatReviewInterval(selectedSession.agent?.reviewPlan?.nextReviewAt) }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.reviewReason') }}</span>
                    <strong>{{ selectedSession.agent?.reviewPlan?.reason || $t('pages.sessions.labels.none') }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.activeChat') }}</span>
                    <strong>{{ selectedSession.agent?.activeChatState ? `${selectedSession.agent.activeChatState.interestValue.toFixed(1)} / ${selectedSession.agent.activeChatState.tickCount}` : $t('pages.sessions.labels.none') }}</strong>
                  </div>
                </v-card-text>
              </v-card>

              <v-card class="sessions-panel mb-4" elevation="0">
                <v-card-title class="text-subtitle-1 font-weight-bold">
                  {{ $t('pages.sessions.sections.review') }}
                </v-card-title>
                <v-card-text class="pt-1">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.latestReview') }}</span>
                    <strong>{{ formatSummary(selectedSession.latestReviewSummary) }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.activeChatSummary') }}</span>
                    <strong>{{ formatSummary(selectedSession.latestActiveChatSummary) }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.overflowSummary') }}</span>
                    <strong>{{ formatSummary(selectedSession.latestOverflowSummary) }}</strong>
                  </div>
                </v-card-text>
              </v-card>

              <v-card class="sessions-panel" elevation="0">
                <v-card-title class="text-subtitle-1 font-weight-bold">
                  {{ $t('pages.sessions.sections.workflow') }}
                </v-card-title>
                <v-card-text class="pt-1">
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.workflowRun') }}</span>
                    <strong>{{ selectedSession.latestWorkflowRun?.id || $t('pages.sessions.labels.none') }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.workflowProfile') }}</span>
                    <strong>{{ selectedSession.latestWorkflowRun?.responseProfile || $t('pages.sessions.labels.none') }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.workflowResult') }}</span>
                    <strong>{{ selectedSession.latestWorkflowRun?.responseSummary || selectedSession.latestWorkflowRun?.finishReason || $t('pages.sessions.labels.none') }}</strong>
                  </div>
                  <div class="session-meta-row">
                    <span>{{ $t('pages.sessions.fields.lastAudit') }}</span>
                    <strong>{{ selectedSession.latestAudit ? formatDateTime(selectedSession.latestAudit.timestamp) : t('pages.sessions.labels.none') }}</strong>
                  </div>
                </v-card-text>
              </v-card>
            </v-col>
          </v-row>
        </template>
      </template>
    </dual-pane-list-view>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useI18n } from 'vue-i18n'

import AppPageHeader from '@/components/AppPageHeader.vue'
import DualPaneListView from '@/components/DualPaneListView.vue'
import SidebarListCard from '@/components/SidebarListCard.vue'
import { sessionsApi, type SessionOverviewItem, type SessionSummary } from '@/api/sessions'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useFormatters } from '@/composables/useFormatters'
import { useSystemSettingsStore } from '@/stores/systemSettings'

const systemSettingsStore = useSystemSettingsStore()
const { locale, t } = useI18n()
const { pricingCurrency } = storeToRefs(systemSettingsStore)
const loading = ref(false)
const error = ref('')
const sessions = ref<SessionOverviewItem[]>([])
const selectedSessionId = ref('')

const displayCurrency = computed(() => pricingCurrency.value || 'CNY')
const { formatDateTime } = useFormatters(locale, displayCurrency)

const initialSkeletonRequested = computed(() => loading.value && sessions.value.length === 0)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const boolLabel = (value: boolean | null | undefined) =>
  value
    ? t('common.actions.status.enabled')
    : t('common.actions.status.disabled')

const stringifyContent = (content: unknown[]) => {
  try {
    return JSON.stringify(content)
  } catch {
    return ''
  }
}

const formatSummary = (summary: SessionSummary | null) => {
  if (!summary) {
    return t('pages.sessions.labels.none')
  }
  return summary.summary || summary.reason || t('pages.sessions.labels.none')
}

const formatTimestamp = (value: number | string | null | undefined) => {
  if (value === null || value === undefined) {
    return t('pages.sessions.labels.none')
  }
  return formatDateTime(new Date(value).toISOString())
}

const formatReviewInterval = (value: number | null | undefined) => {
  if (!value) {
    return t('pages.sessions.labels.none')
  }
  const diffMs = value - Date.now()
  if (diffMs <= 0) {
    return t('pages.sessions.labels.reviewDue')
  }
  const totalMinutes = Math.max(1, Math.round(diffMs / 60_000))
  if (totalMinutes < 60) {
    return t('pages.sessions.labels.reviewInMinutes', { count: totalMinutes })
  }
  const totalHours = Math.round(totalMinutes / 60)
  if (totalHours < 48) {
    return t('pages.sessions.labels.reviewInHours', { count: totalHours })
  }
  const totalDays = Math.round(totalHours / 24)
  return t('pages.sessions.labels.reviewInDays', { count: totalDays })
}

const routingColor = (status: string) => {
  if (status === 'routed' || status === 'done') return 'success'
  if (status === 'failed') return 'error'
  if (status === 'skipped') return 'grey'
  return 'info'
}

const sidebarItems = computed(() =>
  sessions.value.map((item) => ({
    id: item.session.id,
    title: item.session.displayName || item.session.id,
    subtitle: `${item.session.platform || item.session.sessionType} · ${item.session.instanceId}`,
    icon: 'mdi-forum-outline',
    badge: item.messageCount,
    badgeColor: item.agent?.state === 'active_chat' ? 'success' : 'primary',
  }))
)

const filteredSessions = computed(() => sessions.value)

const refresh = async () => {
  loading.value = true
  error.value = ''
  try {
    const resp = await sessionsApi.overview()
    sessions.value = resp.data.data || []
    if (!selectedSessionId.value && sessions.value.length > 0) {
      selectedSessionId.value = sessions.value[0].session.id
    }
    if (selectedSessionId.value && !sessions.value.some((item) => item.session.id === selectedSessionId.value)) {
      selectedSessionId.value = sessions.value[0]?.session.id || ''
    }
  } catch (err) {
    error.value = err instanceof Error ? err.message : t('pages.sessions.messages.loadFailed')
  } finally {
    loading.value = false
  }
}

const selectedSessionItem = computed(() =>
  sessions.value.find((item) => item.session.id === selectedSessionId.value) || sessions.value[0] || null
)

const selectedSession = selectedSessionItem

onMounted(() => {
  void refresh()
})
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.sessions-content {
  min-height: 0;
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
</style>
