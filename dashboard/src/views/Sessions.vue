<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.sessions.title')"
      :subtitle="$t('pages.sessions.subtitle')"
      :kicker="$t('pages.sessions.kicker')"
    >
      <template #actions>
        <v-btn
          :color="selectionMode ? 'primary' : 'secondary'"
          variant="tonal"
          :prepend-icon="selectionMode ? 'mdi-close-circle-outline' : 'mdi-checkbox-multiple-marked-outline'"
          :disabled="isMutating"
          rounded="lg"
          class="me-2"
          @click="toggleSelectionMode"
        >
          {{
            selectionMode
              ? $t('pages.sessions.actions.cancelSelection')
              : $t('pages.sessions.actions.select')
          }}
        </v-btn>
        <v-chip
          v-if="selectionMode"
          color="primary"
          variant="tonal"
          class="me-2"
        >
          {{ $t('pages.sessions.labels.selectionCount', { count: selectedSessionIds.length }) }}
        </v-chip>
        <v-menu location="bottom end">
          <template #activator="{ props }">
            <v-btn
              v-bind="props"
              color="warning"
              variant="tonal"
              prepend-icon="mdi-database-cog-outline"
              :disabled="!hasActionableTargets || isMutating"
              :loading="isMutating"
              rounded="lg"
              class="me-2"
            >
              {{ $t('pages.sessions.actions.manage') }}
            </v-btn>
          </template>

          <v-list density="comfortable" min-width="220">
            <v-list-item
              prepend-icon="mdi-broom"
              :disabled="!hasActionableTargets || isMutating"
              @click="runSessionAction('history')"
            >
              <v-list-item-title>
                {{ $t('pages.sessions.actions.clearHistory') }}
              </v-list-item-title>
            </v-list-item>
            <v-list-item
              prepend-icon="mdi-shield-remove-outline"
              :disabled="!hasActionableTargets || isMutating"
              @click="runSessionAction('audit')"
            >
              <v-list-item-title>
                {{ $t('pages.sessions.actions.clearAuditLogs') }}
              </v-list-item-title>
            </v-list-item>
            <v-divider class="my-1" />
            <v-list-item
              prepend-icon="mdi-delete-outline"
              base-color="error"
              :disabled="!hasActionableTargets || isMutating"
              @click="runSessionAction('delete')"
            >
              <v-list-item-title>
                {{ $t('pages.sessions.actions.delete') }}
              </v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="loading && !isMutating"
          :disabled="isMutating"
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
          :selection-mode="selectionMode"
          :selected-ids="selectedSessionIds"
          :show-add-button="false"
          @select="selectedSessionId = $event"
          @toggle-select="toggleSelectedSession"
        />
      </template>

      <template #content>
        <session-detail-panel
          :session="selectedSession"
          :history-items="historyItems"
          :history-loading="historyLoading"
          :history-page="historyPage"
          :history-page-size="historyPageSize"
          :history-total="historyTotal"
          :history-has-next-page="historyHasNextPage"
          :empty-label="$t('pages.sessions.empty.title')"
          :format-timestamp="formatTimestamp"
          :format-date-time="formatDateTime"
          :format-review-interval="formatReviewInterval"
          :bool-label="boolLabel"
          :format-summary="formatSummary"
          :routing-color="routingColor"
          :stringify-content="stringifyContent"
          @update:history-page-size="updateHistoryPageSize"
          @previous-history-page="previousHistoryPage"
          @next-history-page="nextHistoryPage"
        />
      </template>
    </dual-pane-list-view>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useI18n } from 'vue-i18n'

import AppPageHeader from '@/components/AppPageHeader.vue'
import DualPaneListView from '@/components/DualPaneListView.vue'
import SidebarListCard from '@/components/SidebarListCard.vue'
import SessionDetailPanel from '@/components/sessions/SessionDetailPanel.vue'
import {
  sessionsApi,
  type SessionMessage,
  type SessionOverviewItem,
  type SessionPlatformState,
  type SessionSummary,
} from '@/api/sessions'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useFormatters } from '@/composables/useFormatters'
import { useUiStore } from '@/stores/ui'
import { useSystemSettingsStore } from '@/stores/systemSettings'
import { getErrorMessage } from '@/utils/error'
import { normalizeTimestampMs } from '@/utils/time'

type SessionActionKind = 'history' | 'audit' | 'delete'

const systemSettingsStore = useSystemSettingsStore()
const uiStore = useUiStore()
const { locale, t } = useI18n()
const { pricingCurrency } = storeToRefs(systemSettingsStore)
const { confirm } = useConfirmDialog()
const loading = ref(false)
const pendingAction = ref<SessionActionKind | ''>('')
const error = ref('')
const sessions = ref<SessionOverviewItem[]>([])
const selectedSessionId = ref('')
const selectionMode = ref(false)
const selectedSessionIds = ref<string[]>([])
const historyLoading = ref(false)
const historyItems = ref<SessionMessage[]>([])
const historyPage = ref(1)
const historyPageSize = ref(10)
const historyTotal = ref(0)

const displayCurrency = computed(() => pricingCurrency.value || 'CNY')
const { formatDateTime } = useFormatters(locale, displayCurrency)

const initialSkeletonRequested = computed(() => loading.value && sessions.value.length === 0)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)
const isMutating = computed(() => pendingAction.value !== '')
const actionableSessionIds = computed(() =>
  selectionMode.value
    ? selectedSessionIds.value.filter((session_id) =>
        sessions.value.some((item) => item.session.id === session_id)
      )
    : selectedSession.value
      ? [selectedSession.value.session.id]
      : []
)
const hasActionableTargets = computed(() => actionableSessionIds.value.length > 0)
const historyHasNextPage = computed(
  () => historyPage.value * historyPageSize.value < historyTotal.value
)

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
  if (typeof value === 'number') {
    const normalized = normalizeTimestampMs(value)
    if (normalized === null) {
      return t('pages.sessions.labels.none')
    }
    return formatDateTime(new Date(normalized).toISOString())
  }
  return formatDateTime(value)
}

const formatReviewInterval = (value: number | null | undefined) => {
  if (!value) {
    return t('pages.sessions.labels.none')
  }
  const normalized = normalizeTimestampMs(value)
  if (normalized === null) {
    return t('pages.sessions.labels.none')
  }
  const diffMs = normalized - Date.now()
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

const platformStatus = (platformState: SessionPlatformState) => {
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

const sidebarItems = computed(() =>
  sessions.value.map((item) => {
    const status = platformStatus(item.platformState)
    return {
      id: item.session.id,
      title: item.session.displayName || item.session.id,
      subtitle: `${item.session.platform || item.session.sessionType} · ${item.session.instanceId}`,
      icon: 'mdi-forum-outline',
      statusLabel: status.label,
      statusColor: status.color,
      statusIcon: status.icon,
      badge: item.messageCount,
      badgeColor: item.agent?.state === 'active_chat' ? 'success' : 'primary',
    }
  })
)

const filteredSessions = computed(() => sessions.value)

const refresh = async () => {
  loading.value = true
  error.value = ''
  try {
    const resp = await sessionsApi.overview(historyPageSize.value)
    sessions.value = resp.data.data || []
    selectedSessionIds.value = selectedSessionIds.value.filter((session_id) =>
      sessions.value.some((item) => item.session.id === session_id)
    )
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

const loadHistory = async (sessionId: string, page = historyPage.value) => {
  historyLoading.value = true
  try {
    const resp = await sessionsApi.history(sessionId, page, historyPageSize.value)
    const payload = resp.data.data
    historyItems.value = payload?.items || []
    historyTotal.value = payload?.total || 0
    historyPage.value = payload?.page || page
  } finally {
    historyLoading.value = false
  }
}

const selectedSession = computed(() =>
  sessions.value.find((item) => item.session.id === selectedSessionId.value) || sessions.value[0] || null
)

const previousHistoryPage = async () => {
  if (!selectedSession.value || historyPage.value <= 1 || historyLoading.value) {
    return
  }
  await loadHistory(selectedSession.value.session.id, historyPage.value - 1)
}

const nextHistoryPage = async () => {
  if (!selectedSession.value || !historyHasNextPage.value || historyLoading.value) {
    return
  }
  await loadHistory(selectedSession.value.session.id, historyPage.value + 1)
}

const updateHistoryPageSize = async (value: number) => {
  if (!Number.isFinite(value) || historyPageSize.value === value) {
    return
  }
  historyPageSize.value = value
  historyPage.value = 1
  if (selectedSession.value) {
    await loadHistory(selectedSession.value.session.id, 1)
  }
}

const toggleSelectionMode = () => {
  if (selectionMode.value) {
    selectionMode.value = false
    selectedSessionIds.value = []
    return
  }
  selectionMode.value = true
  selectedSessionIds.value = selectedSession.value ? [selectedSession.value.session.id] : []
}

const toggleSelectedSession = (sessionId: string) => {
  const items = new Set(selectedSessionIds.value)
  if (items.has(sessionId)) {
    items.delete(sessionId)
  } else {
    items.add(sessionId)
  }
  selectedSessionIds.value = Array.from(items)
}

const targetLabelForSessionIds = (sessionIds: string[]) => {
  if (sessionIds.length !== 1) {
    return t('pages.sessions.labels.selectionSummary', { count: sessionIds.length })
  }
  return (
    sessions.value.find((item) => item.session.id === sessionIds[0])?.session.displayName ||
    sessions.value.find((item) => item.session.id === sessionIds[0])?.session.id ||
    sessionIds[0]
  )
}

const sessionActionConfig = (action: SessionActionKind) => {
  switch (action) {
    case 'history':
      return {
        title: t('pages.sessions.confirmClearHistory.title'),
        messageKey: 'pages.sessions.confirmClearHistory.message',
        batchTitle: t('pages.sessions.confirmClearHistoryBatch.title'),
        batchMessageKey: 'pages.sessions.confirmClearHistoryBatch.message',
        confirmText: t('pages.sessions.confirmClearHistory.confirm'),
        confirmColor: 'warning',
        icon: 'mdi-broom',
        iconColor: 'warning',
        successMessage: t('pages.sessions.messages.historyCleared'),
        batchSuccessKey: 'pages.sessions.messages.historyBatchCleared',
        failureMessage: t('pages.sessions.messages.historyClearFailed'),
        batchFailureMessage: t('pages.sessions.messages.historyBatchClearFailed'),
      }
    case 'audit':
      return {
        title: t('pages.sessions.confirmClearAudit.title'),
        messageKey: 'pages.sessions.confirmClearAudit.message',
        batchTitle: t('pages.sessions.confirmClearAuditBatch.title'),
        batchMessageKey: 'pages.sessions.confirmClearAuditBatch.message',
        confirmText: t('pages.sessions.confirmClearAudit.confirm'),
        confirmColor: 'warning',
        icon: 'mdi-shield-remove-outline',
        iconColor: 'warning',
        successMessage: t('pages.sessions.messages.auditCleared'),
        batchSuccessKey: 'pages.sessions.messages.auditBatchCleared',
        failureMessage: t('pages.sessions.messages.auditClearFailed'),
        batchFailureMessage: t('pages.sessions.messages.auditBatchClearFailed'),
      }
    case 'delete':
      return {
        title: t('pages.sessions.confirmDelete.title'),
        messageKey: 'pages.sessions.confirmDelete.message',
        batchTitle: t('pages.sessions.confirmDeleteBatch.title'),
        batchMessageKey: 'pages.sessions.confirmDeleteBatch.message',
        confirmText: t('pages.sessions.confirmDelete.confirm'),
        confirmColor: 'error',
        icon: 'mdi-delete-alert-outline',
        iconColor: 'error',
        successMessage: t('pages.sessions.messages.deleted'),
        batchSuccessKey: 'pages.sessions.messages.batchDeleted',
        failureMessage: t('pages.sessions.messages.deleteFailed'),
        batchFailureMessage: t('pages.sessions.messages.batchDeleteFailed'),
      }
  }
}

const runSessionAction = async (action: SessionActionKind) => {
  const targetIds = actionableSessionIds.value
  if (targetIds.length === 0 || pendingAction.value) {
    return
  }

  const config = sessionActionConfig(action)
  const batchMode = selectionMode.value && targetIds.length > 1
  const targetLabel = targetLabelForSessionIds(targetIds)
  const confirmed = await confirm({
    title: batchMode ? config.batchTitle : config.title,
    message: t(batchMode ? config.batchMessageKey : config.messageKey, {
      count: targetIds.length,
      name: targetLabel,
    }),
    confirmText: config.confirmText,
    cancelText: t('common.actions.action.cancel'),
    confirmColor: config.confirmColor,
    icon: config.icon,
    iconColor: config.iconColor,
  })
  if (!confirmed) {
    return
  }

  pendingAction.value = action
  error.value = ''
  try {
    let processedCount = targetIds.length
    if (batchMode) {
      let response
      if (action === 'history') {
        response = await sessionsApi.clearHistoryBatch(targetIds)
      } else if (action === 'audit') {
        response = await sessionsApi.clearAuditLogsBatch(targetIds)
      } else {
        response = await sessionsApi.deleteBatch(targetIds)
      }
      processedCount = response.data.data?.processedCount || targetIds.length
    } else {
      const sessionId = targetIds[0]
      if (action === 'history') {
        await sessionsApi.clearHistory(sessionId)
      } else if (action === 'audit') {
        await sessionsApi.clearAuditLogs(sessionId)
      } else {
        await sessionsApi.delete(sessionId)
      }
    }
    uiStore.showSnackbar(
      batchMode
        ? t(config.batchSuccessKey, { count: processedCount })
        : config.successMessage,
      'success'
    )
    if (action === 'delete') {
      selectedSessionIds.value = selectedSessionIds.value.filter(
        (session_id) => !targetIds.includes(session_id)
      )
    }
    await refresh()
  } catch (err) {
    error.value = getErrorMessage(
      err,
      batchMode ? config.batchFailureMessage : config.failureMessage
    )
  } finally {
    pendingAction.value = ''
  }
}

onMounted(() => {
  void refresh()
})

watch(
  selectedSession,
  (session) => {
    historyPage.value = 1
    historyItems.value = session?.history || []
    historyTotal.value = session?.messageCount || 0
    if (session) {
      void loadHistory(session.session.id, 1)
    }
  },
  { immediate: true }
)
</script>
