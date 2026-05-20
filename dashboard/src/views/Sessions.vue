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
        <session-detail-panel
          :session="selectedSession"
          :empty-label="$t('pages.sessions.empty.title')"
          :format-timestamp="formatTimestamp"
          :format-date-time="formatDateTime"
          :format-review-interval="formatReviewInterval"
          :bool-label="boolLabel"
          :format-summary="formatSummary"
          :routing-color="routingColor"
          :stringify-content="stringifyContent"
        />
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
import SessionDetailPanel from '@/components/sessions/SessionDetailPanel.vue'
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

const selectedSession = computed(() =>
  sessions.value.find((item) => item.session.id === selectedSessionId.value) || sessions.value[0] || null
)

onMounted(() => {
  void refresh()
})
</script>
