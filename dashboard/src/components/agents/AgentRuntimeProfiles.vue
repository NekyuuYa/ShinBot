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
              </v-expansion-panel-text>
            </v-expansion-panel>
          </v-expansion-panels>
        </v-card-text>
      </v-card>
    </v-col>
  </v-row>
</template>

<script setup lang="ts">
import type { AgentRuntimeProfile } from '@/api/agents'

defineProps<{
  profiles: AgentRuntimeProfile[]
  bindingsLabel: string
  sessionsLabel: string
  reviewLabel: string
  reviewIntervalLabel: string
  unreadLabel: string
  activeChatLabel: string
  lastReviewLabel: string
  reviewNoteLabel: string
  lastAuditLabel: string
  noValueLabel: string
  formatTimestamp: (value: number) => string
  formatReviewInterval: (value: number | null | undefined) => string
}>()
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
</style>
