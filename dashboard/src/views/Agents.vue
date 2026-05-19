<template>
  <v-container fluid class="pa-0 agents-page">
    <app-page-header
      :title="$t('pages.agents.title')"
      :subtitle="$t('pages.agents.subtitle')"
      :kicker="$t('pages.agents.kicker')"
    >
      <template #actions>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="isLoading"
          rounded="lg"
          @click="refreshPage"
        >
          {{ $t("pages.agents.actions.refresh") }}
        </v-btn>
        <v-btn
          color="primary"
          prepend-icon="mdi-account-plus"
          rounded="lg"
          @click="openCreate"
        >
          {{ $t("pages.agents.actions.addAgent") }}
        </v-btn>
      </template>
    </app-page-header>

    <v-alert
      v-if="error || configStore.error"
      type="error"
      variant="tonal"
      density="comfortable"
      class="mb-6"
    >
      {{ error || configStore.error }}
    </v-alert>

    <v-row v-if="runtimeProfiles.length > 0" class="mb-6">
      <v-col
        v-for="profile in runtimeProfiles"
        :key="profile.botId"
        cols="12"
        lg="6"
      >
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
              <span>Bindings</span>
              <strong>{{ profile.bindings.length }}</strong>
            </div>
            <div class="runtime-meta-row">
              <span>Sessions</span>
              <strong>{{ profile.sessions.length }}</strong>
            </div>
            <v-expansion-panels class="mt-3" variant="accordion">
              <v-expansion-panel
                v-for="session in profile.sessions"
                :key="session.sessionId"
              >
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
                    <span>Review</span>
                    <strong>{{
                      session.reviewPlan?.nextReviewAt
                        ? formatTimestamp(session.reviewPlan.nextReviewAt)
                        : $t("pages.agents.labels.noValue")
                    }}</strong>
                  </div>
                  <div class="runtime-meta-row">
                    <span>{{ $t("pages.agents.labels.reviewInterval") }}</span>
                    <strong>{{ formatReviewInterval(session.reviewPlan?.nextReviewAt) }}</strong>
                  </div>
                  <div class="runtime-meta-row">
                    <span>Unread</span>
                    <strong>{{ session.unreadCount }}</strong>
                  </div>
                  <div class="runtime-meta-row">
                    <span>Active Chat</span>
                    <strong>{{
                      session.activeChatState
                        ? `${session.activeChatState.interestValue.toFixed(1)} / ${session.activeChatState.tickCount}`
                        : $t("pages.agents.labels.noValue")
                    }}</strong>
                  </div>
                  <div class="runtime-meta-row">
                    <span>Last Review</span>
                    <strong>{{
                      session.latestReviewSummary
                        ? formatTimestamp(session.latestReviewSummary.createdAt)
                        : session.latestReviewRun
                          ? formatTimestamp(session.latestReviewRun.startedAt)
                        : $t("pages.agents.labels.noValue")
                    }}</strong>
                  </div>
                  <div class="runtime-meta-row">
                    <span>Review Note</span>
                    <strong>{{
                      session.latestReviewSummary?.summary ||
                      session.latestReviewRun?.responseSummary ||
                      $t("pages.agents.labels.noValue")
                    }}</strong>
                  </div>
                  <div class="runtime-meta-row">
                    <span>Last Audit</span>
                    <strong>{{
                      session.latestAudit
                        ? session.latestAudit.timestamp
                        : $t("pages.agents.labels.noValue")
                    }}</strong>
                  </div>
                </v-expansion-panel-text>
              </v-expansion-panel>
            </v-expansion-panels>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <div class="agents-toolbar mb-6">
      <v-text-field
        v-model="searchQuery"
        :label="$t('common.actions.action.search')"
        prepend-inner-icon="mdi-magnify"
        single-line
        hide-details
        density="comfortable"
        variant="outlined"
        bg-color="surface"
        class="agents-search"
      />
      <v-spacer />
      <v-chip color="primary" variant="tonal" size="small">
        {{ $t("pages.agents.labels.profileCount", { count: profiles.length }) }}
      </v-chip>
    </div>

    <v-row v-if="showInitialSkeleton">
      <v-col cols="12">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row
      v-else-if="!initialSkeletonRequested && filteredProfiles.length === 0"
      justify="center"
      class="py-12"
    >
      <v-col cols="12" sm="8" md="6" class="text-center">
        <v-icon
          size="112"
          color="grey-lighten-1"
          icon="mdi-account-search-outline"
        />
        <h3 class="text-h6 my-4">{{ $t("pages.agents.empty.title") }}</h3>
        <p class="text-body-2 text-medium-emphasis mb-4">
          {{ $t("pages.agents.empty.subtitle") }}
        </p>
        <v-btn
          color="primary"
          prepend-icon="mdi-account-plus"
          @click="openCreate"
        >
          {{ $t("pages.agents.actions.addAgent") }}
        </v-btn>
      </v-col>
    </v-row>

    <v-row v-else>
      <v-col
        v-for="profile in filteredProfiles"
        :key="profile.fileName"
        cols="12"
        sm="6"
        lg="4"
      >
        <v-card class="agent-card h-100 d-flex flex-column" elevation="0">
          <v-card-item>
            <template #prepend>
              <v-avatar
                color="primary"
                variant="tonal"
                icon="mdi-account-cog-outline"
              />
            </template>
            <v-card-title class="text-break">
              {{ profile.agentId || profile.fileName }}
            </v-card-title>
            <v-card-subtitle>{{ profile.path }}</v-card-subtitle>
            <template #append>
              <v-chip
                :color="profile.issues.length > 0 ? 'warning' : 'success'"
                size="small"
                variant="tonal"
              >
                {{
                  profile.issues.length > 0
                    ? $t("pages.agents.labels.issueCount", {
                        count: profile.issues.length,
                      })
                    : $t("pages.agents.labels.valid")
                }}
              </v-chip>
            </template>
          </v-card-item>

          <v-card-text class="pt-1 flex-grow-1">
            <div class="agent-meta-row">
              <span>{{ $t("pages.agents.fields.mode") }}</span>
              <strong>{{
                profile.mode || $t("pages.agents.labels.noValue")
              }}</strong>
            </div>
            <div class="agent-meta-row">
              <span>{{ $t("pages.agents.fields.persona") }}</span>
              <strong>{{
                profile.personaId || $t("pages.agents.labels.noValue")
              }}</strong>
            </div>
            <div class="agent-meta-row">
              <span>{{ $t("pages.agents.labels.updated") }}</span>
              <strong>{{ formatTimestamp(profile.lastModified) }}</strong>
            </div>
          </v-card-text>

          <v-card-actions>
            <v-btn
              variant="text"
              prepend-icon="mdi-pencil"
              @click="openEdit(profile)"
            >
              {{ $t("common.actions.action.edit") }}
            </v-btn>
            <v-spacer />
            <v-btn
              color="error"
              variant="text"
              prepend-icon="mdi-delete-outline"
              @click="removeProfile(profile)"
            >
              {{ $t("common.actions.action.delete") }}
            </v-btn>
          </v-card-actions>
        </v-card>
      </v-col>
    </v-row>

    <v-dialog v-model="dialogVisible" max-width="1100" scrollable>
      <v-card class="agent-dialog">
        <v-card-title class="agent-dialog__title">
          {{
            editingFileName
              ? $t("pages.agents.overlay.editTitle")
              : $t("pages.agents.overlay.createTitle")
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
                :field-prefixes="[
                  'agent.context',
                  'agent.summaries',
                  'agent.media',
                ]"
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
              {{ $t("pages.agents.validation.title") }}
            </div>
            <div
              v-for="issue in profileIssues.slice(0, 6)"
              :key="`${issue.path}:${issue.code}:${issue.message}`"
              class="text-body-2 validation-issue-line"
            >
              <span class="font-weight-medium">{{ issue.path || "-" }}</span>
              <span>{{ issue.message }}</span>
            </div>
          </v-alert>

          <v-alert
            v-if="dialogError"
            type="error"
            variant="tonal"
            class="mx-6 mb-6"
          >
            {{ dialogError }}
          </v-alert>
        </v-card-text>

        <v-card-actions class="agent-dialog__actions">
          <v-spacer />
          <v-btn variant="text" @click="dialogVisible = false">
            {{ $t("common.actions.action.cancel") }}
          </v-btn>
          <v-btn color="primary" :loading="isSaving" @click="saveProfile">
            {{
              editingFileName
                ? $t("common.actions.action.save")
                : $t("common.actions.action.create")
            }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { useI18n } from "vue-i18n";

import { agentConfigsApi, type AgentConfigProfile } from "@/api/agentConfigs";
import { agentsApi, type AgentRuntimeProfile } from "@/api/agents";
import { apiClient } from "@/api/client";
import {
  extractConfigValidationIssues,
  type ConfigRecord,
  type ConfigWorkspaceProvider,
} from "@/api/config";
import AppPageHeader from "@/components/AppPageHeader.vue";
import ProviderSchemaForm from "@/components/config/ProviderSchemaForm.vue";
import { useConfirmDialog } from "@/composables/useConfirmDialog";
import { useDelayedFlag } from "@/composables/useDelayedFlag";
import { translate } from "@/plugins/i18n";
import { useConfigWorkspaceStore } from "@/stores/configWorkspace";
import { useUiStore } from "@/stores/ui";
import { getErrorMessage } from "@/utils/error";

type AgentFormTab = "identity" | "defaults" | "review" | "active" | "advanced";

const AGENT_RUNTIME_PROVIDER_ID = "shinbot.agent.runtime";

const { t, locale } = useI18n();
const configStore = useConfigWorkspaceStore();
const uiStore = useUiStore();
const { confirm } = useConfirmDialog();

const profiles = ref<AgentConfigProfile[]>([]);
const runtimeProfiles = ref<AgentRuntimeProfile[]>([]);
const isLoading = ref(false);
const hasLoadedProfiles = ref(false);
const isSaving = ref(false);
const error = ref("");
const dialogError = ref("");
const dialogVisible = ref(false);
const editingFileName = ref("");
const searchQuery = ref("");
const activeAgentTab = ref<AgentFormTab>("identity");
const profileIssues = ref<AgentConfigProfile["issues"]>([]);

const form = reactive({
  fileName: "",
  config: {} as ConfigRecord,
});

const agentTabs = computed(() => [
  {
    value: "identity" as const,
    label: t("pages.agents.tabs.identity"),
    icon: "mdi-card-account-details-outline",
  },
  {
    value: "defaults" as const,
    label: t("pages.agents.tabs.defaults"),
    icon: "mdi-tune-variant",
  },
  {
    value: "review" as const,
    label: t("pages.agents.tabs.review"),
    icon: "mdi-message-processing-outline",
  },
  {
    value: "active" as const,
    label: t("pages.agents.tabs.active"),
    icon: "mdi-chat-processing-outline",
  },
  {
    value: "advanced" as const,
    label: t("pages.agents.tabs.advanced"),
    icon: "mdi-code-json",
  },
]);

const agentProvider = computed<ConfigWorkspaceProvider | null>(
  () => configStore.agentProvidersById[AGENT_RUNTIME_PROVIDER_ID] ?? null,
);

const initialSkeletonRequested = computed(
  () =>
    isLoading.value && !hasLoadedProfiles.value && profiles.value.length === 0,
);
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested);

const filteredProfiles = computed(() => {
  const query = searchQuery.value.trim().toLowerCase();
  if (!query) {
    return profiles.value;
  }
  return profiles.value.filter((profile) => {
    const haystack = [
      profile.fileName,
      profile.path,
      profile.agentId,
      profile.mode,
      profile.personaId,
    ]
      .join("\n")
      .toLowerCase();
    return haystack.includes(query);
  });
});

function cloneConfig<T extends ConfigRecord>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function defaultAgentConfig() {
  return cloneConfig(
    agentProvider.value?.defaults ?? {
      agent: {
        id: "",
        mode: "full",
        persona_id: "",
      },
    },
  );
}

function formatTimestamp(value: number) {
  if (!value) {
    return t("pages.agents.labels.noValue");
  }
  const normalized = value > 1_000_000_000_000 ? value : value * 1000;
  return new Intl.DateTimeFormat(locale.value, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(normalized));
}

function formatReviewInterval(value: number | null | undefined) {
  if (!value) {
    return t("pages.agents.labels.noValue");
  }
  const normalized = value > 1_000_000_000_000 ? value : value * 1000;
  const diffMs = normalized - Date.now();
  if (diffMs <= 0) {
    return t("pages.agents.labels.reviewDue");
  }
  const totalMinutes = Math.max(1, Math.round(diffMs / 60_000));
  if (totalMinutes < 60) {
    return t("pages.agents.labels.reviewInMinutes", { count: totalMinutes });
  }
  const totalHours = Math.round(totalMinutes / 60);
  if (totalHours < 48) {
    return t("pages.agents.labels.reviewInHours", { count: totalHours });
  }
  const totalDays = Math.round(totalHours / 24);
  return t("pages.agents.labels.reviewInDays", { count: totalDays });
}

async function loadProfiles() {
  const data = await apiClient.unwrap(
    agentConfigsApi.list({ suppressErrorNotify: true }),
  );
  profiles.value = data;
  return data;
}

async function loadRuntimeProfiles() {
  const data = await apiClient.unwrap(
    agentsApi.runtimeOverview({ suppressErrorNotify: true }),
  );
  runtimeProfiles.value = data;
  return data;
}

async function refreshPage() {
  isLoading.value = true;
  error.value = "";
  try {
    await Promise.all([
      configStore.loadWorkspace({ preserveDraft: configStore.isDirty }),
      loadProfiles(),
      loadRuntimeProfiles(),
    ]);
  } catch (errorDetail: unknown) {
    error.value = getErrorMessage(
      errorDetail,
      translate("pages.agents.messages.loadFailed"),
    );
  } finally {
    hasLoadedProfiles.value = true;
    isLoading.value = false;
  }
}

function openCreate() {
  editingFileName.value = "";
  activeAgentTab.value = "identity";
  dialogError.value = "";
  profileIssues.value = [];
  form.fileName = "";
  form.config = defaultAgentConfig();
  dialogVisible.value = true;
}

function openEdit(profile: AgentConfigProfile) {
  editingFileName.value = profile.fileName;
  activeAgentTab.value = "identity";
  dialogError.value = "";
  profileIssues.value = profile.issues;
  form.fileName = profile.fileName;
  form.config = cloneConfig(profile.config);
  dialogVisible.value = true;
}

function applySavedProfile(profile: AgentConfigProfile) {
  const index = profiles.value.findIndex(
    (item) => item.fileName === profile.fileName,
  );
  if (index >= 0) {
    profiles.value.splice(index, 1, profile);
  } else {
    profiles.value.push(profile);
    profiles.value.sort((a, b) => a.fileName.localeCompare(b.fileName));
  }
}

async function saveProfile() {
  isSaving.value = true;
  dialogError.value = "";
  profileIssues.value = [];
  try {
    const payload = {
      fileName: form.fileName.trim(),
      config: cloneConfig(form.config),
      validateBeforeSave: true,
    };
    const saved = editingFileName.value
      ? await apiClient.unwrap(
          agentConfigsApi.update(editingFileName.value, payload),
        )
      : await apiClient.unwrap(agentConfigsApi.create(payload));
    applySavedProfile(saved);
    dialogVisible.value = false;
    uiStore.showSnackbar(
      translate("common.actions.message.operationSuccess"),
      "success",
    );
  } catch (errorDetail: unknown) {
    const issues = extractConfigValidationIssues(errorDetail);
    if (issues.length > 0) {
      profileIssues.value = issues;
    }
    dialogError.value = getErrorMessage(
      errorDetail,
      translate("pages.agents.messages.saveFailed"),
    );
  } finally {
    isSaving.value = false;
  }
}

async function removeProfile(profile: AgentConfigProfile) {
  if (
    !(await confirm({
      title: translate("common.actions.action.delete"),
      message: translate("pages.agents.messages.confirmDelete", {
        name: profile.agentId || profile.fileName,
      }),
      confirmText: translate("common.actions.action.delete"),
      confirmColor: "error",
      icon: "mdi-alert-outline",
      iconColor: "error",
    }))
  ) {
    return;
  }

  try {
    await apiClient.unwrap(agentConfigsApi.delete(profile.fileName));
    profiles.value = profiles.value.filter(
      (item) => item.fileName !== profile.fileName,
    );
    uiStore.showSnackbar(
      translate("common.actions.message.operationSuccess"),
      "success",
    );
  } catch (errorDetail: unknown) {
    error.value = getErrorMessage(
      errorDetail,
      translate("pages.agents.messages.deleteFailed"),
    );
  }
}

onMounted(() => {
  void refreshPage();
});
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.agents-toolbar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 14px;
  @include surface-card;
}

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

.agents-search {
  max-width: 420px;
}

.agent-card {
  @include surface-card;
  @include hover-lift;
}

.agent-meta-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 0;
  border-bottom: 1px solid $border-color-soft;

  &:last-child {
    border-bottom: 0;
  }

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
  content: "";
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

@include respond-to("mobile") {
  .agents-toolbar {
    flex-direction: column;
    align-items: stretch;
  }

  .agents-search {
    max-width: none;
  }

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
