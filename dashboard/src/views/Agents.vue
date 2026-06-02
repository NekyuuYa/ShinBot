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

    <agent-runtime-profiles
      :profiles="runtimeProfiles"
      :bindings-label="$t('pages.agents.labels.bindings')"
      :sessions-label="$t('pages.agents.labels.sessions')"
      :platform-label="$t('pages.agents.labels.platform')"
      :platform-status-label="$t('pages.agents.labels.platformStatus')"
      :review-label="$t('pages.agents.labels.review')"
      :review-interval-label="$t('pages.agents.labels.reviewInterval')"
      :unread-label="$t('pages.agents.labels.unread')"
      :active-chat-label="$t('pages.agents.labels.activeChat')"
      :last-review-label="$t('pages.agents.labels.lastReview')"
      :review-note-label="$t('pages.agents.labels.reviewNote')"
      :last-audit-label="$t('pages.agents.labels.lastAudit')"
      :no-value-label="$t('pages.agents.labels.noValue')"
      :format-timestamp="formatTimestamp"
      :format-review-interval="formatReviewInterval"
    />

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
        <agent-config-card
          :profile="profile"
          :mode-label="$t('pages.agents.fields.mode')"
          :persona-label="$t('pages.agents.fields.persona')"
          :updated-label="$t('pages.agents.labels.updated')"
          :no-value-label="$t('pages.agents.labels.noValue')"
          :valid-label="$t('pages.agents.labels.valid')"
          :edit-label="$t('common.actions.action.edit')"
          :delete-label="$t('common.actions.action.delete')"
          :issue-count-label="
            (count: number) =>
              $t('pages.agents.labels.issueCount', {
                count,
              })
          "
          :format-timestamp="formatTimestamp"
          @edit="openEdit"
          @remove="removeProfile"
        />
      </v-col>
    </v-row>

    <agent-config-dialog
      v-model:visible="dialogVisible"
      v-model:active-agent-tab="activeAgentTab"
      v-model:form="form"
      :editing-file-name="editingFileName"
      :agent-provider="agentProvider"
      :profile-issues="profileIssues"
      :dialog-error="dialogError"
      :is-saving="isSaving"
      :model-ref-route-options="modelRefRouteOptions"
      :model-ref-provider-groups="modelRefProviderGroups"
      @save="saveProfile"
    />
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
import AgentConfigCard from "@/components/agents/AgentConfigCard.vue";
import AgentRuntimeProfiles from "@/components/agents/AgentRuntimeProfiles.vue";
import AgentConfigDialog from "@/components/agents/AgentConfigDialog.vue";
import { useConfirmDialog } from "@/composables/useConfirmDialog";
import { useDelayedFlag } from "@/composables/useDelayedFlag";
import { translate } from "@/plugins/i18n";
import { useConfigWorkspaceStore } from "@/stores/configWorkspace";
import { useModelRuntimeStore } from "@/stores/modelRuntime";
import { useUiStore } from "@/stores/ui";
import { normalizeTimestampMs } from "@/utils/time";
import { getErrorMessage } from "@/utils/error";
import { resolveProviderSource } from "@/utils/modelRuntimeSources";

type AgentFormTab = "identity" | "defaults" | "review" | "active" | "advanced";

const AGENT_RUNTIME_PROVIDER_ID = "shinbot.agent.runtime";

const { t, locale } = useI18n();
const configStore = useConfigWorkspaceStore();
const modelRuntimeStore = useModelRuntimeStore();
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

const agentProvider = computed<ConfigWorkspaceProvider | null>(
  () => configStore.agentProvidersById[AGENT_RUNTIME_PROVIDER_ID] ?? null,
);

const routeTitle = (route: { id: string; purpose: string; metadata: Record<string, unknown> }) => {
  const metadata = route.metadata || {};
  for (const key of ["displayName", "name", "title"]) {
    const value = metadata[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return route.purpose || route.id;
};

const modelRefRouteOptions = computed(() =>
  modelRuntimeStore.routes
    .map((route) => ({
      id: `[route]${route.id}`,
      title: routeTitle(route),
      subtitle: `${route.id}${route.strategy ? ` · ${route.strategy}` : ""}`,
      enabled: route.enabled,
    }))
    .sort((left, right) => {
      if (left.enabled !== right.enabled) {
        return left.enabled ? -1 : 1;
      }
      return left.title.localeCompare(right.title);
    }),
);

const modelRefProviderGroups = computed(() =>
  modelRuntimeStore.providers
    .map((provider) => ({
      providerId: provider.id,
      providerName: provider.displayName || provider.id,
      providerType: resolveProviderSource(provider.type)?.label || provider.type,
      items: (modelRuntimeStore.modelsByProvider[provider.id] || [])
        .map((model) => ({
          value: `[model]${model.id}`,
          title: model.displayName || model.id,
          subtitle:
            model.backendModel && model.backendModel !== model.id
              ? `${model.id} · ${model.backendModel}`
              : model.id,
          kind: "configured" as const,
        }))
        .sort((left, right) => left.title.localeCompare(right.title)),
    }))
    .filter((group) => group.items.length > 0)
    .sort((left, right) => left.providerName.localeCompare(right.providerName)),
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
  const normalized = normalizeTimestampMs(value);
  if (normalized === null) {
    return t("pages.agents.labels.noValue");
  }
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
  const normalized = normalizeTimestampMs(value);
  if (normalized === null) {
    return t("pages.agents.labels.noValue");
  }
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
      modelRuntimeStore.fetchAll(),
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

@include respond-to("mobile") {
  .agents-toolbar {
    flex-direction: column;
    align-items: stretch;
  }

  .agents-search {
    max-width: none;
  }
}
</style>
