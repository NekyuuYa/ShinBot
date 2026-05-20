<template>
  <div v-if="!selectedProvider && !isCreatingProvider" class="d-flex flex-column ga-4">
    <v-sheet rounded="xl" class="empty-state-panel empty-provider-panel pa-8">
      <div class="text-overline section-label mb-3">
        {{ $t("pages.modelRuntime.sidebar.providers") }}
      </div>
      <div class="text-h6 mb-2">
        {{ $t("pages.modelRuntime.hints.selectProviderSourceTitle") }}
      </div>
      <div class="text-body-2 text-medium-emphasis">
        {{ $t("pages.modelRuntime.hints.selectProviderSource") }}
      </div>
    </v-sheet>
  </div>

  <div v-else class="d-flex flex-column ga-4">
    <v-card class="editor-card setup-flow-card">
      <v-card-text>
        <div class="setup-flow-header mb-4">
          <div>
            <div class="section-label">
              {{ $t("pages.modelRuntime.cards.providerSetup") }}
            </div>
            <div class="text-caption text-medium-emphasis mt-1">
              {{ providerSetupSummary }}
            </div>
          </div>
          <v-chip
            size="small"
            variant="tonal"
            :color="providerSetupComplete ? 'success' : 'primary'"
          >
            {{
              providerSetupComplete
                ? $t("pages.modelRuntime.labels.setupComplete")
                : $t("pages.modelRuntime.labels.setupInProgress")
            }}
          </v-chip>
        </div>

        <div class="setup-flow-steps">
          <div
            v-for="step in providerSetupSteps"
            :key="step.key"
            class="setup-flow-step"
            :class="`setup-flow-step--${step.state}`"
          >
            <div class="setup-step-marker">
              <v-icon :icon="step.icon" size="18" />
            </div>
            <div class="setup-step-copy">
              <div class="setup-step-title">{{ step.title }}</div>
              <div class="setup-step-detail">{{ step.detail }}</div>
            </div>
            <v-chip size="x-small" variant="tonal" :color="step.color">
              {{ step.statusLabel }}
            </v-chip>
            <v-btn
              v-if="step.action"
              size="small"
              variant="text"
              :color="step.color"
              :disabled="step.actionDisabled"
              :loading="step.loading"
              @click="runProviderSetupAction(step.action)"
            >
              {{ step.actionLabel }}
            </v-btn>
          </div>
        </div>
      </v-card-text>
    </v-card>

    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{ $t("pages.modelRuntime.cards.providerIdentity") }}</v-card-title>
        <template #append>
          <div class="d-flex ga-2">
            <v-btn
              color="error"
              variant="outlined"
              rounded="xl"
              :disabled="isCreatingProvider || !selectedProvider"
              @click="deleteCurrentProvider"
            >
              {{ $t("common.actions.action.delete") }}
            </v-btn>
            <v-btn
              color="primary"
              variant="tonal"
              rounded="xl"
              class="action-btn"
              :loading="store.isSaving"
              @click="saveProvider"
            >
              {{ providerSaveLabel }}
            </v-btn>
          </div>
        </template>
      </v-card-item>
      <v-card-text>
        <v-row>
          <v-col cols="12" md="6">
            <div class="text-caption text-medium-emphasis mb-2">
              {{ $t("pages.modelRuntime.fields.source") }}
            </div>
            <button
              type="button"
              class="provider-source-picker-tile"
              @click="showProviderSourcePicker = true"
            >
              <v-avatar
                size="36"
                color="primary"
                variant="tonal"
                class="selector-avatar"
              >
                <v-icon :icon="providerSourceIcon(providerForm.sourceType)" size="20" />
              </v-avatar>
              <span class="selector-copy">
                <span class="selector-title">{{ currentProviderSourceTitle }}</span>
                <span class="selector-subtitle">{{ currentProviderSourceSubtitle }}</span>
              </span>
              <v-icon icon="mdi-chevron-right" size="20" class="selector-arrow" />
            </button>
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              :model-value="$t(`pages.modelRuntime.labels.${providerCapabilityType}`)"
              :label="$t('pages.modelRuntime.fields.capabilityType')"
              density="comfortable"
              variant="outlined"
              readonly
              persistent-hint
              :hint="$t('pages.modelRuntime.hints.capabilityType')"
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="providerForm.id"
              :label="$t('pages.modelRuntime.fields.id')"
              density="comfortable"
              variant="outlined"
              :hint="$t('pages.modelRuntime.hints.idEditable')"
              persistent-hint
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="providerForm.displayName"
              :label="$t('pages.modelRuntime.fields.displayName')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="providerForm.baseUrl"
              :label="$t('pages.modelRuntime.fields.baseUrl')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col v-if="showProviderTokenField" cols="12" md="8">
            <v-text-field
              v-model="providerForm.token"
              :label="$t('pages.modelRuntime.fields.token')"
              density="comfortable"
              variant="outlined"
              :type="tokenVisible ? 'text' : 'password'"
              :append-inner-icon="tokenVisible ? 'mdi-eye-off-outline' : 'mdi-eye-outline'"
              :hint="tokenHint"
              persistent-hint
              @click:append-inner="tokenVisible = !tokenVisible"
            />
            <div v-if="hasStoredCredential" class="credential-state-row mt-2">
              <v-chip
                size="small"
                variant="tonal"
                :color="credentialWillBeCleared ? 'warning' : 'success'"
              >
                {{ credentialStateLabel }}
              </v-chip>
              <v-btn
                size="small"
                variant="text"
                :color="credentialWillBeCleared ? 'primary' : 'warning'"
                @click="toggleStoredCredentialClear"
              >
                {{ credentialActionLabel }}
              </v-btn>
            </div>
          </v-col>
          <v-col v-else-if="credentialWillBeCleared" cols="12" md="8">
            <v-alert type="warning" variant="tonal" density="comfortable">
              {{ $t("pages.modelRuntime.hints.credentialWillBeCleared") }}
            </v-alert>
          </v-col>
          <v-col
            cols="12"
            :md="showProviderTokenField || credentialWillBeCleared ? 4 : 6"
            class="d-flex align-center"
          >
            <v-switch
              v-model="providerForm.enabled"
              color="primary"
              inset
              :label="$t('pages.modelRuntime.fields.enabled')"
            />
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{ $t("pages.modelRuntime.cards.advanced") }}</v-card-title>
        <template #append>
          <v-btn
            color="info"
            variant="outlined"
            rounded="xl"
            :loading="probingProviderId === selectedProvider?.id"
            :disabled="!selectedProvider || isCreatingProvider"
            @click="probeSelectedProvider()"
          >
            {{ $t("pages.modelRuntime.actions.testConnection") }}
          </v-btn>
        </template>
      </v-card-item>
      <v-card-text class="d-flex flex-column ga-5">
        <v-alert
          v-if="lastProviderProbeResult"
          :type="lastProviderProbeResult.success ? 'success' : 'warning'"
          variant="tonal"
          density="comfortable"
        >
          <div class="font-weight-medium">
            {{ probeResultTitle }}
          </div>
          <div class="text-caption mt-1">
            {{ probeResultSubtitle }}
          </div>
        </v-alert>

        <v-row>
          <v-col v-if="showApiVersionField" cols="12" md="6">
            <v-text-field
              v-model="providerForm.apiVersion"
              :label="$t('pages.modelRuntime.fields.apiVersion')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" :md="showApiVersionField ? 6 : 12">
            <v-text-field
              v-model="providerForm.proxyAddress"
              :label="$t('pages.modelRuntime.fields.proxyAddress')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
        </v-row>

        <div>
          <div class="text-caption text-medium-emphasis mb-2">
            {{ $t("pages.modelRuntime.fields.requestHeaders") }}
          </div>
          <key-value-editor v-model="providerHeaderRows" />
        </div>

        <v-textarea
          v-if="sourceSupportsThinking"
          v-model="providerForm.thinkingJson"
          :label="$t('pages.modelRuntime.fields.thinkingConfig')"
          :hint="$t('pages.modelRuntime.hints.thinking')"
          persistent-hint
          rows="4"
          variant="outlined"
        />

        <v-textarea
          v-if="sourceSupportsFilters"
          v-model="providerForm.filtersJson"
          :label="$t('pages.modelRuntime.fields.filtersConfig')"
          :hint="$t('pages.modelRuntime.hints.filters')"
          persistent-hint
          rows="4"
          variant="outlined"
        />

        <v-row>
          <v-col cols="12" md="6">
            <v-textarea
              v-model="providerForm.defaultParamsJson"
              :label="$t('pages.modelRuntime.fields.defaultParamsJson')"
              :hint="$t('pages.modelRuntime.hints.defaultParamsJson')"
              :error-messages="defaultParamsJsonError ? [defaultParamsJsonError] : []"
              persistent-hint
              auto-grow
              rows="7"
              variant="outlined"
              class="json-editor-field"
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-textarea
              :model-value="defaultParamsPreviewJson"
              :label="$t('pages.modelRuntime.fields.defaultParamsPreview')"
              :hint="$t('pages.modelRuntime.hints.defaultParamsPreview')"
              persistent-hint
              readonly
              auto-grow
              rows="7"
              variant="outlined"
              class="json-editor-field"
            />
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <provider-models-panel />
  </div>

  <generic-picker-dialog
    :model-value="showProviderSourcePicker"
    :title="$t('pages.modelRuntime.dialogs.providerSourcePicker')"
    :subtitle="$t('pages.modelRuntime.hints.providerSourcePicker')"
    :sections="providerSourcePickerSections"
    :selected="providerForm.sourceType ? [providerForm.sourceType] : []"
    :empty-text="$t('pages.modelRuntime.hints.providerSourcePickerEmpty')"
    :no-results-text="$t('pages.modelRuntime.hints.providerSourcePickerNoMatches')"
    @update:model-value="showProviderSourcePicker = $event"
    @update:selected="applyProviderSourcePick"
  />
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useI18n } from "vue-i18n";

import { useModelRuntimeContext } from "@/composables/useModelRuntimePage";
import { providerSourceIcon } from "@/utils/modelRuntimeSources";
import GenericPickerDialog, { type GenericPickerSection } from "./GenericPickerDialog.vue";
import KeyValueEditor from "./KeyValueEditor.vue";
import ProviderModelsPanel from "./ProviderModelsPanel.vue";

type ProviderSetupAction = "source" | "save" | "probe";
type ProviderSetupState = "complete" | "active" | "pending" | "skipped";

interface ProviderSetupStep {
  key: string;
  icon: string;
  title: string;
  detail: string;
  complete: boolean;
  skipped?: boolean;
  action?: ProviderSetupAction;
  actionLabel?: string;
  actionDisabled?: boolean;
  loading?: boolean;
  state: ProviderSetupState;
  color: string;
  statusLabel: string;
}

const { t } = useI18n();
const showProviderSourcePicker = ref(false);
const tokenVisible = ref(false);

const {
  store,
  isCreatingProvider,
  selectedProvider,
  providerSaveLabel,
  providerForm,
  providerSourceOptions,
  providerCapabilityType,
  onProviderSourceChange,
  showProviderTokenField,
  hasStoredCredential,
  credentialWillBeCleared,
  toggleStoredCredentialClear,
  defaultParamsJsonError,
  defaultParamsPreviewJson,
  selectedProviderSource,
  sourceSupportsThinking,
  sourceSupportsFilters,
  showApiVersionField,
  providerHeaderRows,
  probingProviderId,
  lastProviderProbeResult,
  probeSelectedProvider,
  deleteCurrentProvider,
  saveProvider,
} = useModelRuntimeContext();

const currentProviderSourceTitle = computed(
  () =>
    selectedProviderSource.value?.label ||
    providerForm.value.sourceType ||
    t("pages.modelRuntime.fields.source"),
);

const currentProviderSourceSubtitle = computed(() => {
  if (selectedProviderSource.value?.defaultBaseUrl) {
    return selectedProviderSource.value.defaultBaseUrl;
  }
  return providerForm.value.sourceType || t("pages.modelRuntime.hints.providerSourcePickerEmpty");
});

const tokenHint = computed(() => {
  if (credentialWillBeCleared.value) {
    return t("pages.modelRuntime.hints.credentialClearOnSave");
  }
  if (selectedProvider.value?.hasAuth) {
    return t("pages.modelRuntime.hints.tokenConfigured");
  }
  return t("pages.modelRuntime.hints.token");
});

const credentialStateLabel = computed(() =>
  credentialWillBeCleared.value
    ? t("pages.modelRuntime.labels.credentialClearPending")
    : t("pages.modelRuntime.labels.credentialStored"),
);

const credentialActionLabel = computed(() =>
  credentialWillBeCleared.value
    ? t("pages.modelRuntime.actions.keepCredential")
    : t("pages.modelRuntime.actions.clearCredential"),
);

const providerSourceReady = computed(() => Boolean(providerForm.value.sourceType));

const providerCredentialReady = computed(() => {
  if (!showProviderTokenField.value) {
    return true;
  }
  if (providerForm.value.token.trim()) {
    return true;
  }
  return hasStoredCredential.value && !credentialWillBeCleared.value;
});

const providerSaved = computed(
  () => Boolean(selectedProvider.value) && !isCreatingProvider.value,
);

const providerProbePassed = computed(
  () => lastProviderProbeResult.value?.success === true,
);

const providerSetupComplete = computed(
  () =>
    providerSourceReady.value &&
    providerCredentialReady.value &&
    providerSaved.value &&
    providerProbePassed.value,
);

const providerSetupSummary = computed(() => {
  if (providerSetupComplete.value) {
    return t("pages.modelRuntime.labels.setupSummaryReady");
  }
  if (!providerSourceReady.value) {
    return t("pages.modelRuntime.labels.setupSummarySource");
  }
  if (!providerCredentialReady.value) {
    return t("pages.modelRuntime.labels.setupSummaryCredential");
  }
  if (!providerSaved.value) {
    return t("pages.modelRuntime.labels.setupSummarySave");
  }
  if (!providerProbePassed.value) {
    return t("pages.modelRuntime.labels.setupSummaryProbe");
  }
  return t("pages.modelRuntime.labels.setupSummaryReady");
});

const statusLabelForSetupStep = (state: ProviderSetupState) => {
  if (state === "complete") {
    return t("pages.modelRuntime.labels.setupDone");
  }
  if (state === "active") {
    return t("pages.modelRuntime.labels.setupCurrent");
  }
  if (state === "skipped") {
    return t("pages.modelRuntime.labels.setupSkipped");
  }
  return t("pages.modelRuntime.labels.setupPending");
};

const colorForSetupStep = (state: ProviderSetupState) => {
  if (state === "complete") {
    return "success";
  }
  if (state === "active") {
    return "primary";
  }
  if (state === "skipped") {
    return "default";
  }
  return "default";
};

const providerSetupSteps = computed<ProviderSetupStep[]>(() => {
  const baseSteps: ProviderSetupStep[] = [
    {
      key: "source",
      icon: "mdi-source-branch",
      title: t("pages.modelRuntime.labels.setupStepSource"),
      detail: currentProviderSourceTitle.value,
      complete: providerSourceReady.value,
      action: "source",
      actionLabel: t("pages.modelRuntime.actions.selectSource"),
      state: "pending",
      color: "default",
      statusLabel: "",
    },
    {
      key: "credential",
      icon: "mdi-key-outline",
      title: t("pages.modelRuntime.labels.setupStepCredential"),
      detail: !showProviderTokenField.value
        ? t("pages.modelRuntime.labels.credentialNotRequired")
        : providerCredentialReady.value
          ? t("pages.modelRuntime.labels.credentialReady")
          : t("pages.modelRuntime.labels.credentialRequired"),
      complete: providerCredentialReady.value,
      state: "pending",
      color: "default",
      statusLabel: "",
    },
    {
      key: "save",
      icon: "mdi-content-save-outline",
      title: t("pages.modelRuntime.labels.setupStepSave"),
      detail: providerSaved.value
        ? t("pages.modelRuntime.labels.providerSaved")
        : t("pages.modelRuntime.labels.providerDraft"),
      complete: providerSaved.value,
      action: "save",
      actionLabel: providerSaveLabel.value,
      actionDisabled: !providerSourceReady.value || !providerForm.value.id.trim(),
      loading: store.isSaving,
      state: "pending",
      color: "default",
      statusLabel: "",
    },
    {
      key: "probe",
      icon: "mdi-connection",
      title: t("pages.modelRuntime.labels.setupStepProbe"),
      detail: providerProbePassed.value
        ? t("pages.modelRuntime.labels.probePassed")
        : t("pages.modelRuntime.labels.probePending"),
      complete: providerProbePassed.value,
      action: "probe",
      actionLabel: t("pages.modelRuntime.actions.testConnection"),
      actionDisabled: !providerSaved.value,
      loading: probingProviderId.value === selectedProvider.value?.id,
      state: "pending",
      color: "default",
      statusLabel: "",
    },
  ];

  const activeIndex = baseSteps.findIndex((step) => !step.complete);
  return baseSteps.map((step, index) => {
    const state: ProviderSetupState = step.complete
      ? "complete"
      : index === activeIndex
        ? "active"
        : "pending";
    return {
      ...step,
      state,
      color: colorForSetupStep(state),
      statusLabel: statusLabelForSetupStep(state),
    };
  });
});

const probeResultTitle = computed(() =>
  t("pages.modelRuntime.labels.probeResult", {
    mode: lastProviderProbeResult.value?.mode || "",
  }),
);

const probeResultSubtitle = computed(() => {
  const result = lastProviderProbeResult.value;
  if (!result) {
    return "";
  }
  if (result.executionId) {
    return t("pages.modelRuntime.labels.probeExecution", {
      id: result.executionId,
    });
  }
  if (result.catalogSize !== undefined) {
    return t("pages.modelRuntime.labels.probeCatalogSize", {
      count: result.catalogSize,
    });
  }
  return t("pages.modelRuntime.labels.probeCheckedAt", {
    time: result.checkedAt,
  });
});

const runProviderSetupAction = async (action?: ProviderSetupAction) => {
  if (action === "source") {
    showProviderSourcePicker.value = true;
    return;
  }
  if (action === "save") {
    await saveProvider();
    return;
  }
  if (action === "probe") {
    await probeSelectedProvider();
  }
};

const providerSourcePickerSections = computed<GenericPickerSection[]>(() => [
  {
    id: "provider-sources",
    label: t("pages.modelRuntime.fields.source"),
    items: providerSourceOptions.map((source) => ({
      value: source.key,
      title: source.label,
      subtitle: source.defaultBaseUrl,
      icon: providerSourceIcon(source.type),
      iconColor: "primary",
      tag: source.type,
      tagColor: source.supportsCatalog ? "primary" : "default",
    })),
  },
]);

const applyProviderSourcePick = async (values: string[]) => {
  await onProviderSourceChange(values[0] ?? null);
};
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.action-btn {
  box-shadow: none;
}

.editor-card {
  @include surface-card;
}

.editor-card :deep(.v-card-item) {
  padding: 24px 24px 16px;
}

.setup-flow-card :deep(.v-card-text) {
  padding: 20px 24px 24px;
}

.setup-flow-header,
.setup-flow-steps,
.setup-flow-step {
  display: flex;
  align-items: center;
}

.setup-flow-header {
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.setup-flow-steps {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;

  @include respond-to("tablet") {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @include respond-to("mobile") {
    grid-template-columns: minmax(0, 1fr);
  }
}

.setup-flow-step {
  min-height: 98px;
  align-items: flex-start;
  align-content: flex-start;
  gap: 10px;
  flex-wrap: wrap;
  padding: 12px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.78);
}

.setup-flow-step--active {
  border-color: $border-color-primary;
  background: rgba(var(--v-theme-primary), 0.05);
}

.setup-flow-step--complete {
  border-color: rgba(var(--v-theme-success), 0.28);
}

.setup-flow-step--pending {
  opacity: 0.78;
}

.setup-step-marker {
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  flex: 0 0 auto;
  border-radius: 999px;
  background: rgba(var(--v-theme-primary), 0.1);
  color: rgb(var(--v-theme-primary));
}

.setup-flow-step--complete .setup-step-marker {
  background: rgba(var(--v-theme-success), 0.12);
  color: rgb(var(--v-theme-success));
}

.setup-flow-step--pending .setup-step-marker {
  background: rgba(var(--v-theme-on-surface), 0.06);
  color: rgba(var(--v-theme-on-surface), 0.56);
}

.setup-step-copy {
  min-width: 0;
  flex: 1 1 calc(100% - 38px);
}

.setup-step-title {
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: $font-size-sm;
  font-weight: 700;
  line-height: 1.3;
}

.setup-step-detail {
  margin-top: 2px;
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.58);
  font-size: $font-size-xs;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.setup-flow-step :deep(.v-btn) {
  align-self: flex-end;
  margin-left: auto;
}

.provider-source-picker-tile {
  width: 100%;
  min-height: 64px;
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 12px 16px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.82);
  color: inherit;
  text-align: left;
  cursor: pointer;
  transition: all $transition-fast;

  &:hover {
    border-color: $border-color-primary;
    background: rgba(var(--v-theme-primary), 0.04);
    @include hover-lift($show-shadow: false);
  }
}

.selector-avatar {
  flex: 0 0 auto;
}

.selector-copy {
  min-width: 0;
  display: flex;
  flex: 1;
  flex-direction: column;
  gap: 2px;
}

.selector-title {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.92);
  font-size: $font-size-base;
  font-weight: 700;
  line-height: 1.25;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.selector-subtitle {
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.58);
  font-size: $font-size-xs;
  line-height: 1.25;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.selector-arrow {
  flex: 0 0 auto;
  color: rgba(var(--v-theme-on-surface), 0.4);
}

.credential-state-row {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.json-editor-field {
  font-family: "JetBrains Mono", "Fira Code", Consolas, monospace;
}

.json-editor-field :deep(textarea) {
  font-family: "JetBrains Mono", "Fira Code", Consolas, monospace;
  font-size: $font-size-sm;
  line-height: 1.5;
}

.section-label {
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: rgb(var(--v-theme-primary));
  opacity: 0.82;
}

.empty-state-panel {
  border: 2px dashed $border-color-soft;
  background: $surface-subtle;
  border-radius: $radius-lg;
}

.empty-provider-panel {
  min-height: 400px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  text-align: center;
}
</style>
