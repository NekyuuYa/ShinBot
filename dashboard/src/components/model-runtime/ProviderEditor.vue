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
    <provider-setup-flow-card
      :summary="providerSetupSummary"
      :complete="providerSetupComplete"
      :steps="providerSetupSteps"
      @action="runProviderSetupAction"
    />

    <provider-identity-card
      v-model:form="providerForm"
      v-model:token-visible="tokenVisible"
      :selected-provider="selectedProvider"
      :is-creating="isCreatingProvider"
      :saving="store.isSaving"
      :save-label="providerSaveLabel"
      :capability-type="providerCapabilityType"
      :source-icon="providerSourceIcon(providerForm.sourceType)"
      :source-title="currentProviderSourceTitle"
      :source-subtitle="currentProviderSourceSubtitle"
      :show-token-field="showProviderTokenField"
      :token-hint="tokenHint"
      :has-stored-credential="hasStoredCredential"
      :credential-will-be-cleared="credentialWillBeCleared"
      :credential-state-label="credentialStateLabel"
      :credential-action-label="credentialActionLabel"
      @pick-source="showProviderSourcePicker = true"
      @toggle-stored-credential-clear="toggleStoredCredentialClear"
      @delete="deleteCurrentProvider"
      @save="saveProvider"
    />

    <provider-advanced-card
      v-model:form="providerForm"
      v-model:header-rows="providerHeaderRows"
      :selected-provider="selectedProvider"
      :is-creating="isCreatingProvider"
      :probing="probingProviderId === selectedProvider?.id"
      :probe-result="lastProviderProbeResult"
      :probe-result-title="probeResultTitle"
      :probe-result-subtitle="probeResultSubtitle"
      :show-api-version-field="showApiVersionField"
      :source-supports-thinking="sourceSupportsThinking"
      :source-supports-filters="sourceSupportsFilters"
      :default-params-json-error="defaultParamsJsonError"
      :default-params-preview-json="defaultParamsPreviewJson"
      @probe="probeSelectedProvider()"
    />

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
import ProviderAdvancedCard from "./ProviderAdvancedCard.vue";
import ProviderIdentityCard from "./ProviderIdentityCard.vue";
import ProviderModelsPanel from "./ProviderModelsPanel.vue";
import ProviderSetupFlowCard, {
  type ProviderSetupAction,
  type ProviderSetupState,
  type ProviderSetupStep,
} from "./ProviderSetupFlowCard.vue";

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
  if (selectedProviderSource.value?.description) {
    return selectedProviderSource.value.description;
  }
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
    items: providerSourceOptions.value.map((source) => ({
      value: source.key,
      title: source.label,
      subtitle: source.description || source.defaultBaseUrl,
      icon: source.icon || providerSourceIcon(source.type),
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
