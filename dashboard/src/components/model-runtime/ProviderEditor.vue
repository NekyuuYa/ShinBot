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
    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{
          $t("pages.modelRuntime.cards.providerIdentity")
        }}</v-card-title>
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
                <v-icon
                  :icon="providerSourceIcon(providerForm.sourceType)"
                  size="20"
                />
              </v-avatar>
              <span class="selector-copy">
                <span class="selector-title">{{
                  currentProviderSourceTitle
                }}</span>
                <span class="selector-subtitle">{{
                  currentProviderSourceSubtitle
                }}</span>
              </span>
              <v-icon icon="mdi-chevron-right" size="20" class="selector-arrow" />
            </button>
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
              type="password"
              :hint="
                selectedProvider?.hasAuth
                  ? $t('pages.modelRuntime.hints.tokenConfigured')
                  : $t('pages.modelRuntime.hints.token')
              "
              persistent-hint
            />
          </v-col>
          <v-col
            cols="12"
            :md="showProviderTokenField ? 4 : 6"
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
      </v-card-text>
    </v-card>

    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{ $t("pages.modelRuntime.cards.models") }}</v-card-title>
        <template #append>
          <div class="d-flex ga-2 flex-wrap justify-end">
            <v-btn
              variant="outlined"
              rounded="xl"
              color="info"
              :disabled="
                !selectedProvider ||
                isCreatingProvider ||
                !selectedProviderSource?.supportsCatalog
              "
              :loading="catalogLoading"
              @click="fetchCatalogInline"
            >
              {{ $t("pages.modelRuntime.actions.fetchCatalog") }}
            </v-btn>
            <v-btn
              color="primary"
              variant="tonal"
              rounded="xl"
              :disabled="!providerCanManageModels"
              @click="openInlineModelEditor()"
            >
              {{ $t("pages.modelRuntime.actions.addModel") }}
            </v-btn>
          </div>
        </template>
      </v-card-item>

      <v-card-text class="d-flex flex-column ga-5">
        <v-card v-if="showInlineModelEditor" class="model-editor-card" variant="outlined">
          <v-card-item>
            <v-card-title>{{
              $t("pages.modelRuntime.cards.modelEditor")
            }}</v-card-title>
            <template #append>
              <div class="d-flex ga-2">
                <v-btn variant="text" @click="cancelInlineModelEditor">
                  {{ $t("common.actions.action.cancel") }}
                </v-btn>
                <v-btn
                  color="primary"
                  variant="tonal"
                  rounded="xl"
                  class="action-btn"
                  @click="saveModel"
                >
                  {{ inlineModelSaveLabel }}
                </v-btn>
              </div>
            </template>
          </v-card-item>
          <v-card-text>
            <v-row>
              <v-col cols="12" md="6">
                <v-text-field
                  v-model="modelForm.id"
                  :label="$t('pages.modelRuntime.fields.id')"
                  density="comfortable"
                  variant="outlined"
                  :readonly="!!editingModelId"
                />
              </v-col>
              <v-col cols="12" md="6">
                <v-text-field
                  v-model="modelForm.displayName"
                  :label="$t('pages.modelRuntime.fields.displayName')"
                  density="comfortable"
                  variant="outlined"
                />
              </v-col>
              <v-col cols="12">
                <v-text-field
                  v-model="modelForm.litellmModel"
                  :label="$t('pages.modelRuntime.fields.litellmModel')"
                  density="comfortable"
                  variant="outlined"
                  append-inner-icon="mdi-database-search-outline"
                  :hint="$t('pages.modelRuntime.hints.modelIdPicker')"
                  persistent-hint
                  @click:append-inner="openModelIdPicker"
                />
              </v-col>
              <v-col cols="12" md="6">
                <v-alert
                  type="info"
                  variant="tonal"
                  density="comfortable"
                  class="model-context-window-alert"
                >
                  {{
                    $t("pages.modelRuntime.hints.contextWindowAuto", {
                      value: modelForm.contextWindow || "-",
                    })
                  }}
                </v-alert>
              </v-col>
              <v-col cols="12" md="6">
                <v-text-field
                  v-model="modelForm.inputPrice"
                  :label="$t('pages.modelRuntime.fields.inputPrice')"
                  :hint="priceHint"
                  persistent-hint
                  density="comfortable"
                  variant="outlined"
                  type="number"
                  min="0"
                  step="any"
                />
              </v-col>
              <v-col cols="12" md="6">
                <v-text-field
                  v-model="modelForm.outputPrice"
                  :label="$t('pages.modelRuntime.fields.outputPrice')"
                  :hint="priceHint"
                  persistent-hint
                  density="comfortable"
                  variant="outlined"
                  type="number"
                  min="0"
                  step="any"
                />
              </v-col>
              <v-col cols="12" md="6">
                <v-text-field
                  v-model="modelForm.cacheWritePrice"
                  :label="$t('pages.modelRuntime.fields.cacheWritePrice')"
                  :hint="priceHint"
                  persistent-hint
                  density="comfortable"
                  variant="outlined"
                  type="number"
                  min="0"
                  step="any"
                />
              </v-col>
              <v-col cols="12" md="6">
                <v-text-field
                  v-model="modelForm.cacheReadPrice"
                  :label="$t('pages.modelRuntime.fields.cacheReadPrice')"
                  :hint="priceHint"
                  persistent-hint
                  density="comfortable"
                  variant="outlined"
                  type="number"
                  min="0"
                  step="any"
                />
              </v-col>
              <v-col cols="12">
                <v-switch
                  v-model="modelForm.enabled"
                  color="primary"
                  inset
                  :label="$t('pages.modelRuntime.fields.enabled')"
                />
              </v-col>
            </v-row>
          </v-card-text>
        </v-card>

        <div>
          <div class="section-label mb-3">
            {{ $t("pages.modelRuntime.cards.configuredModels") }}
          </div>
          <v-row v-if="selectedProviderModels.length > 0">
            <v-col
              v-for="model in selectedProviderModels"
              :key="model.id"
              cols="12"
              lg="6"
            >
              <model-member-card
                :title="model.displayName || model.id"
                :subtitle="model.litellmModel"
                :enabled="model.enabled"
                :chips="model.capabilities"
                :meta-lines="providerModelMeta(model)"
                :show-probe="true"
                @edit="openInlineModelEditor(model.id)"
                @probe="probeSelectedProvider(model.id)"
                @remove="removeModel(model.id)"
                @toggle="toggleModel(model.id, $event)"
              />
            </v-col>
          </v-row>
          <v-sheet
            v-else
            rounded="xl"
            class="empty-state-panel text-body-2 text-medium-emphasis py-6 px-5"
          >
            {{ $t("pages.modelRuntime.hints.noConfiguredModels") }}
          </v-sheet>
        </div>

        <div v-if="availableCatalogItems.length > 0">
          <v-divider class="mb-4" />
          <div class="d-flex align-center justify-space-between mb-3">
            <span class="section-label">
              {{ $t("pages.modelRuntime.cards.availableModels") }}
            </span>
            <span class="text-caption text-medium-emphasis">
              {{ filteredCatalogItems.length }} / {{ availableCatalogItems.length }}
            </span>
          </div>
          <v-text-field
            v-model="catalogSearch"
            :placeholder="$t('common.actions.action.search')"
            prepend-inner-icon="mdi-magnify"
            variant="outlined"
            density="compact"
            clearable
            hide-details
            class="mb-3"
          />
          <div class="d-flex flex-column ga-3">
            <v-card
              v-for="item in filteredCatalogItems"
              :key="item.id"
              variant="outlined"
              class="catalog-item-card"
            >
              <v-card-text
                class="d-flex justify-space-between align-start ga-4 flex-wrap"
              >
                <div>
                  <div class="text-body-1 font-weight-medium">
                    {{ item.displayName }}
                  </div>
                  <div class="text-caption text-medium-emphasis">
                    {{ item.litellmModel }}
                  </div>
                  <div class="text-caption text-medium-emphasis mt-1">
                    {{
                      $t("pages.modelRuntime.hints.contextWindowAuto", {
                        value: item.contextWindow || "-",
                      })
                    }}
                  </div>
                </div>
                <v-btn
                  color="primary"
                  variant="tonal"
                  rounded="xl"
                  class="action-btn"
                  @click="importCatalogItem(item.id)"
                >
                  {{ $t("pages.modelRuntime.actions.addToConfigured") }}
                </v-btn>
              </v-card-text>
            </v-card>
          </div>
        </div>
      </v-card-text>
    </v-card>
  </div>

  <model-id-picker-dialog
    :model-value="showModelIdPicker"
    :current-value="modelForm.litellmModel"
    :route-options="modelIdPickerRouteOptions"
    :provider-groups="modelIdPickerProviderGroups"
    @update:model-value="closeModelIdPicker"
    @select="applyPickedModelId"
  />

  <generic-picker-dialog
    :model-value="showProviderSourcePicker"
    :title="$t('pages.modelRuntime.dialogs.providerSourcePicker')"
    :subtitle="$t('pages.modelRuntime.hints.providerSourcePicker')"
    :sections="providerSourcePickerSections"
    :selected="providerForm.sourceType ? [providerForm.sourceType] : []"
    :empty-text="$t('pages.modelRuntime.hints.providerSourcePickerEmpty')"
    :no-results-text="
      $t('pages.modelRuntime.hints.providerSourcePickerNoMatches')
    "
    @update:model-value="showProviderSourcePicker = $event"
    @update:selected="applyProviderSourcePick"
  />
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useI18n } from "vue-i18n";

import { useModelRuntimeContext } from "@/composables/useModelRuntimePage";
import { providerSourceIcon } from "@/utils/modelRuntimeSources";
import GenericPickerDialog, {
  type GenericPickerSection,
} from "./GenericPickerDialog.vue";
import KeyValueEditor from "./KeyValueEditor.vue";
import ModelIdPickerDialog from "./ModelIdPickerDialog.vue";
import ModelMemberCard from "./ModelMemberCard.vue";

const { t } = useI18n();
const showProviderSourcePicker = ref(false);

const {
  store,
  isCreatingProvider,
  selectedProvider,
  providerSaveLabel,
  providerForm,
  providerSourceOptions,
  onProviderSourceChange,
  showProviderTokenField,
  selectedProviderSource,
  sourceSupportsThinking,
  sourceSupportsFilters,
  showApiVersionField,
  probingProviderId,
  probeSelectedProvider,
  providerHeaderRows,
  fetchCatalogInline,
  catalogLoading,
  catalogSearch,
  pricingCurrency,
  pricingTokenUnit,
  providerCanManageModels,
  openInlineModelEditor,
  showInlineModelEditor,
  showModelIdPicker,
  cancelInlineModelEditor,
  saveModel,
  inlineModelSaveLabel,
  editingModelId,
  modelForm,
  modelIdPickerRouteOptions,
  modelIdPickerProviderGroups,
  openModelIdPicker,
  closeModelIdPicker,
  applyPickedModelId,
  selectedProviderModels,
  providerModelMeta,
  removeModel,
  toggleModel,
  availableCatalogItems,
  filteredCatalogItems,
  importCatalogItem,
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
  return (
    providerForm.value.sourceType ||
    t("pages.modelRuntime.hints.providerSourcePickerEmpty")
  );
});

const priceHint = computed(() =>
  t("pages.modelRuntime.hints.pricePerUnit", {
    currency: pricingCurrency,
    unit: t(`pages.settings.pricing.units.${pricingTokenUnit}`),
  }),
);

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

const applyProviderSourcePick = (values: string[]) => {
  onProviderSourceChange(values[0] ?? null);
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

.model-editor-card,
.catalog-item-card {
  border-radius: $radius-lg;
  border: 1px solid $border-color-soft;
  background: rgba(var(--v-theme-surface), 0.66);
  transition: all $transition-fast;
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
