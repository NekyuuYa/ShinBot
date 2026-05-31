<template>
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
          <v-card-title>{{ $t("pages.modelRuntime.cards.modelEditor") }}</v-card-title>
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
                v-model="modelForm.backendModel"
                :label="$t('pages.modelRuntime.fields.backendModel')"
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

      <configured-models-section
        :models="selectedProviderModels"
        :provider-model-meta="providerModelMeta"
        :no-matches-text="$t('pages.modelRuntime.hints.noModelMatches')"
        :no-configured-text="$t('pages.modelRuntime.hints.noConfiguredModels')"
        :is-probing="probingProviderId === selectedProvider?.id"
        @edit="openInlineModelEditor"
        @probe="probeSelectedProvider"
        @remove="removeModel"
        @toggle="toggleModel"
      />

      <catalog-import-section
        :items="availableCatalogItems"
        @import="importCatalogItem"
      />
    </v-card-text>
  </v-card>

  <model-id-picker-dialog
    :model-value="showModelIdPicker"
    :current-value="modelForm.backendModel"
    :route-options="modelIdPickerRouteOptions"
    :provider-groups="modelIdPickerProviderGroups"
    @update:model-value="closeModelIdPicker"
    @select="applyPickedModelId"
  />
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import { useModelRuntimeContext } from "@/composables/useModelRuntimePage";
import ConfiguredModelsSection from "./ConfiguredModelsSection.vue";
import CatalogImportSection from "./CatalogImportSection.vue";
import ModelIdPickerDialog from "./ModelIdPickerDialog.vue";

const { t } = useI18n();

const {
  isCreatingProvider,
  selectedProvider,
  selectedProviderSource,
  providerCanManageModels,
  selectedProviderModels,
  providerModelMeta,
  removeModel,
  toggleModel,
  availableCatalogItems,
  importCatalogItem,
  openInlineModelEditor,
  showInlineModelEditor,
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
  fetchCatalogInline,
  catalogLoading,
  showModelIdPicker,
  probeSelectedProvider,
  probingProviderId,
  pricingCurrency,
  pricingTokenUnit,
} = useModelRuntimeContext();

const priceHint = computed(() =>
  t("pages.modelRuntime.hints.pricePerUnit", {
    currency: pricingCurrency,
    unit: t(`pages.settings.pricing.units.${pricingTokenUnit}`),
  }),
);
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

.model-editor-card {
  @include surface-card;
}

.model-context-window-alert {
  height: 100%;
}
</style>
