<template>
  <v-card class="model-editor-card" variant="outlined">
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
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import { useModelRuntimeContext } from "./runtimePageContext";

const { t } = useI18n();

const {
  pricingCurrency,
  pricingTokenUnit,
  cancelInlineModelEditor,
  saveModel,
  inlineModelSaveLabel,
  editingModelId,
  modelForm,
  openModelIdPicker,
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

.model-editor-card {
  border-radius: $radius-lg;
  border: 1px solid $border-color-soft;
  background: rgba(var(--v-theme-surface), 0.66);
  transition: all $transition-fast;
}
</style>
