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
      <provider-model-form-card v-if="showInlineModelEditor" />
      <provider-configured-models />
      <provider-catalog-list />
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import ProviderCatalogList from "./ProviderCatalogList.vue";
import ProviderConfiguredModels from "./ProviderConfiguredModels.vue";
import ProviderModelFormCard from "./ProviderModelFormCard.vue";
import { useModelRuntimeContext } from "./runtimePageContext";

const {
  isCreatingProvider,
  selectedProvider,
  selectedProviderSource,
  fetchCatalogInline,
  catalogLoading,
  providerCanManageModels,
  openInlineModelEditor,
  showInlineModelEditor,
} = useModelRuntimeContext();
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.editor-card {
  @include surface-card;
}

.editor-card :deep(.v-card-item) {
  padding: 24px 24px 16px;
}
</style>
