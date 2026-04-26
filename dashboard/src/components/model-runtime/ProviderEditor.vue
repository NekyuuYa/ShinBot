<template>
  <provider-empty-state v-if="!selectedProvider && !isCreatingProvider" />

  <div v-else class="d-flex flex-column ga-4">
    <provider-identity-card @pick-source="showProviderSourcePicker = true" />
    <provider-advanced-card />
    <provider-models-card />
  </div>

  <provider-runtime-dialogs
    v-model:source-picker-open="showProviderSourcePicker"
  />
</template>

<script setup lang="ts">
import { ref } from "vue";

import ProviderAdvancedCard from "./ProviderAdvancedCard.vue";
import ProviderEmptyState from "./ProviderEmptyState.vue";
import ProviderIdentityCard from "./ProviderIdentityCard.vue";
import ProviderModelsCard from "./ProviderModelsCard.vue";
import ProviderRuntimeDialogs from "./ProviderRuntimeDialogs.vue";
import { useModelRuntimeContext } from "./runtimePageContext";

const showProviderSourcePicker = ref(false);

const { isCreatingProvider, selectedProvider } = useModelRuntimeContext();
</script>
