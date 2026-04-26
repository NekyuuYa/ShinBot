<template>
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
</template>

<script setup lang="ts">
import ModelMemberCard from "./ModelMemberCard.vue";
import { useModelRuntimeContext } from "./runtimePageContext";

const {
  openInlineModelEditor,
  selectedProviderModels,
  providerModelMeta,
  removeModel,
  toggleModel,
  probeSelectedProvider,
} = useModelRuntimeContext();
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
</style>
