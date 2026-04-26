<template>
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
</template>

<script setup lang="ts">
import KeyValueEditor from "./KeyValueEditor.vue";
import { useModelRuntimeContext } from "./runtimePageContext";

const {
  isCreatingProvider,
  selectedProvider,
  providerForm,
  sourceSupportsThinking,
  sourceSupportsFilters,
  showApiVersionField,
  probingProviderId,
  probeSelectedProvider,
  providerHeaderRows,
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
