<template>
  <v-card class="editor-card">
    <v-card-item>
      <v-card-title>{{ $t("pages.modelRuntime.cards.advanced") }}</v-card-title>
      <template #append>
        <v-btn
          color="info"
          variant="outlined"
          rounded="xl"
          :loading="probing"
          :disabled="!selectedProvider || isCreating"
          @click="$emit('probe')"
        >
          {{ $t("pages.modelRuntime.actions.testConnection") }}
        </v-btn>
      </template>
    </v-card-item>
    <v-card-text class="d-flex flex-column ga-5">
      <v-alert
        v-if="probeResult"
        :type="probeResult.success ? 'success' : 'warning'"
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
            v-model="form.apiVersion"
            :label="$t('pages.modelRuntime.fields.apiVersion')"
            density="comfortable"
            variant="outlined"
          />
        </v-col>
        <v-col cols="12" :md="showApiVersionField ? 6 : 12">
          <v-text-field
            v-model="form.proxyAddress"
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
        <key-value-editor v-model="headerRows" />
      </div>

      <v-textarea
        v-if="sourceSupportsThinking"
        v-model="form.thinkingJson"
        :label="$t('pages.modelRuntime.fields.thinkingConfig')"
        :hint="$t('pages.modelRuntime.hints.thinking')"
        persistent-hint
        rows="4"
        variant="outlined"
      />

      <v-textarea
        v-if="sourceSupportsFilters"
        v-model="form.filtersJson"
        :label="$t('pages.modelRuntime.fields.filtersConfig')"
        :hint="$t('pages.modelRuntime.hints.filters')"
        persistent-hint
        rows="4"
        variant="outlined"
      />

      <v-row>
        <v-col cols="12" md="6">
          <v-textarea
            v-model="form.defaultParamsJson"
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
</template>

<script setup lang="ts">
import type { ModelRuntimeProvider, ProviderProbeResult } from "@/api/modelRuntime";
import type { KeyValueEntry, ProviderFormState } from "@/composables/modelRuntime/types";
import KeyValueEditor from "./KeyValueEditor.vue";

const form = defineModel<ProviderFormState>("form", { required: true });
const headerRows = defineModel<KeyValueEntry[]>("headerRows", { required: true });

defineProps<{
  selectedProvider: ModelRuntimeProvider | null;
  isCreating: boolean;
  probing: boolean;
  probeResult: ProviderProbeResult | null;
  probeResultTitle: string;
  probeResultSubtitle: string;
  showApiVersionField: boolean;
  sourceSupportsThinking: boolean;
  sourceSupportsFilters: boolean;
  defaultParamsJsonError: string;
  defaultParamsPreviewJson: string;
}>();

defineEmits<{
  probe: [];
}>();
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.editor-card {
  @include surface-card;
}

.editor-card :deep(.v-card-item) {
  padding: 24px 24px 16px;
}

.json-editor-field {
  font-family: "JetBrains Mono", "Fira Code", Consolas, monospace;
}

.json-editor-field :deep(textarea) {
  font-family: "JetBrains Mono", "Fira Code", Consolas, monospace;
  font-size: $font-size-sm;
  line-height: 1.5;
}
</style>
