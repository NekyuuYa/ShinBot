<template>
  <v-card class="editor-card">
    <v-card-item>
      <v-card-title>{{ $t("pages.modelRuntime.cards.providerIdentity") }}</v-card-title>
      <template #append>
        <div class="d-flex ga-2">
          <v-btn
            color="error"
            variant="outlined"
            rounded="xl"
            :disabled="isCreating || !selectedProvider"
            @click="$emit('delete')"
          >
            {{ $t("common.actions.action.delete") }}
          </v-btn>
          <v-btn
            color="primary"
            variant="tonal"
            rounded="xl"
            :loading="saving"
            @click="$emit('save')"
          >
            {{ saveLabel }}
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
            @click="$emit('pick-source')"
          >
            <v-avatar
              size="36"
              color="primary"
              variant="tonal"
              class="selector-avatar"
            >
              <v-icon :icon="sourceIcon" size="20" />
            </v-avatar>
            <span class="selector-copy">
              <span class="selector-title">{{ sourceTitle }}</span>
              <span class="selector-subtitle">{{ sourceSubtitle }}</span>
            </span>
            <v-icon icon="mdi-chevron-right" size="20" class="selector-arrow" />
          </button>
        </v-col>
        <v-col cols="12" md="6">
          <v-text-field
            :model-value="$t(`pages.modelRuntime.labels.${capabilityType}`)"
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
            v-model="form.id"
            :label="$t('pages.modelRuntime.fields.id')"
            density="comfortable"
            variant="outlined"
            :hint="$t('pages.modelRuntime.hints.idEditable')"
            persistent-hint
          />
        </v-col>
        <v-col cols="12" md="6">
          <v-text-field
            v-model="form.displayName"
            :label="$t('pages.modelRuntime.fields.displayName')"
            density="comfortable"
            variant="outlined"
          />
        </v-col>
        <v-col cols="12" md="6">
          <v-text-field
            v-model="form.baseUrl"
            :label="$t('pages.modelRuntime.fields.baseUrl')"
            density="comfortable"
            variant="outlined"
          />
        </v-col>
        <v-col v-if="showTokenField" cols="12" md="8">
          <v-text-field
            v-model="form.token"
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
              @click="$emit('toggle-stored-credential-clear')"
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
          :md="showTokenField || credentialWillBeCleared ? 4 : 6"
          class="d-flex align-center"
        >
          <v-switch
            v-model="form.enabled"
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
import type { ModelRuntimeProvider } from "@/api/modelRuntime";
import type { ProviderFormState } from "@/composables/modelRuntime/types";

const form = defineModel<ProviderFormState>("form", { required: true });
const tokenVisible = defineModel<boolean>("tokenVisible", { required: true });

defineProps<{
  selectedProvider: ModelRuntimeProvider | null;
  isCreating: boolean;
  saving: boolean;
  saveLabel: string;
  capabilityType: string;
  sourceIcon: string;
  sourceTitle: string;
  sourceSubtitle: string;
  showTokenField: boolean;
  tokenHint: string;
  hasStoredCredential: boolean;
  credentialWillBeCleared: boolean;
  credentialStateLabel: string;
  credentialActionLabel: string;
}>();

defineEmits<{
  save: [];
  delete: [];
  "pick-source": [];
  "toggle-stored-credential-clear": [];
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
  transition: border-color $transition-fast, background-color $transition-fast, transform $transition-fast;

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
</style>
