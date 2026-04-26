<template>
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
            @click="$emit('pick-source')"
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
          :cols="showProviderTokenField ? 12 : 12"
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
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import { providerSourceIcon } from "./providerSourceIcon";
import { useModelRuntimeContext } from "./runtimePageContext";

defineEmits<{
  "pick-source": [];
}>();

const { t } = useI18n();

const {
  store,
  isCreatingProvider,
  selectedProvider,
  providerSaveLabel,
  providerForm,
  showProviderTokenField,
  selectedProviderSource,
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
</style>
