<template>
  <model-id-picker-dialog
    :model-value="showModelIdPicker"
    :current-value="modelForm.litellmModel"
    :route-options="modelIdPickerRouteOptions"
    :provider-groups="modelIdPickerProviderGroups"
    @update:model-value="closeModelIdPicker"
    @select="applyPickedModelId"
  />

  <generic-picker-dialog
    :model-value="sourcePickerOpen"
    :title="$t('pages.modelRuntime.dialogs.providerSourcePicker')"
    :subtitle="$t('pages.modelRuntime.hints.providerSourcePicker')"
    :sections="providerSourcePickerSections"
    :selected="providerForm.sourceType ? [providerForm.sourceType] : []"
    :empty-text="$t('pages.modelRuntime.hints.providerSourcePickerEmpty')"
    :no-results-text="
      $t('pages.modelRuntime.hints.providerSourcePickerNoMatches')
    "
    @update:model-value="$emit('update:sourcePickerOpen', $event)"
    @update:selected="applyProviderSourcePick"
  />
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import GenericPickerDialog, {
  type GenericPickerSection,
} from "./GenericPickerDialog.vue";
import ModelIdPickerDialog from "./ModelIdPickerDialog.vue";
import { providerSourceIcon } from "./providerSourceIcon";
import { useModelRuntimeContext } from "./runtimePageContext";

defineProps<{
  sourcePickerOpen: boolean;
}>();

defineEmits<{
  "update:sourcePickerOpen": [value: boolean];
}>();

const { t } = useI18n();

const {
  providerForm,
  providerSourceOptions,
  onProviderSourceChange,
  showModelIdPicker,
  modelForm,
  modelIdPickerRouteOptions,
  modelIdPickerProviderGroups,
  closeModelIdPicker,
  applyPickedModelId,
} = useModelRuntimeContext();

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
