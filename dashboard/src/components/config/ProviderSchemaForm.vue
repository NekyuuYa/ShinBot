<template>
  <div class="provider-schema-form d-flex flex-column ga-4">
    <v-alert
      v-if="!provider"
      type="warning"
      variant="tonal"
      density="comfortable"
    >
      {{ emptyText }}
    </v-alert>

    <template v-else>
      <v-row>
        <v-col
          v-for="field in basicFields"
          :key="field.key"
          cols="12"
          :md="fieldColumnSpan(field)"
        >
          <provider-field-control
            :field="field"
            :model-value="fieldValue(field)"
            :model-ref-route-options="modelRefRouteOptions"
            :model-ref-provider-groups="modelRefProviderGroups"
            :disabled="disabled"
            :density="density"
            :json-error-text="jsonErrorText"
            @update:model-value="(value) => updateField(field, value)"
          />
        </v-col>
      </v-row>

      <v-expansion-panels v-if="advancedFields.length > 0" variant="accordion">
        <v-expansion-panel rounded="lg">
          <v-expansion-panel-title>
            {{ advancedLabel }}
          </v-expansion-panel-title>
          <v-expansion-panel-text>
            <v-row>
              <v-col
                v-for="field in advancedFields"
                :key="field.key"
                cols="12"
                :md="fieldColumnSpan(field)"
              >
                <provider-field-control
                  :field="field"
                  :model-value="fieldValue(field)"
                  :model-ref-route-options="modelRefRouteOptions"
                  :model-ref-provider-groups="modelRefProviderGroups"
                  :disabled="disabled"
                  :density="density"
                  :json-error-text="jsonErrorText"
                  @update:model-value="(value) => updateField(field, value)"
                />
              </v-col>
            </v-row>
          </v-expansion-panel-text>
        </v-expansion-panel>
      </v-expansion-panels>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

import type {
  ConfigProviderDefinition,
  ConfigRecord,
  ConfigValidationIssue,
  ConfigValue,
  ConfigWorkspaceProvider,
} from "@/api/config";
import {
  buildProviderFormFields,
  createProviderConfigDraft,
  getProviderFieldValue,
  isProviderFieldVisible,
  setProviderFieldValue,
  type ConfigFormField,
} from "@/config";
import ProviderFieldControl from "./ProviderFieldControl.vue";

type FieldDensity = "default" | "comfortable" | "compact";

interface ModelRefRouteOption {
  id: string;
  title: string;
  subtitle: string;
  enabled: boolean;
}

interface ModelRefProviderGroupItem {
  value: string;
  title: string;
  subtitle: string;
  kind: "catalog" | "configured";
}

interface ModelRefProviderGroup {
  providerId: string;
  providerName: string;
  providerType: string;
  items: ModelRefProviderGroupItem[];
}

interface Props {
  provider: ConfigProviderDefinition | ConfigWorkspaceProvider | null;
  modelValue: ConfigRecord;
  issues?: ConfigValidationIssue[];
  pathPrefix?: string;
  includeDeprecated?: boolean;
  fieldPrefixes?: string[];
  disabled?: boolean;
  density?: FieldDensity;
  modelRefRouteOptions?: ModelRefRouteOption[];
  modelRefProviderGroups?: ModelRefProviderGroup[];
  advancedLabel?: string;
  emptyText?: string;
  jsonErrorText?: string;
}

const props = withDefaults(defineProps<Props>(), {
  issues: () => [],
  pathPrefix: "",
  includeDeprecated: false,
  fieldPrefixes: () => [],
  disabled: false,
  density: "comfortable",
  modelRefRouteOptions: () => [],
  modelRefProviderGroups: () => [],
  advancedLabel: "Advanced",
  emptyText: "No configurable fields.",
  jsonErrorText: "Invalid JSON.",
});

const emit = defineEmits<{
  "update:modelValue": [value: ConfigRecord];
}>();

const { locale } = useI18n();

const fields = computed<ConfigFormField[]>(() => {
  if (!props.provider) {
    return [];
  }
  return buildProviderFormFields(props.provider, {
    issues: props.issues,
    pathPrefix: props.pathPrefix,
    includeDeprecated: props.includeDeprecated,
    locale: locale.value,
  });
});

const formValues = computed<ConfigRecord>(() => {
  if (!props.provider) {
    return props.modelValue;
  }
  return createProviderConfigDraft(props.provider, props.modelValue);
});

const matchesFieldPrefixes = (field: ConfigFormField) => {
  if (props.fieldPrefixes.length === 0) {
    return true;
  }
  return props.fieldPrefixes.some(
    (prefix) => field.path === prefix || field.path.startsWith(`${prefix}.`),
  );
};

const visibleFields = computed(() =>
  fields.value.filter(
    (field) =>
      matchesFieldPrefixes(field) &&
      isProviderFieldVisible(field, formValues.value),
  ),
);
const basicFields = computed(() =>
  visibleFields.value.filter((field) => !field.advanced),
);
const advancedFields = computed(() =>
  visibleFields.value.filter((field) => field.advanced),
);

function fieldColumnSpan(field: ConfigFormField) {
  if (
    field.component === "json" ||
    field.component === "array-object" ||
    field.component === "string-list" ||
    field.component === "integer-list"
  ) {
    return 12;
  }
  return 6;
}

function fieldValue(field: ConfigFormField) {
  return getProviderFieldValue(formValues.value, field);
}

function updateField(field: ConfigFormField, value: ConfigValue) {
  emit(
    "update:modelValue",
    setProviderFieldValue(props.modelValue, field, value),
  );
}
</script>
