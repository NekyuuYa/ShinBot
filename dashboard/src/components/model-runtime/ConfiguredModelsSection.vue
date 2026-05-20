<template>
  <div>
    <div class="configured-model-header mb-3">
      <div>
        <div class="section-label">
          {{ $t("pages.modelRuntime.cards.configuredModels") }}
        </div>
        <div class="text-caption text-medium-emphasis mt-1">
          {{
            $t("pages.modelRuntime.labels.providerCount", {
              count: filteredModels.length,
            })
          }}
        </div>
      </div>

      <div v-if="models.length > 0" class="configured-model-tools">
        <v-text-field
          v-model="searchQuery"
          :placeholder="$t('common.actions.action.search')"
          prepend-inner-icon="mdi-magnify"
          variant="outlined"
          density="compact"
          clearable
          hide-details
          class="configured-model-search"
        />
        <v-btn-toggle
          v-model="viewMode"
          mandatory
          density="compact"
          class="model-view-toggle"
        >
          <v-btn
            value="table"
            icon="mdi-table"
            :title="$t('pages.modelRuntime.labels.tableView')"
            :aria-label="$t('pages.modelRuntime.labels.tableView')"
          />
          <v-btn
            value="card"
            icon="mdi-view-grid-outline"
            :title="$t('pages.modelRuntime.labels.cardView')"
            :aria-label="$t('pages.modelRuntime.labels.cardView')"
          />
        </v-btn-toggle>
      </div>
    </div>

    <v-table
      v-if="viewMode === 'table' && filteredModels.length > 0"
      density="compact"
      class="configured-model-table"
    >
      <thead>
        <tr>
          <th>{{ $t("pages.modelRuntime.fields.model") }}</th>
          <th>{{ $t("pages.modelRuntime.fields.litellmModel") }}</th>
          <th>{{ $t("pages.modelRuntime.fields.capabilities") }}</th>
          <th>{{ $t("pages.modelRuntime.fields.details") }}</th>
          <th>{{ $t("pages.modelRuntime.fields.enabled") }}</th>
          <th class="text-right">
            {{ $t("pages.modelRuntime.fields.actions") }}
          </th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="model in filteredModels" :key="model.id">
          <td class="model-name-cell">
            <div class="text-body-2 font-weight-medium table-text-clip">
              {{ model.displayName || model.id }}
            </div>
            <div class="text-caption text-medium-emphasis table-text-clip">
              {{ model.id }}
            </div>
          </td>
          <td class="litellm-cell">
            <code>{{ model.litellmModel }}</code>
          </td>
          <td>
            <div class="model-capability-chips">
              <v-chip
                v-for="capability in model.capabilities"
                :key="capability"
                size="x-small"
                variant="tonal"
                color="primary"
              >
                {{ capability }}
              </v-chip>
              <span
                v-if="model.capabilities.length === 0"
                class="text-caption text-medium-emphasis"
              >
                -
              </span>
            </div>
          </td>
          <td>
            <div class="model-meta-lines">
              <div v-for="line in providerModelMeta(model)" :key="line">
                {{ line }}
              </div>
            </div>
          </td>
          <td class="model-enabled-cell">
            <v-switch
              :model-value="model.enabled"
              color="primary"
              density="compact"
              hide-details
              inset
              @update:model-value="toggleModel(model.id, Boolean($event))"
            />
          </td>
          <td class="model-table-actions">
            <v-tooltip location="top">
              <template #activator="{ props }">
                <v-btn
                  v-bind="props"
                  icon="mdi-pencil-outline"
                  variant="text"
                  size="small"
                  color="primary"
                  @click="editModel(model.id)"
                />
              </template>
              <span>{{ $t("common.actions.action.edit") }}</span>
            </v-tooltip>
            <v-tooltip location="top">
              <template #activator="{ props }">
                <v-btn
                  v-bind="props"
                  icon="mdi-connection"
                  variant="text"
                  size="small"
                  color="info"
                  :loading="isProbing"
                  @click="probeModel(model.id)"
                />
              </template>
              <span>{{ $t("pages.modelRuntime.actions.testConnection") }}</span>
            </v-tooltip>
            <v-tooltip location="top">
              <template #activator="{ props }">
                <v-btn
                  v-bind="props"
                  icon="mdi-delete-outline"
                  variant="text"
                  size="small"
                  color="error"
                  @click="removeModel(model.id)"
                />
              </template>
              <span>{{ $t("common.actions.action.delete") }}</span>
            </v-tooltip>
          </td>
        </tr>
      </tbody>
    </v-table>

    <v-row v-else-if="filteredModels.length > 0">
      <v-col v-for="model in filteredModels" :key="model.id" cols="12" lg="6">
        <model-member-card
          :title="model.displayName || model.id"
          :subtitle="model.litellmModel"
          :enabled="model.enabled"
          :chips="model.capabilities"
          :meta-lines="providerModelMeta(model)"
          :show-probe="true"
          @edit="editModel(model.id)"
          @probe="probeModel(model.id)"
          @remove="removeModel(model.id)"
          @toggle="toggleModel(model.id, $event)"
        />
      </v-col>
    </v-row>

    <v-sheet v-else rounded="xl" class="empty-state-panel text-body-2 text-medium-emphasis py-6 px-5">
      {{
        models.length > 0 ? noMatchesText : noConfiguredText
      }}
    </v-sheet>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";

import type { ModelRuntimeModel } from "@/api/modelRuntime";
import ModelMemberCard from "./ModelMemberCard.vue";

const props = defineProps<{
  models: ModelRuntimeModel[]
  providerModelMeta: (model: ModelRuntimeModel) => string[]
  noMatchesText: string
  noConfiguredText: string
  isProbing: boolean
}>()

const emit = defineEmits<{
  edit: [modelId: string]
  probe: [modelId: string]
  remove: [modelId: string]
  toggle: [modelId: string, enabled: boolean]
}>()

const searchQuery = ref("")
const viewMode = ref<"table" | "card">("table")

const filteredModels = computed(() => {
  const keyword = searchQuery.value.trim().toLowerCase()
  if (!keyword) {
    return props.models
  }
  return props.models.filter((model) =>
    [
      model.id,
      model.displayName,
      model.litellmModel,
      ...model.capabilities,
      ...props.providerModelMeta(model),
    ]
      .join(" ")
      .toLowerCase()
      .includes(keyword),
  )
})

const editModel = (modelId: string) => emit("edit", modelId)
const probeModel = (modelId: string) => emit("probe", modelId)
const removeModel = (modelId: string) => emit("remove", modelId)
const toggleModel = (modelId: string, enabled: boolean) =>
  emit("toggle", modelId, enabled)
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.configured-model-header,
.configured-model-tools {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.configured-model-search {
  min-width: 280px;
  flex: 1 1 320px;
}

.model-view-toggle {
  flex: 0 0 auto;
}

.model-view-toggle :deep(.v-btn) {
  min-width: 40px;
}

.configured-model-table {
  overflow: hidden;
  border: 1px solid $border-color-soft;
  border-radius: $radius-lg;
  background: rgba(var(--v-theme-surface), 0.72);
}

.configured-model-table :deep(th) {
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: $font-size-xs;
  font-weight: 700;
  text-transform: uppercase;
}

.configured-model-table :deep(td) {
  vertical-align: middle;
}

.model-name-cell {
  min-width: 180px;
}

.litellm-cell {
  max-width: 220px;
}

.litellm-cell code {
  display: inline-block;
  max-width: 100%;
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.78);
  font-size: $font-size-xs;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.table-text-clip {
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.model-capability-chips {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.model-meta-lines {
  display: flex;
  flex-direction: column;
  gap: 2px;
  color: rgba(var(--v-theme-on-surface), 0.62);
  font-size: $font-size-xs;
  line-height: 1.35;
}

.model-enabled-cell {
  width: 80px;
}

.model-table-actions {
  width: 132px;
  white-space: nowrap;
}
</style>
