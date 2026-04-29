<template>
  <div class="d-flex flex-column ga-4">
    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{
          $t("pages.modelRuntime.cards.routeIdentity")
        }}</v-card-title>
        <template #append>
          <div class="d-flex ga-2">
            <v-btn
              color="error"
              variant="outlined"
              rounded="xl"
              :disabled="isCreatingRoute || !selectedRoute"
              @click="deleteCurrentRoute"
            >
              {{ $t("common.actions.action.delete") }}
            </v-btn>
            <v-btn
              color="primary"
              variant="tonal"
              rounded="xl"
              class="action-btn"
              :loading="store.isSaving"
              @click="saveRoute"
            >
              {{ routeSaveLabel }}
            </v-btn>
          </div>
        </template>
      </v-card-item>
      <v-card-text>
        <v-row>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="routeForm.id"
              :label="$t('pages.modelRuntime.fields.id')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="routeForm.purpose"
              :label="$t('pages.modelRuntime.fields.purpose')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="routeForm.domain"
              :label="$t('pages.modelRuntime.fields.domain')"
              :items="routeDomainOptions"
              item-title="label"
              item-value="value"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="4">
            <v-select
              v-model="routeForm.strategy"
              :label="$t('pages.modelRuntime.fields.strategy')"
              :items="routeStrategies"
              density="comfortable"
              variant="outlined"
            />
          </v-col>
          <v-col cols="12" md="4" class="d-flex align-center">
            <v-switch
              v-model="routeForm.enabled"
              color="primary"
              inset
              :label="$t('pages.modelRuntime.fields.enabled')"
            />
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{
          $t("pages.modelRuntime.cards.routePolicy")
        }}</v-card-title>
      </v-card-item>
      <v-card-text>
        <div class="d-flex flex-wrap ga-3 mb-4">
          <v-chip color="primary" variant="tonal">
            {{
              $t("pages.modelRuntime.labels.routeMemberCount", {
                count: routeMembersEditor.length,
              })
            }}
          </v-chip>
          <v-chip color="info" variant="tonal">
            {{ activeRouteDomainLabel }}
          </v-chip>
        </div>
        <v-row>
          <v-col cols="12" md="6">
            <v-switch
              v-model="routeForm.stickySessions"
              color="primary"
              inset
              :label="$t('pages.modelRuntime.fields.stickySessions')"
            />
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-card class="editor-card">
      <v-card-item>
        <v-card-title>{{
          $t("pages.modelRuntime.cards.routeMembersWorkbench")
        }}</v-card-title>
      </v-card-item>
      <v-card-text class="d-flex flex-column ga-4">
        <v-alert
          v-if="availableRouteModels.length === 0"
          variant="tonal"
          type="info"
        >
          {{ $t("pages.modelRuntime.hints.noRouteModels") }}
        </v-alert>

        <template v-else>
          <div
            v-for="group in availableRouteModelsGrouped"
            :key="group.providerId"
            class="route-member-group"
          >
            <div class="text-caption text-medium-emphasis mb-2 px-1">
              {{ group.providerName }}
            </div>
            <v-card
              v-for="model in group.models"
              :key="model.id"
              class="route-member-row mb-3"
              variant="outlined"
            >
              <v-card-text>
                <div
                  class="d-flex align-start justify-space-between ga-4 flex-wrap"
                >
                  <div>
                    <div class="text-body-1 font-weight-medium">
                      {{ model.displayName || model.id }}
                    </div>
                    <div class="text-caption text-medium-emphasis">
                      {{ model.id }}
                    </div>
                    <div class="d-flex flex-wrap ga-2 mt-2">
                      <v-chip
                        v-for="capability in model.capabilities"
                        :key="capability"
                        size="x-small"
                        variant="tonal"
                        color="primary"
                      >
                        {{ capability }}
                      </v-chip>
                    </div>
                  </div>

                  <v-switch
                    :model-value="isRouteMemberEnabled(model.id)"
                    color="primary"
                    inset
                    hide-details
                    @update:model-value="
                      toggleRouteMember(model.id, Boolean($event))
                    "
                  />
                </div>

                <v-expand-transition>
                  <div v-if="routeMemberByModel(model.id)" class="mt-4">
                    <v-row>
                      <v-col cols="12" md="4">
                        <v-text-field
                          :model-value="
                            routeMemberByModel(model.id)?.priority ?? 0
                          "
                          type="number"
                          density="comfortable"
                          variant="outlined"
                          :label="$t('pages.modelRuntime.fields.priority')"
                          @update:model-value="
                            updateRouteMemberField(
                              model.id,
                              'priority',
                              Number($event || 0),
                            )
                          "
                        />
                      </v-col>
                      <v-col cols="12" md="4">
                        <v-text-field
                          :model-value="
                            routeMemberByModel(model.id)?.weight ?? 1
                          "
                          type="number"
                          step="0.1"
                          density="comfortable"
                          variant="outlined"
                          :label="$t('pages.modelRuntime.fields.weight')"
                          @update:model-value="
                            updateRouteMemberField(
                              model.id,
                              'weight',
                              Number($event || 1),
                            )
                          "
                        />
                      </v-col>
                      <v-col cols="12" md="4">
                        <v-text-field
                          :model-value="
                            routeMemberByModel(model.id)?.timeoutOverride ?? ''
                          "
                          type="number"
                          step="0.1"
                          density="comfortable"
                          variant="outlined"
                          :label="
                            $t('pages.modelRuntime.fields.timeoutOverride')
                          "
                          @update:model-value="
                            updateRouteTimeout(model.id, $event)
                          "
                        />
                      </v-col>
                    </v-row>
                  </div>
                </v-expand-transition>
              </v-card-text>
            </v-card>
          </div>
        </template>
      </v-card-text>
    </v-card>
  </div>
</template>

<script setup lang="ts">
import { useModelRuntimeContext } from "@/composables/useModelRuntimePage";

const {
  store,
  isCreatingRoute,
  selectedRoute,
  routeSaveLabel,
  routeForm,
  routeDomainOptions,
  routeStrategies,
  saveRoute,
  deleteCurrentRoute,
  routeMembersEditor,
  activeRouteDomainLabel,
  availableRouteModels,
  availableRouteModelsGrouped,
  isRouteMemberEnabled,
  toggleRouteMember,
  routeMemberByModel,
  updateRouteMemberField,
  updateRouteTimeout,
} = useModelRuntimeContext();
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

.route-member-row {
  border-radius: $radius-lg;
  border: 1px solid $border-color-soft;
  background: rgba(var(--v-theme-surface), 0.66);
  transition: all $transition-fast;
}
</style>
