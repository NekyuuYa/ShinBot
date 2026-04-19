<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.modelRuntime.title')"
      :subtitle="$t('pages.modelRuntime.subtitle')"
      :kicker="$t('pages.modelRuntime.labels.workspace')"
    >
      <template #actions>
        <v-btn
          color="secondary"
          variant="tonal"
          prepend-icon="mdi-refresh"
          :loading="store.isLoading"
          rounded="lg"
          class="page-action-btn"
          @click="refreshPage"
        >
          {{ $t('pages.modelRuntime.actions.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <div class="runtime-toolbar mb-6">
      <v-btn-toggle
        v-model="activeTab"
        mandatory
        density="comfortable"
        class="runtime-tab-toggle"
        :style="{ '--runtime-tab-count': String(Math.max(runtimeTabs.length, 1)) }"
      >
        <v-btn
          v-for="tab in runtimeTabs"
          :key="tab.value"
          :value="tab.value"
          rounded="lg"
        >
          <v-icon :icon="tab.icon" size="18" class="me-1" />
          <span>{{ tab.label }}</span>
        </v-btn>
      </v-btn-toggle>
    </div>

    <v-row class="ma-0" align="start">
      <v-col cols="12" md="4" class="pa-0 pe-md-4">
        <sidebar-list-card
          :title="sidebarTitle"
          :empty-text="sidebarEmptyText"
          :items="sidebarItems"
          :active-id="sidebarActiveId"
          :add-icon="isRouteMode ? 'mdi-router-plus' : 'mdi-cloud-plus-outline'"
          :add-label="sidebarAddLabel"
          @add="startCreateCurrent"
          @select="handleSidebarSelect"
        />
      </v-col>

      <v-col cols="12" md="8" class="pa-0 runtime-main-pane">
        <div v-if="isRouteMode" class="d-flex flex-column ga-4">
          <v-card class="editor-card">
            <v-card-item>
              <v-card-title>{{ $t('pages.modelRuntime.cards.routeIdentity') }}</v-card-title>
              <template #append>
                <div class="d-flex ga-2">
                  <v-btn
                    color="error"
                    variant="outlined"
                    rounded="xl"
                    :disabled="isCreatingRoute || !selectedRoute"
                    @click="deleteCurrentRoute"
                  >
                    {{ $t('common.actions.action.delete') }}
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
              <v-card-title>{{ $t('pages.modelRuntime.cards.routePolicy') }}</v-card-title>
            </v-card-item>
            <v-card-text>
              <div class="d-flex flex-wrap ga-3 mb-4">
                <v-chip color="primary" variant="tonal">
                  {{ $t('pages.modelRuntime.labels.routeMemberCount', { count: routeMembersEditor.length }) }}
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
              <v-card-title>{{ $t('pages.modelRuntime.cards.routeMembersWorkbench') }}</v-card-title>
            </v-card-item>
            <v-card-text class="d-flex flex-column ga-4">
              <v-alert v-if="availableRouteModels.length === 0" variant="tonal" type="info">
                {{ $t('pages.modelRuntime.hints.noRouteModels') }}
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
                      <div class="d-flex align-start justify-space-between ga-4 flex-wrap">
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
                      @update:model-value="toggleRouteMember(model.id, Boolean($event))"
                    />
                  </div>

                  <v-expand-transition>
                    <div v-if="routeMemberByModel(model.id)" class="mt-4">
                      <v-row>
                        <v-col cols="12" md="4">
                          <v-text-field
                            :model-value="routeMemberByModel(model.id)?.priority ?? 0"
                            type="number"
                            density="comfortable"
                            variant="outlined"
                            :label="$t('pages.modelRuntime.fields.priority')"
                            @update:model-value="updateRouteMemberField(model.id, 'priority', Number($event || 0))"
                          />
                        </v-col>
                        <v-col cols="12" md="4">
                          <v-text-field
                            :model-value="routeMemberByModel(model.id)?.weight ?? 1"
                            type="number"
                            step="0.1"
                            density="comfortable"
                            variant="outlined"
                            :label="$t('pages.modelRuntime.fields.weight')"
                            @update:model-value="updateRouteMemberField(model.id, 'weight', Number($event || 1))"
                          />
                        </v-col>
                        <v-col cols="12" md="4">
                          <v-text-field
                            :model-value="routeMemberByModel(model.id)?.timeoutOverride ?? ''"
                            type="number"
                            step="0.1"
                            density="comfortable"
                            variant="outlined"
                            :label="$t('pages.modelRuntime.fields.timeoutOverride')"
                            @update:model-value="updateRouteTimeout(model.id, $event)"
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

        <div v-else-if="!selectedProvider && !isCreatingProvider" class="d-flex flex-column ga-4">
          <v-sheet rounded="xl" class="empty-state-panel empty-provider-panel pa-8">
            <div class="text-overline section-label mb-3">
              {{ $t('pages.modelRuntime.sidebar.providers') }}
            </div>
            <div class="text-h6 mb-2">
              {{ $t('pages.modelRuntime.hints.selectProviderSourceTitle') }}
            </div>
            <div class="text-body-2 text-medium-emphasis">
              {{ $t('pages.modelRuntime.hints.selectProviderSource') }}
            </div>
          </v-sheet>
        </div>

        <div v-else class="d-flex flex-column ga-4">
          <v-card class="editor-card">
            <v-card-item>
              <v-card-title>{{ $t('pages.modelRuntime.cards.providerIdentity') }}</v-card-title>
              <template #append>
                <div class="d-flex ga-2">
                  <v-btn
                    color="error"
                    variant="outlined"
                    rounded="xl"
                    :disabled="isCreatingProvider || !selectedProvider"
                    @click="deleteCurrentProvider"
                  >
                    {{ $t('common.actions.action.delete') }}
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
                  <v-select
                    :model-value="providerForm.sourceType"
                    :label="$t('pages.modelRuntime.fields.source')"
                    :items="providerSourceOptions"
                    item-title="label"
                    item-value="type"
                    density="comfortable"
                    variant="outlined"
                    @update:model-value="onProviderSourceChange"
                  />
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
                    :hint="selectedProvider?.hasAuth ? $t('pages.modelRuntime.hints.tokenConfigured') : $t('pages.modelRuntime.hints.token')"
                    persistent-hint
                  />
                </v-col>
                <v-col :cols="showProviderTokenField ? 12 : 12" :md="showProviderTokenField ? 4 : 6" class="d-flex align-center">
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

          <v-card class="editor-card">
            <v-card-item>
              <v-card-title>{{ $t('pages.modelRuntime.cards.advanced') }}</v-card-title>
              <template #append>
                <v-btn
                  color="info"
                  variant="outlined"
                  rounded="xl"
                  :loading="probingProviderId === selectedProvider?.id"
                  :disabled="!selectedProvider || isCreatingProvider"
                  @click="probeSelectedProvider()"
                >
                  {{ $t('pages.modelRuntime.actions.testConnection') }}
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
                  {{ $t('pages.modelRuntime.fields.requestHeaders') }}
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

          <v-card class="editor-card">
            <v-card-item>
              <v-card-title>{{ $t('pages.modelRuntime.cards.models') }}</v-card-title>
              <template #append>
                <div class="d-flex ga-2 flex-wrap justify-end">
                  <v-btn
                    variant="outlined"
                    rounded="xl"
                    color="info"
                    :disabled="!selectedProvider || isCreatingProvider || !selectedProviderSource?.supportsCatalog"
                    :loading="catalogLoading"
                    @click="fetchCatalogInline"
                  >
                    {{ $t('pages.modelRuntime.actions.fetchCatalog') }}
                  </v-btn>
                  <v-btn
                    color="primary"
                    variant="tonal"
                    rounded="xl"
                    :disabled="!providerCanManageModels"
                    @click="openInlineModelEditor()"
                  >
                    {{ $t('pages.modelRuntime.actions.addModel') }}
                  </v-btn>
                </div>
              </template>
            </v-card-item>
            <v-card-text class="d-flex flex-column ga-5">
              <v-card
                v-if="showInlineModelEditor"
                class="model-editor-card"
                variant="outlined"
              >
                <v-card-item>
                  <v-card-title>{{ $t('pages.modelRuntime.cards.modelEditor') }}</v-card-title>
                  <template #append>
                    <div class="d-flex ga-2">
                      <v-btn variant="text" @click="cancelInlineModelEditor">
                        {{ $t('common.actions.action.cancel') }}
                      </v-btn>
                      <v-btn color="primary" variant="tonal" rounded="xl" class="action-btn" @click="saveModel">
                        {{ inlineModelSaveLabel }}
                      </v-btn>
                    </div>
                  </template>
                </v-card-item>
                <v-card-text>
                  <v-row>
                    <v-col cols="12" md="6">
                      <v-text-field
                        v-model="modelForm.id"
                        :label="$t('pages.modelRuntime.fields.id')"
                        density="comfortable"
                        variant="outlined"
                        :readonly="!!editingModelId"
                      />
                    </v-col>
                    <v-col cols="12" md="6">
                      <v-text-field
                        v-model="modelForm.displayName"
                        :label="$t('pages.modelRuntime.fields.displayName')"
                        density="comfortable"
                        variant="outlined"
                      />
                    </v-col>
                    <v-col cols="12">
                      <v-text-field
                        v-model="modelForm.litellmModel"
                        :label="$t('pages.modelRuntime.fields.litellmModel')"
                        density="comfortable"
                        variant="outlined"
                      />
                    </v-col>
                    <v-col cols="12" md="6">
                      <v-alert
                        type="info"
                        variant="tonal"
                        density="comfortable"
                        class="model-context-window-alert"
                      >
                        {{
                          $t('pages.modelRuntime.hints.contextWindowAuto', {
                            value: modelForm.contextWindow || '—',
                          })
                        }}
                      </v-alert>
                    </v-col>
                    <v-col cols="12">
                      <v-switch
                        v-model="modelForm.enabled"
                        color="primary"
                        inset
                        :label="$t('pages.modelRuntime.fields.enabled')"
                      />
                    </v-col>
                  </v-row>
                </v-card-text>
              </v-card>

              <div>
                <div class="section-label mb-3">
                  {{ $t('pages.modelRuntime.cards.configuredModels') }}
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
                  {{ $t('pages.modelRuntime.hints.noConfiguredModels') }}
                </v-sheet>
              </div>

              <div v-if="availableCatalogItems.length > 0">
                <v-divider class="mb-4" />
                <div class="section-label mb-3">
                  {{ $t('pages.modelRuntime.cards.availableModels') }}
                </div>
                <div class="d-flex flex-column ga-3">
                  <v-card
                    v-for="item in availableCatalogItems"
                    :key="item.id"
                    variant="outlined"
                    class="catalog-item-card"
                  >
                    <v-card-text class="d-flex justify-space-between align-start ga-4 flex-wrap">
                      <div>
                        <div class="text-body-1 font-weight-medium">{{ item.displayName }}</div>
                        <div class="text-caption text-medium-emphasis">{{ item.litellmModel }}</div>
                        <div class="text-caption text-medium-emphasis mt-1">
                          {{
                            $t('pages.modelRuntime.hints.contextWindowAuto', {
                              value: item.contextWindow || '—',
                            })
                          }}
                        </div>
                      </div>
                      <v-btn
                        color="primary"
                        variant="tonal"
                        rounded="xl"
                        class="action-btn"
                        @click="importCatalogItem(item.id)"
                      >
                        {{ $t('pages.modelRuntime.actions.addToConfigured') }}
                      </v-btn>
                    </v-card-text>
                  </v-card>
                </div>
              </div>
            </v-card-text>
          </v-card>
        </div>
      </v-col>
    </v-row>

    <v-alert v-if="store.error" type="error" class="mt-6">
      {{ store.error }}
    </v-alert>
  </v-container>
</template>

<script setup lang="ts">
import AppPageHeader from '@/components/AppPageHeader.vue'
import SidebarListCard from '@/components/model-runtime/SidebarListCard.vue'
import KeyValueEditor from '@/components/model-runtime/KeyValueEditor.vue'
import ModelMemberCard from '@/components/model-runtime/ModelMemberCard.vue'
import { useModelRuntimePage } from '@/composables/useModelRuntimePage'

const {
  store,
  activeTab,
  runtimeTabs,
  isRouteMode,
  sidebarTitle,
  sidebarEmptyText,
  sidebarItems,
  sidebarActiveId,
  sidebarAddLabel,
  startCreateCurrent,
  handleSidebarSelect,
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
  isCreatingProvider,
  selectedProvider,
  providerSaveLabel,
  providerForm,
  providerSourceOptions,
  onProviderSourceChange,
  showProviderTokenField,
  selectedProviderSource,
  sourceSupportsThinking,
  sourceSupportsFilters,
  showApiVersionField,
  probingProviderId,
  probeSelectedProvider,
  providerHeaderRows,
  fetchCatalogInline,
  catalogLoading,
  providerCanManageModels,
  openInlineModelEditor,
  showInlineModelEditor,
  cancelInlineModelEditor,
  saveModel,
  inlineModelSaveLabel,
  editingModelId,
  modelForm,
  selectedProviderModels,
  providerModelMeta,
  removeModel,
  toggleModel,
  availableCatalogItems,
  importCatalogItem,
  deleteCurrentProvider,
  saveProvider,
  refreshPage,
} = useModelRuntimePage()
</script>

<style scoped>
.page-action-btn,
.action-btn {
  box-shadow: none;
}

.runtime-toolbar {
  padding: 14px 14px;
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  border-radius: 24px;
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.98) 0%, rgba(var(--v-theme-background), 0.98) 100%);
}

.runtime-tab-toggle {
  width: 100%;
  display: grid;
  grid-template-columns: repeat(var(--runtime-tab-count, 8), minmax(0, 1fr));
  gap: 8px;
  padding: 0;
  border: 0;
  border-radius: 0;
  background: transparent;
  overflow: visible;
}

.runtime-tab-toggle :deep(.v-btn) {
  width: 100%;
  min-width: 0;
  justify-content: center;
  min-height: 44px;
  padding-inline: 6px;
  border: 1px solid rgba(var(--v-theme-primary), 0.14);
  border-radius: 16px;
  font-weight: 700;
  font-size: 0.8rem;
  line-height: 1.1;
  text-transform: none;
  background: rgba(var(--v-theme-surface), 0.88);
  white-space: nowrap;
}

.runtime-tab-toggle :deep(.v-btn .v-btn__content) {
  min-width: 0;
  flex-wrap: nowrap;
}

.runtime-tab-toggle :deep(.v-btn .v-btn__content span) {
  overflow: hidden;
  text-overflow: ellipsis;
}

.runtime-tab-toggle :deep(.v-btn--active) {
  background: linear-gradient(180deg, rgba(var(--v-theme-primary), 0.3) 0%, rgba(var(--v-theme-primary), 0.18) 100%);
  color: rgba(var(--v-theme-on-surface), 0.95);
}

.editor-card {
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  border-radius: 24px;
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.98) 0%, rgba(var(--v-theme-background), 0.98) 100%);
  box-shadow: 0 10px 28px rgba(var(--v-theme-primary), 0.06);
}

.editor-card :deep(.v-card-item) {
  padding-bottom: 12px;
}

.runtime-main-pane :deep(.v-field--variant-outlined .v-field__outline) {
  --v-field-border-opacity: 1;
  color: rgba(var(--v-theme-primary), 0.16);
}

.runtime-main-pane :deep(.v-field) {
  border-radius: 18px;
  background: rgba(var(--v-theme-surface), 0.92);
}

.runtime-main-pane :deep(.v-field__input) {
  color: rgba(var(--v-theme-on-surface), 0.92);
}

.runtime-main-pane :deep(.v-label.v-field-label) {
  color: rgba(var(--v-theme-on-surface), 0.74);
}

.runtime-main-pane :deep(.v-input__details) {
  padding-inline: 6px;
}

.runtime-main-pane :deep(.v-messages__message) {
  color: rgba(var(--v-theme-primary), 0.72);
}

.route-member-row,
.catalog-item-card,
.model-editor-card {
  border-radius: 20px;
  border-color: rgba(var(--v-theme-primary), 0.14);
  background: rgba(var(--v-theme-surface), 0.78);
}

.section-label {
  font-size: 0.86rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(var(--v-theme-primary), 0.68);
}

.empty-state-panel {
  border: 1px dashed rgba(var(--v-theme-primary), 0.16);
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.95) 0%, rgba(var(--v-theme-surface), 0.78) 100%);
}

.empty-provider-panel {
  min-height: 340px;
  display: flex;
  flex-direction: column;
  justify-content: center;
}
</style>
