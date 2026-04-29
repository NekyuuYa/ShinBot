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
          {{ $t("pages.modelRuntime.actions.refresh") }}
        </v-btn>
      </template>
    </app-page-header>

    <div class="runtime-toolbar mb-6">
      <v-btn-toggle
        v-model="activeTab"
        mandatory
        density="comfortable"
        class="runtime-tab-toggle"
        :style="{
          '--runtime-tab-count': String(Math.max(runtimeTabs.length, 1)),
        }"
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
        <route-editor v-if="isRouteMode" />
        <provider-editor v-else />
      </v-col>
    </v-row>

    <v-alert v-if="store.error" type="error" class="mt-6">
      {{ store.error }}
    </v-alert>
  </v-container>
</template>

<script setup lang="ts">
import { provide } from "vue";

import AppPageHeader from "@/components/AppPageHeader.vue";
import ProviderEditor from "@/components/model-runtime/ProviderEditor.vue";
import RouteEditor from "@/components/model-runtime/RouteEditor.vue";
import SidebarListCard from "@/components/SidebarListCard.vue";
import {
  modelRuntimePageKey,
  useModelRuntimePage,
} from "@/composables/useModelRuntimePage";

const runtimePage = useModelRuntimePage();
provide(modelRuntimePageKey, runtimePage);

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
  refreshPage,
} = runtimePage;
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.page-action-btn {
  box-shadow: none;
}

.runtime-toolbar {
  padding: 14px 14px;
  @include surface-card;
}

.runtime-tab-toggle {
  width: 100%;
  display: grid;
  grid-template-columns: repeat(var(--runtime-tab-count, 8), minmax(0, 1fr));
  gap: 12px;
  padding: 0;
  border: 0;
  border-radius: 0;
  background: transparent;
  overflow: visible;

  @include respond-to("tablet") {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  @include respond-to("mobile") {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

.runtime-tab-toggle :deep(.v-btn) {
  width: 100%;
  min-width: 0;
  justify-content: center;
  min-height: 48px;
  padding-inline: 8px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  font-weight: 700;
  font-size: $font-size-sm;
  line-height: 1.1;
  text-transform: none;
  background: rgba(var(--v-theme-surface), 0.82);
  white-space: nowrap;
  transition: all $transition-fast;

  &:hover {
    border-color: $border-color-primary;
    background: rgba(var(--v-theme-primary), 0.04);
  }
}

.runtime-tab-toggle :deep(.v-btn--active) {
  background: linear-gradient(
    180deg,
    rgba(var(--v-theme-primary), 0.16) 0%,
    rgba(var(--v-theme-primary), 0.08) 100%
  );
  border-color: $border-color-primary;
  color: rgb(var(--v-theme-primary));
  box-shadow: 0 4px 12px rgba(var(--v-theme-primary), 0.08);
}

.runtime-main-pane :deep(.v-field) {
  border-radius: $radius-base;
}
</style>
