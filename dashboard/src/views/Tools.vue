<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.tools.title')"
      :subtitle="$t('pages.tools.subtitle')"
      :kicker="$t('pages.tools.kicker')"
    >
      <template #actions>
        <v-btn color="secondary" prepend-icon="mdi-refresh" @click="handleRefresh">
          {{ $t('pages.tools.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-row class="mx-0 mb-6" align="stretch">
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.tools.summary.total') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ toolsStore.tools.length }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.tools.summary.enabled') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ toolsStore.enabledCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.tools.summary.publicVisible') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ toolsStore.publicCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-card rounded="xl" elevation="0" class="filter-card mb-6">
      <v-card-text>
        <v-row class="mx-0" align="center">
          <v-col cols="12" md="4" class="pa-2">
            <v-text-field
              v-model="searchQuery"
              :label="$t('common.actions.action.search')"
              prepend-inner-icon="mdi-magnify"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              bg-color="white"
            />
          </v-col>
          <v-col cols="12" sm="6" md="3" class="pa-2">
            <v-select
              v-model="ownerTypeFilter"
              :label="$t('pages.tools.filters.ownerType')"
              :items="ownerTypeItems"
              item-title="title"
              item-value="value"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              bg-color="white"
            />
          </v-col>
          <v-col cols="12" sm="6" md="3" class="pa-2">
            <v-select
              v-model="visibilityFilter"
              :label="$t('pages.tools.filters.visibility')"
              :items="visibilityItems"
              item-title="title"
              item-value="value"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              bg-color="white"
            />
          </v-col>
          <v-col cols="12" md="2" class="pa-2 d-flex justify-end">
            <layout-mode-button
              :model-value="toolsStore.layoutMode"
              :list-label="$t('pages.tools.layout.list')"
              :card-label="$t('pages.tools.layout.card')"
              @update:model-value="handleLayoutChange"
            />
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-row v-if="toolsStore.isLoading && toolsStore.tools.length === 0" class="mx-0">
      <v-col cols="12" class="pa-0">
        <v-skeleton-loader type="list-item-two-line, list-item-two-line, list-item-two-line" />
      </v-col>
    </v-row>

    <v-row v-else-if="filteredTools.length === 0" justify="center" class="mx-0 py-12">
      <v-col cols="12" md="6" class="text-center pa-0">
        <v-icon size="120" color="grey-lighten-1" icon="mdi-tools" />
        <h3 class="text-h6 my-4">{{ $t('pages.tools.empty.title') }}</h3>
        <p class="text-body-2 text-medium-emphasis">{{ $t('pages.tools.empty.subtitle') }}</p>
      </v-col>
    </v-row>

    <tool-collection
      v-else
      :tools="filteredTools"
      :layout-mode="toolsStore.layoutMode"
    />

    <v-alert v-if="toolsStore.error" type="error" class="mt-4">
      {{ toolsStore.error }}
    </v-alert>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import ToolCollection from '@/components/tools/ToolCollection.vue'
import { useToolsStore } from '@/stores/tools'
import { translate } from '@/plugins/i18n'
import type { ToolLayoutMode } from '@/stores/tools'

const toolsStore = useToolsStore()

const searchQuery = ref('')
const ownerTypeFilter = ref('all')
const visibilityFilter = ref('all')

const ownerTypeItems = computed(() => [
  { title: translate('pages.tools.filters.all'), value: 'all' },
  { title: translate('pages.tools.ownerTypeOptions.builtin_module'), value: 'builtin_module' },
  { title: translate('pages.tools.ownerTypeOptions.plugin'), value: 'plugin' },
  { title: translate('pages.tools.ownerTypeOptions.adapter_bridge'), value: 'adapter_bridge' },
  { title: translate('pages.tools.ownerTypeOptions.skill_module'), value: 'skill_module' },
  { title: translate('pages.tools.ownerTypeOptions.external_bridge'), value: 'external_bridge' },
])

const visibilityItems = computed(() => [
  { title: translate('pages.tools.filters.all'), value: 'all' },
  { title: translate('pages.tools.visibilityOptions.public'), value: 'public' },
  { title: translate('pages.tools.visibilityOptions.scoped'), value: 'scoped' },
  { title: translate('pages.tools.visibilityOptions.private'), value: 'private' },
])

const filteredTools = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()

  return toolsStore.tools.filter((tool) => {
    const matchesQuery =
      !query ||
      [
        tool.id,
        tool.name,
        tool.displayName,
        tool.description,
        tool.ownerId,
        tool.permission,
        ...tool.tags,
      ]
        .filter(Boolean)
        .some((value) => value.toLowerCase().includes(query))

    const matchesOwnerType =
      ownerTypeFilter.value === 'all' || tool.ownerType === ownerTypeFilter.value

    const matchesVisibility =
      visibilityFilter.value === 'all' || tool.visibility === visibilityFilter.value

    return matchesQuery && matchesOwnerType && matchesVisibility
  })
})

onMounted(() => {
  toolsStore.fetchTools()
})

const handleRefresh = () => {
  toolsStore.fetchTools()
}

const handleLayoutChange = (mode: ToolLayoutMode) => {
  if (mode) {
    toolsStore.setLayoutMode(mode)
  }
}
</script>

<style scoped>
.summary-card,
.filter-card {
  border: 1px solid rgba(120, 86, 0, 0.12);
  background: linear-gradient(180deg, #fffef4 0%, #fffdf8 100%);
}
</style>
