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

    <summary-metric-band :metrics="summaryMetrics" />

    <resource-filter-toolbar
      v-model:search="searchQuery"
      :filters="toolbarFilters"
      :search-label="$t('common.actions.action.search')"
      :layout-mode="toolsStore.layoutMode"
      :list-label="$t('pages.tools.layout.list')"
      :card-label="$t('pages.tools.layout.card')"
      @update:filter="handleFilterChange"
      @update:layout-mode="handleLayoutChange"
    />

    <resource-collection-view
      :items="filteredTools"
      :loading="showInitialSkeleton"
      :loaded="hasLoadedTools"
      :layout-mode="toolsStore.layoutMode"
      :empty-config="emptyConfig"
      :get-item-key="(tool) => tool.id"
    >
      <template #card="{ item: tool }">
        <tool-card :tool="tool" />
      </template>
      <template #row="{ item: tool }">
        <tool-list-row :key="tool.id" :tool="tool" />
      </template>
    </resource-collection-view>

    <v-alert v-if="toolsStore.error" type="error" class="mt-4">
      {{ toolsStore.error }}
    </v-alert>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import ResourceCollectionView from '@/components/resources/ResourceCollectionView.vue'
import ResourceFilterToolbar, {
  type ResourceFilter,
} from '@/components/resources/ResourceFilterToolbar.vue'
import SummaryMetricBand, {
  type SummaryMetric,
} from '@/components/resources/SummaryMetricBand.vue'
import ToolCard from '@/components/tools/ToolCard.vue'
import ToolListRow from '@/components/tools/ToolListRow.vue'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useToolsStore } from '@/stores/tools'
import { translate } from '@/plugins/i18n'
import type { ToolLayoutMode } from '@/stores/tools'

const toolsStore = useToolsStore()

const searchQuery = ref('')
const ownerTypeFilter = ref('all')
const visibilityFilter = ref('all')
const hasLoadedTools = ref(false)

const initialSkeletonRequested = computed(
  () => toolsStore.isLoading && toolsStore.tools.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

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

const summaryMetrics = computed<SummaryMetric[]>(() => [
  { key: 'total', label: translate('pages.tools.summary.total'), value: toolsStore.tools.length },
  { key: 'enabled', label: translate('pages.tools.summary.enabled'), value: toolsStore.enabledCount },
  { key: 'public', label: translate('pages.tools.summary.publicVisible'), value: toolsStore.publicCount },
])

const toolbarFilters = computed<ResourceFilter[]>(() => [
  {
    key: 'ownerType',
    label: translate('pages.tools.filters.ownerType'),
    value: ownerTypeFilter.value,
    items: ownerTypeItems.value,
  },
  {
    key: 'visibility',
    label: translate('pages.tools.filters.visibility'),
    value: visibilityFilter.value,
    items: visibilityItems.value,
  },
])

const emptyConfig = computed(() => ({
  icon: 'mdi-tools',
  title: translate('pages.tools.empty.title'),
  subtitle: translate('pages.tools.empty.subtitle'),
}))

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

async function loadTools(force = false) {
  try {
    await toolsStore.fetchTools({ force })
  } finally {
    hasLoadedTools.value = true
  }
}

onMounted(() => {
  void loadTools()
})

const handleRefresh = () => {
  void loadTools(true)
}

const handleLayoutChange = (mode: ToolLayoutMode) => {
  if (mode) {
    toolsStore.setLayoutMode(mode)
  }
}

const handleFilterChange = (key: string, value: string) => {
  if (key === 'ownerType') {
    ownerTypeFilter.value = value
  } else if (key === 'visibility') {
    visibilityFilter.value = value
  }
}
</script>
