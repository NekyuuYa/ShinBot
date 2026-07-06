<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.plugins.title')"
      :subtitle="$t('pages.plugins.subtitle')"
      :kicker="$t('pages.plugins.kicker')"
    >
      <template #actions>
        <v-btn color="primary" prepend-icon="mdi-puzzle-plus" @click="installDialogVisible = true" class="me-2">
          {{ $t('pages.plugins.install.open') }}
        </v-btn>
        <v-btn color="secondary" prepend-icon="mdi-reload" @click="handleReload" class="me-2">
          {{ $t('pages.plugins.reload') }}
        </v-btn>
        <v-btn color="secondary" prepend-icon="mdi-magnify" @click="handleRescan">
          {{ $t('pages.plugins.rescan') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-row class="mb-4 mx-0" align="center">
      <v-col cols="12" md="6" class="pa-0">
        <v-tabs v-model="activeTab" color="primary" density="comfortable">
          <v-tab value="installed">
            <v-icon start icon="mdi-puzzle" />
            {{ $t('pages.plugins.tabs.installed') }}
          </v-tab>
          <v-tab value="marketplace">
            <v-icon start icon="mdi-storefront-outline" />
            {{ $t('pages.plugins.tabs.marketplace') }}
          </v-tab>
        </v-tabs>
      </v-col>
      <v-spacer />
      <v-col v-if="activeTab === 'marketplace'" cols="12" md="auto" class="pa-0">
        <v-select
          v-if="pluginsStore.marketplaceSources.length > 1"
          v-model="selectedSourceId"
          :items="sourceOptions"
          item-title="name"
          item-value="id"
          density="compact"
          variant="outlined"
          hide-details
          @update:model-value="handleSourceChange"
        />
        <v-chip v-else-if="pluginsStore.marketplaceSource" color="primary" variant="tonal" class="text-break">
          <v-icon start icon="mdi-github" />
          {{ pluginsStore.marketplaceSource.name }}
        </v-chip>
      </v-col>
    </v-row>

    <v-row class="mb-6 mx-0" align="center">
      <v-col cols="12" sm="8" md="4" class="pa-0">
        <v-text-field
          v-model="searchQuery"
          :label="$t('common.actions.action.search')"
          prepend-inner-icon="mdi-magnify"
          single-line
          hide-details
          density="comfortable"
          variant="outlined"
          bg-color="surface"
          rounded="lg"
        />
      </v-col>
      <v-spacer />
      <v-col cols="auto" class="pa-0">
        <v-btn
          icon="mdi-refresh"
          variant="outlined"
          :loading="activeTab === 'marketplace' ? pluginsStore.isMarketplaceLoading : pluginsStore.isLoading"
          @click="handleRefresh"
          rounded="lg"
        />
      </v-col>
    </v-row>

    <v-row v-if="activeTab === 'installed' && showInitialSkeleton" class="mx-0">
      <v-col cols="12" class="pa-0">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row
      v-else-if="activeTab === 'installed' && !initialSkeletonRequested && filteredPlugins.length === 0"
      justify="center"
      class="py-12 mx-0"
    >
      <v-col cols="12" sm="8" md="6" class="text-center pa-0">
        <v-icon size="120" color="grey-lighten-1" icon="mdi-puzzle-outline" />
        <h3 class="text-h6 my-4">{{ $t('pages.plugins.noData') }}</h3>
      </v-col>
    </v-row>

    <v-row v-else-if="activeTab === 'installed'" class="mx-n4">
      <v-col v-for="plugin in filteredPlugins" :key="plugin.id" cols="12" sm="6" md="4" lg="3" class="pa-4">
        <plugin-card
          :plugin="plugin"
          @configure="openConfigDialog"
          @update="handleUpdatePlugin"
          @uninstall="handleUninstallPlugin"
        />
      </v-col>
    </v-row>

    <v-row v-else-if="pluginsStore.isMarketplaceLoading && pluginsStore.marketplaceItems.length === 0" class="mx-0">
      <v-col cols="12" class="pa-0">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row
      v-else-if="filteredMarketplaceItems.length === 0"
      justify="center"
      class="py-12 mx-0"
    >
      <v-col cols="12" sm="8" md="6" class="text-center pa-0">
        <v-icon size="120" color="grey-lighten-1" icon="mdi-storefront-outline" />
        <h3 class="text-h6 my-4">{{ $t('pages.plugins.marketplace.noData') }}</h3>
      </v-col>
    </v-row>

    <v-row v-else class="mx-n4">
      <v-col
        v-for="item in filteredMarketplaceItems"
        :key="item.plugin_id"
        cols="12"
        sm="6"
        md="4"
        lg="3"
        class="pa-4"
      >
        <plugin-marketplace-card
          :item="item"
          :loading="marketplaceActionId === item.plugin_id"
          @install="handleInstallMarketplacePlugin"
          @update="handleUpdateMarketplacePlugin"
        />
      </v-col>
    </v-row>

    <v-alert v-if="activeTab === 'installed' && pluginsStore.error" type="error" class="mt-4">
      {{ pluginsStore.error }}
    </v-alert>

    <v-alert v-if="activeTab === 'marketplace' && pluginsStore.marketplaceError" type="error" class="mt-4">
      {{ pluginsStore.marketplaceError }}
    </v-alert>

    <plugin-install-dialog
      v-model="installDialogVisible"
      @completed="handleInstallCompleted"
    />

    <v-dialog v-model="dialogVisible" max-width="760">
      <v-card>
        <v-card-title>
          {{ $t('pages.plugins.dialog.title', { name: activePlugin?.name ?? '' }) }}
        </v-card-title>
        <v-card-text>
          <v-alert type="info" variant="tonal" class="mb-4">
            {{ $t('pages.plugins.dialog.hint') }}
          </v-alert>

          <schema-form
            v-if="activeSchema"
            v-model="schemaForm"
            :schema="activeSchema"
          />

          <v-alert v-else type="warning" variant="tonal">
            {{ $t('pages.plugins.schema.empty') }}
          </v-alert>
        </v-card-text>
        <v-card-actions>
          <v-spacer />
          <v-btn variant="text" @click="dialogVisible = false">{{ $t('common.actions.action.cancel') }}</v-btn>
          <v-btn color="primary" @click="saveConfig">{{ $t('common.actions.action.save') }}</v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { usePluginsStore } from '@/stores/plugins'
import PluginCard from '@/components/PluginCard.vue'
import PluginInstallDialog from '@/components/plugins/PluginInstallDialog.vue'
import PluginMarketplaceCard from '@/components/plugins/PluginMarketplaceCard.vue'
import SchemaForm from '@/components/SchemaForm.vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import type { Plugin, PluginConfigSchema, PluginMarketplaceItem } from '@/api/plugins'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { translate } from '@/plugins/i18n'

const pluginsStore = usePluginsStore()
const { confirm } = useConfirmDialog()
const searchQuery = ref('')
const activeTab = ref<'installed' | 'marketplace'>('installed')
const marketplaceActionId = ref('')
const dialogVisible = ref(false)
const installDialogVisible = ref(false)
const activePlugin = ref<Plugin | null>(null)
const activeSchema = ref<PluginConfigSchema | null>(null)
const schemaForm = ref<Record<string, unknown>>({})
const selectedSourceId = ref('official')

const sourceOptions = computed(() =>
  pluginsStore.marketplaceSources.map((s) => ({ id: s.id, name: s.name }))
)

const handleSourceChange = (sourceId: string) => {
  void pluginsStore.fetchMarketplace(sourceId)
}

const initialSkeletonRequested = computed(
  () => pluginsStore.isLoading && pluginsStore.plugins.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const filteredPlugins = computed(() =>
  pluginsStore.plugins.filter((plugin) =>
    pluginMatchesQuery(plugin.name, plugin.id, plugin.description)
  )
)

const filteredMarketplaceItems = computed(() =>
  pluginsStore.marketplaceItems.filter((plugin) =>
    pluginMatchesQuery(plugin.name, plugin.plugin_id, plugin.description)
  )
)

onMounted(() => {
  void pluginsStore.fetchPlugins()
  void pluginsStore.fetchInstallSources()
  void pluginsStore.fetchMarketplaceSources()
})

watch(activeTab, (next) => {
  if (next === 'marketplace' && pluginsStore.marketplaceItems.length === 0) {
    void pluginsStore.fetchMarketplace()
  }
})

const pluginMatchesQuery = (...values: Array<string | undefined>) => {
  const query = searchQuery.value.trim().toLowerCase()
  if (!query) {
    return true
  }
  return values.some((value) => (value ?? '').toLowerCase().includes(query))
}

const handleRefresh = () => {
  if (activeTab.value === 'marketplace') {
    void pluginsStore.fetchMarketplace(pluginsStore.marketplaceSource?.id ?? 'official', {
      refresh: true,
    })
    return
  }
  void pluginsStore.fetchPlugins({ force: true })
}

const handleReload = async () => {
  await pluginsStore.reloadPlugins()
}

const handleRescan = async () => {
  await pluginsStore.rescanPlugins()
}

const handleInstallCompleted = () => {
  void pluginsStore.fetchPlugins({ force: true })
  void pluginsStore.fetchInstallSources()
  if (pluginsStore.marketplaceItems.length > 0 || activeTab.value === 'marketplace') {
    void pluginsStore.fetchMarketplace(pluginsStore.marketplaceSource?.id ?? 'official')
  }
}

const handleInstallMarketplacePlugin = async (item: PluginMarketplaceItem) => {
  marketplaceActionId.value = item.plugin_id
  try {
    await pluginsStore.installMarketplacePlugin(item.plugin_id, {
      source: pluginsStore.marketplaceSource?.id ?? 'official',
      enable_after_install: true,
      allow_overwrite: false,
    })
  } finally {
    marketplaceActionId.value = ''
  }
}

const handleUpdateMarketplacePlugin = async (item: PluginMarketplaceItem) => {
  const confirmed = await confirm({
    title: translate('pages.plugins.marketplace.confirmUpdateTitle'),
    message: translate('pages.plugins.marketplace.confirmUpdateMessage', { name: item.name }),
    confirmText: translate('pages.plugins.marketplace.update'),
    confirmColor: 'primary',
  })

  if (!confirmed) {
    return
  }

  marketplaceActionId.value = item.plugin_id
  try {
    await pluginsStore.installMarketplacePlugin(item.plugin_id, {
      source: pluginsStore.marketplaceSource?.id ?? 'official',
      enable_after_install: true,
      allow_overwrite: true,
    })
  } finally {
    marketplaceActionId.value = ''
  }
}

const handleUpdatePlugin = async (plugin: Plugin) => {
  const confirmed = await confirm({
    title: translate('pages.plugins.install.confirmUpdateTitle'),
    message: translate('pages.plugins.install.confirmUpdateMessage', { name: plugin.name }),
    confirmText: translate('pages.plugins.card.update'),
    confirmColor: 'primary',
  })

  if (!confirmed) {
    return
  }

  await pluginsStore.updateInstalledPlugin(plugin.id, true)
}

const handleUninstallPlugin = async (plugin: Plugin) => {
  const confirmed = await confirm({
    title: translate('pages.plugins.install.confirmUninstallTitle'),
    message: translate('pages.plugins.install.confirmUninstallMessage', { name: plugin.name }),
    confirmText: translate('pages.plugins.card.uninstall'),
    confirmColor: 'error',
  })

  if (!confirmed) {
    return
  }

  await pluginsStore.uninstallInstalledPlugin(plugin.id)
}

const flattenConfig = (
  value: Record<string, unknown> | undefined,
  parent = ''
): Record<string, unknown> => {
  if (!value) {
    return {}
  }

  return Object.entries(value).reduce<Record<string, unknown>>((acc, [key, item]) => {
    const nextKey = parent ? `${parent}.${key}` : key
    if (item && typeof item === 'object' && !Array.isArray(item)) {
      Object.assign(acc, flattenConfig(item as Record<string, unknown>, nextKey))
      return acc
    }

    acc[nextKey] = item
    return acc
  }, {})
}

const buildSchemaDefaults = (schema: PluginConfigSchema) => {
  const defaults: Record<string, unknown> = {}

  const walk = (properties: PluginConfigSchema['properties'], parent = '') => {
    if (!properties) {
      return
    }

    for (const [key, field] of Object.entries(properties)) {
      const fieldKey = parent ? `${parent}.${key}` : key
      if (field.type === 'object' && field.properties) {
        walk(field.properties, fieldKey)
      } else if (field.default !== undefined && field.default !== null) {
        defaults[fieldKey] = field.default
      }
    }
  }

  walk(schema.properties)
  return defaults
}

const openConfigDialog = async (plugin: Plugin) => {
  if (plugin.role === 'adapter') {
    return
  }

  activePlugin.value = plugin
  const schema = await pluginsStore.fetchPluginSchema(plugin.id)
  activeSchema.value = schema

  if (schema) {
    schemaForm.value = {
      ...buildSchemaDefaults(schema),
      ...flattenConfig(plugin.metadata?.config),
    }
  } else {
    schemaForm.value = {}
  }

  dialogVisible.value = true
}

const saveConfig = async () => {
  if (!activePlugin.value) {
    return
  }

  const saved = await pluginsStore.updatePluginConfig(activePlugin.value.id, schemaForm.value)
  if (saved) {
    dialogVisible.value = false
  }
}
</script>
