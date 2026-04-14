<template>
  <v-container fluid class="pa-0">
    <app-page-header :title="$t('pages.plugins.title')">
      <template #actions>
        <v-btn color="secondary" prepend-icon="mdi-reload" @click="handleReload" class="me-2">
          {{ $t('pages.plugins.reload') }}
        </v-btn>
        <v-btn color="secondary" prepend-icon="mdi-magnify" @click="handleRescan">
          {{ $t('pages.plugins.rescan') }}
        </v-btn>
      </template>
    </app-page-header>

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
          bg-color="white"
          rounded="lg"
        />
      </v-col>
      <v-spacer />
      <v-col cols="auto" class="pa-0">
        <v-btn icon="mdi-refresh" variant="outlined" @click="handleRefresh" rounded="lg" />
      </v-col>
    </v-row>

    <v-row v-if="pluginsStore.isLoading && pluginsStore.plugins.length === 0" class="mx-0">
      <v-col cols="12" class="pa-0">
        <v-skeleton-loader type="card" :count="3" />
      </v-col>
    </v-row>

    <v-row v-else-if="filteredPlugins.length === 0" justify="center" class="py-12 mx-0">
      <v-col cols="12" sm="8" md="6" class="text-center pa-0">
        <v-icon size="120" color="grey-lighten-1" icon="mdi-puzzle-outline" />
        <h3 class="text-h6 my-4">{{ $t('pages.plugins.noData') }}</h3>
      </v-col>
    </v-row>

    <v-row v-else class="mx-n4">
      <v-col v-for="plugin in filteredPlugins" :key="plugin.id" cols="12" sm="6" md="4" lg="3" class="pa-4">
        <plugin-card :plugin="plugin" @configure="openConfigDialog" />
      </v-col>
    </v-row>

    <v-alert v-if="pluginsStore.error" type="error" class="mt-4">
      {{ pluginsStore.error }}
    </v-alert>

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
import { computed, onMounted, ref } from 'vue'
import { usePluginsStore } from '@/stores/plugins'
import PluginCard from '@/components/PluginCard.vue'
import SchemaForm from '@/components/SchemaForm.vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import type { Plugin, PluginConfigSchema } from '@/api/plugins'

const pluginsStore = usePluginsStore()
const searchQuery = ref('')
const dialogVisible = ref(false)
const activePlugin = ref<Plugin | null>(null)
const activeSchema = ref<PluginConfigSchema | null>(null)
const schemaForm = ref<Record<string, unknown>>({})

const filteredPlugins = computed(() =>
  pluginsStore.plugins.filter((plugin) =>
    plugin.name.toLowerCase().includes(searchQuery.value.toLowerCase())
  )
)

onMounted(() => {
  pluginsStore.fetchPlugins()
})

const handleRefresh = () => {
  pluginsStore.fetchPlugins()
}

const handleReload = async () => {
  await pluginsStore.reloadPlugins()
}

const handleRescan = async () => {
  await pluginsStore.rescanPlugins()
}

const applySchemaDefaults = (schema: PluginConfigSchema) => {
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
  schemaForm.value = defaults
}

const openConfigDialog = async (plugin: Plugin) => {
  if (plugin.role === 'adapter') {
    return
  }

  activePlugin.value = plugin
  const schema = await pluginsStore.fetchPluginSchema(plugin.id)
  activeSchema.value = schema

  if (schema) {
    applySchemaDefaults(schema)
  } else {
    schemaForm.value = {}
  }

  dialogVisible.value = true
}

const saveConfig = async () => {
  if (!activePlugin.value) {
    return
  }

  await pluginsStore.updatePluginConfig(activePlugin.value.id, schemaForm.value)
  dialogVisible.value = false
}
</script>
