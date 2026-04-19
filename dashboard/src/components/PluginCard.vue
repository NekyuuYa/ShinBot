<template>
  <v-card class="h-100 d-flex flex-column">
    <!-- Card Header -->
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="secondary" icon="mdi-puzzle" />
      </template>
      <v-card-title class="text-break">
        {{ plugin.name }}
      </v-card-title>
      <template #append>
        <v-menu>
          <template #activator="{ props }">
            <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
          </template>
          <v-list>
            <v-list-item @click="handleToggle">
              <v-list-item-title>
                {{
                  plugin.status === 'enabled'
                    ? $t('pages.plugins.card.disable')
                    : $t('pages.plugins.card.enable')
                }}
              </v-list-item-title>
            </v-list-item>
            <v-list-item v-if="canConfigure" @click="handleConfigure">
              <v-list-item-title>{{ $t('pages.plugins.card.configure') }}</v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
      </template>
    </v-card-item>

    <!-- Card Body -->
    <v-card-text class="py-2 flex-grow-1">
      <v-chip
        :color="plugin.status === 'enabled' ? 'success' : 'grey'"
        class="mb-3"
        small
      >
        {{
          plugin.status === 'enabled'
            ? $t('pages.plugins.card.enabled')
            : $t('pages.plugins.card.disabled')
        }}
      </v-chip>

      <div class="text-caption text-medium-emphasis mb-2">
        <strong>{{ $t('pages.plugins.form.version') }}:</strong>
        {{ plugin.version }}
      </div>

      <div v-if="plugin.author" class="text-caption text-medium-emphasis mb-2">
        <strong>{{ $t('pages.plugins.form.author') }}:</strong>
        {{ plugin.author }}
      </div>

      <div v-if="plugin.description" class="text-caption text-medium-emphasis">
        {{ plugin.description }}
      </div>
    </v-card-text>

    <!-- Card Footer -->
    <v-card-actions class="pt-0">
      <v-btn size="small" @click="handleToggle">
        {{
          plugin.status === 'enabled'
            ? $t('pages.plugins.card.disable')
            : $t('pages.plugins.card.enable')
        }}
      </v-btn>
      <v-btn v-if="canConfigure" color="primary" variant="text" size="small" @click="handleConfigure">
        {{ $t('pages.plugins.card.configure') }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { usePluginsStore } from '@/stores/plugins'
import type { Plugin } from '@/api/plugins'

interface Props {
  plugin: Plugin
}

const props = defineProps<Props>()
const emit = defineEmits<{
  configure: [plugin: Plugin]
}>()

const pluginsStore = usePluginsStore()
const canConfigure = computed(() => props.plugin.role !== 'adapter')

const handleToggle = async () => {
  if (props.plugin.status === 'enabled') {
    await pluginsStore.disablePlugin(props.plugin.id)
  } else {
    await pluginsStore.enablePlugin(props.plugin.id)
  }
}

const handleConfigure = () => {
  emit('configure', props.plugin)
}
</script>
