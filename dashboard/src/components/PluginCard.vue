<template>
  <v-card class="h-100 d-flex flex-column plugin-card" elevation="0">
    <!-- Card Header -->
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="secondary" variant="tonal" icon="mdi-puzzle" />
      </template>
      <v-card-title class="text-break">
        {{ plugin.name }}
      </v-card-title>
      <template #append>
        <v-menu>
          <template #activator="{ props }">
            <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
          </template>
          <v-list density="compact">
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
        size="small"
        variant="tonal"
      >
        <template #prepend>
          <v-icon
            :icon="plugin.status === 'enabled' ? 'mdi-check-circle' : 'mdi-minus-circle'"
            size="14"
            class="me-1"
          />
        </template>
        {{
          plugin.status === 'enabled'
            ? $t('pages.plugins.card.enabled')
            : $t('pages.plugins.card.disabled')
        }}
      </v-chip>

      <div class="text-caption text-medium-emphasis mb-1">
        <strong>{{ $t('pages.plugins.form.version') }}:</strong>
        {{ plugin.version }}
      </div>

      <div v-if="plugin.author" class="text-caption text-medium-emphasis mb-2">
        <strong>{{ $t('pages.plugins.form.author') }}:</strong>
        {{ plugin.author }}
      </div>

      <div v-if="plugin.description" class="text-caption text-medium-emphasis mt-2 line-clamp-3">
        {{ plugin.description }}
      </div>
    </v-card-text>

    <!-- Card Footer -->
    <v-card-actions class="pt-0">
      <v-btn
        variant="text"
        size="small"
        :color="plugin.status === 'enabled' ? 'warning' : 'success'"
        @click="handleToggle"
      >
        {{
          plugin.status === 'enabled'
            ? $t('pages.plugins.card.disable')
            : $t('pages.plugins.card.enable')
        }}
      </v-btn>
      <v-spacer />
      <v-btn v-if="canConfigure" color="primary" variant="tonal" size="small" @click="handleConfigure">
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

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.plugin-card {
  @include surface-card;
  @include hover-border;
}

.line-clamp-3 {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
</style>
