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
            <v-divider v-if="canUpdate || canUninstall" />
            <v-list-item v-if="canUpdate" @click="handleUpdate">
              <template #prepend>
                <v-icon icon="mdi-update" />
              </template>
              <v-list-item-title>{{ $t('pages.plugins.card.update') }}</v-list-item-title>
            </v-list-item>
            <v-list-item v-if="canUninstall" @click="handleUninstall">
              <template #prepend>
                <v-icon color="error" icon="mdi-delete-outline" />
              </template>
              <v-list-item-title class="text-error">
                {{ $t('pages.plugins.card.uninstall') }}
              </v-list-item-title>
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

      <div class="d-flex flex-wrap ga-2 mb-2">
        <v-chip :color="sourceColor" size="small" variant="tonal">
          <template #prepend>
            <v-icon :icon="sourceIcon" size="14" class="me-1" />
          </template>
          {{ sourceLabel }}
        </v-chip>
        <v-chip
          v-if="installSource?.ref"
          color="secondary"
          size="small"
          variant="tonal"
        >
          {{ installSource.ref }}
        </v-chip>
      </div>

      <div v-if="installSource?.source_url" class="text-caption text-medium-emphasis mb-2 text-truncate">
        {{ installSource.source_url }}
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
      <v-btn v-if="canUpdate" color="secondary" variant="tonal" size="small" @click="handleUpdate">
        {{ $t('pages.plugins.card.update') }}
      </v-btn>
      <v-btn v-if="canConfigure" color="primary" variant="tonal" size="small" @click="handleConfigure">
        {{ $t('pages.plugins.card.configure') }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { usePluginsStore } from '@/stores/plugins'
import type { Plugin } from '@/api/plugins'

interface Props {
  plugin: Plugin
}

const props = defineProps<Props>()
const emit = defineEmits<{
  configure: [plugin: Plugin]
  update: [plugin: Plugin]
  uninstall: [plugin: Plugin]
}>()

const { t } = useI18n()
const pluginsStore = usePluginsStore()
const canConfigure = computed(() => props.plugin.role !== 'adapter')
const installSource = computed(() => props.plugin.metadata?.install_source)
const isBuiltin = computed(() =>
  Boolean(props.plugin.metadata?.builtin || props.plugin.metadata?.source === 'builtin')
)
const isWebuiManaged = computed(() => Boolean(installSource.value?.managed_by_webui))
const canUpdate = computed(() => Boolean(isWebuiManaged.value && installSource.value?.can_update))
const canUninstall = computed(() => Boolean(isWebuiManaged.value && installSource.value?.can_uninstall))
const sourceLabel = computed(() => {
  if (isBuiltin.value) {
    return t('pages.plugins.card.builtinSource')
  }
  if (!installSource.value) {
    return t('pages.plugins.card.localSource')
  }
  if (installSource.value.source_type === 'github') {
    return t('pages.plugins.install.githubSource')
  }
  return t('pages.plugins.install.archiveSource')
})
const sourceIcon = computed(() => {
  if (isBuiltin.value) {
    return 'mdi-package-variant-closed-check'
  }
  if (!installSource.value) {
    return 'mdi-folder-outline'
  }
  return installSource.value.source_type === 'github' ? 'mdi-github' : 'mdi-folder-zip'
})
const sourceColor = computed(() => {
  if (isBuiltin.value) {
    return 'info'
  }
  if (!installSource.value) {
    return 'grey'
  }
  return installSource.value.source_type === 'github' ? 'primary' : 'secondary'
})

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

const handleUpdate = () => {
  emit('update', props.plugin)
}

const handleUninstall = () => {
  emit('uninstall', props.plugin)
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.plugin-card {
  @include surface-card;
  @include hover-lift;
}

.line-clamp-3 {
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
</style>
