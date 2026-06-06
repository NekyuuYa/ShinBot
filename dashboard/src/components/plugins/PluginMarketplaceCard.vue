<template>
  <v-card class="h-100 d-flex flex-column marketplace-card" elevation="0">
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar :color="statusColor" variant="tonal" :icon="statusIcon" />
      </template>
      <v-card-title class="text-break">
        {{ item.name }}
      </v-card-title>
      <v-card-subtitle class="text-break">
        {{ item.plugin_id }}
      </v-card-subtitle>
    </v-card-item>

    <v-card-text class="py-2 flex-grow-1">
      <div class="d-flex flex-wrap ga-2 mb-3">
        <v-chip :color="statusColor" size="small" variant="tonal">
          <template #prepend>
            <v-icon :icon="statusIcon" size="14" class="me-1" />
          </template>
          {{ statusLabel }}
        </v-chip>
        <v-chip v-if="item.role" color="secondary" size="small" variant="tonal">
          {{ item.role }}
        </v-chip>
        <v-chip color="primary" size="small" variant="tonal">
          {{ item.version }}
        </v-chip>
      </div>

      <div v-if="item.author" class="text-caption text-medium-emphasis mb-1">
        <strong>{{ $t('pages.plugins.form.author') }}:</strong>
        {{ item.author }}
      </div>

      <div class="text-caption text-medium-emphasis mb-2 text-break">
        <strong>{{ $t('pages.plugins.install.pluginPath') }}:</strong>
        {{ item.plugin_path }}
      </div>

      <div v-if="item.installed_version" class="text-caption text-medium-emphasis mb-2">
        <strong>{{ $t('pages.plugins.marketplace.installedVersion') }}:</strong>
        {{ item.installed_version }}
      </div>

      <div v-if="item.description" class="text-caption text-medium-emphasis mb-3 line-clamp-3">
        {{ item.description }}
      </div>

      <div v-if="item.required_dependencies.length > 0" class="mb-2">
        <div class="text-caption text-medium-emphasis mb-1">
          {{ $t('pages.plugins.install.requiredDependencies') }}
        </div>
        <div class="d-flex flex-wrap ga-1">
          <v-chip
            v-for="dependency in item.required_dependencies"
            :key="dependency"
            :color="item.missing_required_dependencies.includes(dependency) ? 'error' : 'success'"
            size="x-small"
            variant="tonal"
            class="text-break"
          >
            {{ dependency }}
          </v-chip>
        </div>
      </div>

      <v-alert
        v-if="blockedReason"
        type="warning"
        variant="tonal"
        density="compact"
        class="mt-3"
      >
        {{ blockedReason }}
      </v-alert>
    </v-card-text>

    <v-card-actions class="pt-0">
      <v-btn
        color="primary"
        variant="tonal"
        size="small"
        :disabled="!primaryActionEnabled || loading"
        :loading="loading"
        @click="handlePrimaryAction"
      >
        {{ primaryActionLabel }}
      </v-btn>
      <v-spacer />
      <v-chip
        v-if="item.tags.length > 0"
        color="grey"
        size="small"
        variant="tonal"
        class="text-truncate"
      >
        {{ item.tags[0] }}
      </v-chip>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { PluginMarketplaceItem } from '@/api/plugins'

const props = defineProps<{
  item: PluginMarketplaceItem
  loading?: boolean
}>()

const emit = defineEmits<{
  install: [item: PluginMarketplaceItem]
  update: [item: PluginMarketplaceItem]
}>()

const { t } = useI18n()

const isBlockedByDependencies = computed(() => props.item.missing_required_dependencies.length > 0)
const isBlockedByLocalPlugin = computed(() => props.item.installed && !props.item.managed_by_webui)
const isBlockedByDifferentSource = computed(
  () => props.item.installed && props.item.managed_by_webui && !props.item.can_update
)
const canUpdate = computed(() => props.item.can_update)
const canInstall = computed(() => props.item.can_install)
const primaryActionEnabled = computed(() => canInstall.value || canUpdate.value)

const statusLabel = computed(() => {
  if (canUpdate.value) {
    return t('pages.plugins.marketplace.updateAvailable')
  }
  if (props.item.installed) {
    return t('pages.plugins.marketplace.installed')
  }
  if (isBlockedByDependencies.value || isBlockedByLocalPlugin.value || isBlockedByDifferentSource.value) {
    return t('pages.plugins.marketplace.blocked')
  }
  return t('pages.plugins.marketplace.notInstalled')
})

const statusColor = computed(() => {
  if (canUpdate.value) {
    return 'warning'
  }
  if (props.item.installed) {
    return 'success'
  }
  if (isBlockedByDependencies.value || isBlockedByLocalPlugin.value || isBlockedByDifferentSource.value) {
    return 'error'
  }
  return 'primary'
})

const statusIcon = computed(() => {
  if (canUpdate.value) {
    return 'mdi-update'
  }
  if (props.item.installed) {
    return 'mdi-check-circle'
  }
  if (isBlockedByDependencies.value || isBlockedByLocalPlugin.value || isBlockedByDifferentSource.value) {
    return 'mdi-alert-circle'
  }
  return 'mdi-download'
})

const primaryActionLabel = computed(() => {
  if (canUpdate.value) {
    return t('pages.plugins.marketplace.update')
  }
  return t('pages.plugins.marketplace.install')
})

const blockedReason = computed(() => {
  if (isBlockedByDependencies.value) {
    return t('pages.plugins.marketplace.missingRequired', {
      dependencies: props.item.missing_required_dependencies.join(', '),
    })
  }
  if (isBlockedByLocalPlugin.value) {
    return t('pages.plugins.marketplace.localExists')
  }
  if (isBlockedByDifferentSource.value) {
    return t('pages.plugins.marketplace.differentSource')
  }
  return ''
})

const handlePrimaryAction = () => {
  if (canUpdate.value) {
    emit('update', props.item)
    return
  }
  if (canInstall.value) {
    emit('install', props.item)
  }
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.marketplace-card {
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
