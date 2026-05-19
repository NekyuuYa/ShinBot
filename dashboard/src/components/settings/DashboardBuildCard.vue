<template>
  <v-card class="pa-6 settings-card" elevation="0">
    <div class="settings-card-header">
      <div>
        <v-card-title class="px-0 pt-0">
          {{ $t('pages.settings.build.title') }}
        </v-card-title>
        <v-card-subtitle class="px-0 pb-0">
          {{ $t('pages.settings.build.subtitle') }}
        </v-card-subtitle>
      </div>

      <v-chip :color="buildChipColor" variant="flat" size="small">
        {{ buildChipLabel }}
      </v-chip>
    </div>

    <v-alert
      v-if="props.error"
      type="error"
      variant="tonal"
      density="comfortable"
      class="mt-4"
    >
      {{ props.error }}
    </v-alert>

    <v-alert
      v-else-if="props.status.blockMessage"
      :type="buildAlertType"
      variant="tonal"
      density="comfortable"
      class="mt-4"
    >
      {{ props.status.blockMessage }}
    </v-alert>

    <div class="settings-detail-grid mt-6">
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.build.dashboardPath') }}</div>
        <div class="settings-detail-item__value">{{ props.status.dashboardPath || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.build.distPath') }}</div>
        <div class="settings-detail-item__value">{{ props.status.distPath || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.build.command') }}</div>
        <div class="settings-detail-item__value">{{ props.status.command || emptyText }}</div>
      </div>
    </div>

    <v-alert
      v-if="props.lastResult"
      type="success"
      variant="tonal"
      density="comfortable"
      class="mt-6"
    >
      {{ $t('pages.settings.build.builtResult') }}
    </v-alert>

    <div v-if="props.lastResult?.output" class="settings-output mt-4">
      <div class="settings-output__label">{{ $t('pages.settings.update.commandOutput') }}</div>
      <pre class="settings-output__body">{{ props.lastResult.output }}</pre>
    </div>

    <div class="settings-actions mt-6">
      <v-btn
        variant="text"
        prepend-icon="mdi-refresh"
        :loading="props.isLoading"
        @click="emit('refresh')"
      >
        {{ $t('common.actions.action.refresh') }}
      </v-btn>

      <v-btn
        color="primary"
        prepend-icon="mdi-hammer-wrench"
        :disabled="!props.status.canBuild || props.isLoading"
        :loading="props.isSubmitting"
        @click="confirmDialog = true"
      >
        {{ $t('pages.settings.build.action') }}
      </v-btn>
    </div>
  </v-card>

  <v-dialog v-model="confirmDialog" max-width="640">
    <v-card class="pa-2">
      <v-card-title>{{ $t('pages.settings.build.confirmTitle') }}</v-card-title>
      <v-card-text>
        <p class="settings-confirm-copy">
          {{ $t('pages.settings.build.confirmText') }}
        </p>

        <div class="settings-detail-grid">
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.build.dashboardPath') }}</div>
            <div class="settings-detail-item__value">{{ props.status.dashboardPath || emptyText }}</div>
          </div>
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.build.command') }}</div>
            <div class="settings-detail-item__value">{{ props.status.command || emptyText }}</div>
          </div>
        </div>

        <v-alert type="info" variant="tonal" density="comfortable" class="mt-4">
          {{ $t('pages.settings.build.confirmHint') }}
        </v-alert>
      </v-card-text>

      <v-card-actions class="px-4 pb-4">
        <v-spacer />
        <v-btn variant="text" @click="confirmDialog = false">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn color="primary" :loading="props.isSubmitting" @click="emit('submit')">
          {{ $t('common.actions.action.confirm') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed } from 'vue'

import type { DashboardBuildResult, DashboardBuildStatus } from '@/api/system'
import { translate } from '@/plugins/i18n'

const props = defineProps<{
  status: DashboardBuildStatus
  error: string
  lastResult: DashboardBuildResult | null
  isLoading: boolean
  isSubmitting: boolean
  confirmVisible: boolean
}>()

const emit = defineEmits<{
  refresh: []
  submit: []
  'update:confirmVisible': [value: boolean]
}>()

const emptyText = '\u2014'

const confirmDialog = computed({
  get: () => props.confirmVisible,
  set: (value: boolean) => emit('update:confirmVisible', value),
})

const buildChipColor = computed(() => {
  if (props.status.buildInProgress) return 'warning'
  if (props.status.canBuild) return 'success'
  if (props.status.enabled) return 'warning'
  return 'grey'
})

const buildChipLabel = computed(() => {
  if (props.status.buildInProgress) return translate('pages.settings.update.stateRunning')
  if (props.status.canBuild) return translate('pages.settings.build.stateReady')
  if (props.status.enabled) return translate('pages.settings.update.stateBlocked')
  return translate('pages.settings.build.stateUnavailable')
})

const buildAlertType = computed(() => {
  if (props.status.canBuild) return 'info'
  return 'warning'
})
</script>

<style scoped lang="scss">
@use '@/styles/settings-card';
</style>
