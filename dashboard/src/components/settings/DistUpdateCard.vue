<template>
  <v-card class="pa-6 settings-card" elevation="0">
    <div class="settings-card-header">
      <div>
        <v-card-title class="px-0 pt-0">
          {{ $t('pages.settings.dist.title') }}
        </v-card-title>
        <v-card-subtitle class="px-0 pb-0">
          {{ $t('pages.settings.dist.subtitle') }}
        </v-card-subtitle>
      </div>

      <v-chip :color="distChipColor" variant="flat" size="small">
        {{ distChipLabel }}
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
      :type="distAlertType"
      variant="tonal"
      density="comfortable"
      class="mt-4"
    >
      {{ props.status.blockMessage }}
    </v-alert>

    <div class="settings-detail-grid mt-6">
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.packageSource') }}</div>
        <div class="settings-detail-item__value">
          {{ props.status.packageSource || props.status.sourceRepoPath || emptyText }}
        </div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.targetDist') }}</div>
        <div class="settings-detail-item__value">{{ props.status.targetDistPath || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.packageSha256') }}</div>
        <div class="settings-detail-item__value">{{ packageShaShort }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.deployedPackageSha256') }}</div>
        <div class="settings-detail-item__value">{{ deployedPackageShaShort }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.expectedPackageSha256') }}</div>
        <div class="settings-detail-item__value">{{ expectedPackageShaShort }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.sourceType') }}</div>
        <div class="settings-detail-item__value">{{ props.status.sourceType || 'zip' }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.dist.mode') }}</div>
        <div class="settings-detail-item__value">{{ $t('pages.settings.dist.modeValue') }}</div>
      </div>
    </div>

    <v-alert
      v-if="props.lastResult"
      :type="props.lastResult.copied ? 'success' : 'info'"
      variant="tonal"
      density="comfortable"
      class="mt-6"
    >
      {{
        props.lastResult.copied
          ? $t('pages.settings.dist.replacedResult', { commit: props.lastResult.sourceCommitShort })
          : $t('pages.settings.dist.upToDateResult')
      }}
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
        prepend-icon="mdi-folder-sync-outline"
        :disabled="!props.status.canUpdate || props.isLoading"
        :loading="props.isSubmitting"
        @click="confirmDialog = true"
      >
        {{ $t('pages.settings.dist.action') }}
      </v-btn>
    </div>
  </v-card>

  <v-dialog v-model="confirmDialog" max-width="640">
    <v-card class="pa-2">
      <v-card-title>{{ $t('pages.settings.dist.confirmTitle') }}</v-card-title>
      <v-card-text>
        <p class="settings-confirm-copy">
          {{ $t('pages.settings.dist.confirmText') }}
        </p>

        <div class="settings-detail-grid">
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.dist.packageSha256') }}</div>
            <div class="settings-detail-item__value">{{ packageShaShort }}</div>
          </div>
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.dist.targetDist') }}</div>
            <div class="settings-detail-item__value">{{ props.status.targetDistPath || emptyText }}</div>
          </div>
        </div>

        <v-alert type="info" variant="tonal" density="comfortable" class="mt-4">
          {{ $t('pages.settings.dist.confirmHint') }}
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

import type { DashboardDistUpdateResult, DashboardDistUpdateStatus } from '@/api/system'
import { translate } from '@/plugins/i18n'

const props = defineProps<{
  status: DashboardDistUpdateStatus
  error: string
  lastResult: DashboardDistUpdateResult | null
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

const distChipColor = computed(() => {
  if (props.status.updateInProgress) return 'warning'
  if (props.status.canUpdate) return 'success'
  if (props.status.blockCode === 'already_up_to_date') return 'info'
  if (props.status.enabled) return 'warning'
  return 'error'
})

const distChipLabel = computed(() => {
  if (props.status.updateInProgress) return translate('pages.settings.update.stateRunning')
  if (props.status.canUpdate) return translate('pages.settings.dist.stateReady')
  if (props.status.blockCode === 'already_up_to_date') {
    return translate('pages.settings.update.stateUpToDate')
  }
  if (props.status.enabled) return translate('pages.settings.update.stateBlocked')
  return translate('pages.settings.dist.stateNotConfigured')
})

const distAlertType = computed(() => {
  if (props.status.canUpdate || props.status.blockCode === 'already_up_to_date') {
    return 'info'
  }
  return 'warning'
})

const shortHash = (value?: string) => (value ? value.slice(0, 12) : emptyText)

const packageShaShort = computed(() => shortHash(props.status.packageSha256))
const expectedPackageShaShort = computed(() => shortHash(props.status.expectedPackageSha256))
const deployedPackageShaShort = computed(() => shortHash(props.status.deployedPackageSha256))
</script>

<style scoped lang="scss">
@use '@/styles/settings-card';
</style>
