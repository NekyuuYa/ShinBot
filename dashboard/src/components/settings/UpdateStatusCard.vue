<template>
  <v-card class="pa-6 h-100 settings-card" elevation="0">
    <div class="settings-card-header">
      <div>
        <v-card-title class="px-0 pt-0">
          {{ $t('pages.settings.update.title') }}
        </v-card-title>
        <v-card-subtitle class="px-0 pb-0">
          {{ $t('pages.settings.update.subtitle') }}
        </v-card-subtitle>
      </div>

      <v-chip
        :color="statusChipColor"
        variant="flat"
        size="small"
      >
        {{ statusChipLabel }}
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
      :type="statusAlertType"
      variant="tonal"
      density="comfortable"
      class="mt-4"
    >
      {{ props.status.blockMessage }}
    </v-alert>

    <div class="settings-detail-grid mt-6">
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.repoPath') }}</div>
        <div class="settings-detail-item__value">{{ props.status.repoPath || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.branch') }}</div>
        <div class="settings-detail-item__value">{{ props.status.branch || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.upstream') }}</div>
        <div class="settings-detail-item__value">{{ props.status.upstream || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.commit') }}</div>
        <div class="settings-detail-item__value">{{ props.status.currentCommitShort || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.remoteHead') }}</div>
        <div class="settings-detail-item__value">{{ props.status.remoteHeadCommitShort || emptyText }}</div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.aheadBehind') }}</div>
        <div class="settings-detail-item__value">
          {{ $t('pages.settings.update.aheadBehindValue', {
            ahead: props.status.aheadCount,
            behind: props.status.behindCount,
          }) }}
        </div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.allowedBranches') }}</div>
        <div class="settings-detail-item__value">
          {{ allowedBranches }}
        </div>
      </div>
      <div class="settings-detail-item">
        <div class="settings-detail-item__label">{{ $t('pages.settings.update.workingTree') }}</div>
        <div class="settings-detail-item__value">
          {{
            props.status.dirty
              ? $t('pages.settings.update.workingTreeDirty', { count: props.status.dirtyCount })
              : $t('pages.settings.update.workingTreeClean')
          }}
        </div>
      </div>
    </div>

    <v-alert
      v-if="props.status.restartRequested"
      type="info"
      variant="tonal"
      density="comfortable"
      class="mt-6"
    >
      {{
        $t('pages.settings.update.restartRequestedHint', {
          reason: props.status.restartRequest?.reason || 'update',
          requestedBy: props.status.restartRequest?.requested_by || authStore.username || 'admin',
        })
      }}
    </v-alert>

    <v-alert
      v-if="props.lastResult"
      :type="props.lastResult.updated ? 'success' : 'info'"
      variant="tonal"
      density="comfortable"
      class="mt-6"
    >
      {{
        props.lastResult.updated
          ? $t('pages.settings.update.updatedResult', {
              before: props.lastResult.beforeCommitShort,
              after: props.lastResult.afterCommitShort,
            })
          : $t('pages.settings.update.upToDateResult')
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
        color="warning"
        prepend-icon="mdi-source-pull"
        :disabled="!props.status.canUpdate || props.isLoading"
        :loading="props.isSubmitting"
        @click="confirmDialog = true"
      >
        {{ $t('pages.settings.update.action') }}
      </v-btn>
    </div>
  </v-card>

  <v-dialog v-model="confirmDialog" max-width="640">
    <v-card class="pa-2">
      <v-card-title>{{ $t('pages.settings.update.confirmTitle') }}</v-card-title>
      <v-card-text>
        <p class="settings-confirm-copy">
          {{ $t('pages.settings.update.confirmText') }}
        </p>

        <div class="settings-detail-grid">
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.update.branch') }}</div>
            <div class="settings-detail-item__value">{{ props.status.branch || emptyText }}</div>
          </div>
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.update.commit') }}</div>
            <div class="settings-detail-item__value">{{ props.status.currentCommitShort || emptyText }}</div>
          </div>
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.update.upstream') }}</div>
            <div class="settings-detail-item__value">{{ props.status.upstream || emptyText }}</div>
          </div>
          <div class="settings-detail-item">
            <div class="settings-detail-item__label">{{ $t('pages.settings.update.pullMode') }}</div>
            <div class="settings-detail-item__value">{{ $t('pages.settings.update.pullModeValue') }}</div>
          </div>
        </div>

        <v-alert type="warning" variant="tonal" density="comfortable" class="mt-4">
          {{ $t('pages.settings.update.confirmWarning') }}
        </v-alert>
      </v-card-text>

      <v-card-actions class="px-4 pb-4">
        <v-spacer />
        <v-btn variant="text" @click="confirmDialog = false">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn
          color="warning"
          :loading="props.isSubmitting"
          @click="emit('submit')"
        >
          {{ $t('common.actions.action.confirm') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed } from 'vue'

import type { SystemUpdateResult, SystemUpdateStatus } from '@/api/system'
import { translate } from '@/plugins/i18n'
import { useAuthStore } from '@/stores/auth'

const props = defineProps<{
  status: SystemUpdateStatus
  error: string
  lastResult: SystemUpdateResult | null
  isLoading: boolean
  isSubmitting: boolean
  confirmVisible: boolean
}>()

const emit = defineEmits<{
  refresh: []
  submit: []
  'update:confirmVisible': [value: boolean]
}>()

const authStore = useAuthStore()
const emptyText = '\u2014'

const confirmDialog = computed({
  get: () => props.confirmVisible,
  set: (value: boolean) => emit('update:confirmVisible', value),
})

const statusChipColor = computed(() => {
  if (props.status.updateInProgress) return 'warning'
  if (props.status.canUpdate) return 'success'
  if (props.status.blockCode === 'already_up_to_date') return 'info'
  if (props.status.repoDetected) return 'warning'
  return 'error'
})

const statusChipLabel = computed(() => {
  if (props.status.updateInProgress) return translate('pages.settings.update.stateRunning')
  if (props.status.canUpdate) return translate('pages.settings.update.stateReady')
  if (props.status.blockCode === 'already_up_to_date') {
    return translate('pages.settings.update.stateUpToDate')
  }
  if (props.status.repoDetected) return translate('pages.settings.update.stateBlocked')
  return translate('pages.settings.update.stateUnavailable')
})

const statusAlertType = computed(() => {
  if (props.status.canUpdate || props.status.blockCode === 'already_up_to_date') {
    return 'info'
  }
  return 'warning'
})

const allowedBranches = computed(() =>
  props.status.allowedBranches.length ? props.status.allowedBranches.join(', ') : emptyText
)
</script>

<style scoped lang="scss">
@use '@/styles/settings-card';
</style>
