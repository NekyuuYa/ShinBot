<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.settings.title')"
      :subtitle="$t('pages.settings.subtitle')"
      :kicker="$t('pages.settings.kicker')"
    />

    <v-row>
      <v-col cols="12" lg="6">
        <v-card class="pa-6" elevation="4">
          <v-card-title class="px-0 pt-0">
            {{ $t('pages.settings.credentials.title') }}
          </v-card-title>
          <v-card-subtitle class="px-0 pb-4">
            {{ $t('pages.settings.credentials.subtitle') }}
          </v-card-subtitle>
          <credentials-update-form @updated="loadUpdateStatus" />
        </v-card>
      </v-col>

      <v-col cols="12" lg="6">
        <v-card class="pa-6 h-100" elevation="4">
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
            v-if="updateError"
            type="error"
            variant="tonal"
            density="comfortable"
            class="mt-4"
          >
            {{ updateError }}
          </v-alert>

          <v-alert
            v-else-if="updateStatus.blockMessage"
            :type="statusAlertType"
            variant="tonal"
            density="comfortable"
            class="mt-4"
          >
            {{ updateStatus.blockMessage }}
          </v-alert>

          <div class="settings-detail-grid mt-6">
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.repoPath') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.repoPath || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.branch') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.branch || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.upstream') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.upstream || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.commit') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.currentCommitShort || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.remoteHead') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.remoteHeadCommitShort || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.aheadBehind') }}</div>
              <div class="settings-detail-item__value">
                {{ $t('pages.settings.update.aheadBehindValue', {
                  ahead: updateStatus.aheadCount,
                  behind: updateStatus.behindCount,
                }) }}
              </div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.allowedBranches') }}</div>
              <div class="settings-detail-item__value">
                {{ updateStatus.allowedBranches.length ? updateStatus.allowedBranches.join(', ') : '—' }}
              </div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.workingTree') }}</div>
              <div class="settings-detail-item__value">
                {{
                  updateStatus.dirty
                    ? $t('pages.settings.update.workingTreeDirty', { count: updateStatus.dirtyCount })
                    : $t('pages.settings.update.workingTreeClean')
                }}
              </div>
            </div>
          </div>

          <v-alert
            v-if="updateStatus.restartRequested"
            type="info"
            variant="tonal"
            density="comfortable"
            class="mt-6"
          >
            {{
              $t('pages.settings.update.restartRequestedHint', {
                reason: updateStatus.restartRequest?.reason || 'update',
                requestedBy: updateStatus.restartRequest?.requested_by || authStore.username || 'admin',
              })
            }}
          </v-alert>

          <v-alert
            v-if="lastResult"
            :type="lastResult.updated ? 'success' : 'info'"
            variant="tonal"
            density="comfortable"
            class="mt-6"
          >
            {{
              lastResult.updated
                ? $t('pages.settings.update.updatedResult', {
                    before: lastResult.beforeCommitShort,
                    after: lastResult.afterCommitShort,
                  })
                : $t('pages.settings.update.upToDateResult')
            }}
          </v-alert>

          <div v-if="lastResult?.output" class="settings-output mt-4">
            <div class="settings-output__label">{{ $t('pages.settings.update.commandOutput') }}</div>
            <pre class="settings-output__body">{{ lastResult.output }}</pre>
          </div>

          <div class="settings-actions mt-6">
            <v-btn
              variant="text"
              prepend-icon="mdi-refresh"
              :loading="isLoadingUpdateStatus"
              @click="loadUpdateStatus"
            >
              {{ $t('common.actions.action.refresh') }}
            </v-btn>

            <v-btn
              color="warning"
              prepend-icon="mdi-source-pull"
              :disabled="!updateStatus.canUpdate || isLoadingUpdateStatus"
              :loading="isSubmittingUpdate"
              @click="confirmDialog = true"
            >
              {{ $t('pages.settings.update.action') }}
            </v-btn>
          </div>
        </v-card>
      </v-col>

      <v-col cols="12">
        <v-card class="pa-6" elevation="4">
          <div class="settings-card-header">
            <div>
              <v-card-title class="px-0 pt-0">
                {{ $t('pages.settings.pricing.title') }}
              </v-card-title>
              <v-card-subtitle class="px-0 pb-0">
                {{ $t('pages.settings.pricing.subtitle') }}
              </v-card-subtitle>
            </div>

            <v-chip color="primary" variant="flat" size="small">
              {{ pricingPreview }}
            </v-chip>
          </div>

          <v-row class="mt-2">
            <v-col cols="12" md="6">
              <v-select
                v-model="systemSettingsStore.pricingCurrency"
                :items="pricingCurrencyOptions"
                item-title="label"
                item-value="value"
                :label="$t('pages.settings.pricing.currency')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-select
                v-model="systemSettingsStore.pricingTokenUnit"
                :items="pricingTokenUnitOptions"
                item-title="label"
                item-value="value"
                :label="$t('pages.settings.pricing.unit')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
          </v-row>

          <v-alert type="info" variant="tonal" density="comfortable" class="mt-2">
            {{ $t('pages.settings.pricing.hint') }}
          </v-alert>
        </v-card>
      </v-col>

      <v-col cols="12">
        <v-card class="pa-6" elevation="4">
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
            v-if="distError"
            type="error"
            variant="tonal"
            density="comfortable"
            class="mt-4"
          >
            {{ distError }}
          </v-alert>

          <v-alert
            v-else-if="distStatus.blockMessage"
            :type="distAlertType"
            variant="tonal"
            density="comfortable"
            class="mt-4"
          >
            {{ distStatus.blockMessage }}
          </v-alert>

          <div class="settings-detail-grid mt-6">
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.dist.packageSource') }}</div>
              <div class="settings-detail-item__value">
                {{ distStatus.packageSource || distStatus.sourceRepoPath || '—' }}
              </div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.dist.targetDist') }}</div>
              <div class="settings-detail-item__value">{{ distStatus.targetDistPath || '—' }}</div>
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
              <div class="settings-detail-item__value">{{ distStatus.sourceType || 'zip' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.dist.mode') }}</div>
              <div class="settings-detail-item__value">{{ $t('pages.settings.dist.modeValue') }}</div>
            </div>
          </div>

          <v-alert
            v-if="lastDistResult"
            :type="lastDistResult.copied ? 'success' : 'info'"
            variant="tonal"
            density="comfortable"
            class="mt-6"
          >
            {{
              lastDistResult.copied
                ? $t('pages.settings.dist.replacedResult', { commit: lastDistResult.sourceCommitShort })
                : $t('pages.settings.dist.upToDateResult')
            }}
          </v-alert>

          <div v-if="lastDistResult?.output" class="settings-output mt-4">
            <div class="settings-output__label">{{ $t('pages.settings.update.commandOutput') }}</div>
            <pre class="settings-output__body">{{ lastDistResult.output }}</pre>
          </div>

          <div class="settings-actions mt-6">
            <v-btn
              variant="text"
              prepend-icon="mdi-refresh"
              :loading="isLoadingDistStatus"
              @click="loadDistStatus"
            >
              {{ $t('common.actions.action.refresh') }}
            </v-btn>

            <v-btn
              color="primary"
              prepend-icon="mdi-folder-sync-outline"
              :disabled="!distStatus.canUpdate || isLoadingDistStatus"
              :loading="isSubmittingDist"
              @click="distConfirmDialog = true"
            >
              {{ $t('pages.settings.dist.action') }}
            </v-btn>
          </div>
        </v-card>
      </v-col>
    </v-row>

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
              <div class="settings-detail-item__value">{{ updateStatus.branch || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.commit') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.currentCommitShort || '—' }}</div>
            </div>
            <div class="settings-detail-item">
              <div class="settings-detail-item__label">{{ $t('pages.settings.update.upstream') }}</div>
              <div class="settings-detail-item__value">{{ updateStatus.upstream || '—' }}</div>
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
            :loading="isSubmittingUpdate"
            @click="submitUpdate"
          >
            {{ $t('common.actions.action.confirm') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <v-dialog v-model="distConfirmDialog" max-width="640">
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
              <div class="settings-detail-item__value">{{ distStatus.targetDistPath || '—' }}</div>
            </div>
          </div>

          <v-alert type="info" variant="tonal" density="comfortable" class="mt-4">
            {{ $t('pages.settings.dist.confirmHint') }}
          </v-alert>
        </v-card-text>

        <v-card-actions class="px-4 pb-4">
          <v-spacer />
          <v-btn variant="text" @click="distConfirmDialog = false">
            {{ $t('common.actions.action.cancel') }}
          </v-btn>
          <v-btn color="primary" :loading="isSubmittingDist" @click="submitDistUpdate">
            {{ $t('common.actions.action.confirm') }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import CredentialsUpdateForm from '@/components/CredentialsUpdateForm.vue'
import { translate } from '@/plugins/i18n'
import {
  systemApi,
  type DashboardDistUpdateResult,
  type DashboardDistUpdateStatus,
  type SystemUpdateResult,
  type SystemUpdateStatus,
} from '@/api/system'
import { getErrorMessage } from '@/utils/error'
import { useAuthStore } from '@/stores/auth'
import { useUiStore } from '@/stores/ui'
import { useSystemSettingsStore } from '@/stores/systemSettings'

const authStore = useAuthStore()
const uiStore = useUiStore()
const systemSettingsStore = useSystemSettingsStore()

const confirmDialog = ref(false)
const distConfirmDialog = ref(false)
const isLoadingUpdateStatus = ref(false)
const isSubmittingUpdate = ref(false)
const isLoadingDistStatus = ref(false)
const isSubmittingDist = ref(false)
const updateError = ref('')
const distError = ref('')
const lastResult = ref<SystemUpdateResult | null>(null)
const lastDistResult = ref<DashboardDistUpdateResult | null>(null)
const updateStatus = ref<SystemUpdateStatus>({
  repoDetected: false,
  repoPath: '',
  branch: '',
  upstream: '',
  upstreamRef: '',
  upstreamTrackingCommit: '',
  upstreamTrackingCommitShort: '',
  remoteName: '',
  remoteUrl: '',
  remoteHeadCommit: '',
  remoteHeadCommitShort: '',
  remoteCheckOk: false,
  updateAvailable: false,
  aheadCount: 0,
  behindCount: 0,
  currentCommit: '',
  currentCommitShort: '',
  dirty: false,
  dirtyCount: 0,
  dirtyEntries: [],
  allowedBranches: ['main', 'master'],
  canUpdate: false,
  blockCode: null,
  blockMessage: null,
  updateInProgress: false,
  credentialsChangeRequired: false,
  restartRequested: false,
  restartRequest: null,
})
const distStatus = ref<DashboardDistUpdateStatus>({
  enabled: false,
  sourceType: 'zip',
  packageSource: '',
  packageSha256: '',
  expectedPackageSha256: '',
  expectedPackageSha256Url: '',
  deployedPackageSha256: '',
  sourceRepoPath: '',
  sourceSubdir: '.',
  sourceDistPath: '',
  targetDistPath: '',
  branch: '',
  upstream: '',
  upstreamRef: '',
  remoteName: '',
  remoteUrl: '',
  currentCommit: '',
  currentCommitShort: '',
  remoteHeadCommit: '',
  remoteHeadCommitShort: '',
  remoteCheckOk: false,
  updateAvailable: false,
  replaceRequired: false,
  deployedSourceCommit: '',
  deployedSourceCommitShort: '',
  dirty: false,
  dirtyCount: 0,
  dirtyEntries: [],
  allowedBranches: ['main', 'master'],
  canUpdate: false,
  blockCode: null,
  blockMessage: null,
  updateInProgress: false,
  credentialsChangeRequired: false,
})

const statusChipColor = computed(() => {
  if (updateStatus.value.updateInProgress) return 'warning'
  if (updateStatus.value.canUpdate) return 'success'
  if (updateStatus.value.blockCode === 'already_up_to_date') return 'info'
  if (updateStatus.value.repoDetected) return 'warning'
  return 'error'
})

const statusChipLabel = computed(() => {
  if (updateStatus.value.updateInProgress) return translate('pages.settings.update.stateRunning')
  if (updateStatus.value.canUpdate) return translate('pages.settings.update.stateReady')
  if (updateStatus.value.blockCode === 'already_up_to_date') {
    return translate('pages.settings.update.stateUpToDate')
  }
  if (updateStatus.value.repoDetected) return translate('pages.settings.update.stateBlocked')
  return translate('pages.settings.update.stateUnavailable')
})

const statusAlertType = computed(() => {
  if (updateStatus.value.canUpdate || updateStatus.value.blockCode === 'already_up_to_date') {
    return 'info'
  }
  return 'warning'
})

const distChipColor = computed(() => {
  if (distStatus.value.updateInProgress) return 'warning'
  if (distStatus.value.canUpdate) return 'success'
  if (distStatus.value.blockCode === 'already_up_to_date') return 'info'
  if (distStatus.value.enabled) return 'warning'
  return 'error'
})

const distChipLabel = computed(() => {
  if (distStatus.value.updateInProgress) return translate('pages.settings.update.stateRunning')
  if (distStatus.value.canUpdate) return translate('pages.settings.dist.stateReady')
  if (distStatus.value.blockCode === 'already_up_to_date') {
    return translate('pages.settings.update.stateUpToDate')
  }
  if (distStatus.value.enabled) return translate('pages.settings.update.stateBlocked')
  return translate('pages.settings.dist.stateNotConfigured')
})

const distAlertType = computed(() => {
  if (distStatus.value.canUpdate || distStatus.value.blockCode === 'already_up_to_date') {
    return 'info'
  }
  return 'warning'
})

const shortHash = (value?: string) => value ? value.slice(0, 12) : '—'
const packageShaShort = computed(() => shortHash(distStatus.value.packageSha256))
const expectedPackageShaShort = computed(() => shortHash(distStatus.value.expectedPackageSha256))
const deployedPackageShaShort = computed(() => shortHash(distStatus.value.deployedPackageSha256))
const pricingCurrencyOptions = computed(() => [
  { label: 'CNY', value: 'CNY' },
  { label: 'USD', value: 'USD' },
])
const pricingTokenUnitOptions = computed(() => [
  { label: translate('pages.settings.pricing.units.mtokens'), value: 'mtokens' },
  { label: translate('pages.settings.pricing.units.ktokens'), value: 'ktokens' },
])
const pricingPreview = computed(
  () =>
    `${systemSettingsStore.pricingCurrency} / ${translate(`pages.settings.pricing.units.${systemSettingsStore.pricingTokenUnit}`)}`
)

const loadUpdateStatus = async () => {
  isLoadingUpdateStatus.value = true
  updateError.value = ''

  try {
    const response = await systemApi.getUpdateStatus()
    if (response.data.success && response.data.data) {
      updateStatus.value = response.data.data
      return
    }

    updateError.value =
      response.data.error?.message || translate('pages.settings.update.loadFailed')
  } catch (errorDetail: unknown) {
    updateError.value = getErrorMessage(
      errorDetail,
      translate('common.actions.message.networkError')
    )
  } finally {
    isLoadingUpdateStatus.value = false
  }
}

const submitUpdate = async () => {
  isSubmittingUpdate.value = true
  updateError.value = ''

  try {
    const response = await systemApi.pullAndRestart()
    if (response.data.success && response.data.data) {
      lastResult.value = response.data.data
      confirmDialog.value = false

      if (response.data.data.updated) {
        updateStatus.value = {
          ...updateStatus.value,
          canUpdate: false,
          blockCode: 'restart_pending',
          blockMessage: translate('pages.settings.update.restartPending'),
          restartRequested: response.data.data.restartRequested,
          restartRequest: response.data.data.restartRequest,
        }
        uiStore.showSnackbar(translate('pages.settings.update.updatedToast'), 'success', 5000)
      } else {
        uiStore.showSnackbar(translate('pages.settings.update.upToDateToast'), 'info', 5000)
        await loadUpdateStatus()
      }
      return
    }

    updateError.value =
      response.data.error?.message || translate('pages.settings.update.runFailed')
    uiStore.showSnackbar(updateError.value, 'error', 5000)
  } catch (errorDetail: unknown) {
    updateError.value = getErrorMessage(
      errorDetail,
      translate('common.actions.message.networkError')
    )
    uiStore.showSnackbar(updateError.value, 'error', 5000)
  } finally {
    isSubmittingUpdate.value = false
  }
}

const loadDistStatus = async () => {
  isLoadingDistStatus.value = true
  distError.value = ''

  try {
    const response = await systemApi.getDashboardDistStatus()
    if (response.data.success && response.data.data) {
      distStatus.value = response.data.data
      return
    }

    distError.value =
      response.data.error?.message || translate('pages.settings.dist.loadFailed')
  } catch (errorDetail: unknown) {
    distError.value = getErrorMessage(
      errorDetail,
      translate('common.actions.message.networkError')
    )
  } finally {
    isLoadingDistStatus.value = false
  }
}

const submitDistUpdate = async () => {
  isSubmittingDist.value = true
  distError.value = ''

  try {
    const response = await systemApi.updateDashboardDist()
    if (response.data.success && response.data.data) {
      lastDistResult.value = response.data.data
      distConfirmDialog.value = false
      uiStore.showSnackbar(
        response.data.data.copied
          ? translate('pages.settings.dist.replacedToast')
          : translate('pages.settings.dist.upToDateToast'),
        response.data.data.copied ? 'success' : 'info',
        5000
      )
      await loadDistStatus()
      return
    }

    distError.value =
      response.data.error?.message || translate('pages.settings.dist.runFailed')
    uiStore.showSnackbar(distError.value, 'error', 5000)
  } catch (errorDetail: unknown) {
    distError.value = getErrorMessage(
      errorDetail,
      translate('common.actions.message.networkError')
    )
    uiStore.showSnackbar(distError.value, 'error', 5000)
  } finally {
    isSubmittingDist.value = false
  }
}

onMounted(() => {
  void loadUpdateStatus()
  void loadDistStatus()
})
</script>

<style scoped>
.settings-card-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}

.settings-detail-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}

.settings-detail-item {
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
  border-radius: 16px;
  padding: 14px 16px;
  background: rgba(var(--v-theme-surface), 0.82);
}

.settings-detail-item__label {
  color: rgba(var(--v-theme-on-surface), 0.6);
  font-size: 0.78rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.settings-detail-item__value {
  margin-top: 8px;
  color: rgba(var(--v-theme-on-surface), 0.92);
  font-size: 0.96rem;
  line-height: 1.5;
  word-break: break-word;
}

.settings-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.settings-output__label {
  margin-bottom: 8px;
  color: rgba(var(--v-theme-on-surface), 0.65);
  font-size: 0.82rem;
  font-weight: 600;
}

.settings-output__body {
  margin: 0;
  padding: 16px;
  border-radius: 16px;
  background: rgba(var(--v-theme-surface-variant), 0.42);
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: 0.82rem;
  line-height: 1.6;
  white-space: pre-wrap;
  word-break: break-word;
}

.settings-confirm-copy {
  margin: 0 0 16px;
  color: rgba(var(--v-theme-on-surface), 0.76);
  line-height: 1.7;
}

@media (max-width: 960px) {
  .settings-detail-grid {
    grid-template-columns: 1fr;
  }
}
</style>
