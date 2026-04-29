import { ref } from 'vue'

import { apiClient } from '@/api/client'
import {
  systemApi,
  type DashboardDistUpdateResult,
  type DashboardDistUpdateStatus,
  type SystemUpdateResult,
  type SystemUpdateStatus,
} from '@/api/system'
import { translate } from '@/plugins/i18n'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'

const createDefaultUpdateStatus = (): SystemUpdateStatus => ({
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

const createDefaultDistStatus = (): DashboardDistUpdateStatus => ({
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

export function useSystemUpdate() {
  const uiStore = useUiStore()

  const updateConfirmDialog = ref(false)
  const distConfirmDialog = ref(false)
  const isLoadingUpdateStatus = ref(false)
  const isSubmittingUpdate = ref(false)
  const isLoadingDistStatus = ref(false)
  const isSubmittingDist = ref(false)
  const updateError = ref('')
  const distError = ref('')
  const lastResult = ref<SystemUpdateResult | null>(null)
  const lastDistResult = ref<DashboardDistUpdateResult | null>(null)
  const updateStatus = ref<SystemUpdateStatus>(createDefaultUpdateStatus())
  const distStatus = ref<DashboardDistUpdateStatus>(createDefaultDistStatus())

  const loadUpdateStatus = async () => {
    isLoadingUpdateStatus.value = true
    updateError.value = ''

    try {
      updateStatus.value = await apiClient.unwrap(systemApi.getUpdateStatus())
    } catch (errorDetail: unknown) {
      updateError.value = getErrorMessage(
        errorDetail,
        translate('pages.settings.update.loadFailed')
      )
    } finally {
      isLoadingUpdateStatus.value = false
    }
  }

  const submitUpdate = async () => {
    isSubmittingUpdate.value = true
    updateError.value = ''

    try {
      const data = await apiClient.unwrap(systemApi.pullAndRestart())
      lastResult.value = data
      updateConfirmDialog.value = false

      if (data.updated) {
        updateStatus.value = {
          ...updateStatus.value,
          canUpdate: false,
          blockCode: 'restart_pending',
          blockMessage: translate('pages.settings.update.restartPending'),
          restartRequested: data.restartRequested,
          restartRequest: data.restartRequest,
        }
        uiStore.showSnackbar(translate('pages.settings.update.updatedToast'), 'success', 5000)
      } else {
        uiStore.showSnackbar(translate('pages.settings.update.upToDateToast'), 'info', 5000)
        await loadUpdateStatus()
      }
    } catch (errorDetail: unknown) {
      updateError.value = getErrorMessage(
        errorDetail,
        translate('pages.settings.update.runFailed')
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
      distStatus.value = await apiClient.unwrap(systemApi.getDashboardDistStatus())
    } catch (errorDetail: unknown) {
      distError.value = getErrorMessage(
        errorDetail,
        translate('pages.settings.dist.loadFailed')
      )
    } finally {
      isLoadingDistStatus.value = false
    }
  }

  const submitDistUpdate = async () => {
    isSubmittingDist.value = true
    distError.value = ''

    try {
      const data = await apiClient.unwrap(systemApi.updateDashboardDist())
      lastDistResult.value = data
      distConfirmDialog.value = false
      uiStore.showSnackbar(
        data.copied
          ? translate('pages.settings.dist.replacedToast')
          : translate('pages.settings.dist.upToDateToast'),
        data.copied ? 'success' : 'info',
        5000
      )
      await loadDistStatus()
    } catch (errorDetail: unknown) {
      distError.value = getErrorMessage(
        errorDetail,
        translate('pages.settings.dist.runFailed')
      )
      uiStore.showSnackbar(distError.value, 'error', 5000)
    } finally {
      isSubmittingDist.value = false
    }
  }

  return {
    updateConfirmDialog,
    distConfirmDialog,
    updateStatus,
    distStatus,
    updateError,
    distError,
    lastResult,
    lastDistResult,
    isLoadingUpdateStatus,
    isSubmittingUpdate,
    isLoadingDistStatus,
    isSubmittingDist,
    loadUpdateStatus,
    submitUpdate,
    loadDistStatus,
    submitDistUpdate,
  }
}
