import { ref } from 'vue'

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
        updateConfirmDialog.value = false

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
