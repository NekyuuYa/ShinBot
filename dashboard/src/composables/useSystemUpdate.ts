import { ref } from 'vue'

import { apiClient } from '@/api/client'
import {
  systemApi,
  type DashboardBuildResult,
  type DashboardBuildStatus,
  type SystemUpdateResult,
  type SystemUpdateStatus,
} from '@/api/system'
import { translate } from '@/plugins/i18n'
import { useUiStore } from '@/stores/ui'
import { getErrorMessage } from '@/utils/error'
import { createCachedRequest, type CachedRequestOptions } from '@/utils/requestCache'

const SYSTEM_UPDATE_STATUS_STALE_TIME_MS = 30_000

const loadCachedUpdateStatus = createCachedRequest(
  () => apiClient.unwrap(systemApi.getUpdateStatus()),
  SYSTEM_UPDATE_STATUS_STALE_TIME_MS
)

const loadCachedBuildStatus = createCachedRequest(
  () => apiClient.unwrap(systemApi.getDashboardBuildStatus()),
  SYSTEM_UPDATE_STATUS_STALE_TIME_MS
)

const createDefaultUpdateStatus = (): SystemUpdateStatus => ({
  enabled: false,
  workdir: '',
  command: '',
  restartAfterSuccess: true,
  canUpdate: false,
  blockCode: null,
  blockMessage: null,
  updateInProgress: false,
  credentialsChangeRequired: false,
  restartRequested: false,
  restartRequest: null,
})

const createDefaultBuildStatus = (): DashboardBuildStatus => ({
  enabled: true,
  dashboardPath: '',
  distPath: '',
  command: '',
  canBuild: false,
  blockCode: null,
  blockMessage: null,
  buildInProgress: false,
  credentialsChangeRequired: false,
})

export function useSystemUpdate() {
  const uiStore = useUiStore()

  const updateConfirmDialog = ref(false)
  const buildConfirmDialog = ref(false)
  const isLoadingUpdateStatus = ref(false)
  const isSubmittingUpdate = ref(false)
  const isLoadingBuildStatus = ref(false)
  const isSubmittingBuild = ref(false)
  const updateError = ref('')
  const buildError = ref('')
  const lastResult = ref<SystemUpdateResult | null>(null)
  const lastBuildResult = ref<DashboardBuildResult | null>(null)
  const updateStatus = ref<SystemUpdateStatus>(createDefaultUpdateStatus())
  const buildStatus = ref<DashboardBuildStatus>(createDefaultBuildStatus())

  const loadUpdateStatus = async (options: CachedRequestOptions = {}) => {
    isLoadingUpdateStatus.value = true
    updateError.value = ''

    try {
      updateStatus.value = await loadCachedUpdateStatus(options)
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
      const data = await apiClient.unwrap(systemApi.runFrameworkUpdate())
      lastResult.value = data
      updateConfirmDialog.value = false

      updateStatus.value = {
        ...updateStatus.value,
        canUpdate: false,
        blockCode: data.restartRequested ? 'restart_pending' : null,
        blockMessage: data.restartRequested ? translate('pages.settings.update.restartPending') : null,
        restartRequested: data.restartRequested,
        restartRequest: data.restartRequest,
      }
      uiStore.showSnackbar(translate('pages.settings.update.updatedToast'), 'success', 5000)
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

  const loadBuildStatus = async (options: CachedRequestOptions = {}) => {
    isLoadingBuildStatus.value = true
    buildError.value = ''

    try {
      buildStatus.value = await loadCachedBuildStatus(options)
    } catch (errorDetail: unknown) {
      buildError.value = getErrorMessage(
        errorDetail,
        translate('pages.settings.build.loadFailed')
      )
    } finally {
      isLoadingBuildStatus.value = false
    }
  }

  const submitBuild = async () => {
    isSubmittingBuild.value = true
    buildError.value = ''

    try {
      const data = await apiClient.unwrap(systemApi.buildDashboard())
      lastBuildResult.value = data
      buildConfirmDialog.value = false
      uiStore.showSnackbar(translate('pages.settings.build.builtToast'), 'success', 5000)
      await loadBuildStatus({ force: true })
    } catch (errorDetail: unknown) {
      buildError.value = getErrorMessage(
        errorDetail,
        translate('pages.settings.build.runFailed')
      )
      uiStore.showSnackbar(buildError.value, 'error', 5000)
    } finally {
      isSubmittingBuild.value = false
    }
  }

  return {
    updateConfirmDialog,
    buildConfirmDialog,
    updateStatus,
    buildStatus,
    updateError,
    buildError,
    lastResult,
    lastBuildResult,
    isLoadingUpdateStatus,
    isSubmittingUpdate,
    isLoadingBuildStatus,
    isSubmittingBuild,
    loadUpdateStatus,
    submitUpdate,
    loadBuildStatus,
    submitBuild,
  }
}
