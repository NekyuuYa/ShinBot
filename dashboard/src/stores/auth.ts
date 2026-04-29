import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import {
  authApi,
  type LoginRequest,
  type LoginResponse,
  type UpdateProfileRequest,
} from '@/api/auth'
import { apiClient } from '@/api/client'
import { useUiStore } from './ui'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export const useAuthStore = defineStore(
  'auth',
  () => {
    const username = ref('')
    const mustChangeCredentials = ref(false)
    const authenticated = ref(false)
    const sessionResolved = ref(false)
    const isLoading = ref(false)
    const error = ref<string>('')
    let sessionPromise: Promise<boolean> | null = null

    const isAuthenticated = computed(() => authenticated.value)
    const displayName = computed(() => username.value || translate('pages.auth.defaultUsername'))

    const applySession = (payload: Pick<LoginResponse, 'username' | 'must_change_credentials'>) => {
      username.value = payload.username
      mustChangeCredentials.value = payload.must_change_credentials
      authenticated.value = true
      sessionResolved.value = true
      error.value = ''
    }

    const clearAuthState = () => {
      username.value = ''
      mustChangeCredentials.value = false
      authenticated.value = false
      sessionResolved.value = true
      error.value = ''
    }

    const fetchSession = async () => {
      try {
        const data = await apiClient.unwrap(
          authApi.getProfile({ suppressErrorNotify: true })
        )
        applySession(data)
        return true
      } catch {
        clearAuthState()
        return false
      }
    }

    const ensureSession = async (force = false) => {
      if (!force && sessionResolved.value) {
        return authenticated.value
      }

      if (sessionPromise) {
        return sessionPromise
      }

      sessionPromise = fetchSession().finally(() => {
        sessionPromise = null
      })
      return sessionPromise
    }

    const login = async (credentials: LoginRequest) => {
      isLoading.value = true
      error.value = ''

      try {
        const data = await apiClient.unwrap(
          authApi.login(credentials, { suppressErrorNotify: true })
        )
        applySession(data)
        if (mustChangeCredentials.value) {
          useUiStore().showSnackbar(
            translate('pages.auth.credentialsChangeRequired'),
            'warning'
          )
        } else {
          useUiStore().showSnackbar(translate('pages.auth.loginSuccess'), 'success')
        }
        return true
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.auth.loginFailed')
        )
        return false
      } finally {
        isLoading.value = false
      }
    }

    const refreshProfile = async () => {
      return ensureSession(true)
    }

    const updateCredentials = async (payload: UpdateProfileRequest) => {
      isLoading.value = true
      error.value = ''

      try {
        const data = await apiClient.unwrap(
          authApi.updateProfile(payload, { suppressErrorNotify: true })
        )
        applySession(data)
        useUiStore().showSnackbar(
          translate('pages.settings.credentials.updateSuccess'),
          'success'
        )
        return true
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('pages.settings.credentials.updateFailed')
        )
        return false
      } finally {
        isLoading.value = false
      }
    }

    const logout = async () => {
      try {
        await authApi.logout({ suppressErrorNotify: true })
      } catch {
        // Keep local state cleanup even if the server-side cookie revoke fails.
      }

      clearAuthState()
      useUiStore().showSnackbar(translate('pages.auth.logoutSuccess'), 'info')
    }

    const initializeSession = () => {
      void ensureSession()
    }

    return {
      username,
      mustChangeCredentials,
      isLoading,
      error,
      isAuthenticated,
      displayName,
      clearAuthState,
      ensureSession,
      login,
      refreshProfile,
      updateCredentials,
      logout,
      initializeSession,
    }
  }
)
