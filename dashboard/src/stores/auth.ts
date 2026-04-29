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
import { hasValidJwtToken } from '@/utils/jwt'
import { translate } from '@/plugins/i18n'

export const useAuthStore = defineStore(
  'auth',
  () => {
    const token = ref<string>(localStorage.getItem('auth_token') || '')
    const username = ref<string>(localStorage.getItem('auth_username') || '')
    const mustChangeCredentials = ref(
      localStorage.getItem('auth_must_change_credentials') === 'true'
    )
    const isLoading = ref(false)
    const error = ref<string>('')

    const isAuthenticated = computed(() => hasValidJwtToken(token.value))
    const displayName = computed(() => username.value || translate('pages.auth.defaultUsername'))

    const persistAuthState = () => {
      localStorage.setItem('auth_token', token.value)
      localStorage.setItem('auth_username', username.value)
      localStorage.setItem(
        'auth_must_change_credentials',
        String(mustChangeCredentials.value)
      )
    }

    const applyLoginPayload = (payload: LoginResponse) => {
      token.value = payload.token
      username.value = payload.username
      mustChangeCredentials.value = payload.must_change_credentials
      persistAuthState()
    }

    const clearAuthState = () => {
      authApi.logout()
      token.value = ''
      username.value = ''
      mustChangeCredentials.value = false
      error.value = ''
    }

    const login = async (credentials: LoginRequest) => {
      isLoading.value = true
      error.value = ''

      try {
        const data = await apiClient.unwrap(authApi.login(credentials))
        applyLoginPayload(data)
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
      if (!token.value) {
        return false
      }

      try {
        const data = await apiClient.unwrap(authApi.getProfile())
        username.value = data.username
        mustChangeCredentials.value = data.must_change_credentials
        persistAuthState()
        return true
      } catch {
        return false
      }
    }

    const updateCredentials = async (payload: UpdateProfileRequest) => {
      isLoading.value = true
      error.value = ''

      try {
        const data = await apiClient.unwrap(authApi.updateProfile(payload))
        applyLoginPayload(data)
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

    const logout = () => {
      clearAuthState()
      useUiStore().showSnackbar(translate('pages.auth.logoutSuccess'), 'info')
    }

    const initFromStorage = () => {
      const storedToken = localStorage.getItem('auth_token')
      if (!storedToken) {
        return
      }

      if (!hasValidJwtToken(storedToken)) {
        clearAuthState()
        return
      }

      token.value = storedToken
      void refreshProfile()
    }

    return {
      token,
      username,
      mustChangeCredentials,
      isLoading,
      error,
      isAuthenticated,
      displayName,
      clearAuthState,
      login,
      refreshProfile,
      updateCredentials,
      logout,
      initFromStorage,
    }
  }
)
