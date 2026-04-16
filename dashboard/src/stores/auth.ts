import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import {
  authApi,
  type LoginRequest,
  type LoginResponse,
  type UpdateProfileRequest,
} from '@/api/auth'
import { useUiStore } from './ui'
import { getErrorMessage } from '@/utils/error'
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

    const isAuthenticated = computed(() => !!token.value)
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

    const login = async (credentials: LoginRequest) => {
      isLoading.value = true
      error.value = ''

      try {
        const response = await authApi.login(credentials)

        if (response.data.success && response.data.data) {
          applyLoginPayload(response.data.data)
          if (mustChangeCredentials.value) {
            useUiStore().showSnackbar(
              translate('pages.auth.credentialsChangeRequired'),
              'warning'
            )
          } else {
            useUiStore().showSnackbar(translate('pages.auth.loginSuccess'), 'success')
          }
          return true
        } else {
          error.value = response.data.error?.message || translate('pages.auth.loginFailed')
          return false
        }
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('common.actions.message.networkError')
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
        const response = await authApi.getProfile()
        if (!response.data.success || !response.data.data) {
          return false
        }

        username.value = response.data.data.username
        mustChangeCredentials.value = response.data.data.must_change_credentials
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
        const response = await authApi.updateProfile(payload)
        if (response.data.success && response.data.data) {
          applyLoginPayload(response.data.data)
          useUiStore().showSnackbar(
            translate('pages.settings.credentials.updateSuccess'),
            'success'
          )
          return true
        }

        error.value =
          response.data.error?.message ||
          translate('pages.settings.credentials.updateFailed')
        return false
      } catch (errorDetail: unknown) {
        error.value = getErrorMessage(
          errorDetail,
          translate('common.actions.message.networkError')
        )
        return false
      } finally {
        isLoading.value = false
      }
    }

    const logout = () => {
      authApi.logout()
      token.value = ''
      username.value = ''
      mustChangeCredentials.value = false
      error.value = ''
      useUiStore().showSnackbar(translate('pages.auth.logoutSuccess'), 'info')
    }

    const initFromStorage = () => {
      const storedToken = localStorage.getItem('auth_token')
      if (!storedToken) {
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
      login,
      refreshProfile,
      updateCredentials,
      logout,
      initFromStorage,
    }
  }
)
