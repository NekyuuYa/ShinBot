import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { authApi, type LoginRequest } from '@/api/auth'
import { useUiStore } from './ui'
import { getErrorMessage } from '@/utils/error'
import { translate } from '@/plugins/i18n'

export const useAuthStore = defineStore(
  'auth',
  () => {
    const token = ref<string>('')
    const username = ref<string>('')
    const isLoading = ref(false)
    const error = ref<string>('')

    const isAuthenticated = computed(() => !!token.value)

    const login = async (credentials: LoginRequest) => {
      isLoading.value = true
      error.value = ''

      try {
        const response = await authApi.login(credentials)

        if (response.data.success && response.data.data) {
          const { token: newToken } = response.data.data
          token.value = newToken
          // 直接使用登录时输入的用户名
          username.value = credentials.username
          localStorage.setItem('auth_token', newToken)
          useUiStore().showSnackbar(translate('pages.auth.loginSuccess'), 'success')
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

    const logout = () => {
      authApi.logout()
      token.value = ''
      username.value = ''
      error.value = ''
      useUiStore().showSnackbar(translate('pages.auth.logoutSuccess'), 'info')
    }

    const initFromStorage = () => {
      const storedToken = localStorage.getItem('auth_token')
      if (storedToken) {
        token.value = storedToken
      }
    }

    return {
      token,
      username,
      isLoading,
      error,
      isAuthenticated,
      login,
      logout,
      initFromStorage,
    }
  }
)
