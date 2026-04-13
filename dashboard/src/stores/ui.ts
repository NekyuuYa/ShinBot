import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

export type SnackbarColor = 'success' | 'error' | 'warning' | 'info'

export const useUiStore = defineStore(
  'ui',
  () => {
    const snackbarVisible = ref(false)
    const snackbarMessage = ref('')
    const snackbarColor = ref<SnackbarColor>('info')
    const snackbarTimeout = ref(3500)
    const loadingCount = ref(0)
    const isRail = ref(false)

    const isLoading = computed(() => loadingCount.value > 0)

    const showSnackbar = (message: string, color: SnackbarColor = 'info', timeout = 3500) => {
      snackbarMessage.value = message
      snackbarColor.value = color
      snackbarTimeout.value = timeout
      snackbarVisible.value = true
    }

    const hideSnackbar = () => {
      snackbarVisible.value = false
    }

    const startLoading = () => {
      loadingCount.value += 1
    }

    const stopLoading = () => {
      loadingCount.value = Math.max(0, loadingCount.value - 1)
    }

    const resetLoading = () => {
      loadingCount.value = 0
    }

    const toggleRail = () => {
      isRail.value = !isRail.value
    }

    return {
      snackbarVisible,
      snackbarMessage,
      snackbarColor,
      snackbarTimeout,
      loadingCount,
      isLoading,
      isRail,
      showSnackbar,
      hideSnackbar,
      startLoading,
      stopLoading,
      resetLoading,
      toggleRail,
    }
  },
  {
    persist: true,
  }
)
