import { reactive, readonly } from 'vue'

import { translate } from '@/plugins/i18n'

export interface ConfirmDialogOptions {
  title?: string
  message: string
  confirmText?: string
  cancelText?: string
  confirmColor?: string
  icon?: string
  iconColor?: string
  maxWidth?: number | string
  persistent?: boolean
}

interface ConfirmDialogState {
  visible: boolean
  title: string
  message: string
  confirmText: string
  cancelText: string
  confirmColor: string
  icon: string
  iconColor: string
  maxWidth: number | string
  persistent: boolean
}

const state = reactive<ConfirmDialogState>({
  visible: false,
  title: '',
  message: '',
  confirmText: '',
  cancelText: '',
  confirmColor: 'primary',
  icon: 'mdi-help-circle-outline',
  iconColor: 'primary',
  maxWidth: 460,
  persistent: false,
})

let pendingResolve: ((value: boolean) => void) | null = null

const applyOptions = (options: ConfirmDialogOptions) => {
  state.title = options.title ?? translate('common.actions.action.confirm')
  state.message = options.message
  state.confirmText = options.confirmText ?? translate('common.actions.action.confirm')
  state.cancelText = options.cancelText ?? translate('common.actions.action.cancel')
  state.confirmColor = options.confirmColor ?? 'primary'
  state.icon = options.icon ?? 'mdi-help-circle-outline'
  state.iconColor = options.iconColor ?? state.confirmColor
  state.maxWidth = options.maxWidth ?? 460
  state.persistent = options.persistent ?? false
}

const settle = (value: boolean) => {
  state.visible = false

  const resolve = pendingResolve
  pendingResolve = null
  resolve?.(value)
}

export function useConfirmDialog() {
  const confirm = (options: ConfirmDialogOptions) => {
    if (pendingResolve) {
      settle(false)
    }

    applyOptions(options)
    state.visible = true

    return new Promise<boolean>((resolve) => {
      pendingResolve = resolve
    })
  }

  const handleVisibilityChange = (value: boolean) => {
    if (value) {
      state.visible = true
      return
    }

    if (state.visible) {
      settle(false)
    }
  }

  const confirmAction = () => {
    settle(true)
  }

  const cancelAction = () => {
    settle(false)
  }

  return {
    state: readonly(state),
    confirm,
    confirmAction,
    cancelAction,
    handleVisibilityChange,
  }
}