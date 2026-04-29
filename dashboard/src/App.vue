<template>
  <v-app>
    <router-view />
    <v-dialog
      :model-value="confirmDialog.state.visible"
      :max-width="confirmDialog.state.maxWidth"
      :persistent="confirmDialog.state.persistent"
      @update:model-value="confirmDialog.handleVisibilityChange"
    >
      <v-card class="confirm-dialog-card">
        <v-card-item>
          <template #prepend>
            <v-avatar :color="confirmDialog.state.iconColor" variant="tonal" size="40">
              <v-icon :icon="confirmDialog.state.icon" />
            </v-avatar>
          </template>
          <v-card-title>{{ confirmDialog.state.title }}</v-card-title>
        </v-card-item>

        <v-card-text class="confirm-dialog-message">
          {{ confirmDialog.state.message }}
        </v-card-text>

        <v-card-actions class="px-6 pb-6 pt-0">
          <v-spacer />
          <v-btn variant="text" @click="confirmDialog.cancelAction">
            {{ confirmDialog.state.cancelText }}
          </v-btn>
          <v-btn :color="confirmDialog.state.confirmColor" @click="confirmDialog.confirmAction">
            {{ confirmDialog.state.confirmText }}
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

    <v-snackbar
      v-model="uiStore.snackbarVisible"
      :color="uiStore.snackbarColor"
      :timeout="uiStore.snackbarTimeout"
      location="top"
    >
      {{ uiStore.snackbarMessage }}
      <template #actions>
        <v-btn variant="text" @click="uiStore.hideSnackbar">
          {{ $t('common.actions.action.close') }}
        </v-btn>
      </template>
    </v-snackbar>
  </v-app>
</template>

<script setup lang="ts">
import { onMounted } from 'vue'

import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useAuthStore } from '@/stores/auth'
import { useUiStore } from '@/stores/ui'

const authStore = useAuthStore()
const confirmDialog = useConfirmDialog()
const uiStore = useUiStore()

onMounted(() => {
  authStore.initFromStorage()
})
</script>

<style scoped lang="scss">
.confirm-dialog-card {
  border-radius: 20px;
}

.confirm-dialog-message {
  white-space: pre-wrap;
  line-height: 1.7;
}
</style>
