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
import { useConfirmDialog } from '@/composables/useConfirmDialog'
import { useUiStore } from '@/stores/ui'

const confirmDialog = useConfirmDialog()
const uiStore = useUiStore()
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

<style lang="scss">
html.shinbot-theme-transitioning,
html.shinbot-theme-transitioning body,
html.shinbot-theme-transitioning .v-application,
html.shinbot-theme-transitioning .v-overlay-container {
  transition:
    background-color 300ms cubic-bezier(0.4, 0, 0.2, 1) !important,
    color 300ms cubic-bezier(0.4, 0, 0.2, 1) !important;
}

html.shinbot-theme-transitioning
  :where(
    .v-application *,
    .v-overlay-container *,
    .v-application ::before,
    .v-application ::after,
    .v-overlay-container ::before,
    .v-overlay-container ::after
  ) {
  transition-duration: 300ms !important;
  transition-property:
    background-color,
    border-color,
    box-shadow,
    color,
    fill,
    outline-color,
    stroke !important;
  transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1) !important;
}

html.shinbot-theme-view-transition::view-transition-old(root),
html.shinbot-theme-view-transition::view-transition-new(root) {
  animation-duration: 300ms;
  animation-timing-function: cubic-bezier(0.4, 0, 0.2, 1);
  mix-blend-mode: normal;
}

html.shinbot-theme-view-transition::view-transition-old(root) {
  animation-name: shinbot-theme-fade-out;
}

html.shinbot-theme-view-transition::view-transition-new(root) {
  animation-name: shinbot-theme-fade-in;
}

@keyframes shinbot-theme-fade-out {
  from {
    opacity: 1;
  }

  to {
    opacity: 0;
  }
}

@keyframes shinbot-theme-fade-in {
  from {
    opacity: 0;
  }

  to {
    opacity: 1;
  }
}

@media (prefers-reduced-motion: reduce) {
  html.shinbot-theme-transitioning,
  html.shinbot-theme-transitioning *,
  html.shinbot-theme-transitioning ::before,
  html.shinbot-theme-transitioning ::after,
  html.shinbot-theme-view-transition::view-transition-old(root),
  html.shinbot-theme-view-transition::view-transition-new(root) {
    transition: none !important;
    animation: none !important;
  }
}
</style>
