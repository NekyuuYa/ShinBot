<template>
  <v-app>
    <router-view />
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
import { useAuthStore } from '@/stores/auth'
import { useUiStore } from '@/stores/ui'

const authStore = useAuthStore()
const uiStore = useUiStore()

onMounted(() => {
  authStore.initFromStorage()
})
</script>
