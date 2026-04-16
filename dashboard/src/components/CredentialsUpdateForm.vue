<template>
  <v-alert
    v-if="props.forceChange || authStore.mustChangeCredentials"
    type="warning"
    variant="tonal"
    class="mb-4"
  >
    {{ $t('pages.settings.credentials.mustChangeHint') }}
  </v-alert>

  <v-form @submit.prevent="handleUpdateCredentials">
    <v-text-field
      v-model="form.username"
      :label="$t('pages.settings.credentials.username')"
      prepend-inner-icon="mdi-account"
      :disabled="authStore.isLoading"
      class="mb-3"
    />

    <v-text-field
      v-model="form.currentPassword"
      :label="$t('pages.settings.credentials.currentPassword')"
      type="password"
      prepend-inner-icon="mdi-lock-outline"
      :disabled="authStore.isLoading"
      class="mb-3"
    />

    <v-text-field
      v-model="form.newPassword"
      :label="$t('pages.settings.credentials.newPassword')"
      type="password"
      prepend-inner-icon="mdi-lock"
      :disabled="authStore.isLoading"
      class="mb-3"
    />

    <v-text-field
      v-model="form.confirmPassword"
      :label="$t('pages.settings.credentials.confirmPassword')"
      type="password"
      prepend-inner-icon="mdi-lock-check"
      :disabled="authStore.isLoading"
      class="mb-4"
    />

    <v-alert v-if="localError || authStore.error" type="error" class="mb-4">
      {{ localError || authStore.error }}
    </v-alert>

    <v-btn type="submit" color="primary" :loading="authStore.isLoading">
      {{ $t('pages.settings.credentials.updateAction') }}
    </v-btn>
  </v-form>
</template>

<script setup lang="ts">
import { reactive, ref, watch } from 'vue'

import { translate } from '@/plugins/i18n'
import { useAuthStore } from '@/stores/auth'

const props = withDefaults(
  defineProps<{
    forceChange?: boolean
  }>(),
  {
    forceChange: false,
  }
)

const emit = defineEmits<{
  updated: []
}>()

const authStore = useAuthStore()

const form = reactive({
  username: '',
  currentPassword: '',
  newPassword: '',
  confirmPassword: '',
})

const localError = ref('')

watch(
  () => authStore.username,
  (value) => {
    if (!form.username) {
      form.username = value
    }
  },
  { immediate: true }
)

const handleUpdateCredentials = async () => {
  localError.value = ''

  const username = form.username.trim()
  const currentPassword = form.currentPassword
  const newPassword = form.newPassword.trim()

  if (!username || !currentPassword || !newPassword || !form.confirmPassword) {
    localError.value = translate('pages.settings.credentials.allFieldsRequired')
    return
  }

  if (newPassword !== form.confirmPassword) {
    localError.value = translate('pages.settings.credentials.passwordMismatch')
    return
  }

  if (authStore.mustChangeCredentials && (username === 'admin' || newPassword === 'admin')) {
    localError.value = translate('pages.settings.credentials.defaultNotAllowed')
    return
  }

  const success = await authStore.updateCredentials({
    username,
    current_password: currentPassword,
    new_password: newPassword,
  })

  if (!success) {
    return
  }

  form.currentPassword = ''
  form.newPassword = ''
  form.confirmPassword = ''
  emit('updated')
}
</script>
