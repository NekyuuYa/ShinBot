<template>
  <v-container class="fill-height" fluid>
    <v-row align="center" justify="center" class="h-100">
      <v-col cols="12" sm="8" md="4">
        <v-card class="pa-6">
          <v-card-title class="text-center mb-6">
            {{ $t('pages.auth.loginTitle') }}
          </v-card-title>

          <v-form @submit.prevent="handleLogin">
            <v-text-field
              v-model="form.username"
              :label="$t('pages.auth.username')"
              prepend-inner-icon="mdi-account"
              class="mb-4"
              :disabled="authStore.isLoading"
            />

            <v-text-field
              v-model="form.password"
              :label="$t('pages.auth.password')"
              type="password"
              prepend-inner-icon="mdi-lock"
              class="mb-4"
              :disabled="authStore.isLoading"
            />

            <v-checkbox
              v-model="form.rememberMe"
              :label="$t('pages.auth.rememberMe')"
              class="mb-4"
            />

            <v-alert v-if="authStore.error" type="error" class="mb-4">
              {{ authStore.error }}
            </v-alert>

            <v-btn
              type="submit"
              color="primary"
              class="w-100 mb-4"
              :loading="authStore.isLoading"
            >
              {{ $t('pages.auth.login') }}
            </v-btn>
          </v-form>

          <v-divider class="my-4" />

          <v-row dense class="text-center mt-4">
            <v-col cols="12">
              <v-btn variant="text" size="small">
                {{ $t('pages.auth.forgotPassword') }}
              </v-btn>
            </v-col>
          </v-row>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { reactive } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const router = useRouter()
const authStore = useAuthStore()

const form = reactive({
  username: '',
  password: '',
  rememberMe: false,
})

const handleLogin = async () => {
  if (!form.username || !form.password) {
    return
  }

  const success = await authStore.login({
    username: form.username,
    password: form.password,
  })

  if (success) {
    router.push('/dashboard')
  }
}
</script>
