<template>
  <v-container class="fill-height login-page" fluid>
    <div class="login-background" />

    <v-row align="center" justify="center" class="h-100 position-relative">
      <v-col cols="12" sm="8" md="5" lg="4" xl="3">
        <div class="text-center mb-10">
          <v-avatar color="primary" size="72" class="mb-4 elevation-12">
            <v-icon icon="mdi-shield-lock" size="36" color="white" />
          </v-avatar>
          <h1 class="text-h4 font-weight-black mb-1">ShinBot</h1>
          <p class="text-subtitle-1 text-medium-emphasis">{{ $t('pages.auth.loginSubtitle') || 'Control Center' }}</p>
        </div>

        <v-card class="pa-8 login-card" elevation="0">
          <v-card-title class="text-h5 font-weight-bold px-0 mb-6">
            {{ $t('pages.auth.loginTitle') }}
          </v-card-title>

          <v-form @submit.prevent="handleLogin">
            <v-text-field
              v-model="form.username"
              :label="$t('pages.auth.username')"
              prepend-inner-icon="mdi-account-outline"
              variant="outlined"
              density="comfortable"
              class="mb-2"
              :disabled="authStore.isLoading"
            />

            <v-text-field
              v-model="form.password"
              :label="$t('pages.auth.password')"
              type="password"
              prepend-inner-icon="mdi-lock-outline"
              variant="outlined"
              density="comfortable"
              class="mb-2"
              :disabled="authStore.isLoading"
            />

            <v-checkbox
              v-model="form.rememberMe"
              :label="$t('pages.auth.rememberMe')"
              color="primary"
              hide-details
              class="mb-6"
            />

            <v-alert v-if="authStore.error" type="error" variant="tonal" class="mb-6" rounded="lg">
              {{ authStore.error }}
            </v-alert>

            <v-btn
              type="submit"
              color="primary"
              size="large"
              block
              rounded="lg"
              elevation="0"
              class="font-weight-bold"
              :loading="authStore.isLoading"
            >
              {{ $t('pages.auth.login') }}
            </v-btn>
          </v-form>

          <v-divider class="my-8" />

          <div class="text-center">
            <v-btn variant="text" size="small" color="medium-emphasis">
              {{ $t('pages.auth.forgotPassword') }}
            </v-btn>
          </div>
        </v-card>

        <div class="text-center mt-10 text-caption text-medium-emphasis">
          &copy; 2026 ShinBot Project. All rights reserved.
        </div>
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

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.login-page {
  position: relative;
  overflow: hidden;
  background-color: rgb(var(--v-theme-background));
}

.login-background {
  position: absolute;
  top: -10%;
  right: -5%;
  width: 60%;
  height: 60%;
  background: radial-gradient(circle, rgba(var(--v-theme-primary), 0.08) 0%, transparent 70%);
  filter: blur(60px);
  z-index: 0;
}

.login-card {
  @include surface-card(rgba(var(--v-theme-primary), 0.08), 32px, 0 20px 50px rgba(0,0,0,0.1));
  backdrop-filter: blur(10px);
}
</style>
