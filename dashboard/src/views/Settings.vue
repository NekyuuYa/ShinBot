<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.settings.title')"
      :subtitle="$t('pages.settings.subtitle')"
      :kicker="$t('pages.settings.kicker')"
    />

    <v-row>
      <v-col cols="12" md="8" lg="6">
        <v-card class="pa-6" elevation="4">
          <v-card-title class="px-0 pt-0">
            {{ $t('pages.settings.credentials.title') }}
          </v-card-title>
          <v-card-subtitle class="px-0 pb-4">
            {{ $t('pages.settings.credentials.subtitle') }}
          </v-card-subtitle>
          <credentials-update-form />
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import AppPageHeader from '@/components/AppPageHeader.vue'
import CredentialsUpdateForm from '@/components/CredentialsUpdateForm.vue'
</script>
