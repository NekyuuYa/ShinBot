<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.settings.title')"
      :subtitle="$t('pages.settings.subtitle')"
      :kicker="$t('pages.settings.kicker')"
    />
  </v-container>
</template>

<script setup lang="ts">
import AppPageHeader from '@/components/AppPageHeader.vue'
</script>
