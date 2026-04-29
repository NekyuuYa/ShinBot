<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.settings.title')"
      :subtitle="$t('pages.settings.subtitle')"
      :kicker="$t('pages.settings.kicker')"
    />

    <v-row>
      <v-col cols="12" lg="6">
        <v-card class="pa-6 settings-card" elevation="0">
          <v-card-title class="px-0 pt-0">
            {{ $t('pages.settings.credentials.title') }}
          </v-card-title>
          <v-card-subtitle class="px-0 pb-4">
            {{ $t('pages.settings.credentials.subtitle') }}
          </v-card-subtitle>
          <credentials-update-form @updated="() => loadUpdateStatus({ force: true })" />
        </v-card>
      </v-col>

      <v-col cols="12" lg="6">
        <update-status-card
          v-model:confirm-visible="updateConfirmDialog"
          :status="updateStatus"
          :error="updateError"
          :last-result="lastResult"
          :is-loading="isLoadingUpdateStatus"
          :is-submitting="isSubmittingUpdate"
          @refresh="() => loadUpdateStatus({ force: true })"
          @submit="submitUpdate"
        />
      </v-col>

      <v-col cols="12">
        <v-card class="pa-6 settings-card" elevation="0">
          <div class="settings-card-header">
            <div>
              <v-card-title class="px-0 pt-0">
                {{ $t('pages.settings.pricing.title') }}
              </v-card-title>
              <v-card-subtitle class="px-0 pb-0">
                {{ $t('pages.settings.pricing.subtitle') }}
              </v-card-subtitle>
            </div>

            <v-chip color="primary" variant="flat" size="small">
              {{ pricingPreview }}
            </v-chip>
          </div>

          <v-row class="mt-2">
            <v-col cols="12" md="6">
              <v-select
                v-model="systemSettingsStore.pricingCurrency"
                :items="pricingCurrencyOptions"
                item-title="label"
                item-value="value"
                :label="$t('pages.settings.pricing.currency')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
            <v-col cols="12" md="6">
              <v-select
                v-model="systemSettingsStore.pricingTokenUnit"
                :items="pricingTokenUnitOptions"
                item-title="label"
                item-value="value"
                :label="$t('pages.settings.pricing.unit')"
                variant="outlined"
                density="comfortable"
              />
            </v-col>
          </v-row>

          <v-alert type="info" variant="tonal" density="comfortable" class="mt-2">
            {{ $t('pages.settings.pricing.hint') }}
          </v-alert>
        </v-card>
      </v-col>

      <v-col cols="12">
        <dist-update-card
          v-model:confirm-visible="distConfirmDialog"
          :status="distStatus"
          :error="distError"
          :last-result="lastDistResult"
          :is-loading="isLoadingDistStatus"
          :is-submitting="isSubmittingDist"
          @refresh="() => loadDistStatus({ force: true })"
          @submit="submitDistUpdate"
        />
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'

import AppPageHeader from '@/components/AppPageHeader.vue'
import CredentialsUpdateForm from '@/components/CredentialsUpdateForm.vue'
import DistUpdateCard from '@/components/settings/DistUpdateCard.vue'
import UpdateStatusCard from '@/components/settings/UpdateStatusCard.vue'
import { useSystemUpdate } from '@/composables/useSystemUpdate'
import { translate } from '@/plugins/i18n'
import { useSystemSettingsStore } from '@/stores/systemSettings'

const systemSettingsStore = useSystemSettingsStore()

const {
  updateConfirmDialog,
  distConfirmDialog,
  updateStatus,
  distStatus,
  updateError,
  distError,
  lastResult,
  lastDistResult,
  isLoadingUpdateStatus,
  isSubmittingUpdate,
  isLoadingDistStatus,
  isSubmittingDist,
  loadUpdateStatus,
  submitUpdate,
  loadDistStatus,
  submitDistUpdate,
} = useSystemUpdate()

const pricingCurrencyOptions = computed(() => [
  { label: 'CNY', value: 'CNY' },
  { label: 'USD', value: 'USD' },
])

const pricingTokenUnitOptions = computed(() => [
  { label: translate('pages.settings.pricing.units.mtokens'), value: 'mtokens' },
  { label: translate('pages.settings.pricing.units.ktokens'), value: 'ktokens' },
])

const pricingPreview = computed(
  () =>
    `${systemSettingsStore.pricingCurrency} / ${translate(`pages.settings.pricing.units.${systemSettingsStore.pricingTokenUnit}`)}`
)

onMounted(() => {
  void loadUpdateStatus()
  void loadDistStatus()
})
</script>

<style scoped lang="scss">
@use '@/styles/settings-card';
</style>
