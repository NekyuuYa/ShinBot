<template>
  <v-dialog v-model="visible" max-width="860">
    <v-card class="platform-dialog-card">
      <v-card-title class="px-6 pt-6">
        {{ title }}
      </v-card-title>

      <v-card-text class="px-6">
        <v-alert
          v-if="errorText"
          type="warning"
          variant="tonal"
          density="comfortable"
          class="mb-5"
        >
          {{ errorText }}
        </v-alert>

        <v-row>
          <v-col cols="12" md="6">
            <v-text-field
              v-model="form.name"
              :label="$t('pages.messagePlatforms.form.name')"
              density="comfortable"
              variant="outlined"
            />
          </v-col>

          <v-col cols="12" md="6">
            <v-text-field
              v-model="form.id"
              :label="$t('pages.messagePlatforms.form.id')"
              :disabled="editing"
              density="comfortable"
              variant="outlined"
            />
          </v-col>

          <v-col cols="12" md="6">
            <v-select
              :model-value="form.adapter"
              :items="adapterOptions"
              :label="$t('pages.messagePlatforms.form.adapter')"
              item-title="title"
              item-value="value"
              density="comfortable"
              variant="outlined"
              @update:model-value="updateAdapter"
            />
          </v-col>

          <v-col cols="12" md="6">
            <v-switch
              v-model="form.enabled"
              :label="$t('pages.messagePlatforms.form.enabled')"
              color="primary"
              density="comfortable"
              inset
            />
          </v-col>
        </v-row>

        <v-divider class="my-5" />

        <div class="dialog-section-heading">
          <v-icon icon="mdi-tune-variant" size="18" />
          <span>{{ $t('pages.messagePlatforms.form.config') }}</span>
        </div>

        <provider-schema-form
          v-if="activeProvider"
          v-model="form.config"
          :provider="activeProvider"
          :issues="providerIssues"
          :path-prefix="pathPrefix"
          :advanced-label="$t('pages.messagePlatforms.form.advancedConfig')"
          :empty-text="$t('pages.messagePlatforms.form.noSchema')"
          :json-error-text="$t('pages.messagePlatforms.validation.invalidJson')"
        />

        <v-alert v-else type="warning" variant="tonal" density="comfortable">
          {{ $t('pages.messagePlatforms.form.noProvider') }}
        </v-alert>
      </v-card-text>

      <v-card-actions class="px-6 pb-6">
        <v-spacer />
        <v-btn variant="text" @click="emit('close')">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn
          color="primary"
          :loading="saving"
          :disabled="saving"
          @click="emit('save')"
        >
          {{ $t('common.actions.action.save') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import type { ConfigValidationIssue, ConfigWorkspaceProvider } from '@/api/config'
import ProviderSchemaForm from '@/components/config/ProviderSchemaForm.vue'
import type {
  MessagePlatformAdapterOption,
  MessagePlatformFormState,
} from './types'

interface Props {
  title: string
  adapterOptions: MessagePlatformAdapterOption[]
  activeProvider: ConfigWorkspaceProvider | null
  providerIssues?: ConfigValidationIssue[]
  pathPrefix?: string
  editing?: boolean
  saving?: boolean
  errorText?: string
}

withDefaults(defineProps<Props>(), {
  providerIssues: () => [],
  pathPrefix: '',
  editing: false,
  saving: false,
  errorText: '',
})

const emit = defineEmits<{
  close: []
  save: []
  'adapter-change': [adapter: string, previousAdapter: string]
}>()

const visible = defineModel<boolean>('visible', { required: true })
const form = defineModel<MessagePlatformFormState>('form', { required: true })

function updateAdapter(value: unknown) {
  const adapter = String(value ?? '')
  const previousAdapter = form.value.adapter
  if (adapter === previousAdapter) {
    return
  }

  form.value = {
    ...form.value,
    adapter,
    config: {},
  }
  emit('adapter-change', adapter, previousAdapter)
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.platform-dialog-card {
  border-radius: $radius-sm;
}

.dialog-section-heading {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 16px;
  color: rgba(var(--v-theme-on-surface), 0.82);
  font-size: $font-size-sm;
  font-weight: 700;
}
</style>
