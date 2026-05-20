<template>
  <v-card class="pa-6 settings-card" elevation="0">
    <div class="settings-card-header">
      <div>
        <v-card-title class="px-0 pt-0">
          {{ $t('pages.settings.runtime.title') }}
        </v-card-title>
        <v-card-subtitle class="px-0 pb-0">
          {{ $t('pages.settings.runtime.subtitle') }}
        </v-card-subtitle>
      </div>

      <v-chip :color="configStore.isDirty ? 'warning' : 'success'" variant="flat" size="small">
        {{
          configStore.isDirty
            ? $t('pages.settings.runtime.unsaved')
            : $t('pages.settings.runtime.synced')
        }}
      </v-chip>
    </div>

    <v-alert
      v-if="configStore.error"
      type="error"
      variant="tonal"
      density="comfortable"
      class="mt-4"
    >
      {{ configStore.error }}
    </v-alert>

    <v-row class="mt-2">
      <v-col cols="12" md="6">
        <v-select
          v-model="modelRuntimeMode"
          :items="runtimeModeOptions"
          item-title="title"
          item-value="value"
          :label="$t('pages.settings.runtime.modelMode')"
          :hint="$t('pages.settings.runtime.modelModeHint')"
          persistent-hint
          variant="outlined"
          density="comfortable"
        />
      </v-col>
      <v-col cols="12" md="6">
        <v-switch
          v-model="agentRuntimeEnabled"
          color="primary"
          inset
          :label="$t('pages.settings.runtime.agentEnabled')"
          :hint="$t('pages.settings.runtime.agentEnabledHint')"
          persistent-hint
        />
      </v-col>
      <v-col cols="12" md="6">
        <v-select
          v-model="loggingLevel"
          :items="loggingLevelOptions"
          item-title="title"
          item-value="value"
          :label="$t('pages.settings.runtime.loggingLevel')"
          variant="outlined"
          density="comfortable"
        />
      </v-col>
      <v-col cols="12" md="6">
        <v-select
          v-model="thirdPartyNoise"
          :items="thirdPartyNoiseOptions"
          item-title="title"
          item-value="value"
          :label="$t('pages.settings.runtime.thirdPartyNoise')"
          :hint="$t('pages.settings.runtime.thirdPartyNoiseHint')"
          persistent-hint
          variant="outlined"
          density="comfortable"
        />
      </v-col>
      <v-col cols="12" md="8">
        <v-text-field
          v-model="databaseUrl"
          :label="$t('pages.settings.runtime.databaseUrl')"
          :hint="$t('pages.settings.runtime.databaseUrlHint')"
          persistent-hint
          variant="outlined"
          density="comfortable"
        />
      </v-col>
      <v-col cols="12" md="4">
        <v-text-field
          v-model.number="snapshotTtl"
          :label="$t('pages.settings.runtime.snapshotTtl')"
          :hint="$t('pages.settings.runtime.snapshotTtlHint')"
          persistent-hint
          type="number"
          min="0"
          variant="outlined"
          density="comfortable"
        />
      </v-col>
    </v-row>

    <v-alert type="info" variant="tonal" density="comfortable" class="mt-2">
      {{ $t('pages.settings.runtime.restartHint') }}
    </v-alert>

    <div class="settings-actions mt-6">
      <div class="settings-actions__group">
        <v-btn
          variant="text"
          prepend-icon="mdi-refresh"
          :loading="configStore.isLoading"
          @click="loadConfig"
        >
          {{ $t('common.actions.action.refresh') }}
        </v-btn>
        <v-btn
          variant="text"
          prepend-icon="mdi-restore"
          :disabled="!configStore.isDirty || configStore.isSaving"
          @click="configStore.resetDraft"
        >
          {{ $t('common.actions.action.reset') }}
        </v-btn>
      </div>

      <div class="settings-actions__group">
        <v-btn
          variant="tonal"
          prepend-icon="mdi-check-decagram-outline"
          :loading="configStore.isValidating"
          @click="configStore.validateDraft"
        >
          {{ $t('pages.settings.runtime.validate') }}
        </v-btn>
        <v-btn
          color="primary"
          prepend-icon="mdi-content-save-outline"
          :loading="configStore.isSaving"
          @click="saveConfig"
        >
          {{ $t('common.actions.action.save') }}
        </v-btn>
      </div>
    </div>
  </v-card>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'

import { apiClient } from '@/api/client'
import { systemApi } from '@/api/system'
import type { ConfigRecord, ConfigValue } from '@/api/config'
import { translate } from '@/plugins/i18n'
import { useConfigWorkspaceStore } from '@/stores/configWorkspace'

type RuntimeMode = 'auto' | 'enabled' | 'disabled'
type ThirdPartyNoise = 'off' | 'debug' | 'on'

const configStore = useConfigWorkspaceStore()

const runtimeModeOptions = computed(() => [
  { title: translateRuntime('modelModeAuto'), value: 'auto' },
  { title: translateRuntime('modelModeEnabled'), value: 'enabled' },
  { title: translateRuntime('modelModeDisabled'), value: 'disabled' },
])

const loggingLevelOptions = ['DEBUG', 'INFO', 'WARNING', 'ERROR'].map((level) => ({
  title: level,
  value: level,
}))

const thirdPartyNoiseOptions = computed(() => [
  { title: translateRuntime('thirdPartyNoiseOff'), value: 'off' },
  { title: translateRuntime('thirdPartyNoiseDebug'), value: 'debug' },
  { title: translateRuntime('thirdPartyNoiseOn'), value: 'on' },
])

function translateRuntime(key: string) {
  return translate(`pages.settings.runtime.${key}`)
}

function recordAt(section: string): ConfigRecord {
  const value = configStore.draft[section]
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as ConfigRecord
    : {}
}

function setRecordValue(section: string, key: string, value: ConfigValue | undefined) {
  const nextSection = { ...recordAt(section) }
  if (value === undefined) {
    delete nextSection[key]
  } else {
    nextSection[key] = value
  }
  configStore.setDraftSection(section, nextSection)
}

const modelRuntimeMode = computed<RuntimeMode>({
  get() {
    const value = recordAt('runtime').model
    if (value === true) return 'enabled'
    if (value === false) return 'disabled'
    return 'auto'
  },
  set(value) {
    setRecordValue(
      'runtime',
      'model',
      value === 'auto' ? undefined : value === 'enabled'
    )
  },
})

const agentRuntimeEnabled = computed<boolean>({
  get() {
    return recordAt('runtime').agent !== false
  },
  set(value) {
    setRecordValue('runtime', 'agent', value ? undefined : false)
  },
})

const loggingLevel = computed<string>({
  get() {
    const value = recordAt('logging').level
    return typeof value === 'string' && value.trim() ? value.trim().toUpperCase() : 'INFO'
  },
  set(value) {
    setRecordValue('logging', 'level', value)
  },
})

const thirdPartyNoise = computed<ThirdPartyNoise>({
  get() {
    const value = recordAt('logging').third_party_noise
    return value === 'off' || value === 'on' ? value : 'debug'
  },
  set(value) {
    setRecordValue('logging', 'third_party_noise', value)
  },
})

const databaseUrl = computed<string>({
  get() {
    const value = recordAt('database').url
    return typeof value === 'string'
      ? value
      : configStore.workspace?.templates.database.url ?? 'sqlite:///data/db/shinbot.sqlite3'
  },
  set(value) {
    setRecordValue('database', 'url', value.trim())
  },
})

const snapshotTtl = computed<number>({
  get() {
    const value = recordAt('database').snapshot_ttl
    return typeof value === 'number'
      ? value
      : configStore.workspace?.templates.database.snapshot_ttl ?? 10800
  },
  set(value) {
    const numeric = Number(value)
    setRecordValue('database', 'snapshot_ttl', Number.isFinite(numeric) ? Math.max(0, Math.trunc(numeric)) : 0)
  },
})

async function loadConfig() {
  await configStore.loadWorkspace({ preserveDraft: configStore.isDirty })
}

async function saveConfig() {
  const result = await configStore.saveDraft({ validateBeforeSave: true })
  if (!result) {
    return
  }
  await apiClient.unwrap(systemApi.updateLoggingState({
    level: loggingLevel.value,
    thirdPartyNoise: thirdPartyNoise.value,
    persist: false,
  }))
}

onMounted(() => {
  if (!configStore.hasWorkspace) {
    void configStore.loadWorkspace()
  }
})
</script>

<style scoped lang="scss">
@use '@/styles/settings-card';

.settings-actions__group {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
</style>
