<template>
  <section class="audit-filter-panel">
    <div class="filter-head">
      <div>
        <div class="panel-kicker">{{ title }}</div>
        <div class="panel-total">{{ totalLabel }}</div>
      </div>
      <v-btn
        variant="text"
        color="secondary"
        prepend-icon="mdi-filter-off-outline"
        rounded="lg"
        @click="$emit('clear')"
      >
        {{ clearLabel }}
      </v-btn>
    </div>

    <div class="filter-grid">
      <v-text-field
        v-model="query"
        :label="searchLabel"
        prepend-inner-icon="mdi-magnify"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-select
        v-model="providerId"
        :label="providerLabel"
        :items="providerOptions"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-select
        v-model="modelId"
        :label="modelLabel"
        :items="modelOptions"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-select
        v-model="routeId"
        :label="routeLabel"
        :items="routeOptions"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-text-field
        v-model="caller"
        :label="callerLabel"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-text-field
        v-model="sessionId"
        :label="sessionLabel"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-text-field
        v-model="instanceId"
        :label="instanceLabel"
        density="comfortable"
        variant="outlined"
        hide-details
        clearable
      />
      <v-select
        v-model="status"
        :label="statusLabel"
        :items="statusOptions"
        density="comfortable"
        variant="outlined"
        hide-details
      />
    </div>
  </section>
</template>

<script setup lang="ts">
interface SelectItem {
  title: string
  value: string
}

const query = defineModel<string>('query', { required: true })
const providerId = defineModel<string>('providerId', { required: true })
const modelId = defineModel<string>('modelId', { required: true })
const routeId = defineModel<string>('routeId', { required: true })
const caller = defineModel<string>('caller', { required: true })
const sessionId = defineModel<string>('sessionId', { required: true })
const instanceId = defineModel<string>('instanceId', { required: true })
const status = defineModel<string>('status', { required: true })

defineProps<{
  title: string
  totalLabel: string
  clearLabel: string
  searchLabel: string
  providerLabel: string
  modelLabel: string
  routeLabel: string
  callerLabel: string
  sessionLabel: string
  instanceLabel: string
  statusLabel: string
  providerOptions: SelectItem[]
  modelOptions: SelectItem[]
  routeOptions: SelectItem[]
  statusOptions: SelectItem[]
}>()

defineEmits<{
  clear: []
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.audit-filter-panel {
  @include surface-card;
  padding: 20px;
}

.filter-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}

.panel-kicker {
  color: rgb(var(--v-theme-primary));
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.panel-total {
  margin-top: 4px;
  color: rgba(var(--v-theme-on-surface), 0.68);
  font-size: $font-size-sm;
}

.filter-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(180px, 1fr));
  gap: 12px;
}

@media (max-width: 1280px) {
  .filter-grid {
    grid-template-columns: repeat(2, minmax(180px, 1fr));
  }
}

@media (max-width: 760px) {
  .filter-head {
    align-items: flex-start;
    flex-direction: column;
  }

  .filter-grid {
    grid-template-columns: 1fr;
  }
}
</style>
