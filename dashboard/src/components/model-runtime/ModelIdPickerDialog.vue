<template>
  <v-dialog
    :model-value="modelValue"
    max-width="1120"
    scrollable
    @update:model-value="$emit('update:modelValue', Boolean($event))"
  >
    <v-card class="picker-dialog">
      <v-card-item class="pb-2">
        <v-card-title>{{ $t('pages.modelRuntime.dialogs.modelIdPicker') }}</v-card-title>
        <v-card-subtitle>{{ $t('pages.modelRuntime.hints.modelIdPicker') }}</v-card-subtitle>
        <template #append>
          <v-btn
            icon="mdi-close"
            variant="text"
            @click="$emit('update:modelValue', false)"
          />
        </template>
      </v-card-item>

      <v-card-text class="d-flex flex-column ga-5">
        <v-text-field
          v-model="search"
          :label="$t('common.actions.action.search')"
          prepend-inner-icon="mdi-magnify"
          density="comfortable"
          variant="outlined"
          hide-details
        />

        <div v-if="filteredRouteOptions.length" class="d-flex flex-column ga-3">
          <div class="section-label">
            {{ $t('pages.modelRuntime.labels.routeTargets') }}
          </div>
          <v-card variant="outlined" class="picker-section-card">
            <v-list class="bg-transparent py-0">
              <v-list-item
                v-for="item in filteredRouteOptions"
                :key="item.id"
                :active="item.id === currentValue"
                rounded="lg"
                class="picker-list-item"
                @click="selectValue(item.id)"
              >
                <template #prepend>
                  <v-avatar
                    size="34"
                    :color="item.enabled ? 'primary' : 'surface-variant'"
                    variant="tonal"
                  >
                    <v-icon icon="mdi-transit-connection-variant" size="18" />
                  </v-avatar>
                </template>
                <v-list-item-title class="text-body-2 font-weight-medium">
                  {{ item.title }}
                </v-list-item-title>
                <v-list-item-subtitle class="text-caption">
                  {{ item.subtitle }}
                </v-list-item-subtitle>
                <template #append>
                  <v-chip
                    size="x-small"
                    variant="tonal"
                    :color="item.enabled ? 'primary' : 'default'"
                  >
                    {{
                      item.enabled
                        ? $t('pages.modelRuntime.labels.enabled')
                        : $t('pages.modelRuntime.labels.disabled')
                    }}
                  </v-chip>
                </template>
              </v-list-item>
            </v-list>
          </v-card>
        </div>

        <div v-if="filteredProviderGroups.length" class="d-flex flex-column ga-3">
          <div class="section-label">
            {{ $t('pages.modelRuntime.sidebar.providers') }}
          </div>
          <v-row>
            <v-col
              v-for="group in filteredProviderGroups"
              :key="group.providerId"
              cols="12"
              md="6"
            >
              <v-card variant="outlined" class="picker-section-card fill-height">
                <v-card-item class="pb-1">
                  <v-card-title class="text-body-1">{{ group.providerName }}</v-card-title>
                  <v-card-subtitle>{{ group.providerType }}</v-card-subtitle>
                </v-card-item>
                <v-card-text class="pt-0">
                  <v-list class="bg-transparent py-0">
                    <v-list-item
                      v-for="item in group.items"
                      :key="`${group.providerId}:${item.value}`"
                      :active="item.value === currentValue"
                      rounded="lg"
                      class="picker-list-item"
                      @click="selectValue(item.value)"
                    >
                      <template #prepend>
                        <v-avatar size="34" color="secondary" variant="tonal">
                          <v-icon icon="mdi-cube-outline" size="18" />
                        </v-avatar>
                      </template>
                      <v-list-item-title class="text-body-2 font-weight-medium">
                        {{ item.title }}
                      </v-list-item-title>
                      <v-list-item-subtitle class="text-caption text-break">
                        {{ item.subtitle }}
                      </v-list-item-subtitle>
                      <template #append>
                        <v-chip
                          size="x-small"
                          variant="tonal"
                          :color="item.kind === 'catalog' ? 'info' : 'primary'"
                        >
                          {{
                            item.kind === 'catalog'
                              ? $t('pages.modelRuntime.labels.catalog')
                              : $t('pages.modelRuntime.labels.configured')
                          }}
                        </v-chip>
                      </template>
                    </v-list-item>
                  </v-list>
                </v-card-text>
              </v-card>
            </v-col>
          </v-row>
        </div>

        <v-sheet
          v-if="!hasVisibleOptions"
          rounded="xl"
          class="empty-state-panel text-body-2 text-medium-emphasis py-8 px-6"
        >
          {{ $t(emptyStateTextKey) }}
        </v-sheet>
      </v-card-text>

      <v-card-actions class="px-6 pb-5 pt-0">
        <v-spacer />
        <v-btn variant="text" @click="$emit('update:modelValue', false)">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'

interface RouteOption {
  id: string
  title: string
  subtitle: string
  enabled: boolean
}

interface ProviderGroupItem {
  value: string
  title: string
  subtitle: string
  kind: 'catalog' | 'configured'
}

interface ProviderGroup {
  providerId: string
  providerName: string
  providerType: string
  items: ProviderGroupItem[]
}

interface Props {
  modelValue: boolean
  currentValue: string
  routeOptions: RouteOption[]
  providerGroups: ProviderGroup[]
}

const props = defineProps<Props>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  select: [value: string]
}>()

const search = ref('')

const keyword = computed(() => search.value.trim().toLowerCase())

const filteredRouteOptions = computed(() => {
  if (!keyword.value) {
    return props.routeOptions
  }
  return props.routeOptions.filter((item) =>
    `${item.title} ${item.subtitle}`.toLowerCase().includes(keyword.value)
  )
})

const filteredProviderGroups = computed(() => {
  if (!keyword.value) {
    return props.providerGroups
  }
  return props.providerGroups
    .map((group) => ({
      ...group,
      items: group.items.filter((item) =>
        `${group.providerName} ${group.providerType} ${item.title} ${item.subtitle}`
          .toLowerCase()
          .includes(keyword.value)
      ),
    }))
    .filter((group) => group.items.length > 0)
})

const hasVisibleOptions = computed(
  () => filteredRouteOptions.value.length > 0 || filteredProviderGroups.value.length > 0
)

const emptyStateTextKey = computed(() =>
  keyword.value
    ? 'pages.modelRuntime.hints.modelIdPickerNoMatches'
    : 'pages.modelRuntime.hints.modelIdPickerEmpty'
)

const selectValue = (value: string) => {
  emit('select', value)
}

watch(
  () => props.modelValue,
  (isOpen) => {
    if (!isOpen) {
      search.value = ''
    }
  }
)
</script>

<style scoped>
.picker-dialog {
  border: 1px solid rgba(var(--v-theme-primary), 0.14);
  border-radius: 28px;
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.98) 0%, rgba(var(--v-theme-background), 0.98) 100%);
  box-shadow: 0 16px 40px rgba(var(--v-theme-primary), 0.08);
}

.picker-section-card {
  border-radius: 22px;
  border-color: rgba(var(--v-theme-primary), 0.14);
  background: rgba(var(--v-theme-surface), 0.82);
}

.picker-list-item {
  margin-bottom: 8px;
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
  background: rgba(var(--v-theme-surface), 0.7);
  transition: border-color 0.18s ease, background-color 0.18s ease;
}

.picker-list-item:hover {
  border-color: rgba(var(--v-theme-primary), 0.18);
  background: rgba(var(--v-theme-surface), 0.96);
}

.section-label {
  font-size: 0.86rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(var(--v-theme-primary), 0.68);
}

.empty-state-panel {
  border: 1px dashed rgba(var(--v-theme-primary), 0.16);
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.95) 0%, rgba(var(--v-theme-surface), 0.78) 100%);
}

:deep(.v-list-item--active) {
  background: linear-gradient(180deg, rgba(var(--v-theme-primary), 0.3) 0%, rgba(var(--v-theme-primary), 0.16) 100%);
  border-color: rgba(var(--v-theme-primary), 0.18);
}
</style>
