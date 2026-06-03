<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.commands.title')"
      :subtitle="$t('pages.commands.subtitle')"
      :kicker="$t('pages.commands.kicker')"
    >
      <template #actions>
        <v-btn color="secondary" prepend-icon="mdi-refresh" @click="handleRefresh">
          {{ $t('pages.commands.refresh') }}
        </v-btn>
      </template>
    </app-page-header>

    <v-row class="mx-0 mb-6" align="stretch">
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.summary.total') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ commandsStore.commands.length }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.summary.enabled') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ commandsStore.enabledCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
      <v-col cols="12" md="4" class="pa-2">
        <v-card rounded="xl" elevation="0" class="summary-card">
          <v-card-text>
            <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.summary.pluginOwned') }}</div>
            <div class="text-h4 font-weight-black mt-2">{{ commandsStore.pluginOwnedCount }}</div>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-card rounded="xl" elevation="0" class="filter-card mb-6">
      <v-card-text>
        <v-row class="mx-0" align="center">
          <v-col cols="12" md="4" class="pa-2">
            <v-text-field
              v-model="searchQuery"
              :label="$t('common.actions.action.search')"
              prepend-inner-icon="mdi-magnify"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              bg-color="surface"
            />
          </v-col>
          <v-col cols="12" sm="6" md="3" class="pa-2">
            <v-select
              v-model="statusFilter"
              :label="$t('pages.commands.filters.status')"
              :items="statusItems"
              item-title="title"
              item-value="value"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              bg-color="surface"
            />
          </v-col>
          <v-col cols="12" sm="6" md="3" class="pa-2">
            <v-select
              v-model="ownerFilter"
              :label="$t('pages.commands.filters.owner')"
              :items="ownerItems"
              item-title="title"
              item-value="value"
              variant="outlined"
              density="comfortable"
              hide-details
              rounded="lg"
              bg-color="surface"
            />
          </v-col>
          <v-col cols="12" md="2" class="pa-2 d-flex justify-end">
            <layout-mode-button
              :model-value="commandsStore.layoutMode"
              :list-label="$t('pages.commands.layout.list')"
              :card-label="$t('pages.commands.layout.card')"
              @update:model-value="handleLayoutChange"
            />
          </v-col>
        </v-row>
      </v-card-text>
    </v-card>

    <v-row v-if="showInitialSkeleton" class="mx-0">
      <v-col cols="12" class="pa-0">
        <v-skeleton-loader type="list-item-two-line, list-item-two-line, list-item-two-line" />
      </v-col>
    </v-row>

    <v-row v-else-if="hasLoadedCommands && filteredCommands.length === 0" justify="center" class="mx-0 py-12">
      <v-col cols="12" md="6" class="text-center pa-0">
        <v-icon size="120" color="grey-lighten-1" icon="mdi-console-line" />
        <h3 class="text-h6 my-4">{{ $t('pages.commands.empty.title') }}</h3>
        <p class="text-body-2 text-medium-emphasis">{{ $t('pages.commands.empty.subtitle') }}</p>
      </v-col>
    </v-row>

    <template v-else-if="commandsStore.layoutMode === 'card'">
      <v-row class="mx-n3">
        <v-col
          v-for="command in filteredCommands"
          :key="command.name"
          cols="12"
          md="6"
          xl="4"
          class="pa-3"
        >
          <v-card rounded="xl" elevation="0" class="command-card">
            <v-card-text>
              <div class="d-flex align-start justify-space-between ga-4">
                <div class="min-w-0">
                  <div class="text-overline text-medium-emphasis">{{ command.owner || $t('pages.commands.owner.core') }}</div>
                  <div class="text-h6 font-weight-bold text-break">{{ command.name }}</div>
                </div>
                <v-switch
                  :model-value="command.enabled"
                  color="primary"
                  hide-details
                  inset
                  :loading="commandsStore.isSaving"
                  @update:model-value="(value) => handleEnabledChange(command.name, Boolean(value))"
                />
              </div>

              <div class="mt-3 d-flex flex-wrap ga-2">
                <v-chip size="small" variant="tonal">{{ $t(`pages.commands.mode.${command.mode}`) }}</v-chip>
                <v-chip size="small" variant="outlined">{{ command.priorityLabel }}</v-chip>
                <v-chip size="small" :color="command.enabled ? 'success' : 'default'" variant="tonal">
                  {{ command.enabled ? $t('pages.commands.status.enabled') : $t('pages.commands.status.disabled') }}
                </v-chip>
              </div>

              <p class="text-body-2 text-medium-emphasis mt-4 mb-0">
                {{ command.description || $t('pages.commands.empty.description') }}
              </p>

              <div class="mt-4 d-grid ga-3 text-body-2">
                <div>
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.triggers') }}</div>
                  <div class="text-break">{{ command.triggers.join(', ') }}</div>
                </div>
                <div v-if="command.usage">
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.usage') }}</div>
                  <div class="text-break">{{ command.usage }}</div>
                </div>
                <div>
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.permission') }}</div>
                  <div class="text-break">{{ command.permission || $t('pages.commands.empty.permission') }}</div>
                </div>
                <div v-if="command.pattern">
                  <div class="text-caption text-medium-emphasis">{{ $t('pages.commands.fields.pattern') }}</div>
                  <div class="text-break">{{ command.pattern }}</div>
                </div>
              </div>
            </v-card-text>
          </v-card>
        </v-col>
      </v-row>
    </template>

    <div v-else class="d-grid ga-4">
      <v-card
        v-for="command in filteredCommands"
        :key="command.name"
        rounded="xl"
        elevation="0"
        class="command-row"
      >
        <v-card-text class="d-flex flex-column flex-lg-row align-lg-center ga-4">
          <div class="flex-grow-1 min-w-0">
            <div class="d-flex flex-wrap align-center ga-2 mb-1">
              <div class="text-h6 font-weight-bold text-break">{{ command.name }}</div>
              <v-chip size="small" variant="tonal">{{ $t(`pages.commands.mode.${command.mode}`) }}</v-chip>
              <v-chip size="small" variant="outlined">{{ command.priorityLabel }}</v-chip>
            </div>
            <div class="text-body-2 text-medium-emphasis">
              {{ command.description || $t('pages.commands.empty.description') }}
            </div>
            <div class="mt-3 d-flex flex-wrap ga-4 text-body-2">
              <div class="text-break">
                <span class="text-medium-emphasis">{{ $t('pages.commands.fields.owner') }}:</span>
                {{ command.owner || $t('pages.commands.owner.core') }}
              </div>
              <div class="text-break">
                <span class="text-medium-emphasis">{{ $t('pages.commands.fields.triggers') }}:</span>
                {{ command.triggers.join(', ') }}
              </div>
              <div class="text-break">
                <span class="text-medium-emphasis">{{ $t('pages.commands.fields.permission') }}:</span>
                {{ command.permission || $t('pages.commands.empty.permission') }}
              </div>
            </div>
          </div>

          <div class="d-flex align-center ga-3">
            <v-chip :color="command.enabled ? 'success' : 'default'" variant="tonal">
              {{ command.enabled ? $t('pages.commands.status.enabled') : $t('pages.commands.status.disabled') }}
            </v-chip>
            <v-switch
              :model-value="command.enabled"
              color="primary"
              hide-details
              inset
              :loading="commandsStore.isSaving"
              @update:model-value="(value) => handleEnabledChange(command.name, Boolean(value))"
            />
          </div>
        </v-card-text>
      </v-card>
    </div>

    <v-alert v-if="commandsStore.error" type="error" class="mt-4">
      {{ commandsStore.error }}
    </v-alert>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppPageHeader from '@/components/AppPageHeader.vue'
import LayoutModeButton from '@/components/LayoutModeButton.vue'
import { useDelayedFlag } from '@/composables/useDelayedFlag'
import { useCommandsStore, type CommandLayoutMode } from '@/stores/commands'
import { translate } from '@/plugins/i18n'

const commandsStore = useCommandsStore()

const searchQuery = ref('')
const statusFilter = ref('all')
const ownerFilter = ref('all')
const hasLoadedCommands = ref(false)

const initialSkeletonRequested = computed(
  () => commandsStore.isLoading && commandsStore.commands.length === 0
)
const showInitialSkeleton = useDelayedFlag(initialSkeletonRequested)

const statusItems = computed(() => [
  { title: translate('pages.commands.filters.all'), value: 'all' },
  { title: translate('pages.commands.status.enabled'), value: 'enabled' },
  { title: translate('pages.commands.status.disabled'), value: 'disabled' },
])

const ownerItems = computed(() => [
  { title: translate('pages.commands.filters.all'), value: 'all' },
  { title: translate('pages.commands.owner.core'), value: 'core' },
  { title: translate('pages.commands.owner.plugin'), value: 'plugin' },
])

const filteredCommands = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()

  return commandsStore.commands.filter((command) => {
    const matchesQuery =
      !query ||
      [
        command.name,
        command.description,
        command.usage,
        command.permission,
        command.owner,
        command.pattern,
        ...command.triggers,
      ]
        .filter(Boolean)
        .some((value) => value.toLowerCase().includes(query))

    const matchesStatus =
      statusFilter.value === 'all'
      || (statusFilter.value === 'enabled' && command.enabled)
      || (statusFilter.value === 'disabled' && !command.enabled)

    const isPluginOwned = Boolean(command.owner)
    const matchesOwner =
      ownerFilter.value === 'all'
      || (ownerFilter.value === 'plugin' && isPluginOwned)
      || (ownerFilter.value === 'core' && !isPluginOwned)

    return matchesQuery && matchesStatus && matchesOwner
  })
})

async function loadCommands(force = false) {
  try {
    await commandsStore.fetchCommands({ force })
  } finally {
    hasLoadedCommands.value = true
  }
}

onMounted(() => {
  void loadCommands()
})

const handleRefresh = () => {
  void loadCommands(true)
}

const handleLayoutChange = (mode: CommandLayoutMode) => {
  if (mode) {
    commandsStore.setLayoutMode(mode)
  }
}

const handleEnabledChange = async (name: string, enabled: boolean) => {
  await commandsStore.updateCommandEnabled(name, enabled)
}
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.summary-card,
.filter-card,
.command-card,
.command-row {
  @include surface-card;
  @include hover-lift;
}
</style>
