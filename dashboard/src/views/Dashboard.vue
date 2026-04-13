<template>
  <v-container fluid class="pa-0">
    <v-row class="mb-8">
      <v-col cols="12">
        <h1 class="text-h4 font-weight-bold">{{ $t('pages.dashboard.welcome', { name: authStore.username }) }}</h1>
      </v-col>
    </v-row>

    <v-row class="mb-6">
      <v-col cols="12" sm="6" md="3">
        <v-card class="pa-4" elevation="6">
          <v-row no-gutters align="center">
            <v-col cols="8">
              <div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.totalInstances') }}</div>
                <div class="text-h5">{{ instancesStore.instances.length }}</div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.statusOnline', { value: monitoringStore.status.online ? $t('common.actions.status.online') : $t('common.actions.status.offline') }) }}</div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.memoryUsageMb', { value: monitoringStore.status.memoryUsage }) }}</div>
              </div>
            </v-col>
            <v-col cols="4" class="text-right">
              <v-icon size="40" color="primary" icon="mdi-robot" />
            </v-col>
          </v-row>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card class="pa-4" elevation="6">
          <v-row no-gutters align="center">
            <v-col cols="8">
              <div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.runningInstances') }}</div>
                <div class="text-h5">{{ runningInstances }}</div>
              </div>
            </v-col>
            <v-col cols="4" class="text-right">
              <v-icon size="40" color="success" icon="mdi-check-circle" />
            </v-col>
          </v-row>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card class="pa-4" elevation="6">
          <v-row no-gutters align="center">
            <v-col cols="8">
              <div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.totalPlugins') }}</div>
                <div class="text-h5">{{ pluginsStore.plugins.length }}</div>
              </div>
            </v-col>
            <v-col cols="4" class="text-right">
              <v-icon size="40" color="secondary" icon="mdi-puzzle" />
            </v-col>
          </v-row>
        </v-card>
      </v-col>

      <v-col cols="12" sm="6" md="3">
        <v-card class="pa-4" elevation="6">
          <v-row no-gutters align="center">
            <v-col cols="8">
              <div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.enabledPlugins') }}</div>
                <div class="text-h5">{{ enabledPlugins }}</div>
              </div>
            </v-col>
            <v-col cols="4" class="text-right">
              <v-icon size="40" color="success" icon="mdi-puzzle-check" />
            </v-col>
          </v-row>
        </v-card>
      </v-col>
    </v-row>

    <v-row>
      <v-col cols="12">
        <h2 class="text-h6 mb-4">{{ $t('pages.dashboard.quickActions.title') }}</h2>
      </v-col>
      <v-col cols="12" sm="6" md="4">
        <v-card class="pa-4" elevation="4" @click="navigateTo('/instances')" style="cursor: pointer">
          <v-icon size="40" color="primary" icon="mdi-robot" class="mb-2" />
          <div class="text-subtitle2">{{ $t('pages.dashboard.quickActions.instancesTitle') }}</div>
          <p class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.quickActions.instancesDescription') }}</p>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="4">
        <v-card class="pa-4" elevation="4" @click="navigateTo('/plugins')" style="cursor: pointer">
          <v-icon size="40" color="secondary" icon="mdi-puzzle" class="mb-2" />
          <div class="text-subtitle2">{{ $t('pages.dashboard.quickActions.pluginsTitle') }}</div>
          <p class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.quickActions.pluginsDescription') }}</p>
        </v-card>
      </v-col>
      <v-col cols="12" sm="6" md="4">
        <v-card class="pa-4" elevation="4" @click="navigateTo('/monitoring')" style="cursor: pointer">
          <v-icon size="40" color="warning" icon="mdi-monitor" class="mb-2" />
          <div class="text-subtitle2">{{ $t('pages.dashboard.quickActions.monitoringTitle') }}</div>
          <p class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.quickActions.monitoringDescription') }}</p>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '@/stores/auth'
import { useInstancesStore } from '@/stores/instances'
import { usePluginsStore } from '@/stores/plugins'
import { useMonitoringStore } from '@/stores/monitoring'

const router = useRouter()
const authStore = useAuthStore()
const instancesStore = useInstancesStore()
const pluginsStore = usePluginsStore()
const monitoringStore = useMonitoringStore()
const { t } = useI18n()

const runningInstances = computed(() =>
  instancesStore.instances.filter((instance: (typeof instancesStore.instances)[number]) => instance.status === 'running').length
)

const enabledPlugins = computed(() =>
  pluginsStore.plugins.filter((plugin: (typeof pluginsStore.plugins)[number]) => plugin.status === 'enabled').length
)

onMounted(async () => {
  try {
    await Promise.all([instancesStore.fetchInstances(), pluginsStore.fetchPlugins()])
  } catch (err) {
    console.error(t('pages.dashboard.loadFailed'), err)
  }
})

const navigateTo = (path: string) => {
  router.push(path)
}
</script>
