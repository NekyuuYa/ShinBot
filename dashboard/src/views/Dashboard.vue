<template>
  <v-container fluid class="pa-0">
    <app-page-header
      :title="$t('pages.dashboard.welcome', { name: authStore.displayName })"
      :subtitle="$t('pages.dashboard.subtitle')"
      :kicker="$t('pages.dashboard.kicker')"
    />

    <v-row class="mb-6">
      <v-col cols="12" sm="6" md="3">
        <v-card class="pa-4" elevation="6">
          <v-row no-gutters align="center">
            <v-col cols="8">
              <div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.totalInstances') }}</div>
                <div class="text-h5">{{ instancesStore.instances.length }}</div>
                <div class="text-caption text-medium-emphasis">{{ $t('pages.dashboard.cards.statusOnline', { value: monitoringStore.isOnline ? $t('common.actions.status.online') : $t('common.actions.status.offline') }) }}</div>
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

    <v-row class="mb-6">
      <v-col cols="12" lg="7">
        <v-card class="token-usage-card pa-5" elevation="8">
          <div class="d-flex align-start justify-space-between ga-4 flex-wrap">
            <div>
              <div class="text-overline token-usage-kicker">
                {{ $t('pages.dashboard.tokenStats.kicker', { days: tokenSummary?.windowDays ?? 7 }) }}
              </div>
              <div class="text-h6 mb-1">{{ $t('pages.dashboard.tokenStats.title') }}</div>
              <div class="text-caption token-usage-muted">
                {{ $t('pages.dashboard.tokenStats.subtitle') }}
              </div>
            </div>
            <v-avatar color="white" variant="tonal" size="48" class="token-usage-icon">
              <v-icon icon="mdi-counter" size="28" />
            </v-avatar>
          </div>

          <div class="mt-5">
            <div class="token-usage-total">{{ formatCompactNumber(tokenTotal) }}</div>
            <div class="text-caption token-usage-muted">
              {{ $t('pages.dashboard.tokenStats.totalTokens') }}
            </div>
          </div>

          <v-progress-linear
            class="my-5 token-usage-progress"
            :model-value="tokenOutputShare"
            height="8"
            rounded
          />

          <v-row dense>
            <v-col cols="12" sm="4">
              <div class="token-usage-metric">
                <div class="text-caption token-usage-muted">
                  {{ $t('pages.dashboard.tokenStats.inputTokens') }}
                </div>
                <div class="text-subtitle-1 font-weight-bold">
                  {{ formatNumber(tokenSummary?.inputTokens ?? 0) }}
                </div>
              </div>
            </v-col>
            <v-col cols="12" sm="4">
              <div class="token-usage-metric">
                <div class="text-caption token-usage-muted">
                  {{ $t('pages.dashboard.tokenStats.outputTokens') }}
                </div>
                <div class="text-subtitle-1 font-weight-bold">
                  {{ formatNumber(tokenSummary?.outputTokens ?? 0) }}
                </div>
              </div>
            </v-col>
            <v-col cols="12" sm="4">
              <div class="token-usage-metric">
                <div class="text-caption token-usage-muted">
                  {{ $t('pages.dashboard.tokenStats.cacheTokens') }}
                </div>
                <div class="text-subtitle-1 font-weight-bold">
                  {{ formatNumber(tokenCacheTotal) }}
                </div>
              </div>
            </v-col>
          </v-row>

          <v-divider class="my-4 token-usage-divider" />

          <div class="d-flex align-center justify-space-between ga-4 flex-wrap">
            <div>
              <div class="text-caption token-usage-muted">
                {{ $t('pages.dashboard.tokenStats.calls') }}
              </div>
              <div class="text-body-2">
                {{
                  $t('pages.dashboard.tokenStats.callCount', {
                    success: tokenSummary?.successfulCalls ?? 0,
                    total: tokenSummary?.totalCalls ?? 0,
                  })
                }}
              </div>
            </div>
            <div class="text-sm-right">
              <div class="text-caption token-usage-muted">
                {{ $t('pages.dashboard.tokenStats.topModel') }}
              </div>
              <div class="text-body-2 font-weight-medium">
                {{ topTokenModel?.modelId || $t('pages.dashboard.tokenStats.noModel') }}
              </div>
            </div>
          </div>
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
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import AppPageHeader from '@/components/AppPageHeader.vue'
import { useAuthStore } from '@/stores/auth'
import { useInstancesStore } from '@/stores/instances'
import { usePluginsStore } from '@/stores/plugins'
import { useMonitoringStore } from '@/stores/monitoring'
import { modelRuntimeApi, type ModelTokenSummary } from '@/api/modelRuntime'

const router = useRouter()
const authStore = useAuthStore()
const instancesStore = useInstancesStore()
const pluginsStore = usePluginsStore()
const monitoringStore = useMonitoringStore()
const { t, locale } = useI18n()
const tokenSummary = ref<ModelTokenSummary | null>(null)

const runningInstances = computed(() =>
  instancesStore.instances.filter((instance: (typeof instancesStore.instances)[number]) => instance.status === 'running').length
)

const enabledPlugins = computed(() =>
  pluginsStore.plugins.filter((plugin: (typeof pluginsStore.plugins)[number]) => plugin.status === 'enabled').length
)

const tokenTotal = computed(() => tokenSummary.value?.totalTokens ?? 0)

const tokenCacheTotal = computed(
  () => (tokenSummary.value?.cacheReadTokens ?? 0) + (tokenSummary.value?.cacheWriteTokens ?? 0)
)

const tokenOutputShare = computed(() => {
  if (!tokenTotal.value) return 0
  return Math.round(((tokenSummary.value?.outputTokens ?? 0) / tokenTotal.value) * 100)
})

const topTokenModel = computed(() => tokenSummary.value?.topModels[0] ?? null)

const formatNumber = (value: number) =>
  new Intl.NumberFormat(locale.value, { maximumFractionDigits: 0 }).format(value)

const formatCompactNumber = (value: number) =>
  new Intl.NumberFormat(locale.value, {
    maximumFractionDigits: 1,
    notation: 'compact',
  }).format(value)

const fetchTokenSummary = async () => {
  const response = await modelRuntimeApi.getTokenSummary(7)
  tokenSummary.value = response.data.data ?? null
}

onMounted(async () => {
  try {
    await Promise.all([
      instancesStore.fetchInstances(),
      pluginsStore.fetchPlugins(),
      fetchTokenSummary(),
    ])
  } catch (err) {
    console.error(t('pages.dashboard.loadFailed'), err)
  }
})

const navigateTo = (path: string) => {
  router.push(path)
}
</script>

<style scoped>
.token-usage-card {
  background:
    radial-gradient(circle at top right, rgba(255, 255, 255, 0.3), transparent 34%),
    linear-gradient(135deg, #183a37 0%, #285d4f 48%, #c88b39 100%);
  color: white;
  overflow: hidden;
}

.token-usage-kicker,
.token-usage-muted {
  color: rgba(255, 255, 255, 0.72);
}

.token-usage-icon {
  color: #183a37;
}

.token-usage-total {
  font-size: clamp(2.4rem, 7vw, 4.6rem);
  font-weight: 800;
  letter-spacing: -0.08em;
  line-height: 0.95;
}

.token-usage-progress :deep(.v-progress-linear__determinate) {
  background: rgba(255, 255, 255, 0.92) !important;
}

.token-usage-progress {
  background: rgba(255, 255, 255, 0.18);
}

.token-usage-metric {
  background: rgba(255, 255, 255, 0.12);
  border: 1px solid rgba(255, 255, 255, 0.18);
  border-radius: 18px;
  padding: 14px 16px;
}

.token-usage-divider {
  border-color: rgba(255, 255, 255, 0.24);
}
</style>
