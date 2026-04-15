<template>
  <!-- 1. 顶栏：全宽布局 -->
  <v-app-bar color="#FFFDE7" elevation="0" height="64" class="px-0">
    <div class="toggle-box">
      <v-app-bar-nav-icon @click="uiStore.toggleRail" icon="mdi-menu" />
    </div>
    <v-app-bar-title class="font-weight-black text-body-1">
      {{ t('layout.main.appName') }}
    </v-app-bar-title>
    
    <v-chip class="ms-2" :color="statusChipColor" size="small" variant="flat">
      {{ t(statusChipText) }}
    </v-chip>

    <v-breadcrumbs :items="breadcrumbs" class="ms-4 hidden-sm-and-down">
      <template #divider>
        <v-icon icon="mdi-chevron-right" />
      </template>
    </v-breadcrumbs>

    <v-spacer />

    <v-menu location="bottom end">
      <template #activator="{ props }">
        <v-btn icon="mdi-account-circle-outline" v-bind="props" class="me-4" />
      </template>
      <v-list class="rounded-lg mt-2" elevation="4">
        <v-list-item prepend-icon="mdi-account" :title="authStore.username" />
        <v-divider />
        <v-list-item prepend-icon="mdi-logout" color="error" @click="handleLogout" :title="t('layout.main.nav.logout')" />
      </v-list>
    </v-menu>
  </v-app-bar>

  <!-- 2. 侧边栏：位于顶栏下 -->
  <v-navigation-drawer
    v-model="drawer"
    permanent
    :rail="uiStore.isRail"
    rail-width="72"
    width="260"
    color="#FFFDE7"
    elevation="0"
    border="0"
  >
    <v-list nav class="px-3">
      <v-list-item
        v-for="item in navItems"
        :key="item.to"
        :to="item.to"
        :prepend-icon="item.icon"
        :title="t(item.title)"
        class="nav-item mb-1"
        active-class="nav-item-active"
      />
    </v-list>

    <v-divider class="mx-4 my-2" opacity="0.05" />

    <v-list nav class="px-3">
      <v-list-subheader v-if="!uiStore.isRail" class="text-caption">{{ t('layout.main.nav.instances') }}</v-list-subheader>
      <v-list-item
        v-for="instance in instancesStore.instances"
        :key="instance.id"
        :title="instance.name"
        class="nav-item"
      >
        <template #prepend>
          <v-badge dot :color="instance.status === 'running' ? 'success' : 'error'" offset-x="2" offset-y="2">
            <v-icon icon="mdi-circle-small" />
          </v-badge>
        </template>
      </v-list-item>
    </v-list>

    <template #append>
      <v-list nav class="px-3">
        <v-list-item prepend-icon="mdi-cog-outline" :title="t('layout.main.nav.settings')" to="/settings" class="nav-item" />
      </v-list>
    </template>
  </v-navigation-drawer>

  <!-- 3. 主内容区：实现“悬浮岛屿” -->
  <v-main class="bg-base">
    <div class="content-island">
      <router-view />
    </div>
  </v-main>
</template>

<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, computed } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '@/stores/auth'
import { useMonitoringStore } from '@/stores/monitoring'
import { useInstancesStore } from '@/stores/instances'
import { useUiStore } from '@/stores/ui'

const router = useRouter()
const route = useRoute()
const authStore = useAuthStore()
const monitoringStore = useMonitoringStore()
const instancesStore = useInstancesStore()
const uiStore = useUiStore()
const { t } = useI18n()

const drawer = ref(true)

const navItems = [
  { to: '/dashboard', icon: 'mdi-view-dashboard-outline', title: 'layout.main.nav.dashboard' },
  { to: '/instances', icon: 'mdi-robot-outline', title: 'layout.main.nav.instancesManage' },
  { to: '/plugins', icon: 'mdi-puzzle-outline', title: 'layout.main.nav.pluginsManage' },
  { to: '/tools', icon: 'mdi-tools', title: 'layout.main.nav.toolsManage' },
  { to: '/model-runtime', icon: 'mdi-router-network', title: 'layout.main.nav.modelRuntime' },
  { to: '/monitoring', icon: 'mdi-monitor-dashboard', title: 'layout.main.nav.monitoring' },
]

const routeTitleMap: Record<string, string> = {
  Dashboard: 'layout.main.nav.dashboard',
  Instances: 'layout.main.nav.instancesManage',
  Plugins: 'layout.main.nav.pluginsManage',
  Tools: 'layout.main.nav.toolsManage',
  ModelRuntime: 'layout.main.nav.modelRuntime',
  Monitoring: 'layout.main.nav.monitoring',
  Settings: 'layout.main.nav.settings',
}

const breadcrumbs = computed(() => {
  const items: Array<{ title: string; href?: string; disabled?: boolean }> = []
  const routeName = route.name as string | undefined

  if (routeName && routeName in routeTitleMap) {
    items.push({ title: t(routeTitleMap[routeName]), disabled: true })
  }

  return items
})

const statusChipText = computed(() =>
  monitoringStore.isOnline ? 'common.actions.status.online' : 'common.actions.status.offline'
)

const statusChipColor = computed(() =>
  monitoringStore.isOnline ? 'success' : 'error'
)

onMounted(() => {
  monitoringStore.connectLogs()
  monitoringStore.connectStatus()
  instancesStore.fetchInstances()
  setTimeout(() => { if (uiStore.isLoading) uiStore.resetLoading() }, 5000)
})

onBeforeUnmount(() => {
  monitoringStore.disconnectLogs()
  monitoringStore.disconnectStatus()
})

const handleLogout = () => {
  authStore.logout()
  router.push('/login')
}
</script>

<style scoped>
.toggle-box {
  width: 72px;
  display: flex;
  justify-content: center;
}

.bg-base {
  background-color: #FFFDE7 !important;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.content-island {
  flex: 1;
  background: #FFFFF8;
  margin-right: 10px;
  margin-bottom: 10px;
  border-radius: 16px;
  border: 1px solid rgba(0, 0, 0, 0.08);
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.02);
  overflow-y: auto;
  overflow-x: hidden;
  padding: 24px !important;
}

.nav-item {
  border-radius: 12px !important;
  min-height: 44px;
}

:deep(.v-navigation-drawer--rail) .nav-item {
  width: 48px !important;
  height: 48px !important;
  margin: 0 auto 8px auto !important;
  display: flex;
  justify-content: center;
  align-items: center;
}

:deep(.v-navigation-drawer--rail) .v-list-item__prepend {
  margin: 0 !important;
  width: 100%;
  display: flex;
  justify-content: center;
}

:deep(.v-navigation-drawer--rail) .v-list-item__content,
:deep(.v-navigation-drawer--rail) .v-list-item__spacer,
:deep(.v-navigation-drawer--rail) .v-list-item-title {
  display: none !important;
}
/* 激活状态：使用更有质感的深黄色 */
.nav-item-active {
  background-color: #FFE082 !important; /* 暖黄色 */
  color: #3f3112 !important; /* 深褐色文字，更具质感 */
  font-weight: 800;
  box-shadow: 0 4px 12px rgba(199, 144, 0, 0.15) !important;
}

</style>
