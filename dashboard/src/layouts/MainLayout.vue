<template>
  <!-- 1. 顶栏：全宽布局 -->
  <v-app-bar
    color="surface"
    elevation="0"
    height="64"
    class="px-0 main-app-bar"
  >
    <div class="toggle-box">
      <v-app-bar-nav-icon @click="uiStore.toggleRail" icon="mdi-menu" />
    </div>
    <v-app-bar-title class="font-weight-black text-body-1">
      {{ t("layout.main.appName") }}
    </v-app-bar-title>

    <v-chip class="ms-2" :color="statusChipColor" size="small" variant="tonal">
      <template #prepend>
        <v-icon icon="mdi-circle" size="10" class="me-1" />
      </template>
      {{ t(statusChipText) }}
    </v-chip>

    <v-breadcrumbs :items="breadcrumbs" class="ms-4 hidden-sm-and-down">
      <template #divider>
        <v-icon
          icon="mdi-chevron-right"
          size="18"
          class="text-medium-emphasis"
        />
      </template>
    </v-breadcrumbs>

    <v-spacer />

    <v-btn
      :icon="
        uiStore.isDarkMode ? 'mdi-weather-night' : 'mdi-white-balance-sunny'
      "
      variant="text"
      class="topbar-icon-btn me-2"
      @click="toggleDarkMode"
    />

    <v-menu location="bottom end">
      <template #activator="{ props }">
        <v-btn
          icon="mdi-account-circle-outline"
          v-bind="props"
          variant="text"
          class="topbar-icon-btn me-4"
        />
      </template>
      <v-list class="rounded-lg mt-2" elevation="4">
        <v-list-item
          prepend-icon="mdi-account"
          :title="authStore.displayName"
        />
        <v-divider />
        <v-list-item
          prepend-icon="mdi-logout"
          color="error"
          @click="handleLogout"
          :title="t('layout.main.nav.logout')"
        />
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
    color="surface"
    elevation="0"
    border="0"
    class="main-navigation-drawer"
  >
    <v-list nav class="px-3 mt-2">
      <v-list-item
        v-for="item in primaryNavItems"
        :key="item.to"
        :to="item.to"
        :prepend-icon="item.icon"
        :title="t(item.title)"
        class="nav-item mb-1"
        active-class="nav-item-active"
      />
    </v-list>

    <v-list nav class="px-3 pt-0">
      <v-list-subheader
        v-if="!uiStore.isRail"
        class="text-caption font-weight-bold text-uppercase letter-spacing-1"
        >{{ t("layout.main.nav.agentCore") }}</v-list-subheader
      >
      <v-list-item
        v-for="item in agentCoreNavItems"
        :key="item.to"
        :to="item.to"
        :prepend-icon="item.icon"
        :title="t(item.title)"
        class="nav-item mb-1"
        active-class="nav-item-active"
      />
    </v-list>

    <template #append>
      <v-list nav class="px-3 mb-2">
        <v-list-item
          prepend-icon="mdi-cog-outline"
          :title="t('layout.main.nav.settings')"
          to="/settings"
          class="nav-item"
          active-class="nav-item-active"
        />
      </v-list>
    </template>
  </v-navigation-drawer>

  <!-- 3. 主内容区：实现“悬浮岛屿” -->
  <v-main class="bg-base-main">
    <div class="content-island">
      <router-view />
    </div>
  </v-main>

  <v-dialog
    :model-value="authStore.mustChangeCredentials"
    persistent
    max-width="560"
  >
    <v-card class="pa-6 rounded-xl">
      <v-card-title class="px-0 pt-0 text-h5 font-weight-bold">
        {{ $t("pages.settings.credentials.title") }}
      </v-card-title>
      <v-card-subtitle class="px-0 pb-6">
        {{ $t("pages.settings.credentials.subtitle") }}
      </v-card-subtitle>

      <credentials-update-form force-change />
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { onMounted, onBeforeUnmount, ref, computed } from "vue";
import { useRouter, useRoute } from "vue-router";
import { useI18n } from "vue-i18n";
import { useTheme } from "vuetify";
import CredentialsUpdateForm from "@/components/CredentialsUpdateForm.vue";
import { useAuthStore } from "@/stores/auth";
import { useMonitoringStore } from "@/stores/monitoring";
import { useUiStore } from "@/stores/ui";
import { resolveThemeName } from "@/theme/themes";

const router = useRouter();
const route = useRoute();
const authStore = useAuthStore();
const monitoringStore = useMonitoringStore();
const uiStore = useUiStore();
const theme = useTheme();
const { t } = useI18n();

const drawer = ref(true);
let themeTransitionTimer: number | undefined;

type ViewTransitionDocument = Document & {
  startViewTransition?: (callback: () => void) => { finished: Promise<void> };
};

type NavItem = {
  to: string;
  icon: string;
  title: string;
};

const primaryNavItems: NavItem[] = [
  {
    to: "/dashboard",
    icon: "mdi-view-dashboard-outline",
    title: "layout.main.nav.dashboard",
  },
  {
    to: "/message-platforms",
    icon: "mdi-message-processing-outline",
    title: "layout.main.nav.messagePlatforms",
  },
  {
    to: "/instances",
    icon: "mdi-robot-outline",
    title: "layout.main.nav.instancesManage",
  },
  {
    to: "/plugins",
    icon: "mdi-puzzle-outline",
    title: "layout.main.nav.pluginsManage",
  },
  {
    to: "/cost-analysis",
    icon: "mdi-chart-areaspline",
    title: "layout.main.nav.costAnalysis",
  },
  {
    to: "/monitoring",
    icon: "mdi-monitor-dashboard",
    title: "layout.main.nav.monitoring",
  },
];

const agentCoreNavItems: NavItem[] = [
  {
    to: "/model-runtime",
    icon: "mdi-router-network",
    title: "layout.main.nav.modelRuntime",
  },
  {
    to: "/agents",
    icon: "mdi-account-group-outline",
    title: "layout.main.nav.agentsManage",
  },
  {
    to: "/personas",
    icon: "mdi-account-badge-outline",
    title: "layout.main.nav.personasManage",
  },
  {
    to: "/prompts",
    icon: "mdi-text-box-multiple-outline",
    title: "layout.main.nav.promptsManage",
  },
  { to: "/tools", icon: "mdi-tools", title: "layout.main.nav.toolsManage" },
  {
    to: "/sessions",
    icon: "mdi-forum-outline",
    title: "layout.main.nav.sessionsManage",
  },
];

const routeTitleMap: Record<string, string> = {
  Dashboard: "layout.main.nav.dashboard",
  MessagePlatforms: "layout.main.nav.messagePlatforms",
  Instances: "layout.main.nav.instancesManage",
  Plugins: "layout.main.nav.pluginsManage",
  Tools: "layout.main.nav.toolsManage",
  Sessions: "layout.main.nav.sessionsManage",
  Agents: "layout.main.nav.agentsManage",
  Prompts: "layout.main.nav.promptsManage",
  Personas: "layout.main.nav.personasManage",
  ModelRuntime: "layout.main.nav.modelRuntime",
  CostAnalysis: "layout.main.nav.costAnalysis",
  Monitoring: "layout.main.nav.monitoring",
  Settings: "layout.main.nav.settings",
};

const breadcrumbs = computed(() => {
  const items: Array<{ title: string; href?: string; disabled?: boolean }> = [];
  const routeName = route.name as string | undefined;

  if (routeName && routeName in routeTitleMap) {
    items.push({ title: t(routeTitleMap[routeName]), disabled: true });
  }

  return items;
});

const statusChipText = computed(() =>
  monitoringStore.isOnline
    ? "common.actions.status.online"
    : "common.actions.status.offline",
);

const statusChipColor = computed(() =>
  monitoringStore.isOnline ? "success" : "error",
);

const prefersReducedMotion = () =>
  window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const startThemeFallbackTransition = () => {
  window.clearTimeout(themeTransitionTimer);
  document.documentElement.classList.add("shinbot-theme-transitioning");
  document.documentElement.getBoundingClientRect();
  themeTransitionTimer = window.setTimeout(() => {
    document.documentElement.classList.remove("shinbot-theme-transitioning");
    themeTransitionTimer = undefined;
  }, 320);
};

const applyTheme = (isDarkMode: boolean, options: { animated?: boolean } = {}) => {
  if (options.animated && !prefersReducedMotion()) {
    const transitionDocument = document as ViewTransitionDocument;
    if (transitionDocument.startViewTransition) {
      document.documentElement.classList.add("shinbot-theme-view-transition");
      const transition = transitionDocument.startViewTransition(() => {
        theme.global.name.value = resolveThemeName(isDarkMode);
      });
      void transition.finished.finally(() => {
        document.documentElement.classList.remove("shinbot-theme-view-transition");
      });
      return;
    }

    startThemeFallbackTransition();
  }
  theme.global.name.value = resolveThemeName(isDarkMode);
};

const toggleDarkMode = () => {
  const nextValue = !uiStore.isDarkMode;
  uiStore.setDarkMode(nextValue);
  applyTheme(nextValue, { animated: true });
};

onMounted(() => {
  applyTheme(uiStore.isDarkMode);
  monitoringStore.connectLogs();
  monitoringStore.connectStatus();
  setTimeout(() => {
    if (uiStore.isLoading) uiStore.resetLoading();
  }, 5000);
});

onBeforeUnmount(() => {
  monitoringStore.disconnectLogs();
  monitoringStore.disconnectStatus();
});

const handleLogout = async () => {
  await authStore.logout();
  await router.push("/login");
};
</script>

<style scoped lang="scss">
@use "@/styles/variables" as *;

.toggle-box {
  width: 72px;
  display: flex;
  justify-content: center;
}

.main-app-bar {
  background: rgb(var(--v-theme-surface)) !important;
  box-shadow: none !important;
}

.main-navigation-drawer {
  background: rgb(var(--v-theme-surface)) !important;
  box-shadow: none !important;
}

.topbar-icon-btn {
  box-shadow: none !important;
}

.bg-base-main {
  background-color: rgb(var(--v-theme-surface));
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.content-island {
  flex: 1;
  background-color: color-mix(
    in srgb,
    rgb(var(--v-theme-surface)) 86%,
    rgb(var(--v-theme-primary)) 4%
  );
  margin: 0 16px 16px 0;
  border-radius: 28px;
  border: 0;
  box-shadow: inset 0 0 0 1px rgba(var(--v-theme-primary), 0.1);
  overflow-y: auto;
  overflow-x: hidden;
  padding: 24px;
}

.nav-item {
  border-radius: 14px;
  min-height: 44px;
  transition: all $transition-base;
}

.letter-spacing-1 {
  letter-spacing: 0.08em;
}

:deep(.v-navigation-drawer--rail) .nav-item {
  width: 44px;
  height: 44px;
  margin: 0 auto 12px;
  display: flex;
  justify-content: center;
  align-items: center;
}

:deep(.v-navigation-drawer--rail) .v-list-item__prepend {
  margin: 0;
  width: 100%;
  display: flex;
  justify-content: center;
}

.nav-item-active {
  background: linear-gradient(
    135deg,
    rgba(var(--v-theme-primary), 0.16) 0%,
    rgba(var(--v-theme-primary), 0.08) 100%
  );
  color: rgb(var(--v-theme-primary)) !important;
  font-weight: 800;
  border: 1px solid rgba(var(--v-theme-primary), 0.12);
}

.nav-item-active :deep(.v-icon) {
  color: rgb(var(--v-theme-primary));
}

@media (max-width: 960px) {
  .content-island {
    margin: 0;
    border-radius: 0;
    border: 0;
  }
}
</style>
