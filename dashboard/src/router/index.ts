import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/Login.vue'),
  },
  {
    path: '/',
    component: () => import('@/layouts/MainLayout.vue'),
    meta: { requiresAuth: true },
    children: [
      {
        path: '',
        redirect: '/dashboard',
      },
      {
        path: 'dashboard',
        name: 'Dashboard',
        component: () => import('@/views/Dashboard.vue'),
      },
      {
        path: 'message-platforms',
        name: 'MessagePlatforms',
        component: () => import('@/views/MessagePlatforms.vue'),
      },
      {
        path: 'instances',
        redirect: '/bot-instances',
      },
      {
        path: 'bot-instances',
        name: 'Instances',
        component: () => import('@/views/Instances.vue'),
      },
      {
        path: 'plugins',
        name: 'Plugins',
        component: () => import('@/views/Plugins.vue'),
      },
      {
        path: 'tools',
        name: 'Tools',
        component: () => import('@/views/Tools.vue'),
      },
      {
        path: 'commands',
        name: 'Commands',
        component: () => import('@/views/Commands.vue'),
      },
      {
        path: 'permissions',
        name: 'Permissions',
        component: () => import('@/views/Permissions.vue'),
      },
      {
        path: 'sessions',
        name: 'Sessions',
        component: () => import('@/views/Sessions.vue'),
      },
      {
        path: 'agents',
        name: 'Agents',
        component: () => import('@/views/Agents.vue'),
      },
      {
        path: 'prompts',
        name: 'Prompts',
        component: () => import('@/views/Prompts.vue'),
      },
      {
        path: 'personas',
        name: 'Personas',
        component: () => import('@/views/Personas.vue'),
      },
      {
        path: 'model-runtime',
        name: 'ModelRuntime',
        component: () => import('@/views/ModelRuntime.vue'),
      },
      {
        path: 'model-audit',
        name: 'ModelAudit',
        component: () => import('@/views/ModelAudit.vue'),
      },
      {
        path: 'cost-analysis',
        name: 'CostAnalysis',
        component: () => import('@/views/CostAnalysis.vue'),
      },
      {
        path: 'monitoring',
        name: 'Monitoring',
        component: () => import('@/views/Monitoring.vue'),
      },
      {
        path: 'settings',
        name: 'Settings',
        component: () => import('@/views/Settings.vue'),
      },
    ],
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
  scrollBehavior() {
    return { top: 0, left: 0 }
  },
})

// Route guard: resolve server-backed auth state before entering protected pages.
router.beforeEach(async (to) => {
  const authStore = useAuthStore()
  const requiresAuth = Boolean(to.meta.requiresAuth)

  if (requiresAuth) {
    const hasSession = await authStore.ensureSession(true)
    if (!hasSession) {
      return '/login'
    }

    return true
  }

  if (to.path === '/login') {
    const hasSession = await authStore.ensureSession()
    if (hasSession) {
      return '/dashboard'
    }
  }

  return true
})

export default router
