import { createApp } from 'vue'
import { createPinia } from 'pinia'
import piniaPluginPersistedstate from 'pinia-plugin-persistedstate'
import App from './App.vue'
import router from './router/index'
import i18n from '@/plugins/i18n'
import vuetify from '@/plugins/vuetify'
import { apiClient } from '@/api/client'
import { useUiStore } from '@/stores/ui'
import { resolveThemeName } from '@/theme/themes'

const app = createApp(App)

// 初始化 Pinia
const pinia = createPinia()
pinia.use(piniaPluginPersistedstate)

// 设置 API 客户端路由
apiClient.setRouter(router)

// 使用插件
app.use(pinia)
app.use(router)
app.use(i18n)
app.use(vuetify)

const uiStore = useUiStore(pinia)
vuetify.theme.global.name.value = resolveThemeName(uiStore.isDarkMode)

apiClient.setRequestTracker({
	start: uiStore.startLoading,
	stop: uiStore.stopLoading,
})
apiClient.setErrorNotifier((message) => {
	uiStore.showSnackbar(message, 'error')
})

// 挂载应用
app.mount('#app')
