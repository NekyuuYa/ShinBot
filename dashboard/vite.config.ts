import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vuetify from 'vite-plugin-vuetify'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [vue(), vuetify({ autoImport: true })],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:3945',
        changeOrigin: true,
        rewrite: (path) => path,
      },
      '/ws': {
        target: 'ws://localhost:3945',
        ws: true,
        changeOrigin: true,
        rewrite: (path) => path,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: false,
    minify: 'terser',
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules')) {
            if (id.includes('vuetify')) {
              // Keep Vuetify in a single stable chunk to avoid TDZ/runtime init-order issues
              // caused by overly granular component-level splitting.
              return 'vuetify'
            }
            if (id.includes('vue-i18n')) {
              return 'vue-i18n'
            }
            if (id.includes('pinia')) {
              return 'pinia'
            }
            if (id.includes('axios')) {
              return 'axios'
            }
            return 'vendor'
          }
        },
      },
    },
  },
})
