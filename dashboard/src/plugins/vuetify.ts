import { createVuetify } from 'vuetify'
import { aliases, mdi } from 'vuetify/iconsets/mdi'
import 'vuetify/styles'
import '@mdi/font/css/materialdesignicons.css'

const vuetify = createVuetify({
  icons: {
    defaultSet: 'mdi',
    aliases,
    sets: {
      mdi,
    },
  },
  theme: {
    defaultTheme: 'shinbotTheme',
    themes: {
      shinbotTheme: {
        colors: {
          primary: '#C79000',
          secondary: '#5F5A4F',
          accent: '#FFE082',
          surface: '#FFFDE7',
          background: '#FFFDF5',
          appBar: '#FFF9C4',
          error: '#f44336',
          warning: '#fb8c00',
          info: '#2196f3',
          success: '#4caf50',
        },
      },
    },
  },
  defaults: {
    VCard: {
      rounded: 'lg',
      elevation: 5,
    },
    VBtn: {
      rounded: 'lg',
      variant: 'tonal',
      elevation: 1,
    },
    VChip: {
      rounded: 'pill',
    },
    VTextField: {
      rounded: 'lg',
      variant: 'solo-filled',
    },
    VDialog: {
      rounded: 'lg',
    },
    VListItem: {
      rounded: 'lg',
    },
  },
})

export default vuetify
