import { createVuetify } from 'vuetify'
import { aliases, mdi } from 'vuetify/iconsets/mdi'
import 'vuetify/lib/styles/main.css'
import '@mdi/font/css/materialdesignicons.css'
import { SHINBOT_THEME_NAMES, shinbotThemes } from '@/theme/themes'

const vuetify = createVuetify({
  icons: {
    defaultSet: 'mdi',
    aliases,
    sets: {
      mdi,
    },
  },
  theme: {
    defaultTheme: SHINBOT_THEME_NAMES.light,
    themes: shinbotThemes,
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
