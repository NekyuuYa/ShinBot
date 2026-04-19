export const SHINBOT_THEME_NAMES = {
  light: 'shinbotLightTheme',
  dark: 'shinbotDarkTheme',
} as const

export const resolveThemeName = (isDarkMode: boolean) =>
  isDarkMode ? SHINBOT_THEME_NAMES.dark : SHINBOT_THEME_NAMES.light

export const shinbotThemes = {
  [SHINBOT_THEME_NAMES.light]: {
    dark: false,
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
  [SHINBOT_THEME_NAMES.dark]: {
    dark: true,
    colors: {
      primary: '#59e3ff',
      secondary: '#5CCFE6',
      accent: '#BAE67E',
      surface: '#11131e',
      background: '#171B24',
      appBar: '#242936',
      error: '#F07178',
      warning: '#FFB454',
      info: '#59C2FF',
      success: '#AAD94C',
    },
  },
}
