import { createI18n } from 'vue-i18n'
import type { I18n } from 'vue-i18n'

type LocaleMessages = Record<string, unknown>

interface LocaleModule {
  default: LocaleMessages
}

// 自动加载所有 locale 文件
const localesModules = import.meta.glob<LocaleModule>(
  '../locales/{zh-CN,en-US}/**/*.json',
  { eager: true }
)

const messages: Record<string, LocaleMessages> = {}

function ensureObject(target: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = target[key]
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    target[key] = {}
  }
  return target[key] as Record<string, unknown>
}

function setNestedMessage(
  root: Record<string, unknown>,
  pathSegments: string[],
  value: LocaleMessages
) {
  let current = root
  for (let i = 0; i < pathSegments.length - 1; i += 1) {
    current = ensureObject(current, pathSegments[i])
  }
  current[pathSegments[pathSegments.length - 1]] = value
}

for (const [path, module] of Object.entries(localesModules)) {
  const match = path.match(/^\.\.\/locales\/(zh-CN|en-US)\/(.+)\.json$/)
  if (match) {
    const [, lang, relativePath] = match
    if (!messages[lang]) {
      messages[lang] = {}
    }

    const namespacePath = relativePath.split('/')
    setNestedMessage(messages[lang] as Record<string, unknown>, namespacePath, module.default)
  }
}

const i18nOptions = {
  legacy: false,
  locale: 'zh-CN',
  fallbackLocale: 'en-US',
  messages,
  globalInjection: true,
  missingWarn: false,
  fallbackWarn: false,
} as any

const i18n: I18n = createI18n(i18nOptions)

export default i18n

export function translate(key: string, params?: Record<string, unknown>): string {
  const t = i18n.global.t as unknown as (k: string, p?: Record<string, unknown>) => unknown
  return String(t(key, params))
}

export function currentLocale(): string {
  const locale = i18n.global.locale as unknown
  if (typeof locale === 'string') {
    return locale
  }
  if (locale && typeof locale === 'object' && 'value' in locale) {
    return String((locale as { value: unknown }).value || 'zh-CN')
  }
  return 'zh-CN'
}
