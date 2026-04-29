import { computed, type Ref } from 'vue'
import { useI18n } from 'vue-i18n'

import type { ModelRuntimeTab } from '@/utils/modelRuntimeSources'
import type { RuntimeDomainOption, RuntimeTabOption } from './types'

export const runtimeTabValues: ModelRuntimeTab[] = [
  'routes',
  'chat',
  'embedding',
  'rerank',
  'tts',
  'stt',
  'image',
  'video',
]

export function isRuntimeTab(value: unknown): value is ModelRuntimeTab {
  return typeof value === 'string' && runtimeTabValues.includes(value as ModelRuntimeTab)
}

export function useRuntimeTabs(activeTab: Ref<ModelRuntimeTab>) {
  const { t } = useI18n()

  const runtimeTabs = computed<RuntimeTabOption[]>(() => [
    {
      value: 'routes',
      label: t('pages.modelRuntime.tabs.routes'),
      icon: 'mdi-transit-connection-variant',
    },
    {
      value: 'chat',
      label: t('pages.modelRuntime.tabs.chat'),
      icon: 'mdi-message-text-outline',
    },
    {
      value: 'embedding',
      label: t('pages.modelRuntime.tabs.embedding'),
      icon: 'mdi-vector-line',
    },
    {
      value: 'rerank',
      label: t('pages.modelRuntime.tabs.rerank'),
      icon: 'mdi-sort-descending',
    },
    {
      value: 'tts',
      label: t('pages.modelRuntime.tabs.tts'),
      icon: 'mdi-text-to-speech',
    },
    {
      value: 'stt',
      label: t('pages.modelRuntime.tabs.stt'),
      icon: 'mdi-microphone-outline',
    },
    {
      value: 'image',
      label: t('pages.modelRuntime.tabs.image'),
      icon: 'mdi-image-outline',
    },
    {
      value: 'video',
      label: t('pages.modelRuntime.tabs.video'),
      icon: 'mdi-video-outline',
    },
  ])

  const routeStrategies = ['priority', 'weighted']

  const routeDomainOptions = computed<RuntimeDomainOption[]>(() => [
    { label: t('pages.modelRuntime.tabs.chat'), value: 'chat' },
    { label: t('pages.modelRuntime.tabs.embedding'), value: 'embedding' },
    { label: t('pages.modelRuntime.tabs.rerank'), value: 'rerank' },
    { label: t('pages.modelRuntime.tabs.tts'), value: 'tts' },
    { label: t('pages.modelRuntime.tabs.stt'), value: 'stt' },
    { label: t('pages.modelRuntime.tabs.image'), value: 'image' },
    { label: t('pages.modelRuntime.tabs.video'), value: 'video' },
  ])

  const routeDomainLabels = computed(() =>
    routeDomainOptions.value.reduce<Record<string, string>>((acc, item) => {
      acc[item.value] = item.label
      return acc
    }, {})
  )

  const isRouteMode = computed(() => activeTab.value === 'routes')

  return {
    runtimeTabs,
    routeStrategies,
    routeDomainOptions,
    routeDomainLabels,
    isRouteMode,
  }
}
