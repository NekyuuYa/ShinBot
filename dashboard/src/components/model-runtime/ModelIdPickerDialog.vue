<template>
  <generic-picker-dialog
    :model-value="modelValue"
    :title="$t('pages.modelRuntime.dialogs.modelIdPicker')"
    :subtitle="$t('pages.modelRuntime.hints.modelIdPicker')"
    :sections="sections"
    :selected="currentValue ? [currentValue] : []"
    :empty-text="$t('pages.modelRuntime.hints.modelIdPickerEmpty')"
    :no-results-text="$t('pages.modelRuntime.hints.modelIdPickerNoMatches')"
    @update:model-value="$emit('update:modelValue', $event)"
    @update:selected="(vals) => $emit('select', vals[0] ?? '')"
  />
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import GenericPickerDialog, { type GenericPickerSection } from './GenericPickerDialog.vue'

const { t } = useI18n()

interface RouteOption {
  id: string
  title: string
  subtitle: string
  enabled: boolean
}

interface ProviderGroupItem {
  value: string
  title: string
  subtitle: string
  kind: 'catalog' | 'configured'
}

interface ProviderGroup {
  providerId: string
  providerName: string
  providerType: string
  items: ProviderGroupItem[]
}

interface Props {
  modelValue: boolean
  currentValue: string
  routeOptions: RouteOption[]
  providerGroups: ProviderGroup[]
}

const props = defineProps<Props>()

defineEmits<{
  'update:modelValue': [value: boolean]
  select: [value: string]
}>()

const sections = computed<GenericPickerSection[]>(() => {
  const result: GenericPickerSection[] = []

  if (props.routeOptions.length > 0) {
    result.push({
      id: 'routes',
      label: t('pages.modelRuntime.labels.routeTargets'),
      items: props.routeOptions.map((r) => ({
        value: r.id,
        title: r.title,
        subtitle: r.subtitle,
        icon: 'mdi-transit-connection-variant',
        iconColor: r.enabled ? 'primary' : 'surface-variant',
        tag: r.enabled
          ? t('pages.modelRuntime.labels.enabled')
          : t('pages.modelRuntime.labels.disabled'),
        tagColor: r.enabled ? 'primary' : 'default',
      })),
    })
  }

  if (props.providerGroups.length > 0) {
    result.push({
      id: 'providers',
      label: t('pages.modelRuntime.sidebar.providers'),
      groups: props.providerGroups.map((g) => ({
        id: g.providerId,
        title: g.providerName,
        subtitle: g.providerType,
        items: g.items.map((item) => ({
          value: item.value,
          title: item.title,
          subtitle: item.subtitle,
          icon: 'mdi-cube-outline',
          iconColor: 'secondary',
          tag:
            item.kind === 'catalog'
              ? t('pages.modelRuntime.labels.catalog')
              : t('pages.modelRuntime.labels.configured'),
          tagColor: item.kind === 'catalog' ? 'info' : 'primary',
        })),
      })),
    })
  }

  return result
})
</script>
