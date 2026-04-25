<template>
  <v-dialog
    :model-value="modelValue"
    :max-width="maxWidth"
    scrollable
    @update:model-value="handleDialogModelUpdate"
  >
    <v-card class="picker-dialog">
      <v-card-item class="pb-2">
        <v-card-title>{{ title }}</v-card-title>
        <v-card-subtitle v-if="subtitle">{{ subtitle }}</v-card-subtitle>
        <template #append>
          <v-btn
            icon="mdi-close"
            variant="text"
            @click="closeDialog('cancel')"
          />
        </template>
      </v-card-item>

      <v-card-text class="d-flex flex-column ga-5">
        <v-text-field
          v-model="search"
          :label="$t('common.actions.action.search')"
          prepend-inner-icon="mdi-magnify"
          density="comfortable"
          variant="outlined"
          hide-details
        />

        <template v-for="section in filteredSections" :key="section.id">
          <!-- Flat list section -->
          <div v-if="section.items && section.items.length" class="d-flex flex-column ga-3">
            <div class="section-label">{{ section.label }}</div>
            <v-card variant="outlined" class="picker-section-card">
              <v-list class="bg-transparent py-0">
                <v-list-item
                  v-for="item in section.items"
                  :key="item.value"
                  :active="isSelected(item.value)"
                  rounded="lg"
                  class="picker-list-item"
                  @click="handleItemClick(item.value)"
                >
                  <template #prepend>
                    <v-checkbox-btn
                      v-if="multiple"
                      :model-value="isSelected(item.value)"
                      class="me-n1"
                      @click.stop="handleItemClick(item.value)"
                    />
                    <v-avatar
                      v-else
                      size="34"
                      :color="isSelected(item.value) ? 'primary' : (item.iconColor ?? 'primary')"
                      variant="tonal"
                    >
                      <v-icon
                        :icon="isSelected(item.value) ? 'mdi-check' : (item.icon ?? 'mdi-check-circle-outline')"
                        size="18"
                      />
                    </v-avatar>
                  </template>
                  <v-list-item-title class="text-body-2 font-weight-medium">
                    {{ item.title }}
                  </v-list-item-title>
                  <v-list-item-subtitle v-if="item.subtitle" class="text-caption">
                    {{ item.subtitle }}
                  </v-list-item-subtitle>
                  <template v-if="item.tag" #append>
                    <v-chip size="x-small" variant="tonal" :color="item.tagColor ?? 'default'">
                      {{ item.tag }}
                    </v-chip>
                  </template>
                </v-list-item>
              </v-list>
            </v-card>
          </div>

          <!-- Grouped grid section -->
          <div v-else-if="section.groups && section.groups.length" class="d-flex flex-column ga-3">
            <div class="section-label">{{ section.label }}</div>
            <v-row>
              <v-col
                v-for="group in section.groups"
                :key="group.id"
                cols="12"
                md="6"
              >
                <v-card variant="outlined" class="picker-section-card fill-height">
                  <v-card-item class="pb-1">
                    <v-card-title class="text-body-1">{{ group.title }}</v-card-title>
                    <v-card-subtitle v-if="group.subtitle">{{ group.subtitle }}</v-card-subtitle>
                  </v-card-item>
                  <v-card-text class="pt-0">
                    <v-list class="bg-transparent py-0">
                      <v-list-item
                        v-for="item in group.items"
                        :key="`${group.id}:${item.value}`"
                        :active="isSelected(item.value)"
                        rounded="lg"
                        class="picker-list-item"
                        @click="handleItemClick(item.value)"
                      >
                        <template #prepend>
                          <v-checkbox-btn
                            v-if="multiple"
                            :model-value="isSelected(item.value)"
                            class="me-n1"
                            @click.stop="handleItemClick(item.value)"
                          />
                          <v-avatar
                            v-else
                            size="34"
                            :color="isSelected(item.value) ? 'primary' : (item.iconColor ?? 'secondary')"
                            variant="tonal"
                          >
                            <v-icon
                              :icon="isSelected(item.value) ? 'mdi-check' : (item.icon ?? 'mdi-cube-outline')"
                              size="18"
                            />
                          </v-avatar>
                        </template>
                        <v-list-item-title class="text-body-2 font-weight-medium">
                          {{ item.title }}
                        </v-list-item-title>
                        <v-list-item-subtitle
                          v-if="item.subtitle"
                          class="text-caption text-break"
                        >
                          {{ item.subtitle }}
                        </v-list-item-subtitle>
                        <template v-if="item.tag" #append>
                          <v-chip
                            size="x-small"
                            variant="tonal"
                            :color="item.tagColor ?? 'default'"
                          >
                            {{ item.tag }}
                          </v-chip>
                        </template>
                      </v-list-item>
                    </v-list>
                  </v-card-text>
                </v-card>
              </v-col>
            </v-row>
          </div>
        </template>

        <v-sheet
          v-if="!hasVisibleOptions"
          rounded="xl"
          class="empty-state-panel text-body-2 text-medium-emphasis py-8 px-6"
        >
          {{ search ? noResultsText : emptyText }}
        </v-sheet>
      </v-card-text>

      <v-card-actions class="px-6 pb-5 pt-0">
        <v-spacer />
        <v-btn variant="text" @click="closeDialog('cancel')">
          {{ $t('common.actions.action.cancel') }}
        </v-btn>
        <v-btn
          v-if="multiple || !closeOnSelect"
          color="primary"
          variant="tonal"
          @click="confirmSelection"
        >
          {{ $t('common.actions.action.confirm') }}
          <template v-if="multiple"> ({{ internalSelected.length }})</template>
        </v-btn>
      </v-card-actions>
    </v-card>
  </v-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'

export interface GenericPickerItem {
  value: string
  title: string
  subtitle?: string
  icon?: string
  iconColor?: string
  tag?: string
  tagColor?: string
}

export interface GenericPickerGroup {
  id: string
  title: string
  subtitle?: string
  items: GenericPickerItem[]
}

export interface GenericPickerSection {
  id: string
  label: string
  items?: GenericPickerItem[]
  groups?: GenericPickerGroup[]
}

interface Props {
  modelValue: boolean
  title: string
  subtitle?: string
  sections: GenericPickerSection[]
  selected: string[]
  multiple?: boolean
  closeOnSelect?: boolean
  maxWidth?: string | number
  emptyText?: string
  noResultsText?: string
}

const props = withDefaults(defineProps<Props>(), {
  multiple: false,
  closeOnSelect: true,
  maxWidth: 1120,
  emptyText: 'No options available.',
  noResultsText: 'No matching options found.',
})

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'update:selected': [values: string[]]
}>()

const search = ref('')
const internalSelected = ref<string[]>([])

const keyword = computed(() => search.value.trim().toLowerCase())

const isSelected = (value: string) => internalSelected.value.includes(value)

const handleItemClick = (value: string) => {
  if (props.multiple) {
    const index = internalSelected.value.indexOf(value)
    if (index === -1) {
      internalSelected.value = [...internalSelected.value, value]
    } else {
      internalSelected.value = internalSelected.value.filter((v) => v !== value)
    }
  } else {
    internalSelected.value = [value]
    if (props.closeOnSelect) {
      closeDialog('apply')
    }
  }
}

const confirmSelection = () => {
  closeDialog('apply')
}

const applySelection = () => {
  emit('update:selected', [...internalSelected.value])
}

const closeDialog = (reason: 'apply' | 'cancel') => {
  if (reason === 'apply') {
    applySelection()
  }
  emit('update:modelValue', false)
}

const handleDialogModelUpdate = (value: boolean) => {
  if (value) {
    emit('update:modelValue', true)
    return
  }
  closeDialog('cancel')
}

const filterItems = (items: GenericPickerItem[]): GenericPickerItem[] => {
  if (!keyword.value) {
    return items
  }
  return items.filter((item) =>
    `${item.title} ${item.subtitle ?? ''}`.toLowerCase().includes(keyword.value)
  )
}

const filteredSections = computed<GenericPickerSection[]>(() => {
  return props.sections
    .map((section) => {
      if (section.items) {
        return { ...section, items: filterItems(section.items) }
      }
      if (section.groups) {
        const filteredGroups = section.groups
          .map((group) => ({
            ...group,
            items: keyword.value
              ? group.items.filter((item) =>
                  `${group.title} ${group.subtitle ?? ''} ${item.title} ${item.subtitle ?? ''}`
                    .toLowerCase()
                    .includes(keyword.value)
                )
              : group.items,
          }))
          .filter((group) => group.items.length > 0)
        return { ...section, groups: filteredGroups }
      }
      return section
    })
    .filter(
      (section) =>
        (section.items && section.items.length > 0) ||
        (section.groups && section.groups.length > 0)
    )
})

const hasVisibleOptions = computed(() => filteredSections.value.length > 0)

watch(
  () => props.modelValue,
  (isOpen) => {
    if (isOpen) {
      internalSelected.value = [...props.selected]
    } else {
      search.value = ''
    }
  }
)

</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.picker-dialog {
  @include surface-card(rgba(var(--v-theme-primary), 0.14), 28px, 0 16px 40px rgba(var(--v-theme-primary), 0.08));
}

.picker-section-card {
  border-radius: 22px;
  border-color: rgba(var(--v-theme-primary), 0.14);
  background: rgba(var(--v-theme-surface), 0.82);
}

.picker-list-item {
  margin-bottom: 8px;
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
  background: rgba(var(--v-theme-surface), 0.7);
  transition: border-color 0.18s ease, background-color 0.18s ease;
}

.picker-list-item:hover {
  border-color: rgba(var(--v-theme-primary), 0.18);
  background: rgba(var(--v-theme-surface), 0.96);
}

.picker-list-item.v-list-item--active {
  border-color: rgba(var(--v-theme-primary), 0.42);
  background: rgba(var(--v-theme-primary), 0.12);
}

.section-label {
  font-size: 0.86rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(var(--v-theme-primary), 0.68);
}

.empty-state-panel {
  border: 1px dashed rgba(var(--v-theme-primary), 0.16);
  background: linear-gradient(180deg, rgba(var(--v-theme-surface), 0.95) 0%, rgba(var(--v-theme-surface), 0.78) 100%);
  border-radius: 20px;
}

.picker-list-item.v-list-item--active:hover {
  border-color: rgba(var(--v-theme-primary), 0.56);
  background: rgba(var(--v-theme-primary), 0.16);
}
</style>
