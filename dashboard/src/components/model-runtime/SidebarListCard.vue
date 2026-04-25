<template>
  <v-card class="sidebar-card">
    <v-card-item class="pb-2">
      <v-card-title class="text-subtitle-1 font-weight-bold">{{ title }}</v-card-title>
      <template v-if="showAddButton" #append>
        <v-btn
          :prepend-icon="addIcon"
          size="small"
          variant="tonal"
          color="primary"
          rounded="xl"
          class="sidebar-add-btn"
          @click="$emit('add')"
        >
          {{ addLabel }}
        </v-btn>
      </template>
    </v-card-item>

    <v-card-text class="pt-0">
      <v-text-field
        v-model="search"
        :label="$t('common.actions.action.search')"
        prepend-inner-icon="mdi-magnify"
        density="comfortable"
        variant="outlined"
        hide-details
        rounded="lg"
        bg-color="surface"
        class="mb-3"
      />

      <div v-if="filteredItems.length === 0" class="text-caption text-medium-emphasis py-4">
        {{ emptyText }}
      </div>

      <v-list v-else class="px-0 py-0 bg-transparent">
        <v-list-item
          v-for="item in filteredItems"
          :key="item.id"
          :active="item.id === activeId"
          rounded="lg"
          class="mb-2 sidebar-item"
          @click="$emit('select', item.id)"
        >
          <template #prepend>
            <v-icon :icon="item.icon" />
          </template>
          <v-list-item-title class="text-body-2 font-weight-medium">
            {{ item.title }}
          </v-list-item-title>
          <v-list-item-subtitle v-if="item.subtitle" class="text-caption">
            {{ item.subtitle }}
          </v-list-item-subtitle>
          <template #append>
            <v-chip
              v-if="item.badge !== undefined && item.badge !== null"
              size="x-small"
              variant="tonal"
              :color="item.badgeColor || 'primary'"
            >
              {{ item.badge }}
            </v-chip>
          </template>
        </v-list-item>
      </v-list>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

interface SidebarItem {
  id: string
  title: string
  subtitle?: string
  icon: string
  badge?: string | number
  badgeColor?: string
}

interface Props {
  title: string
  emptyText: string
  items: SidebarItem[]
  activeId: string
  showAddButton?: boolean
  addIcon?: string
  addLabel?: string
}

const props = withDefaults(defineProps<Props>(), {
  showAddButton: true,
  addIcon: 'mdi-plus',
  addLabel: 'Add',
})

defineEmits<{
  add: []
  select: [id: string]
}>()

const search = ref('')

const filteredItems = computed(() => {
  const keyword = search.value.trim().toLowerCase()
  if (!keyword) {
    return props.items
  }
  return props.items.filter((item) =>
    `${item.title} ${item.subtitle || ''}`.toLowerCase().includes(keyword)
  )
})
</script>

<style scoped>
.sidebar-card {
  @include surface-card;
}

.sidebar-add-btn {
  box-shadow: none;
}

.sidebar-item {
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
  background: rgba(var(--v-theme-surface), 0.66);
  transition: border-color 0.18s ease, background-color 0.18s ease;
}

.sidebar-item:hover {
  border-color: rgba(var(--v-theme-primary), 0.16);
  background: rgba(var(--v-theme-surface), 0.92);
}

:deep(.v-list-item--active) {
  background: linear-gradient(180deg, rgba(var(--v-theme-primary), 0.3) 0%, rgba(var(--v-theme-primary), 0.16) 100%);
  border-color: rgba(var(--v-theme-primary), 0.18);
}
</style>
