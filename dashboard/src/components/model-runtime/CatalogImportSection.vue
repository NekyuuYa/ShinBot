<template>
  <div v-if="items.length > 0">
    <v-divider class="mb-4" />
    <div class="d-flex align-center justify-space-between mb-3">
      <span class="section-label">
        {{ $t("pages.modelRuntime.cards.availableModels") }}
      </span>
      <span class="text-caption text-medium-emphasis">
        {{ filteredItems.length }} / {{ items.length }}
      </span>
    </div>
    <v-text-field
      v-model="searchQuery"
      :placeholder="$t('common.actions.action.search')"
      prepend-inner-icon="mdi-magnify"
      variant="outlined"
      density="compact"
      clearable
      hide-details
      class="mb-3"
    />
    <div class="d-flex flex-column ga-3">
      <v-card
        v-for="item in filteredItems"
        :key="item.id"
        variant="outlined"
        class="catalog-item-card"
      >
        <v-card-text class="d-flex justify-space-between align-start ga-4 flex-wrap">
          <div>
            <div class="text-body-1 font-weight-medium">
              {{ item.displayName }}
            </div>
            <div class="text-caption text-medium-emphasis">
              {{ item.litellmModel }}
            </div>
            <div class="text-caption text-medium-emphasis mt-1">
              {{
                $t("pages.modelRuntime.hints.contextWindowAuto", {
                  value: item.contextWindow || "-",
                })
              }}
            </div>
          </div>
          <v-btn
            color="primary"
            variant="tonal"
            rounded="xl"
            class="action-btn"
            @click="emit('import', item.id)"
          >
            {{ $t("pages.modelRuntime.actions.addToConfigured") }}
          </v-btn>
        </v-card-text>
      </v-card>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";

import type { ProviderCatalogItem } from "@/api/modelRuntime";

const props = defineProps<{
  items: ProviderCatalogItem[]
}>()

const emit = defineEmits<{
  import: [catalogId: string]
}>()

const searchQuery = ref("")

const filteredItems = computed(() => {
  const keyword = searchQuery.value.trim().toLowerCase()
  if (!keyword) {
    return props.items
  }
  return props.items.filter((item) =>
    [item.id, item.displayName, item.litellmModel]
      .join(" ")
      .toLowerCase()
      .includes(keyword),
  )
})
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.action-btn {
  box-shadow: none;
}

.catalog-item-card {
  border-radius: $radius-lg;
  border: 1px solid $border-color-soft;
  background: rgba(var(--v-theme-surface), 0.66);
  transition: all $transition-fast;
}
</style>
