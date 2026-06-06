<template>
  <v-card class="resource-entity-card h-100" hover>
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="primary" variant="tonal" :icon="icon" />
      </template>
      <v-card-title class="text-break">
        {{ title }}
      </v-card-title>
      <v-card-subtitle class="text-truncate">
        {{ subtitle }}
      </v-card-subtitle>
      <template #append>
        <v-menu>
          <template #activator="{ props }">
            <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
          </template>
          <v-list>
            <v-list-item @click="emit('edit')">
              <v-list-item-title>{{ editLabel }}</v-list-item-title>
            </v-list-item>
            <v-list-item @click="emit('delete')">
              <v-list-item-title>{{ deleteLabel }}</v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
      </template>
    </v-card-item>

    <v-card-text class="pt-2">
      <div class="resource-entity-card__chips">
        <slot name="chips" />
      </div>

      <slot />
    </v-card-text>

    <v-card-actions>
      <v-btn
        color="primary"
        variant="text"
        size="small"
        prepend-icon="mdi-pencil"
        @click="emit('edit')"
      >
        {{ configureLabel }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
interface Props {
  title: string
  subtitle: string
  icon: string
  configureLabel: string
  editLabel: string
  deleteLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  edit: []
  delete: []
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.resource-entity-card {
  @include surface-card;
  @include hover-lift;
}

.resource-entity-card__chips {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 14px;
}
</style>
