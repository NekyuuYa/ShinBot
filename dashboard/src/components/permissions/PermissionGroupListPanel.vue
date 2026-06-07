<template>
  <v-card rounded="xl" elevation="0" class="panel-card">
    <v-card-title class="d-flex align-center justify-space-between ga-3">
      <span>{{ $t('pages.permissions.groups.title') }}</span>
      <v-btn size="small" color="primary" prepend-icon="mdi-plus" @click="$emit('create-group')">
        {{ $t('pages.permissions.actions.newGroup') }}
      </v-btn>
    </v-card-title>
    <v-card-text>
      <v-text-field
        v-model="groupSearch"
        :label="$t('common.actions.action.search')"
        prepend-inner-icon="mdi-magnify"
        variant="outlined"
        density="comfortable"
        hide-details
        rounded="lg"
        class="mb-4"
      />

      <v-skeleton-loader
        v-if="loading && groups.length === 0"
        type="list-item-two-line, list-item-two-line, list-item-two-line"
      />

      <div v-else-if="groups.length === 0" class="empty-state py-8">
        <v-icon icon="mdi-shield-key-outline" size="64" color="grey-lighten-1" />
        <div class="text-body-2 text-medium-emphasis mt-3">
          {{ $t('pages.permissions.empty.groups') }}
        </div>
      </div>

      <v-list v-else lines="three" class="group-list pa-0">
        <v-list-item
          v-for="group in groups"
          :key="group.id"
          :active="group.id === selectedGroupId"
          rounded="lg"
          class="group-list-item mb-2"
          @click="$emit('select-group', group.id)"
        >
          <template #prepend>
            <v-avatar color="primary" variant="tonal" size="36">
              <v-icon icon="mdi-shield-account-outline" />
            </v-avatar>
          </template>

          <v-list-item-title class="font-weight-bold text-break">
            {{ displayGroupName(group) }}
          </v-list-item-title>
          <v-list-item-subtitle>
            <div class="d-flex flex-wrap ga-2 mt-2">
              <v-chip size="x-small" variant="tonal">
                {{ $t('pages.permissions.groups.permissionCount', { count: group.permissions.length }) }}
              </v-chip>
              <v-chip size="x-small" variant="outlined">
                {{ $t('pages.permissions.groups.bindingCount', { count: bindingCount(group.id) }) }}
              </v-chip>
              <v-chip v-if="group.builtin" size="x-small" color="info" variant="tonal">
                {{ $t('pages.permissions.groups.builtin') }}
              </v-chip>
              <v-chip v-if="group.protected" size="x-small" color="warning" variant="tonal">
                {{ $t('pages.permissions.groups.protected') }}
              </v-chip>
            </div>
          </v-list-item-subtitle>
        </v-list-item>
      </v-list>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import type { PermissionGroup } from '@/api/permissions'

const groupSearch = defineModel<string>('groupSearch', { required: true })

defineProps<{
  groups: PermissionGroup[]
  selectedGroupId: string
  loading: boolean
  displayGroupName: (group: PermissionGroup) => string
  bindingCount: (groupId: string) => number
}>()

defineEmits<{
  'select-group': [groupId: string]
  'create-group': []
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.panel-card {
  @include surface-card;
  @include hover-lift;
}

.group-list {
  background: transparent;
}

.group-list-item {
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
}

.empty-state {
  text-align: center;
}
</style>
