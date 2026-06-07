<template>
  <v-card rounded="xl" elevation="0" class="panel-card fill-height">
    <v-card-title>{{ $t('pages.permissions.bindings.title') }}</v-card-title>
    <v-card-text>
      <v-row class="mx-n2">
        <v-col cols="12" md="7" class="pa-2">
          <v-combobox
            v-model="scopeKey"
            :items="scopeItems"
            :label="$t('pages.permissions.fields.scopeKey')"
            variant="outlined"
            density="comfortable"
            rounded="lg"
            clearable
          />
        </v-col>
        <v-col cols="12" md="5" class="pa-2">
          <v-select
            v-model="groupIds"
            :items="groupItems"
            :label="$t('pages.permissions.fields.groups')"
            item-title="title"
            item-value="value"
            multiple
            chips
            closable-chips
            variant="outlined"
            density="comfortable"
            rounded="lg"
          />
        </v-col>
      </v-row>
      <div class="d-flex flex-wrap ga-2 mb-4">
        <v-btn
          color="primary"
          prepend-icon="mdi-link-variant"
          :loading="saving"
          :disabled="!canSave"
          @click="$emit('save')"
        >
          {{ $t('pages.permissions.actions.saveBinding') }}
        </v-btn>
        <v-btn
          color="error"
          variant="tonal"
          prepend-icon="mdi-link-variant-off"
          :loading="saving"
          :disabled="!scopeKey.trim()"
          @click="$emit('delete')"
        >
          {{ $t('pages.permissions.actions.deleteBinding') }}
        </v-btn>
      </div>

      <div class="d-grid ga-3">
        <div
          v-for="binding in bindings"
          :key="binding.scopeKey"
          class="binding-row"
          @click="$emit('edit', binding)"
        >
          <div class="text-body-2 font-weight-bold text-break">{{ binding.scopeKey }}</div>
          <div class="d-flex flex-wrap ga-2 mt-2">
            <v-chip
              v-for="groupId in binding.groups"
              :key="groupId"
              size="small"
              variant="tonal"
            >
              {{ displayGroupNameById(groupId) }}
            </v-chip>
          </div>
        </div>
        <div v-if="bindings.length === 0" class="empty-state py-8">
          <v-icon icon="mdi-link-variant-off" size="56" color="grey-lighten-1" />
          <div class="text-body-2 text-medium-emphasis mt-3">
            {{ $t('pages.permissions.empty.bindings') }}
          </div>
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
import type { PermissionBinding } from '@/api/permissions'
import type { PermissionSelectItem } from '@/components/permissions/permissionUtils'

const scopeKey = defineModel<string>('scopeKey', { required: true })
const groupIds = defineModel<string[]>('groupIds', { required: true })

defineProps<{
  bindings: PermissionBinding[]
  scopeItems: string[]
  groupItems: PermissionSelectItem[]
  saving: boolean
  canSave: boolean
  displayGroupNameById: (groupId: string) => string
}>()

defineEmits<{
  save: []
  delete: []
  edit: [binding: PermissionBinding]
}>()
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.panel-card {
  @include surface-card;
  @include hover-lift;
}

.binding-row {
  cursor: pointer;
  border: 1px solid rgba(var(--v-theme-primary), 0.08);
  border-radius: $radius-base;
  padding: 14px;
  transition: border-color 0.16s ease, background-color 0.16s ease;
}

.binding-row:hover {
  border-color: rgba(var(--v-theme-primary), 0.24);
  background: rgba(var(--v-theme-primary), 0.04);
}

.empty-state {
  text-align: center;
}

.d-grid {
  display: grid;
}
</style>
