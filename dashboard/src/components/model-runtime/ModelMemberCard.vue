<template>
  <v-card class="member-card" variant="outlined">
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="secondary" variant="tonal" icon="mdi-cube-outline" />
      </template>
      <v-card-title class="text-body-1 text-break">{{ title }}</v-card-title>
      <v-card-subtitle>{{ subtitle }}</v-card-subtitle>
      <template #append>
      <v-switch
        :model-value="enabled"
        color="primary"
        density="compact"
        hide-details
        inset
        @update:model-value="$emit('toggle', Boolean($event))"
      />
      </template>
    </v-card-item>

    <v-card-text class="pt-0">
      <div class="d-flex flex-wrap ga-2 mb-3">
        <v-chip
          v-for="chip in chips"
          :key="chip"
          size="x-small"
          variant="tonal"
          color="primary"
        >
          {{ chip }}
        </v-chip>
      </div>

      <div v-if="metaLines.length" class="text-caption text-medium-emphasis d-flex flex-column ga-1">
        <div v-for="line in metaLines" :key="line">{{ line }}</div>
      </div>
    </v-card-text>

    <v-card-actions>
      <v-btn
        v-if="showEdit"
        variant="text"
        size="small"
        color="primary"
        @click="$emit('edit')"
      >
        {{ $t('common.actions.action.edit') }}
      </v-btn>
      <v-btn
        v-if="showProbe"
        variant="text"
        size="small"
        color="info"
        @click="$emit('probe')"
      >
        {{ $t('pages.modelRuntime.actions.testConnection') }}
      </v-btn>
      <v-spacer />
      <v-btn
        variant="text"
        size="small"
        color="error"
        @click="$emit('remove')"
      >
        {{ $t('common.actions.action.delete') }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
interface Props {
  title: string
  subtitle: string
  chips: string[]
  metaLines?: string[]
  enabled: boolean
  showEdit?: boolean
  showProbe?: boolean
}

withDefaults(defineProps<Props>(), {
  metaLines: () => [],
  showEdit: true,
  showProbe: false,
})

defineEmits<{
  edit: []
  probe: []
  remove: []
  toggle: [value: boolean]
}>()
</script>

<style scoped>
.member-card {
  border-radius: 16px;
}
</style>
