<template>
  <v-btn
    variant="tonal"
    color="secondary"
    prepend-icon="mdi-refresh"
    :loading="loading"
    rounded="lg"
    @click="emit('refresh')"
  >
    {{ refreshLabel }}
  </v-btn>
  <v-btn
    variant="outlined"
    prepend-icon="mdi-restore"
    :disabled="!dirty || saving"
    rounded="lg"
    @click="emit('reset')"
  >
    {{ resetLabel }}
  </v-btn>
  <v-btn
    variant="outlined"
    prepend-icon="mdi-check-decagram-outline"
    :loading="validating"
    rounded="lg"
    @click="emit('validate')"
  >
    {{ validateLabel }}
  </v-btn>
  <v-btn
    color="primary"
    prepend-icon="mdi-plus"
    rounded="lg"
    @click="emit('create')"
  >
    {{ createLabel }}
  </v-btn>
</template>

<script setup lang="ts">
interface Props {
  loading: boolean
  dirty: boolean
  saving: boolean
  validating: boolean
  refreshLabel: string
  resetLabel: string
  validateLabel: string
  createLabel: string
}

defineProps<Props>()

const emit = defineEmits<{
  refresh: []
  reset: []
  validate: []
  create: []
}>()
</script>
