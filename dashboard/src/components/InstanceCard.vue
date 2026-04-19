<template>
  <v-card class="h-100 d-flex flex-column instance-card" hover>
    <!-- Card Header -->
    <v-card-item class="pb-2">
      <template #prepend>
        <v-avatar color="primary" icon="mdi-robot" />
      </template>
      <v-card-title class="text-break">
        {{ instance.name }}
      </v-card-title>
      <template #append>
        <v-menu>
          <template #activator="{ props }">
            <v-btn icon="mdi-dots-vertical" variant="text" v-bind="props" />
          </template>
          <v-list>
            <v-list-item @click="emit('edit', instance)">
              <v-list-item-title>{{ $t('common.actions.action.edit') }}</v-list-item-title>
            </v-list-item>
            <v-list-item @click="emit('delete', instance)">
              <v-list-item-title>{{ $t('common.actions.action.delete') }}</v-list-item-title>
            </v-list-item>
          </v-list>
        </v-menu>
      </template>
    </v-card-item>

    <!-- Card Body -->
    <v-card-text class="py-2 flex-grow-1">
      <v-chip
        :color="instance.status === 'running' ? 'success' : 'error'"
        class="mb-3"
        small
      >
        <template #prepend>
          <span
            class="status-indicator"
            :class="instance.status === 'running' ? 'status-running' : 'status-stopped'"
          />
        </template>
        {{
          instance.status === 'running'
            ? $t('pages.instances.card.isRunning')
            : $t('pages.instances.card.isStopped')
        }}
      </v-chip>

      <div class="text-caption text-medium-emphasis mb-2">
        <strong>{{ $t('pages.instances.form.adapterType') }}:</strong>
        {{ instance.adapterType }}
      </div>

      <div v-if="instance.config.mode" class="text-caption text-medium-emphasis mb-2">
        <strong>Mode:</strong>
        {{ instance.config.mode }}
      </div>

      <div class="text-caption text-medium-emphasis">
        <strong>{{ $t('pages.instances.form.config') }}:</strong>
        <span v-if="Object.keys(instance.config).length === 0">—</span>
        <span v-else>{{ $t('pages.instances.form.itemCount', { count: Object.keys(instance.config).length }) }}</span>
      </div>
    </v-card-text>

    <!-- Card Footer -->
    <v-card-actions class="pt-0">
      <v-btn
        v-if="instance.status === 'stopped'"
        color="success"
        size="small"
        :loading="instancesStore.pendingActions[instance.id] === 'start'"
        @click="handleStart"
      >
        {{ $t('pages.instances.card.start') }}
      </v-btn>
      <v-btn
        v-else
        color="warning"
        size="small"
        :loading="instancesStore.pendingActions[instance.id] === 'stop'"
        @click="handleStop"
      >
        {{ $t('pages.instances.card.stop') }}
      </v-btn>
      <v-btn
        color="primary"
        variant="text"
        size="small"
        @click="emit('edit', instance)"
      >
        {{ $t('pages.instances.card.editConfig') }}
      </v-btn>
    </v-card-actions>
  </v-card>
</template>

<script setup lang="ts">
import { useInstancesStore } from '@/stores/instances'
import type { Instance } from '@/api/instances'

interface Props {
  instance: Instance
}

const props = defineProps<Props>()
const emit = defineEmits<{
  edit: [instance: Instance]
  delete: [instance: Instance]
}>()

const instancesStore = useInstancesStore()

const handleStart = async () => {
  await instancesStore.startInstance(props.instance.id)
}

const handleStop = async () => {
  await instancesStore.stopInstance(props.instance.id)
}
</script>

<style scoped>
.instance-card {
  transition: transform 0.24s ease, box-shadow 0.24s ease;
}

.instance-card:hover {
  transform: translateY(-6px);
  box-shadow: 0 16px 28px rgba(var(--v-theme-on-surface), 0.2);
}

.status-indicator {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 6px;
}

.status-running {
  background: radial-gradient(circle, rgba(var(--v-theme-success), 0.2) 0%, rgb(var(--v-theme-success)) 75%);
  box-shadow: 0 0 0 0 rgba(var(--v-theme-success), 0.65);
  animation: pulse-running 1.8s infinite;
}

.status-stopped {
  background: radial-gradient(circle, rgba(var(--v-theme-error), 0.2) 0%, rgb(var(--v-theme-error)) 75%);
  box-shadow: 0 0 0 0 rgba(var(--v-theme-error), 0.45);
  animation: pulse-stopped 2.2s infinite;
}

@keyframes pulse-running {
  0% {
    box-shadow: 0 0 0 0 rgba(var(--v-theme-success), 0.6);
  }
  70% {
    box-shadow: 0 0 0 7px rgba(var(--v-theme-success), 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(var(--v-theme-success), 0);
  }
}

@keyframes pulse-stopped {
  0% {
    box-shadow: 0 0 0 0 rgba(var(--v-theme-error), 0.5);
  }
  70% {
    box-shadow: 0 0 0 6px rgba(var(--v-theme-error), 0);
  }
  100% {
    box-shadow: 0 0 0 0 rgba(var(--v-theme-error), 0);
  }
}
</style>
