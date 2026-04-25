<template>
  <header class="page-header-shell">
    <div class="page-header-content">
      <div class="page-header-copy">
        <div v-if="kicker" class="page-kicker">{{ kicker }}</div>
        <h1 class="page-title">{{ title }}</h1>
        <p v-if="subtitle" class="page-subtitle">
          {{ subtitle }}
        </p>
      </div>

      <div v-if="hasActions" class="page-header-actions">
        <slot name="actions" />
      </div>
    </div>
  </header>
</template>

<script setup lang="ts">
import { computed, useSlots } from 'vue'

interface Props {
  title: string
  subtitle?: string
  kicker?: string
}

defineProps<Props>()

const slots = useSlots()
const hasActions = computed(() => Boolean(slots.actions))
</script>

<style scoped lang="scss">
@use '@/styles/mixins' as *;

.page-header-shell {
  @include page-header-shell;
}

.page-header-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 24px;
  padding: 24px 28px;
}

.page-header-copy {
  min-width: 0;
}

.page-kicker {
  margin-bottom: 8px;
  color: rgb(var(--v-theme-primary));
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.page-title {
  margin: 0;
  font-size: clamp(1.6rem, 2.2vw, $font-size-xl);
  font-weight: 800;
  line-height: 1.2;
  color: rgba(var(--v-theme-on-surface), 0.94);
}

.page-subtitle {
  margin: 10px 0 0;
  max-width: 760px;
  color: rgba(var(--v-theme-on-surface), 0.64);
  font-size: $font-size-sm;
  line-height: 1.6;
}

.page-header-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12px;
  flex-wrap: wrap;
}

@include respond-to('tablet') {
  .page-header-shell {
    margin-bottom: 16px;
  }
  
  .page-header-content {
    flex-direction: column;
    align-items: flex-start;
    padding: 20px;
    gap: 20px;
  }

  .page-header-actions {
    width: 100%;
    justify-content: flex-start;
  }
}
</style>
