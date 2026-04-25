<template>
  <section class="page-header-shell mb-8">
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
  </section>
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
  padding: 28px 32px;
}

.page-header-copy {
  min-width: 0;
}

.page-kicker {
  margin-bottom: 10px;
  color: rgba(var(--v-theme-primary), 0.88);
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.page-title {
  margin: 0;
  font-size: clamp(1.8rem, 2.4vw, 2.4rem);
  font-weight: 800;
  line-height: 1.15;
  color: rgba(var(--v-theme-on-surface), 0.94);
}

.page-subtitle {
  margin: 12px 0 0;
  max-width: 760px;
  color: rgba(var(--v-theme-on-surface), 0.72);
  font-size: 0.98rem;
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
  .page-header-content {
    flex-direction: column;
    align-items: flex-start;
    padding: 24px;
  }

  .page-header-actions {
    width: 100%;
    justify-content: flex-start;
  }
}
</style>
