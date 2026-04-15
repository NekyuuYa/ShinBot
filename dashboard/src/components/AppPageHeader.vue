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

<style scoped>
.page-header-shell {
  border: 1px solid rgba(120, 86, 0, 0.14);
  border-radius: 28px;
  background:
    radial-gradient(circle at top right, rgba(216, 176, 58, 0.16), transparent 28%),
    linear-gradient(180deg, #fffef6 0%, #fffaf0 100%);
  box-shadow: 0 14px 34px rgba(145, 103, 0, 0.08);
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
  color: rgba(120, 86, 0, 0.88);
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
  color: rgba(26, 23, 16, 0.94);
}

.page-subtitle {
  margin: 12px 0 0;
  max-width: 760px;
  color: rgba(65, 57, 42, 0.72);
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

@media (max-width: 960px) {
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
