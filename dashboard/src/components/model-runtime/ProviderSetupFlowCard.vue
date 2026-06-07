<template>
  <v-card class="editor-card setup-flow-card">
    <v-card-text>
      <div class="setup-flow-header mb-4">
        <div>
          <div class="section-label">
            {{ $t("pages.modelRuntime.cards.providerSetup") }}
          </div>
          <div class="text-caption text-medium-emphasis mt-1">
            {{ summary }}
          </div>
        </div>
        <v-chip
          size="small"
          variant="tonal"
          :color="complete ? 'success' : 'primary'"
        >
          {{
            complete
              ? $t("pages.modelRuntime.labels.setupComplete")
              : $t("pages.modelRuntime.labels.setupInProgress")
          }}
        </v-chip>
      </div>

      <div class="setup-flow-steps">
        <div
          v-for="step in steps"
          :key="step.key"
          class="setup-flow-step"
          :class="`setup-flow-step--${step.state}`"
        >
          <div class="setup-step-marker">
            <v-icon :icon="step.icon" size="18" />
          </div>
          <div class="setup-step-copy">
            <div class="setup-step-title">{{ step.title }}</div>
            <div class="setup-step-detail">{{ step.detail }}</div>
          </div>
          <v-chip size="x-small" variant="tonal" :color="step.color">
            {{ step.statusLabel }}
          </v-chip>
          <v-btn
            v-if="step.action"
            size="small"
            variant="text"
            :color="step.color"
            :disabled="step.actionDisabled"
            :loading="step.loading"
            @click="$emit('action', step.action)"
          >
            {{ step.actionLabel }}
          </v-btn>
        </div>
      </div>
    </v-card-text>
  </v-card>
</template>

<script setup lang="ts">
export type ProviderSetupAction = "source" | "save" | "probe";
export type ProviderSetupState = "complete" | "active" | "pending" | "skipped";

export interface ProviderSetupStep {
  key: string;
  icon: string;
  title: string;
  detail: string;
  complete: boolean;
  skipped?: boolean;
  action?: ProviderSetupAction;
  actionLabel?: string;
  actionDisabled?: boolean;
  loading?: boolean;
  state: ProviderSetupState;
  color: string;
  statusLabel: string;
}

defineProps<{
  summary: string;
  complete: boolean;
  steps: ProviderSetupStep[];
}>();

defineEmits<{
  action: [action: ProviderSetupAction];
}>();
</script>

<style scoped lang="scss">
@use "@/styles/mixins" as *;

.editor-card {
  @include surface-card;
}

.setup-flow-card :deep(.v-card-text) {
  padding: 20px 24px 24px;
}

.setup-flow-header,
.setup-flow-steps,
.setup-flow-step {
  display: flex;
  align-items: center;
}

.setup-flow-header {
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.setup-flow-steps {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;

  @include respond-to("tablet") {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  @include respond-to("mobile") {
    grid-template-columns: minmax(0, 1fr);
  }
}

.setup-flow-step {
  min-height: 98px;
  align-items: flex-start;
  align-content: flex-start;
  gap: 10px;
  flex-wrap: wrap;
  padding: 12px;
  border: 1px solid $border-color-soft;
  border-radius: $radius-base;
  background: rgba(var(--v-theme-surface), 0.78);
}

.setup-flow-step--active {
  border-color: $border-color-primary;
  background: rgba(var(--v-theme-primary), 0.05);
}

.setup-flow-step--complete {
  border-color: rgba(var(--v-theme-success), 0.28);
}

.setup-flow-step--pending {
  opacity: 0.78;
}

.setup-step-marker {
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  flex: 0 0 auto;
  border-radius: $radius-pill;
  background: rgba(var(--v-theme-primary), 0.1);
  color: rgb(var(--v-theme-primary));
}

.setup-flow-step--complete .setup-step-marker {
  background: rgba(var(--v-theme-success), 0.12);
  color: rgb(var(--v-theme-success));
}

.setup-flow-step--pending .setup-step-marker {
  background: rgba(var(--v-theme-on-surface), 0.06);
  color: rgba(var(--v-theme-on-surface), 0.56);
}

.setup-step-copy {
  min-width: 0;
  flex: 1 1 calc(100% - 38px);
}

.setup-step-title {
  color: rgba(var(--v-theme-on-surface), 0.9);
  font-size: $font-size-sm;
  font-weight: 700;
  line-height: 1.3;
}

.setup-step-detail {
  margin-top: 2px;
  overflow: hidden;
  color: rgba(var(--v-theme-on-surface), 0.58);
  font-size: $font-size-xs;
  line-height: 1.35;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.setup-flow-step :deep(.v-btn) {
  align-self: flex-end;
  margin-left: auto;
}

.section-label {
  font-size: $font-size-xs;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: rgb(var(--v-theme-primary));
  opacity: 0.82;
}
</style>
